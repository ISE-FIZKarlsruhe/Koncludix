/*
 *		Optional ABox reduction for the materialize command (Koncludix extension).
 *
 *		This file is part of the Konclude fork. It is distributed under the same terms as Konclude
 *		(GNU Lesser General Public License, version 3).
 */

#include "CABoxReducer.h"

#include <algorithm>
#include <string>
#include <vector>
#include <unordered_map>

#include <QFile>
#include <QMap>
#include <QSet>
#include <QHash>
#include <QVector>
#include <QStringList>
#include <QTextStream>

#ifdef Q_OS_WIN
#include <windows.h>
#endif

#include "Parser/Expressions/CBuildExpression.h"
#include "Parser/Expressions/CAxiomExpression.h"
#include "Parser/Expressions/CSubExpressionVisitor.h"
#include "Parser/Expressions/CClassAssertionExpression.h"
#include "Parser/Expressions/CObjectPropertyAssertionExpression.h"
#include "Parser/Expressions/CDataPropertyAssertionExpression.h"
#include "Parser/Expressions/CDeclarationAxiomExpression.h"
#include "Parser/Expressions/CNamedIndividualExpression.h"
#include "Parser/Expressions/CAnonymousIndividualExpression.h"
#include "Parser/Expressions/CClassExpression.h"
#include "Parser/Expressions/CObjectPropertyExpression.h"
#include "Parser/Expressions/CDataPropertyExpression.h"
#include "Parser/Expressions/CInverseObjectPropertyOfExpression.h"
#include "Parser/Expressions/CDataLiteralExpression.h"
#include "Parser/Expressions/CDataLexicalValueExpression.h"
#include "Parser/Expressions/CDatatypeExpression.h"
#include "Parser/Expressions/CObjectCardinalityExpression.h"
#include "Parser/Expressions/CObjectHasSelfExpression.h"
#include "Parser/Expressions/CObjectHasValueExpression.h"
#include "Parser/Expressions/CObjectSomeValuesFromExpression.h"
#include "Parser/Expressions/CSubObjectPropertyOfExpression.h"
#include "Parser/Expressions/CObjectPropertyTermExpressionAssociator.h"
#include "Parser/Expressions/CDataPropertyTermExpressionAssociator.h"


namespace Konclude {

	using namespace Parser::Expression;

	namespace Reasoner {

		namespace Generator {


			namespace {

				// logged in the domain of the CLI batch processor, which is shown on the console by default
				const char* const LOG_DOMAIN = "::Konclude::CLIBatchProcessor";

				const char* const IRI_TOP_OBJECT_PROPERTY = "http://www.w3.org/2002/07/owl#topObjectProperty";
				const char* const IRI_BOTTOM_OBJECT_PROPERTY = "http://www.w3.org/2002/07/owl#bottomObjectProperty";


				void logInfo(const QString& message) {
					LOG(INFO, LOG_DOMAIN, message, nullptr);
				}

				void logWarning(const QString& message) {
					LOG(WARNING, LOG_DOMAIN, message, nullptr);
				}


				// ----------------------------------------------------------------------------------------------------
				// helpers for property expressions

				/*! resolves a (possibly inverse) object property expression to the IRI of the named property, "inverse" tells whether the
				 *  property is used in inverse direction. Returns false if the expression is not built from a named property.
				 */
				bool resolveObjectProperty(CObjectPropertyTermExpression* term, QString& name, bool& inverse) {
					inverse = false;
					int depth = 0;
					while (term && depth < 32) {
						CBuildExpression::ExpressionType type = term->getType();
						if (type == CBuildExpression::BETOBJECTPROPERTY) {
							CObjectPropertyExpression* propExp = dynamic_cast<CObjectPropertyExpression*>(term);
							if (!propExp) {
								return false;
							}
							name = propExp->getName();
							return true;
						} else if (type == CBuildExpression::BETINVERSEOBJECTPROPERTYOF) {
							CInverseObjectPropertyOfExpression* invExp = dynamic_cast<CInverseObjectPropertyOfExpression*>(term);
							if (!invExp) {
								return false;
							}
							inverse = !inverse;
							term = invExp->getInverseOfExpression();
						} else {
							return false;
						}
						++depth;
					}
					return false;
				}


				bool resolveDataProperty(CDataPropertyTermExpression* term, QString& name) {
					if (term && term->getType() == CBuildExpression::BETDATAPROPERTY) {
						CDataPropertyExpression* propExp = dynamic_cast<CDataPropertyExpression*>(term);
						if (propExp) {
							name = propExp->getName();
							return true;
						}
					}
					return false;
				}


				bool isEffectivelyAnonymousName(const QString& name) {
					return name.startsWith(QLatin1String("_:"));
				}



				// ----------------------------------------------------------------------------------------------------
				// collects the signature and the relevant constructs of an axiom (or class expression)

				class CAxiomSignature : public CSubExpressionVisitor {
					public:
						CAxiomSignature() {
							mHasAnonymousIndividual = false;
							mHasChain = false;
							mUnsupported = false;
						}

						QMap<QString, CNamedIndividualExpression*> mNamedIndividuals;
						bool mHasAnonymousIndividual;
						QSet<QString> mObjectProperties;			// named properties, inverse stripped
						QSet<QString> mDataProperties;
						bool mHasChain;
						bool mUnsupported;
						QSet<QString> mCardinalityRoles;			// roles of cardinality restrictions (min, max, exact) and has-self restrictions
						QSet<QString> mMergingRoles;				// roles of max and exact cardinality restrictions
						QSet<QString> mNominalRestrictionRoles;		// roles of restrictions that may create an edge to a nominal (has-value, some-values/min/exact cardinality with nominals in the filler)
						QSet<QString> mExactDataProperties;			// data properties used in data restrictions

						void processExpression(CBuildExpression* exp) {
							if (!exp) {
								return;
							}
							switch (exp->getType()) {
								case CBuildExpression::BETNAMEDINDIVIDUAL: {
									CNamedIndividualExpression* indiExp = dynamic_cast<CNamedIndividualExpression*>(exp);
									if (indiExp) {
										if (isEffectivelyAnonymousName(indiExp->getName())) {
											mHasAnonymousIndividual = true;
										} else {
											mNamedIndividuals.insert(indiExp->getName(), indiExp);
										}
									}
									break;
								}
								case CBuildExpression::BETANONYMOUSINDIVIDUAL:
								case CBuildExpression::BETANONYMOUSINDIVIDUALVARIABLE:
								case CBuildExpression::BETNAMEDINDIVIDUALVARIABLE:
								case CBuildExpression::BETNOMINALINDIVIDUALVARIABLE: {
									mHasAnonymousIndividual = true;
									break;
								}
								case CBuildExpression::BETOBJECTPROPERTY: {
									QString name;
									bool inverse;
									CObjectPropertyTermExpression* propTerm = dynamic_cast<CObjectPropertyTermExpression*>(exp);
									if (propTerm && resolveObjectProperty(propTerm, name, inverse)) {
										mObjectProperties.insert(name);
									}
									break;
								}
								case CBuildExpression::BETDATAPROPERTY: {
									QString name;
									CDataPropertyTermExpression* propTerm = dynamic_cast<CDataPropertyTermExpression*>(exp);
									if (propTerm && resolveDataProperty(propTerm, name)) {
										mDataProperties.insert(name);
									}
									break;
								}
								case CBuildExpression::BETOBJECTPROPERTYCHAIN: {
									mHasChain = true;
									break;
								}
								case CBuildExpression::BETOBJECTMINCARDINALITY:
								case CBuildExpression::BETOBJECTMAXCARDINALITY:
								case CBuildExpression::BETOBJECTEXACTLYCARDINALITY: {
									CObjectCardinalityExpression* cardExp = dynamic_cast<CObjectCardinalityExpression*>(exp);
									if (cardExp) {
										QString name;
										bool inverse;
										if (resolveObjectProperty(cardExp->getObjectPropertyTermExpression(), name, inverse)) {
											mCardinalityRoles.insert(name);
											CBuildExpression::ExpressionType type = exp->getType();
											if (type != CBuildExpression::BETOBJECTMINCARDINALITY) {
												mMergingRoles.insert(name);
											}
											if (type != CBuildExpression::BETOBJECTMAXCARDINALITY && cardExp->getCardinality() >= 1) {
												if (fillerHasIndividuals(cardExp->getClassTermExpression())) {
													mNominalRestrictionRoles.insert(name);
												}
											}
										} else {
											mUnsupported = true;
										}
									}
									break;
								}
								case CBuildExpression::BETOBJECTHASSELF: {
									CObjectHasSelfExpression* selfExp = dynamic_cast<CObjectHasSelfExpression*>(exp);
									if (selfExp) {
										QString name;
										bool inverse;
										if (resolveObjectProperty(selfExp->getObjectPropertyTermExpression(), name, inverse)) {
											mCardinalityRoles.insert(name);
										} else {
											mUnsupported = true;
										}
									}
									break;
								}
								case CBuildExpression::BETOBJECTHASVALUE: {
									CObjectHasValueExpression* hasValExp = dynamic_cast<CObjectHasValueExpression*>(exp);
									if (hasValExp) {
										QString name;
										bool inverse;
										if (resolveObjectProperty(hasValExp->getObjectPropertyTermExpression(), name, inverse)) {
											mNominalRestrictionRoles.insert(name);
										} else {
											mUnsupported = true;
										}
									}
									break;
								}
								case CBuildExpression::BETOBJECTSOMEVALUEFROM: {
									CObjectSomeValuesFromExpression* someExp = dynamic_cast<CObjectSomeValuesFromExpression*>(exp);
									if (someExp) {
										QString name;
										bool inverse;
										if (resolveObjectProperty(someExp->getObjectPropertyTermExpression(), name, inverse)) {
											if (fillerHasIndividuals(someExp->getClassTermExpression())) {
												mNominalRestrictionRoles.insert(name);
											}
										} else {
											mUnsupported = true;
										}
									}
									break;
								}
								case CBuildExpression::BETDATASOMEVALUEFROM:
								case CBuildExpression::BETDATAHASVALUE:
								case CBuildExpression::BETDATAALLVALUEFROM:
								case CBuildExpression::BETDATAMINCARDINALITY:
								case CBuildExpression::BETDATAMAXCARDINALITY:
								case CBuildExpression::BETDATAEXACTCARDINALITY: {
									CDataPropertyTermExpressionAssociator* dataAssoc = dynamic_cast<CDataPropertyTermExpressionAssociator*>(exp);
									if (dataAssoc) {
										QString name;
										if (resolveDataProperty(dataAssoc->getDataPropertyTermExpression(), name)) {
											mExactDataProperties.insert(name);
										} else {
											mUnsupported = true;
										}
									}
									break;
								}
								case CBuildExpression::BETRULECLASSATOM:
								case CBuildExpression::BETRULEPROPERTYATOM:
								case CBuildExpression::BETRULEDIFFERENTINDIVIDUALSATOM:
								case CBuildExpression::BETRULEATOMBODYLIST:
								case CBuildExpression::BETRULEATOMHEADLIST: {
									mUnsupported = true;
									break;
								}
								default: {
									break;
								}
							}
						}

						// recursive walk over an axiom or expression (without the expression itself)
						void collect(CBuildExpression* exp) {
							if (exp) {
								exp->visitSubExpressions(this);
							}
						}

						virtual bool visitSubExpression(CBuildExpression* expression, CBuildExpression* subExpression, CSubExpressionVisitor* visitor) {
							processExpression(subExpression);
							if (subExpression) {
								subExpression->visitSubExpressions(visitor);
							}
							return true;
						}

						virtual bool visitEntity(CBuildExpression* expression, CExpressionEntity* entity, CSubExpressionVisitor* visitor) {
							return true;
						}

					protected:
						static bool fillerHasIndividuals(CClassTermExpression* filler) {
							if (!filler) {
								return false;
							}
							CAxiomSignature fillerSig;
							fillerSig.processExpression(filler);
							filler->visitSubExpressions(&fillerSig);
							return !fillerSig.mNamedIndividuals.isEmpty() || fillerSig.mHasAnonymousIndividual;
						}
				};



				// ----------------------------------------------------------------------------------------------------
				// available main memory

				qint64 getAvailableMemoryBytes() {
					qint64 availableBytes = -1;
#ifdef Q_OS_WIN
					MEMORYSTATUSEX memStatus;
					memStatus.dwLength = sizeof(memStatus);
					if (GlobalMemoryStatusEx(&memStatus)) {
						availableBytes = (qint64)memStatus.ullAvailPhys;
					}
#else
					QFile memFile(QString("/proc/meminfo"));
					if (memFile.open(QIODevice::ReadOnly | QIODevice::Text)) {
						QTextStream stream(&memFile);
						qint64 memFreeKb = -1;
						qint64 memAvailableKb = -1;
						QString line;
						while (!(line = stream.readLine()).isNull()) {
							if (line.startsWith(QLatin1String("MemAvailable:"))) {
								memAvailableKb = line.mid(13).trimmed().split(' ').first().toLongLong();
							} else if (line.startsWith(QLatin1String("MemFree:"))) {
								memFreeKb = line.mid(8).trimmed().split(' ').first().toLongLong();
							}
						}
						if (memAvailableKb >= 0) {
							availableBytes = memAvailableKb * 1024;
						} else if (memFreeKb >= 0) {
							availableBytes = memFreeKb * 1024;
						}
					}
#endif
					return availableBytes;
				}



				// ----------------------------------------------------------------------------------------------------
				// the analysis and the reduction

				enum PINREASON { PIN_TBOX = 0, PIN_ASSERTION = 1, PIN_COMPLEXCLASS = 2, PIN_ANONYMOUS = 3, PIN_SELFLOOP = 4, PIN_ROLE = 5, PIN_REASONCOUNT = 6 };

				static const char* const PIN_REASON_TEXT[PIN_REASONCOUNT] = {
					"mentioned in TBox/RBox axioms (nominals)",
					"mentioned in sameAs/differentFrom/negative assertions",
					"subject of a complex class assertion",
					"neighbour of an anonymous individual",
					"asserted self-loop",
					"edge at a role with cardinality/functional/disjointness/transitivity/chain axioms"
				};


				enum AXIOMKIND { AK_CLASSASSERTION, AK_OBJECTASSERTION, AK_DATAASSERTION, AK_DECLARATION };

				struct CAxiomRecord {
					CAxiomExpression* mAxiom;
					qint32 mKind;
					qint32 mSubject;		// individual ids in the order of the axiom, -1 if not a named individual
					qint32 mObject;
					qint32 mEdge;			// index of the normalized edge for object property assertions, otherwise -1
				};

				struct CEdge {
					qint32 mSource;
					qint32 mTarget;
					qint32 mRole;
				};

				struct CDataEntry {
					qint32 mProperty;
					qint32 mDatatype;
					CDataLiteralExpression* mLiteral;
				};



				class CABoxAnalysis {
					public:
						CABoxAnalysis(CConcreteOntology* ontology, CConcreteOntologyUpdateCollectorBuilder* builder, CConfiguration* config, CABoxReducer::REDUCTIONMODE mode) {
							mTBoxHasNominals = false;
							mOntology = ontology;
							mBuilder = builder;
							mConfig = config;
							mMode = mode;
							mAbort = false;
							mAssertionCount = 0;
							for (int i = 0; i < PIN_REASONCOUNT; ++i) {
								mPinCount[i] = 0;
							}
						}

						bool run();

					protected:
						// individuals
						int getIndividualId(CIndividualTermExpression* indiTerm, bool create);
						int registerIndividual(CNamedIndividualExpression* indiExp);
						void pin(int id, PINREASON reason);
						void pinSignatureIndividuals(const CAxiomSignature& sig, PINREASON reason);
						int getRoleId(const QString& iri);
						int getDataPropertyId(const QString& iri);
						int getDatatypeId(const QString& iri);
						void abort(const QString& reason);

						// scanning of the told axioms
						void scanAxioms();
						void handleClassAssertion(CAxiomExpression* axiom);
						void handleObjectPropertyAssertion(CAxiomExpression* axiom);
						void handleDataPropertyAssertion(CAxiomExpression* axiom);
						void handleDeclaration(CAxiomExpression* axiom);
						void handleOtherAxiom(CAxiomExpression* axiom);
						void mergeSignature(const CAxiomSignature& sig);
						static void addRelation(QHash<QString, QSet<QString> >& relation, const QSet<QString>& names);
						static void closeOver(const QHash<QString, QSet<QString> >& relation, const QSet<QString>& seeds, QSet<QString>& closure);

						// analysis
						void computeSensitivity();
						void refineColours();
						void formGroups();
						bool decide();
						bool apply();
						void logResult(bool applied);

					protected:
						CConcreteOntology* mOntology;
						CConcreteOntologyUpdateCollectorBuilder* mBuilder;
						CConfiguration* mConfig;
						CABoxReducer::REDUCTIONMODE mMode;

						bool mAbort;
						QString mAbortReason;

						// individuals (named individuals only)
						QVector<QString> mIndiName;
						QVector<CIndividualTermExpression*> mIndiExpression;
						QHash<QString, int> mIndiIdHash;
						QVector<char> mPinned;
						qint64 mPinCount[PIN_REASONCOUNT];

						// axioms
						QVector<CAxiomRecord> mAxiomRecords;
						qint64 mAssertionCount;
						qint64 mAxiomCount;

						// types of the individuals (type keys are interned)
						QHash<QString, int> mTypeKeyHash;
						QVector<QVector<qint32> > mIndiTypes;

						// data assertions of the individuals
						QVector<QVector<CDataEntry> > mIndiData;
						QVector<QString> mDataPropertyIRI;
						QHash<QString, int> mDataPropertyIdHash;
						QVector<QString> mDatatypeIRI;
						QHash<QString, int> mDatatypeIdHash;

						// edges between named individuals
						QVector<CEdge> mEdges;
						QVector<QString> mRoleIRI;
						QHash<QString, int> mRoleIdHash;

						// TBox/RBox information
						QSet<QString> mSensitiveSeeds;
						QSet<QString> mMergingSeeds;
						QSet<QString> mNominalRestrictionRoles;
						QSet<QString> mToldSameIndividuals;
						QSet<QString> mExactDataSeeds;
						QHash<QString, QSet<QString> > mObjectRoleRelation;
						QHash<QString, QSet<QString> > mDataRoleRelation;
						QSet<QString> mSensitiveRoles;
						QSet<QString> mMergingRoles;
						QSet<QString> mExactDataProperties;
						bool mTBoxHasNominals;

						// colours and groups
						std::vector<int> mColour;
						std::vector<std::vector<int> > mGroups;			// groups of non-pinned individuals with the same colour, sorted by name
						QVector<int> mGroupOfIndi;						// group index of non-pinned individuals, -1 otherwise
						QVector<char> mGroupLooped;
						QVector<int> mGroupRetainedCount;
						QVector<char> mRemoved;
						qint64 mRemovedCount;
						qint64 mReducedGroupCount;
						qint64 mPinnedCount;
				};



				void CABoxAnalysis::abort(const QString& reason) {
					if (!mAbort) {
						mAbort = true;
						mAbortReason = reason;
					}
				}


				int CABoxAnalysis::registerIndividual(CNamedIndividualExpression* indiExp) {
					const QString& name = indiExp->getName();
					QHash<QString, int>::const_iterator it = mIndiIdHash.constFind(name);
					if (it != mIndiIdHash.constEnd()) {
						return it.value();
					}
					int id = mIndiName.size();
					mIndiName.append(name);
					mIndiExpression.append(indiExp);
					mIndiIdHash.insert(name, id);
					mPinned.append(0);
					mIndiTypes.append(QVector<qint32>());
					mIndiData.append(QVector<CDataEntry>());
					return id;
				}


				int CABoxAnalysis::getIndividualId(CIndividualTermExpression* indiTerm, bool create) {
					if (!indiTerm || indiTerm->getType() != CBuildExpression::BETNAMEDINDIVIDUAL) {
						return -1;
					}
					CNamedIndividualExpression* indiExp = dynamic_cast<CNamedIndividualExpression*>(indiTerm);
					if (!indiExp || isEffectivelyAnonymousName(indiExp->getName())) {
						return -1;
					}
					if (create) {
						return registerIndividual(indiExp);
					}
					return mIndiIdHash.value(indiExp->getName(), -1);
				}


				void CABoxAnalysis::pin(int id, PINREASON reason) {
					if (id >= 0 && !mPinned[id]) {
						mPinned[id] = 1;
						++mPinCount[reason];
					}
				}


				void CABoxAnalysis::pinSignatureIndividuals(const CAxiomSignature& sig, PINREASON reason) {
					for (QMap<QString, CNamedIndividualExpression*>::const_iterator it = sig.mNamedIndividuals.constBegin(), itEnd = sig.mNamedIndividuals.constEnd(); it != itEnd; ++it) {
						// all individuals that are mentioned outside of simple ABox assertions are registered, so that they are known as pinned individuals
						pin(registerIndividual(it.value()), reason);
					}
				}


				int CABoxAnalysis::getRoleId(const QString& iri) {
					QHash<QString, int>::const_iterator it = mRoleIdHash.constFind(iri);
					if (it != mRoleIdHash.constEnd()) {
						return it.value();
					}
					int id = mRoleIRI.size();
					mRoleIRI.append(iri);
					mRoleIdHash.insert(iri, id);
					return id;
				}


				int CABoxAnalysis::getDataPropertyId(const QString& iri) {
					QHash<QString, int>::const_iterator it = mDataPropertyIdHash.constFind(iri);
					if (it != mDataPropertyIdHash.constEnd()) {
						return it.value();
					}
					int id = mDataPropertyIRI.size();
					mDataPropertyIRI.append(iri);
					mDataPropertyIdHash.insert(iri, id);
					return id;
				}


				int CABoxAnalysis::getDatatypeId(const QString& iri) {
					QHash<QString, int>::const_iterator it = mDatatypeIdHash.constFind(iri);
					if (it != mDatatypeIdHash.constEnd()) {
						return it.value();
					}
					int id = mDatatypeIRI.size();
					mDatatypeIRI.append(iri);
					mDatatypeIdHash.insert(iri, id);
					return id;
				}


				void CABoxAnalysis::addRelation(QHash<QString, QSet<QString> >& relation, const QSet<QString>& names) {
					for (QSet<QString>::const_iterator it = names.constBegin(), itEnd = names.constEnd(); it != itEnd; ++it) {
						QSet<QString>& related = relation[*it];
						for (QSet<QString>::const_iterator it2 = names.constBegin(); it2 != itEnd; ++it2) {
							if (*it != *it2) {
								related.insert(*it2);
							}
						}
					}
				}


				void CABoxAnalysis::closeOver(const QHash<QString, QSet<QString> >& relation, const QSet<QString>& seeds, QSet<QString>& closure) {
					QList<QString> work;
					for (QSet<QString>::const_iterator it = seeds.constBegin(), itEnd = seeds.constEnd(); it != itEnd; ++it) {
						if (!closure.contains(*it)) {
							closure.insert(*it);
							work.append(*it);
						}
					}
					while (!work.isEmpty()) {
						QString current = work.takeLast();
						QHash<QString, QSet<QString> >::const_iterator relIt = relation.constFind(current);
						if (relIt != relation.constEnd()) {
							for (QSet<QString>::const_iterator it = relIt.value().constBegin(), itEnd = relIt.value().constEnd(); it != itEnd; ++it) {
								if (!closure.contains(*it)) {
									closure.insert(*it);
									work.append(*it);
								}
							}
						}
					}
				}



				// ----------------------------------------------------------------------------------------------------
				// scanning

				void CABoxAnalysis::handleClassAssertion(CAxiomExpression* axiom) {
					CClassAssertionExpression* classAssExp = dynamic_cast<CClassAssertionExpression*>(axiom);
					if (!classAssExp) {
						abort("unexpected class assertion expression");
						return;
					}
					++mAssertionCount;
					CIndividualTermExpression* indiTerm = classAssExp->getIndividualTermExpression();
					CClassTermExpression* classTerm = classAssExp->getClassTermExpression();
					int id = getIndividualId(indiTerm, true);

					QString typeKey;
					bool complexClass = true;
					if (classTerm && classTerm->getType() == CBuildExpression::BETCLASS) {
						CClassExpression* classExp = dynamic_cast<CClassExpression*>(classTerm);
						if (classExp) {
							typeKey = QString("N:") + classExp->getName();
							complexClass = false;
						}
					}
					if (complexClass) {
						typeKey = QString("X:") + QString::number((quintptr)classTerm);
						// the restrictions of a complex class are treated as part of the TBox
						CAxiomSignature sig;
						sig.processExpression(classTerm);
						sig.collect(classTerm);
						mergeSignature(sig);
						pinSignatureIndividuals(sig, PIN_COMPLEXCLASS);
						pin(id, PIN_COMPLEXCLASS);
						if (id < 0 && indiTerm) {
							// anonymous individual with a complex class: nothing else to do (its named neighbours are pinned via the edges)
						}
					}

					if (id >= 0) {
						QHash<QString, int>::iterator typeIt = mTypeKeyHash.find(typeKey);
						if (typeIt == mTypeKeyHash.end()) {
							typeIt = mTypeKeyHash.insert(typeKey, mTypeKeyHash.size());
						}
						mIndiTypes[id].append(typeIt.value());
					}

					CAxiomRecord rec;
					rec.mAxiom = axiom;
					rec.mKind = AK_CLASSASSERTION;
					rec.mSubject = id;
					rec.mObject = -1;
					rec.mEdge = -1;
					mAxiomRecords.append(rec);
				}


				void CABoxAnalysis::handleObjectPropertyAssertion(CAxiomExpression* axiom) {
					CObjectPropertyAssertionExpression* propAssExp = dynamic_cast<CObjectPropertyAssertionExpression*>(axiom);
					if (!propAssExp) {
						abort("unexpected object property assertion expression");
						return;
					}
					++mAssertionCount;
					QString roleName;
					bool inverse = false;
					if (!resolveObjectProperty(propAssExp->getObjectPropertyTermExpression(), roleName, inverse)) {
						abort("object property assertion with a non-named property");
						return;
					}
					if (roleName == QLatin1String(IRI_TOP_OBJECT_PROPERTY) || roleName == QLatin1String(IRI_BOTTOM_OBJECT_PROPERTY)) {
						abort("object property assertion with the top/bottom object property");
						return;
					}
					int s = getIndividualId(propAssExp->getFirstIndividualTermExpression(), true);
					int o = getIndividualId(propAssExp->getSecondIndividualTermExpression(), true);

					CAxiomRecord rec;
					rec.mAxiom = axiom;
					rec.mKind = AK_OBJECTASSERTION;
					rec.mSubject = s;
					rec.mObject = o;
					rec.mEdge = -1;

					if (s >= 0 && o >= 0) {
						if (s == o) {
							pin(s, PIN_SELFLOOP);
						}
						CEdge edge;
						edge.mSource = inverse ? o : s;
						edge.mTarget = inverse ? s : o;
						edge.mRole = getRoleId(roleName);
						rec.mEdge = mEdges.size();
						mEdges.append(edge);
					} else {
						// edge with an anonymous individual: the named neighbour is never merged
						pin(s, PIN_ANONYMOUS);
						pin(o, PIN_ANONYMOUS);
					}
					mAxiomRecords.append(rec);
				}


				void CABoxAnalysis::handleDataPropertyAssertion(CAxiomExpression* axiom) {
					CDataPropertyAssertionExpression* dataAssExp = dynamic_cast<CDataPropertyAssertionExpression*>(axiom);
					if (!dataAssExp) {
						abort("unexpected data property assertion expression");
						return;
					}
					++mAssertionCount;
					int s = getIndividualId(dataAssExp->getIndividualTermExpression(), true);
					QString propName;
					if (!resolveDataProperty(dataAssExp->getDataPropertyTermExpression(), propName)) {
						abort("data property assertion with a non-named data property");
						return;
					}
					CDataLiteralExpression* literalExp = dynamic_cast<CDataLiteralExpression*>(dataAssExp->getDataLiteralTermExpression());
					if (!literalExp || !literalExp->getDataLexicalValueExpression() || !literalExp->getDatatypeExpression()) {
						abort("data property assertion with a non-literal value");
						return;
					}
					if (s >= 0) {
						CDataEntry entry;
						entry.mProperty = getDataPropertyId(propName);
						entry.mDatatype = getDatatypeId(literalExp->getDatatypeExpression()->getName());
						entry.mLiteral = literalExp;
						mIndiData[s].append(entry);
					}
					CAxiomRecord rec;
					rec.mAxiom = axiom;
					rec.mKind = AK_DATAASSERTION;
					rec.mSubject = s;
					rec.mObject = -1;
					rec.mEdge = -1;
					mAxiomRecords.append(rec);
				}


				void CABoxAnalysis::handleDeclaration(CAxiomExpression* axiom) {
					CDeclarationAxiomExpression* declExp = dynamic_cast<CDeclarationAxiomExpression*>(axiom);
					if (!declExp) {
						return;
					}
					CNamedIndividualExpression* indiExp = dynamic_cast<CNamedIndividualExpression*>(declExp->getEntity());
					if (!indiExp) {
						return;
					}
					int id = getIndividualId(indiExp, true);
					if (id >= 0) {
						CAxiomRecord rec;
						rec.mAxiom = axiom;
						rec.mKind = AK_DECLARATION;
						rec.mSubject = id;
						rec.mObject = -1;
						rec.mEdge = -1;
						mAxiomRecords.append(rec);
					}
				}


				void CABoxAnalysis::mergeSignature(const CAxiomSignature& sig) {
					if (sig.mUnsupported) {
						abort("unsupported construct in the TBox/RBox");
					}
					mSensitiveSeeds.unite(sig.mCardinalityRoles);
					mMergingSeeds.unite(sig.mMergingRoles);
					mNominalRestrictionRoles.unite(sig.mNominalRestrictionRoles);
					mExactDataSeeds.unite(sig.mExactDataProperties);
					if (!sig.mNamedIndividuals.isEmpty() || sig.mHasAnonymousIndividual) {
						mTBoxHasNominals = true;
					}
				}


				void CABoxAnalysis::handleOtherAxiom(CAxiomExpression* axiom) {
					CAxiomSignature sig;
					axiom->visitSubExpressions(&sig);
					CBuildExpression::ExpressionType type = axiom->getType();

					bool assertionAxiom = (type == CBuildExpression::BETSAMEINDIVIDUAL || type == CBuildExpression::BETDIFFERENTINDIVIDUAL
							|| type == CBuildExpression::BETNEGATIVEOBJECTPROPERTYASSERTION || type == CBuildExpression::BETNEGATIVEDATAPROPERTYASSERTION);

					mergeSignature(sig);
					pinSignatureIndividuals(sig, assertionAxiom ? PIN_ASSERTION : PIN_TBOX);
					if (type == CBuildExpression::BETSAMEINDIVIDUAL) {
						for (QMap<QString, CNamedIndividualExpression*>::const_iterator it = sig.mNamedIndividuals.constBegin(), itEnd = sig.mNamedIndividuals.constEnd(); it != itEnd; ++it) {
							mToldSameIndividuals.insert(it.key());
						}
					}
					// the top object property is only allowed as super property (it is the universal role otherwise)
					bool topAllowed = false;
					if (type == CBuildExpression::BETSUBOBJECTPROPERTYOF) {
						topAllowed = true;
					} else if (type == CBuildExpression::BETOBJECTPROPERTYDOMAIN || type == CBuildExpression::BETOBJECTPROPERTYRANGE) {
						topAllowed = true;
					}

					switch (type) {
						case CBuildExpression::BETSUBOBJECTPROPERTYOF: {
							CSubObjectPropertyOfExpression* subExp = dynamic_cast<CSubObjectPropertyOfExpression*>(axiom);
							if (!subExp) {
								abort("unexpected sub object property axiom");
								break;
							}
							CEXPRESSIONLIST<CObjectPropertyTermExpression*>* subList = subExp->getSubObjectPropertyTermExpressionList();
							QSet<QString> names;
							if (subList) {
								for (CEXPRESSIONLIST<CObjectPropertyTermExpression*>::const_iterator it = subList->constBegin(), itEnd = subList->constEnd(); it != itEnd; ++it) {
									QString name;
									bool inverse;
									if (resolveObjectProperty(*it, name, inverse)) {
										if (name == QLatin1String(IRI_TOP_OBJECT_PROPERTY)) {
											abort("the top object property is used as sub property (universal role)");
										}
										names.insert(name);
									}
								}
							}
							QString superName;
							bool superInverse;
							if (resolveObjectProperty(subExp->getSuperObjectPropertyTermExpression(), superName, superInverse)) {
								names.insert(superName);
							}
							addRelation(mObjectRoleRelation, names);
							bool chain = sig.mHasChain || (subList && subList->size() > 1);
							if (chain) {
								mSensitiveSeeds.unite(names);
								mSensitiveSeeds.unite(sig.mObjectProperties);
							}
							break;
						}
						case CBuildExpression::BETINVERSEOBJECTPROPERTIES:
						case CBuildExpression::BETEQUIVALENTOBJECTPROPERTIES: {
							addRelation(mObjectRoleRelation, sig.mObjectProperties);
							topAllowed = false;
							break;
						}
						case CBuildExpression::BETFUNCTIONALOBJECTPROPERTY:
						case CBuildExpression::BETINVERSEFUNCTIONALPROPERTY: {
							mSensitiveSeeds.unite(sig.mObjectProperties);
							mMergingSeeds.unite(sig.mObjectProperties);
							break;
						}
						case CBuildExpression::BETASYMMETRICPROPERTY:
						case CBuildExpression::BETIRREFLEXIVEPROPERTY:
						case CBuildExpression::BETREFLEXIVEPROPERTY:
						case CBuildExpression::BETDISJOINTOBJECTPROPERTIES:
						case CBuildExpression::BETTRANSITIVEOBJECTPROPERTY: {
							mSensitiveSeeds.unite(sig.mObjectProperties);
							break;
						}
						case CBuildExpression::BETFUNCTIONALDATAPROPERTY:
						case CBuildExpression::BETDISJOINTDATAPROPERTIES:
						case CBuildExpression::BETDATAPROPERTYRANGE: {
							mExactDataSeeds.unite(sig.mDataProperties);
							break;
						}
						case CBuildExpression::BETSUBDATAPROPERTYOF:
						case CBuildExpression::BETEQUIVALENTDATAPROPERTIES: {
							addRelation(mDataRoleRelation, sig.mDataProperties);
							break;
						}
						default: {
							break;
						}
					}

					if (!topAllowed && sig.mObjectProperties.contains(QLatin1String(IRI_TOP_OBJECT_PROPERTY))) {
						abort("the top object property is used in a TBox/RBox axiom other than as super property");
					}
					if (type == CBuildExpression::BETRULECLASSATOM || type == CBuildExpression::BETRULEPROPERTYATOM || type == CBuildExpression::BETRULEDIFFERENTINDIVIDUALSATOM
							|| type == CBuildExpression::BETRULEATOMBODYLIST || type == CBuildExpression::BETRULEATOMHEADLIST) {
						abort("rules are not supported");
					}
				}


				void CABoxAnalysis::scanAxioms() {
					QVector<CAxiomExpression*> axioms;
					CONTOLOGYAXIOMSET<CAxiomExpression*>* tellSet = mOntology->getBuildData()->getTellAxiomSet();
					axioms.reserve(tellSet->size());
					for (CONTOLOGYAXIOMSET<CAxiomExpression*>::const_iterator it = tellSet->constBegin(), itEnd = tellSet->constEnd(); it != itEnd; ++it) {
						axioms.append(*it);
					}
					struct CSerialLess {
						bool operator()(CAxiomExpression* a, CAxiomExpression* b) const { return a->getSerial() < b->getSerial(); }
					};
					std::sort(axioms.begin(), axioms.end(), CSerialLess());
					mAxiomCount = axioms.size();

					for (int i = 0; i < axioms.size() && !mAbort; ++i) {
						CAxiomExpression* axiom = axioms.at(i);
						switch (axiom->getType()) {
							case CBuildExpression::BETCLASSASSERTION: {
								handleClassAssertion(axiom);
								break;
							}
							case CBuildExpression::BETOBJECTPROPERTYASSERTION: {
								handleObjectPropertyAssertion(axiom);
								break;
							}
							case CBuildExpression::BETDATAPROPERTYASSERTION: {
								handleDataPropertyAssertion(axiom);
								break;
							}
							case CBuildExpression::BETDECLARATION: {
								handleDeclaration(axiom);
								break;
							}
							default: {
								handleOtherAxiom(axiom);
								break;
							}
						}
					}
				}



				// ----------------------------------------------------------------------------------------------------
				// sensitivity of roles and pinned individuals

				void CABoxAnalysis::computeSensitivity() {
					mSensitiveRoles.clear();
					closeOver(mObjectRoleRelation, mSensitiveSeeds, mSensitiveRoles);
					mMergingRoles.clear();
					closeOver(mObjectRoleRelation, mMergingSeeds, mMergingRoles);
					mExactDataProperties.clear();
					closeOver(mDataRoleRelation, mExactDataSeeds, mExactDataProperties);

					QVector<char> roleSensitive(mRoleIRI.size(), 0);
					for (int r = 0; r < mRoleIRI.size(); ++r) {
						roleSensitive[r] = mSensitiveRoles.contains(mRoleIRI.at(r)) ? 1 : 0;
					}
					for (int e = 0; e < mEdges.size(); ++e) {
						const CEdge& edge = mEdges.at(e);
						if (roleSensitive.at(edge.mRole)) {
							pin(edge.mSource, PIN_ROLE);
							pin(edge.mTarget, PIN_ROLE);
						}
					}

					// restrictions with nominals over roles that may merge individuals can force merges of individuals that have no asserted edge
					if (mTBoxHasNominals) {
						for (QSet<QString>::const_iterator it = mNominalRestrictionRoles.constBegin(), itEnd = mNominalRestrictionRoles.constEnd(); it != itEnd; ++it) {
							if (mMergingRoles.contains(*it)) {
								abort(QString("restriction with a nominal over the role '%1' that is subject to functional/cardinality constraints").arg(*it));
								break;
							}
						}
					}
				}



				// ----------------------------------------------------------------------------------------------------
				// colour refinement

				void CABoxAnalysis::refineColours() {
					int n = mIndiName.size();
					mColour.assign(n, 0);

					// exactness of data properties
					QVector<char> dataExact(mDataPropertyIRI.size(), 0);
					for (int p = 0; p < mDataPropertyIRI.size(); ++p) {
						dataExact[p] = mExactDataProperties.contains(mDataPropertyIRI.at(p)) ? 1 : 0;
					}

					// initial colours: pinned individuals are unique, the others are coloured by their asserted types and data
					std::unordered_map<std::string, int> initialIntern;
					for (int i = 0; i < n; ++i) {
						if (mPinned.at(i)) {
							mColour[i] = i;
							continue;
						}
						std::string key;
						QVector<qint32> types(mIndiTypes.at(i));
						std::sort(types.begin(), types.end());
						types.erase(std::unique(types.begin(), types.end()), types.end());
						qint32 typeCount = types.size();
						key.append((const char*)&typeCount, sizeof(typeCount));
						if (typeCount > 0) {
							key.append((const char*)types.constData(), typeCount * sizeof(qint32));
						}
						// data shape: (property, datatype) with the count up to 2; exact values for properties that are used in restrictions
						std::vector<unsigned long long> shape;
						std::vector<std::string> exact;
						const QVector<CDataEntry>& dataEntries = mIndiData.at(i);
						for (int d = 0; d < dataEntries.size(); ++d) {
							const CDataEntry& entry = dataEntries.at(d);
							if (dataExact.at(entry.mProperty)) {
								std::string exactKey;
								qint32 pr = entry.mProperty;
								qint32 dt = entry.mDatatype;
								exactKey.append((const char*)&pr, sizeof(pr));
								exactKey.append((const char*)&dt, sizeof(dt));
								exactKey.append(entry.mLiteral->getDataLexicalValueExpression()->getName().toUtf8().constData());
								exact.push_back(exactKey);
							} else {
								shape.push_back(((unsigned long long)(unsigned int)entry.mProperty << 32) | (unsigned int)entry.mDatatype);
							}
						}
						std::sort(shape.begin(), shape.end());
						std::vector<unsigned long long> shapeCounted;
						for (size_t s = 0; s < shape.size();) {
							size_t e = s;
							while (e < shape.size() && shape[e] == shape[s]) {
								++e;
							}
							shapeCounted.push_back(shape[s]);
							shapeCounted.push_back((unsigned long long)std::min<size_t>(2, e - s));
							s = e;
						}
						qint32 shapeSize = (qint32)shapeCounted.size();
						key.append((const char*)&shapeSize, sizeof(shapeSize));
						if (shapeSize > 0) {
							key.append((const char*)&shapeCounted[0], shapeSize * sizeof(unsigned long long));
						}
						std::sort(exact.begin(), exact.end());
						qint32 exactSize = (qint32)exact.size();
						key.append((const char*)&exactSize, sizeof(exactSize));
						for (size_t x = 0; x < exact.size(); ++x) {
							qint32 len = (qint32)exact[x].size();
							key.append((const char*)&len, sizeof(len));
							key.append(exact[x]);
						}
						std::unordered_map<std::string, int>::iterator it = initialIntern.find(key);
						if (it == initialIntern.end()) {
							it = initialIntern.insert(std::make_pair(key, (int)initialIntern.size())).first;
						}
						mColour[i] = n + it->second;
					}
					int classCount = (int)initialIntern.size();
					initialIntern.clear();

					// adjacency (both directions, labelled with the role and the direction)
					std::vector<int> adjStart(n + 1, 0);
					for (int e = 0; e < mEdges.size(); ++e) {
						++adjStart[mEdges.at(e).mSource + 1];
						++adjStart[mEdges.at(e).mTarget + 1];
					}
					for (int i = 0; i < n; ++i) {
						adjStart[i + 1] += adjStart[i];
					}
					std::vector<int> adjLabel(adjStart[n]);
					std::vector<int> adjNeighbour(adjStart[n]);
					std::vector<int> fill(adjStart.begin(), adjStart.end() - 1);
					for (int e = 0; e < mEdges.size(); ++e) {
						const CEdge& edge = mEdges.at(e);
						adjLabel[fill[edge.mSource]] = edge.mRole * 2;
						adjNeighbour[fill[edge.mSource]++] = edge.mTarget;
						adjLabel[fill[edge.mTarget]] = edge.mRole * 2 + 1;
						adjNeighbour[fill[edge.mTarget]++] = edge.mSource;
					}

					// refinement rounds
					std::vector<int> newColour(n);
					std::vector<unsigned long long> neighbourSignature;
					for (int round = 0; round < 100000; ++round) {
						std::unordered_map<std::string, int> intern;
						for (int i = 0; i < n; ++i) {
							if (mPinned.at(i)) {
								newColour[i] = mColour[i];
								continue;
							}
							neighbourSignature.clear();
							for (int k = adjStart[i]; k < adjStart[i + 1]; ++k) {
								neighbourSignature.push_back(((unsigned long long)(unsigned int)adjLabel[k] << 32) | (unsigned int)mColour[adjNeighbour[k]]);
							}
							std::sort(neighbourSignature.begin(), neighbourSignature.end());
							neighbourSignature.erase(std::unique(neighbourSignature.begin(), neighbourSignature.end()), neighbourSignature.end());
							std::string key;
							int own = mColour[i];
							key.append((const char*)&own, sizeof(own));
							if (!neighbourSignature.empty()) {
								key.append((const char*)&neighbourSignature[0], neighbourSignature.size() * sizeof(unsigned long long));
							}
							std::unordered_map<std::string, int>::iterator it = intern.find(key);
							if (it == intern.end()) {
								it = intern.insert(std::make_pair(key, (int)intern.size())).first;
							}
							newColour[i] = n + it->second;
						}
						int newClassCount = (int)intern.size();
						mColour.swap(newColour);
						if (newClassCount == classCount) {
							break;
						}
						classCount = newClassCount;
					}
				}


				void CABoxAnalysis::formGroups() {
					int n = mIndiName.size();
					std::unordered_map<int, int> groupOfColour;
					mGroups.clear();
					mGroupOfIndi.fill(-1, n);
					// individuals sorted by name
					std::vector<int> order(n);
					for (int i = 0; i < n; ++i) {
						order[i] = i;
					}
					struct CNameLess {
						const QVector<QString>* mNames;
						bool operator()(int a, int b) const { return mNames->at(a) < mNames->at(b); }
					};
					CNameLess nameLess;
					nameLess.mNames = &mIndiName;
					std::sort(order.begin(), order.end(), nameLess);
					for (int k = 0; k < n; ++k) {
						int i = order[k];
						if (mPinned.at(i)) {
							continue;
						}
						std::unordered_map<int, int>::iterator it = groupOfColour.find(mColour[i]);
						if (it == groupOfColour.end()) {
							it = groupOfColour.insert(std::make_pair(mColour[i], (int)mGroups.size())).first;
							mGroups.push_back(std::vector<int>());
						}
						mGroups[it->second].push_back(i);
						mGroupOfIndi[i] = it->second;
					}

					// groups with asserted edges between two different members need two representatives
					mGroupLooped.fill(0, (int)mGroups.size());
					for (int e = 0; e < mEdges.size(); ++e) {
						const CEdge& edge = mEdges.at(e);
						int gs = mGroupOfIndi.at(edge.mSource);
						int gt = mGroupOfIndi.at(edge.mTarget);
						if (gs >= 0 && gs == gt && edge.mSource != edge.mTarget) {
							mGroupLooped[gs] = 1;
						}
					}

					mRemoved.fill(0, n);
					mGroupRetainedCount.fill(1, (int)mGroups.size());
					mRemovedCount = 0;
					mReducedGroupCount = 0;
					for (size_t g = 0; g < mGroups.size(); ++g) {
						int retain = mGroupLooped.at((int)g) ? 2 : 1;
						retain = (int)std::min<size_t>(retain, mGroups[g].size());
						mGroupRetainedCount[(int)g] = retain;
						if ((int)mGroups[g].size() > retain) {
							++mReducedGroupCount;
							for (size_t k = retain; k < mGroups[g].size(); ++k) {
								mRemoved[mGroups[g][k]] = 1;
								++mRemovedCount;
							}
						}
					}
					mPinnedCount = 0;
					for (int i = 0; i < n; ++i) {
						if (mPinned.at(i)) {
							++mPinnedCount;
						}
					}
				}



				// ----------------------------------------------------------------------------------------------------
				// decision and application

				bool CABoxAnalysis::decide() {
					qint64 n = mIndiName.size();
					if (mRemovedCount <= 0) {
						return false;
					}
					if (mMode == CABoxReducer::REDUCTIONON) {
						return true;
					}
					// automatic mode: only if the ABox is large (number of individuals or estimated memory consumption) and the reduction is worthwhile
					qint64 minIndividuals = CConfigDataReader::readConfigInteger(mConfig, "Konclude.CLI.Materialize.ReduceABoxMinIndividuals", 100000);
					qint64 memoryPercent = CConfigDataReader::readConfigInteger(mConfig, "Konclude.CLI.Materialize.ReduceABoxMemoryPercent", 50);
					qint64 minReductionPercent = CConfigDataReader::readConfigInteger(mConfig, "Konclude.CLI.Materialize.ReduceABoxMinReductionPercent", 10);
					qint64 bytesPerIndividual = CConfigDataReader::readConfigInteger(mConfig, "Konclude.CLI.Materialize.ReduceABoxBytesPerIndividual", 20000);
					qint64 bytesPerAssertion = CConfigDataReader::readConfigInteger(mConfig, "Konclude.CLI.Materialize.ReduceABoxBytesPerAssertion", 2000);
					qint64 estimatedBytes = n * bytesPerIndividual + mAssertionCount * bytesPerAssertion;
					qint64 availableBytes = getAvailableMemoryBytes();

					bool large = (minIndividuals > 0 && n >= minIndividuals);
					bool memoryCritical = (availableBytes >= 0 && estimatedBytes >= availableBytes / 100 * memoryPercent);
					bool worthwhile = (mRemovedCount * 100 >= n * minReductionPercent);
					logInfo(QString("ABox reduction (auto): %1 individuals, %2 assertions, estimated memory demand %3 MiB, available memory %4 MiB, %5 individuals (%6 %) could be removed.")
							.arg(n).arg(mAssertionCount).arg(estimatedBytes / (1024 * 1024)).arg(availableBytes >= 0 ? QString::number(availableBytes / (1024 * 1024)) : QString("unknown"))
							.arg(mRemovedCount).arg(n > 0 ? 100.0 * mRemovedCount / n : 0.0, 0, 'f', 1));
					if (!worthwhile) {
						logInfo(QString("ABox reduction (auto): not applied, less than %1 % of the individuals can be removed.").arg(minReductionPercent));
						return false;
					}
					if (!large && !memoryCritical) {
						logInfo(QString("ABox reduction (auto): not applied, the ABox is not large enough (less than %1 individuals and the estimated memory demand is below %2 % of the available memory).").arg(minIndividuals).arg(memoryPercent));
						return false;
					}
					return true;
				}


				bool CABoxAnalysis::apply() {
					int n = mIndiName.size();
					QSet<CAxiomExpression*>* localAxiomSet = mBuilder->getAddedAxiomSet();

					// retained individuals of each group, mapping of individuals to retained individuals
					std::vector<std::vector<int> > groupRetained(mGroups.size());
					for (size_t g = 0; g < mGroups.size(); ++g) {
						for (int k = 0; k < mGroupRetainedCount.at((int)g); ++k) {
							groupRetained[g].push_back(mGroups[g][k]);
						}
					}
					QVector<char> groupReduced(mGroups.size(), 0);
					for (size_t g = 0; g < mGroups.size(); ++g) {
						groupReduced[(int)g] = ((int)mGroups[g].size() > mGroupRetainedCount.at((int)g)) ? 1 : 0;
					}

					// all axioms that are changed have to belong to the current parsing session
					for (int r = 0; r < mAxiomRecords.size(); ++r) {
						const CAxiomRecord& rec = mAxiomRecords.at(r);
						bool change = false;
						if (rec.mKind == AK_OBJECTASSERTION) {
							change = (rec.mSubject >= 0 && mRemoved.at(rec.mSubject)) || (rec.mObject >= 0 && mRemoved.at(rec.mObject));
						} else {
							change = (rec.mSubject >= 0 && mRemoved.at(rec.mSubject));
						}
						if (change && !localAxiomSet->contains(rec.mAxiom)) {
							logInfo(QString("ABox reduction: not applied, axioms of removable individuals originate from an earlier loading step."));
							return false;
						}
					}

					QList<CAxiomExpression*> retractList;
					QList<CAxiomExpression*> tellList;
					QSet<CAxiomExpression*> tellSet;
					QHash<int, QVector<CABoxReductionData::CDataAssertion> > removedData;

					for (int r = 0; r < mAxiomRecords.size(); ++r) {
						const CAxiomRecord& rec = mAxiomRecords.at(r);
						switch (rec.mKind) {
							case AK_CLASSASSERTION:
							case AK_DECLARATION: {
								if (rec.mSubject >= 0 && mRemoved.at(rec.mSubject)) {
									retractList.append(rec.mAxiom);
								}
								break;
							}
							case AK_DATAASSERTION: {
								if (rec.mSubject >= 0 && mRemoved.at(rec.mSubject)) {
									retractList.append(rec.mAxiom);
								}
								break;
							}
							case AK_OBJECTASSERTION: {
								int s = rec.mSubject;
								int o = rec.mObject;
								if (s < 0 || o < 0) {
									break;
								}
								bool sRemoved = mRemoved.at(s);
								bool oRemoved = mRemoved.at(o);
								int gs = mGroupOfIndi.at(s);
								int go = mGroupOfIndi.at(o);
								bool sReduced = gs >= 0 && groupReduced.at(gs);
								bool oReduced = go >= 0 && groupReduced.at(go);
								if (!sReduced && !oReduced) {
									break;
								}
								if (sRemoved || oRemoved) {
									retractList.append(rec.mAxiom);
								}
								CObjectPropertyAssertionExpression* propAssExp = dynamic_cast<CObjectPropertyAssertionExpression*>(rec.mAxiom);
								std::vector<int> sourceCandidates;
								std::vector<int> targetCandidates;
								if (sReduced) {
									sourceCandidates = groupRetained[gs];
								} else {
									sourceCandidates.push_back(s);
								}
								if (oReduced) {
									targetCandidates = groupRetained[go];
								} else {
									targetCandidates.push_back(o);
								}
								for (size_t a = 0; a < sourceCandidates.size(); ++a) {
									for (size_t b = 0; b < targetCandidates.size(); ++b) {
										int x = sourceCandidates[a];
										int y = targetCandidates[b];
										if (x == y) {
											continue;
										}
										CObjectPropertyAssertionExpression* newAxiom = mBuilder->getObjectPropertyAssertion(mIndiExpression.at(x), mIndiExpression.at(y), propAssExp->getObjectPropertyTermExpression());
										if (newAxiom && newAxiom != rec.mAxiom) {
											if (!tellSet.contains(newAxiom)) {
												tellSet.insert(newAxiom);
												tellList.append(newAxiom);
											}
										}
									}
								}
								break;
							}
							default: {
								break;
							}
						}
					}

					// data assertions of the removed individuals are remembered
					for (int i = 0; i < n; ++i) {
						if (mRemoved.at(i)) {
							const QVector<CDataEntry>& dataEntries = mIndiData.at(i);
							QVector<CABoxReductionData::CDataAssertion>& list = removedData[i];
							for (int d = 0; d < dataEntries.size(); ++d) {
								CABoxReductionData::CDataAssertion da;
								da.mPropertyIRI = mDataPropertyIRI.at(dataEntries.at(d).mProperty);
								da.mLexicalValue = dataEntries.at(d).mLiteral->getDataLexicalValueExpression()->getName();
								da.mDatatypeIRI = dataEntries.at(d).mLiteral->getDatatypeExpression()->getName();
								list.append(da);
							}
						}
					}

					// change the told axioms of the open parsing session
					for (QList<CAxiomExpression*>::const_iterator it = retractList.constBegin(), itEnd = retractList.constEnd(); it != itEnd; ++it) {
						mBuilder->retractOntologyAxiom(*it);
					}
					for (QList<CAxiomExpression*>::const_iterator it = tellList.constBegin(), itEnd = tellList.constEnd(); it != itEnd; ++it) {
						mBuilder->tellOntologyAxiom(*it);
					}

					// removed individuals do not have to be built
					{
						CBUILDSET<CIndividualTermExpression*>* buildIndiSet = mOntology->getBuildData()->getBuildIndividualSet();
						CBUILDLIST<CIndividualTermExpression*>* buildIndiList = mOntology->getBuildData()->getBuildIndividualList();
						QSet<CIndividualTermExpression*> removedExpressions;
						for (int i = 0; i < n; ++i) {
							if (mRemoved.at(i)) {
								removedExpressions.insert(mIndiExpression.at(i));
								buildIndiSet->remove(mIndiExpression.at(i));
							}
						}
						CBUILDLIST<CIndividualTermExpression*> keptList;
						for (CBUILDLIST<CIndividualTermExpression*>::const_iterator it = buildIndiList->constBegin(), itEnd = buildIndiList->constEnd(); it != itEnd; ++it) {
							if (!removedExpressions.contains(*it)) {
								keptList.append(*it);
							}
						}
						buildIndiList->clear();
						for (CBUILDLIST<CIndividualTermExpression*>::const_iterator it = keptList.constBegin(), itEnd = keptList.constEnd(); it != itEnd; ++it) {
							buildIndiList->append(*it);
						}
					}

					// information for the writer
					QSharedPointer<CABoxReductionData> data(new CABoxReductionData());
					data->mOriginalIndividuals = n;
					data->mRemovedIndividuals = mRemovedCount;
					data->mRetainedIndividuals = n - mRemovedCount;
					data->mToldSameIndividuals = mToldSameIndividuals;
					for (int i = 0; i < n; ++i) {
						if (mPinned.at(i)) {
							data->mPinnedRetained.insert(mIndiName.at(i));
						}
					}
					for (size_t g = 0; g < mGroups.size(); ++g) {
						if (!groupReduced.at((int)g)) {
							continue;
						}
						int retain = mGroupRetainedCount.at((int)g);
						QStringList members;
						for (size_t k = 0; k < mGroups[g].size(); ++k) {
							int i = mGroups[g][k];
							if ((int)k < retain) {
								data->mAffectedRetained.insert(mIndiName.at(i));
							} else {
								members.append(mIndiName.at(i));
								data->mRemovedDataAssertions.insert(mIndiName.at(i), removedData.value(i));
							}
						}
						data->mMembersOfRepresentative.insert(mIndiName.at(mGroups[g][0]), members);
					}

					// asserted edges between individuals that are not pinned and where at least one of them is in a reduced group are rebuilt by the writer
					{
						QHash<int, int> nameIndex;
						QVector<int> edgeOrder;
						for (int e = 0; e < mEdges.size(); ++e) {
							const CEdge& edge = mEdges.at(e);
							int gs = mGroupOfIndi.at(edge.mSource);
							int gt = mGroupOfIndi.at(edge.mTarget);
							if (gs < 0 || gt < 0) {
								continue;
							}
							if (!groupReduced.at(gs) && !groupReduced.at(gt)) {
								continue;
							}
							edgeOrder.append(e);
						}
						struct CEdgePairLess {
							const QVector<CEdge>* mEdges;
							bool operator()(int a, int b) const {
								const CEdge& ea = mEdges->at(a);
								const CEdge& eb = mEdges->at(b);
								int aLo = std::min(ea.mSource, ea.mTarget), aHi = std::max(ea.mSource, ea.mTarget);
								int bLo = std::min(eb.mSource, eb.mTarget), bHi = std::max(eb.mSource, eb.mTarget);
								if (aLo != bLo) return aLo < bLo;
								if (aHi != bHi) return aHi < bHi;
								return a < b;
							}
						};
						CEdgePairLess pairLess;
						pairLess.mEdges = &mEdges;
						std::sort(edgeOrder.begin(), edgeOrder.end(), pairLess);
						data->mEdgePropertyIRIs = mRoleIRI;
						for (int k = 0; k < edgeOrder.size(); ++k) {
							const CEdge& edge = mEdges.at(edgeOrder.at(k));
							int ids[2] = { edge.mSource, edge.mTarget };
							int mapped[2];
							for (int m = 0; m < 2; ++m) {
								QHash<int, int>::const_iterator it = nameIndex.constFind(ids[m]);
								if (it == nameIndex.constEnd()) {
									int idx = data->mEdgeIndividualNames.size();
									data->mEdgeIndividualNames.append(mIndiName.at(ids[m]));
									it = nameIndex.insert(ids[m], idx);
								}
								mapped[m] = it.value();
							}
							data->mEdgeSubject.append(mapped[0]);
							data->mEdgeObject.append(mapped[1]);
							data->mEdgeProperty.append(edge.mRole);
						}
					}

					mOntology->getLoadData()->setABoxReductionData(data);
					return true;
				}


				void CABoxAnalysis::logResult(bool applied) {
					qint64 n = mIndiName.size();
					QString pinText;
					for (int i = 0; i < PIN_REASONCOUNT; ++i) {
						if (mPinCount[i] > 0) {
							if (!pinText.isEmpty()) {
								pinText += QString("; ");
							}
							pinText += QString("%1 %2").arg(mPinCount[i]).arg(PIN_REASON_TEXT[i]);
						}
					}
					if (applied) {
						logInfo(QString("ABox reduction applied: %1 interchangeable individuals were merged into %2 representatives (%3 of %4 individuals left, %5 percent removed; %6 individuals are never merged%7).")
								.arg(mRemovedCount).arg(mReducedGroupCount).arg(n - mRemovedCount).arg(n).arg(n > 0 ? 100.0 * mRemovedCount / n : 0.0, 0, 'f', 1).arg(mPinnedCount)
								.arg(pinText.isEmpty() ? QString() : QString(": ") + pinText));
					} else {
						logInfo(QString("ABox reduction not applied: %1 individuals, %2 never merged, %3 removable%4.")
								.arg(n).arg(mPinnedCount).arg(mRemovedCount).arg(pinText.isEmpty() ? QString() : QString(" (never merged: ") + pinText + QString(")")));
					}
				}


				bool CABoxAnalysis::run() {
					scanAxioms();
					if (!mAbort) {
						computeSensitivity();
					}
					if (mAbort) {
						logInfo(QString("ABox reduction not applied: %1.").arg(mAbortReason));
						return false;
					}
					refineColours();
					formGroups();
					if (!decide()) {
						logResult(false);
						return false;
					}
					bool applied = apply();
					if (applied) {
						logResult(true);
					}
					return applied;
				}


			} // end anonymous namespace



			CABoxReducer::REDUCTIONMODE CABoxReducer::readReductionMode(CConfiguration* config) {
				QString modeString = CConfigDataReader::readConfigString(config, "Konclude.CLI.Materialize.ReduceABox", "off").trimmed().toLower();
				if (modeString == "off" || modeString.isEmpty()) {
					// the realization command has an own switch
					modeString = CConfigDataReader::readConfigString(config, "Konclude.CLI.Realization.ReduceABox", "off").trimmed().toLower();
				}
				if (modeString == "on" || modeString == "true" || modeString == "yes" || modeString == "1") {
					return REDUCTIONON;
				} else if (modeString == "auto") {
					return REDUCTIONAUTO;
				}
				return REDUCTIONOFF;
			}


			bool CABoxReducer::reduceOntology(CConcreteOntology* ontology, CConcreteOntologyUpdateCollectorBuilder* builder, CConfiguration* config, const QList<QString>& pendingImportList) {
				REDUCTIONMODE mode = readReductionMode(config);
				if (mode == REDUCTIONOFF || !ontology || !builder) {
					return false;
				}
				if (CABoxReductionControl::isReductionSuppressed()) {
					logInfo(QString("ABox reduction not applied: switched off for the recomputation without reduction."));
					return false;
				}
				if (!pendingImportList.isEmpty()) {
					logInfo(QString("ABox reduction not applied: the ontology imports other ontologies."));
					return false;
				}
				if (!CConfigDataReader::readConfigString(config, "Konclude.CLI.IRIName").isEmpty()) {
					logInfo(QString("ABox reduction not applied: the materialization is requested for a single individual."));
					return false;
				}
				if (CConfigDataReader::readConfigBoolean(config, "Konclude.CLI.Output.AbbreviatedIRIs", false)) {
					logInfo(QString("ABox reduction not applied: abbreviated IRIs are written."));
					return false;
				}
				if (!ontology->getLoadData()->getLoadedOntologyIRIs().isEmpty() || ontology->getLoadData()->getABoxReductionData()) {
					logInfo(QString("ABox reduction not applied: the ontology is not loaded in a single step."));
					return false;
				}
				if (ontology->getBuildData()->getTellAxiomSet()->size() != builder->getAddedAxiomSet()->size()) {
					logInfo(QString("ABox reduction not applied: the ontology contains axioms from earlier parsing steps."));
					return false;
				}
				CABoxAnalysis analysis(ontology, builder, config, mode);
				return analysis.run();
			}


		}; // end namespace Generator

	}; // end namespace Reasoner

}; // end namespace Konclude
