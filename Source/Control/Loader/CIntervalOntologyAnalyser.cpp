/*
 *		Copyright (C) 2013, 2014 by the Konclude Developer Team.
 *
 *		This file is part of the reasoning system Konclude.
 *		For details and support, see <http://konclude.com/>.
 *
 *		Konclude is free software: you can redistribute it and/or modify it under
 *		the terms of version 3 of the GNU Lesser General Public License (LGPL3).
 */

#include "CIntervalOntologyAnalyser.h"

#include <algorithm>
#include <functional>

#include <QDateTime>
#include <QRegExp>
#include "Config/CConfigDataReader.h"
#include <QSet>

#include "Parser/Expressions/CBuildExpression.h"
#include "Parser/Expressions/CAxiomExpression.h"
#include "Parser/Expressions/CObjectPropertyTermExpression.h"
#include "Parser/Expressions/CIndividualTermExpression.h"
#include "Parser/CLintExpressionRenderer.h"
#include "Parser/Expressions/CObjectPropertyExpression.h"
#include "Parser/Expressions/CDataPropertyExpression.h"
#include "Parser/Expressions/CNamedIndividualExpression.h"
#include "Parser/Expressions/CAnonymousIndividualExpression.h"
#include "Parser/Expressions/CDatatypeExpression.h"
#include "Parser/Expressions/CDataLexicalValueExpression.h"
#include "Parser/Expressions/CDataLiteralExpression.h"
#include "Parser/Expressions/CInverseObjectPropertyOfExpression.h"
#include "Parser/Expressions/CObjectPropertyAssertionExpression.h"
#include "Parser/Expressions/CDeclarationAxiomExpression.h"
#include "Parser/Expressions/CDataPropertyAssertionExpression.h"

#include "Reasoner/Triples/CTriplesData.h"
#include "Reasoner/Triples/CTriplesDataIterator.h"
#include "Reasoner/Ontology/COntologyTriplesData.h"
#include "Reasoner/Ontology/COntologyBuildData.h"

namespace Konclude {

	using namespace Parser;
	using namespace Parser::Expression;
	using namespace Reasoner::Triples;

	namespace Control {

		namespace Loader {

			CIntervalOntologyAnalyser::CIntervalOntologyAnalyser(CConcreteOntology* ontology, const QString& fileName) {
				mOntology = ontology;
				mFileName = fileName;
				mDeriveFromTimestamps = true;
				mOutputOwlTimeStyle = true;
				mOutputNamespace = "http://www.w3.org/2006/time#";
				mMaxDerivedPairs = 1000000;
				mMaxComponentSize = 150;
				mMaxMinimizationSize = 60;
				mObjectAssertionCount = 0;
				mUnreadableTimes = 0;
				mDataAssertionCount = 0;
				mConflictCount = 0;
				mTimedCount = 0;
				mVocabularyDeclared = false;
				mGroupByConnectivity = false;
				mDeriveOnlyIfDeclared = false;
			}


			void CIntervalOntologyAnalyser::configure(Konclude::Config::CConfiguration* config) {
				using Konclude::Config::CConfigDataReader;
				setRelationNamespace(CConfigDataReader::readConfigString(config, "Konclude.CLI.Interval.RelationNamespace", QString()));
				setGroupByProperty(CConfigDataReader::readConfigString(config, "Konclude.CLI.Interval.GroupByProperty", QString()));
				setDeriveFromTimestamps(CConfigDataReader::readConfigBoolean(config, "Konclude.CLI.Interval.DeriveFromTimestamps", true));
				setMaxDerivedPairs(CConfigDataReader::readConfigInteger(config, "Konclude.CLI.Interval.MaxDerivedPairs", 1000000));
				setMaxComponentSize((int)CConfigDataReader::readConfigInteger(config, "Konclude.CLI.Interval.MaxComponentSize", 150));
				setMaxMinimizationSize((int)CConfigDataReader::readConfigInteger(config, "Konclude.CLI.Interval.MaxMinimizationSize", 60));
				QString vocabulary = CConfigDataReader::readConfigString(config, "Konclude.CLI.Interval.OutputVocabulary", QString("owltime"));
				if (vocabulary.compare("allen", Qt::CaseInsensitive) == 0) {
					setOutputVocabulary(false, CConfigDataReader::readConfigString(config, "Konclude.CLI.Interval.OutputNamespace", QString("http://konclude.com/interval#")));
				} else {
					setOutputVocabulary(true, QString("http://www.w3.org/2006/time#"));
				}
			}


			QString CIntervalOntologyAnalyser::localName(const QString& iri) {
				int i = qMax(iri.lastIndexOf('#'), iri.lastIndexOf('/'));
				return i >= 0 ? iri.mid(i + 1) : iri;
			}


			QString CIntervalOntologyAnalyser::cleanLexical(const QString& lexical) {
				QString t = lexical.trimmed();
				if (t.startsWith('"')) {
					// literal of an RDF triple in N-Triples form: "lexical"^^datatype or "lexical"@language
					int q = t.lastIndexOf('"');
					if (q > 0) { t = t.mid(1, q - 1); }
				}
				return t;
			}


			bool CIntervalOntologyAnalyser::parseTime(const QString& lexical, qint64& value) {
				QString t = cleanLexical(lexical);
				if (t.isEmpty()) {
					return false;
				}
				bool ok = false;
				qlonglong number = t.toLongLong(&ok);
				if (ok) {
					value = number;
					return true;
				}
				QRegExp dateOnly("^\\d{4}-\\d{2}-\\d{2}$");
				if (dateOnly.exactMatch(t)) {
					t += "T00:00:00";
				}
				QRegExp zone("(Z|[+-]\\d{2}:\\d{2})$");
				if (zone.indexIn(t) < 0) {
					t += "Z";
				}
				QDateTime dt = QDateTime::fromString(t, Qt::ISODate);
				if (!dt.isValid()) {
					return false;
				}
				value = dt.toMSecsSinceEpoch();
				return true;
			}


			int CIntervalOntologyAnalyser::nodeId(const QString& name) {
				QHash<QString,int>::const_iterator it = mNodeIds.constFind(name);
				if (it != mNodeIds.constEnd()) {
					return it.value();
				}
				int id = mNodes.size();
				mNodeIds.insert(name, id);
				CNode node;
				node.mName = name;
				mNodes.append(node);
				return id;
			}


			void CIntervalOntologyAnalyser::addRelationAssertion(const QString& subject, int relation, const QString& object, bool inverse) {
				if (subject.isEmpty() || object.isEmpty() || subject.startsWith("_:") || object.startsWith("_:")) {
					return;
				}
				CAssertedRelation r;
				r.mSubject = nodeId(inverse ? object : subject);
				r.mObject = nodeId(inverse ? subject : object);
				r.mRelation = relation;
				if (r.mSubject != r.mObject) {
					mAsserted.append(r);
				}
			}


			void CIntervalOntologyAnalyser::addTimeValue(const QString& individual, bool isStart, const QString& lexical) {
				if (individual.startsWith("_:")) {
					return;
				}
				CNode& node = mNodes[nodeId(individual)];
				qint64 value = 0;
				if (!parseTime(lexical, value)) {
					++mUnreadableTimes;
					if (mUnreadableExamples.size() < 3) { mUnreadableExamples.append(lexical); }
					return;
				}
				if (isStart) {
					node.mHasStart = true; node.mStart = value; node.mStartText = cleanLexical(lexical);
				} else {
					node.mHasEnd = true; node.mEnd = value; node.mEndText = cleanLexical(lexical);
				}
			}


			void CIntervalOntologyAnalyser::registerDeclaredProperty(const QString& iri) {
				int relation = CIntervalAlgebra::relationFromLocalName(localName(iri));
				if (relation >= 0 && (mRelationNamespace.isEmpty() || iri.startsWith(mRelationNamespace))) {
					if (relation < CIntervalAlgebra::RELATION_COUNT) { mVocabularyDeclared = true; }	// precedes / preceded by alone do not declare the 13 relations
					if (mRelationIri[relation].isEmpty()) { mRelationIri[relation] = iri; }
				}
			}


			void CIntervalOntologyAnalyser::scanAxioms() {
				CONTOLOGYAXIOMSET<CAxiomExpression*>* tellAxioms = mOntology->getBuildData()->getTellAxiomSet();
				if (!tellAxioms) {
					return;
				}
				for (CAxiomExpression* axiom : *tellAxioms) {
					if (axiom->getType() == CBuildExpression::BETDECLARATION) {
						CDeclarationAxiomExpression* decl = dynamic_cast<CDeclarationAxiomExpression*>(axiom);
						CBuildExpression* entity = decl ? dynamic_cast<CBuildExpression*>(decl->getEntity()) : nullptr;
						if (entity && entity->getType() == CBuildExpression::BETOBJECTPROPERTY) {
							registerDeclaredProperty(CLintExpressionRenderer::entityName(entity));
						}
						continue;
					}
					if (axiom->getType() == CBuildExpression::BETOBJECTPROPERTYASSERTION) {
						CObjectPropertyAssertionExpression* pa = dynamic_cast<CObjectPropertyAssertionExpression*>(axiom);
						if (!pa) { continue; }
						++mObjectAssertionCount;
						bool inverse = false;
						CBuildExpression* current = pa->getObjectPropertyTermExpression();
						while (current && current->getType() == CBuildExpression::BETINVERSEOBJECTPROPERTYOF) {
							CInverseObjectPropertyOfExpression* invExp = dynamic_cast<CInverseObjectPropertyOfExpression*>(current);
							inverse = !inverse;
							current = invExp ? invExp->getInverseOfExpression() : nullptr;
						}
						QString role = CLintExpressionRenderer::entityName(current);
						QString s = CLintExpressionRenderer::entityName(pa->getFirstIndividualTermExpression());
						QString o = CLintExpressionRenderer::entityName(pa->getSecondIndividualTermExpression());
						if (pa->getFirstIndividualTermExpression()->getType() == CBuildExpression::BETANONYMOUSINDIVIDUAL || pa->getSecondIndividualTermExpression()->getType() == CBuildExpression::BETANONYMOUSINDIVIDUAL) {
							continue;
						}
						QString name = localName(role);
						int relation = CIntervalAlgebra::relationFromLocalName(name);
						if (relation >= 0 && (mRelationNamespace.isEmpty() || role.startsWith(mRelationNamespace))) {
							addRelationAssertion(s, relation, o, inverse);
							if (mRelationIri[relation].isEmpty()) { mRelationIri[relation] = role; }
						} else if (name == "hasBeginning") {
							mBeginningOf.insert(inverse ? o : s, inverse ? s : o);
						} else if (name == "hasEnd") {
							mEndOf.insert(inverse ? o : s, inverse ? s : o);
						}
						if (!mGroupByProperty.isEmpty() && role == mGroupByProperty) {
							mGroupEdges.append(qMakePair(nodeId(inverse ? o : s), nodeId(inverse ? s : o)));
						}
					} else if (axiom->getType() == CBuildExpression::BETDATAPROPERTYASSERTION) {
						CDataPropertyAssertionExpression* da = dynamic_cast<CDataPropertyAssertionExpression*>(axiom);
						if (!da) { continue; }
						++mDataAssertionCount;
						if (da->getIndividualTermExpression()->getType() == CBuildExpression::BETANONYMOUSINDIVIDUAL) { continue; }
						QString s = CLintExpressionRenderer::entityName(da->getIndividualTermExpression());
						QString property = CLintExpressionRenderer::entityName(da->getDataPropertyTermExpression());
						CDataLiteralExpression* literal = dynamic_cast<CDataLiteralExpression*>(da->getDataLiteralTermExpression());
						QString lexical;
						if (literal) {
							CDataLexicalValueExpression* lex = literal->getDataLexicalValueExpression();
							lexical = lex ? lex->getName() : QString();
						}
						QString name = localName(property);
						if (name == "startedAtTime") {
							addTimeValue(s, true, lexical);
						} else if (name == "endedAtTime") {
							addTimeValue(s, false, lexical);
						} else if (name == "inXSDDateTimeStamp" || name == "inXSDDateTime" || name == "inXSDDate") {
							mInstantTimeText.insert(s, lexical);
						}
					}
				}
			}


			void CIntervalOntologyAnalyser::scanAnyEdges() {
				// all object property assertions between named individuals: their connectivity defines the groups of one process or sample
				CONTOLOGYAXIOMSET<CAxiomExpression*>* tellAxioms = mOntology->getBuildData()->getTellAxiomSet();
				if (tellAxioms) {
					for (CAxiomExpression* axiom : *tellAxioms) {
						if (axiom->getType() != CBuildExpression::BETOBJECTPROPERTYASSERTION) { continue; }
						CObjectPropertyAssertionExpression* pa = dynamic_cast<CObjectPropertyAssertionExpression*>(axiom);
						if (!pa || pa->getFirstIndividualTermExpression()->getType() == CBuildExpression::BETANONYMOUSINDIVIDUAL || pa->getSecondIndividualTermExpression()->getType() == CBuildExpression::BETANONYMOUSINDIVIDUAL) { continue; }
						QString s = CLintExpressionRenderer::entityName(pa->getFirstIndividualTermExpression());
						QString o = CLintExpressionRenderer::entityName(pa->getSecondIndividualTermExpression());
						mGroupEdges.append(qMakePair(nodeId(s), nodeId(o)));
					}
				}
				COntologyTriplesData* triplesData = mOntology->getOntologyTriplesData();
				QList<CTriplesData*>* triplesList = triplesData ? triplesData->getAllTriplesData() : nullptr;
				if (triplesList) {
					for (CTriplesData* data : *triplesList) {
						CTriplesDataIterator* it = data->getTriplesDataIterator();
						if (!it) { continue; }
						while (it->hasValue()) {
							if (it->getNodeType(0) == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE && it->getNodeType(1) == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE && it->getNodeType(2) == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE
									&& it->getNodeString(1) != "http://www.w3.org/1999/02/22-rdf-syntax-ns#type") {
								mGroupEdges.append(qMakePair(nodeId(it->getNodeString(0)), nodeId(it->getNodeString(2))));
							}
							it->moveNext();
						}
						delete it;
					}
				}
			}


			void CIntervalOntologyAnalyser::scanTriples() {
				COntologyTriplesData* triplesData = mOntology->getOntologyTriplesData();
				if (!triplesData) {
					return;
				}
				QList<CTriplesData*>* triplesList = triplesData->getAllTriplesData();
				if (!triplesList || triplesList->isEmpty()) {
					return;
				}
				for (CTriplesData* data : *triplesList) {
					CTriplesDataIterator* it = data->getTriplesDataIterator();
					if (!it) {
						continue;
					}
					while (it->hasValue()) {
						if (it->getNodeType(1) == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE && it->getNodeType(0) == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE) {
							QString predicate = it->getNodeString(1);
							QString subject = it->getNodeString(0);
							QString name = localName(predicate);
							if (it->getNodeType(2) == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE) {
								QString object = it->getNodeString(2);
								int relation = CIntervalAlgebra::relationFromLocalName(name);
								if (predicate == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" && object == "http://www.w3.org/2002/07/owl#ObjectProperty") {
									registerDeclaredProperty(subject);
								} else if (relation >= 0 && (mRelationNamespace.isEmpty() || predicate.startsWith(mRelationNamespace))) {
									++mObjectAssertionCount;
									addRelationAssertion(subject, relation, object, false);
									if (mRelationIri[relation].isEmpty()) { mRelationIri[relation] = predicate; }
								} else if (name == "hasBeginning") {
									mBeginningOf.insert(subject, object);
								} else if (name == "hasEnd") {
									mEndOf.insert(subject, object);
								}
								if (!mGroupByProperty.isEmpty() && predicate == mGroupByProperty) {
									mGroupEdges.append(qMakePair(nodeId(subject), nodeId(object)));
								}
							} else if (it->getNodeType(2) == CTriplesDataIterator::TRIPLE_LITERAL_NODE_TYPE) {
								QString lexical = it->getNodeString(2);
								++mDataAssertionCount;
								if (name == "startedAtTime") {
									addTimeValue(subject, true, lexical);
								} else if (name == "endedAtTime") {
									addTimeValue(subject, false, lexical);
								} else if (name == "inXSDDateTimeStamp" || name == "inXSDDateTime" || name == "inXSDDate") {
									mInstantTimeText.insert(subject, lexical);
								}
							}
						}
						it->moveNext();
					}
					delete it;
				}
			}


			void CIntervalOntologyAnalyser::resolveInstants() {
				for (QHash<QString,QString>::const_iterator it = mBeginningOf.constBegin(); it != mBeginningOf.constEnd(); ++it) {
					QHash<QString,QString>::const_iterator t = mInstantTimeText.constFind(it.value());
					if (t != mInstantTimeText.constEnd()) { addTimeValue(it.key(), true, t.value()); }
				}
				for (QHash<QString,QString>::const_iterator it = mEndOf.constBegin(); it != mEndOf.constEnd(); ++it) {
					QHash<QString,QString>::const_iterator t = mInstantTimeText.constFind(it.value());
					if (t != mInstantTimeText.constEnd()) { addTimeValue(it.key(), false, t.value()); }
				}
			}


			QString CIntervalOntologyAnalyser::describeInterval(int node) const {
				const CNode& n = mNodes.at(node);
				return QString("%1 [%2 .. %3]").arg(localName(n.mName)).arg(n.mStartText).arg(n.mEndText);
			}


			QString CIntervalOntologyAnalyser::describeUnit(const CUnit& unit) const {
				if (unit.mTimestamp) {
					return QString("time stamps: ") + describeInterval(unit.mNode);
				}
				return QString("asserted: %1 %2 %3").arg(localName(mNodes.at(unit.mSubject).mName)).arg(QString::fromLatin1(CIntervalAlgebra::name(unit.mRelation))).arg(localName(mNodes.at(unit.mObject).mName));
			}


			// runs path consistency on one component with the active units; closure (if given) receives the refined matrix (indexed like nodes)
			bool CIntervalOntologyAnalyser::analyseComponent(const QVector<int>& nodes, const QList<CUnit>& units, const QVector<bool>& active, QVector<quint16>* closure, int* conflictI, int* conflictJ) {
				int n = nodes.size();
				QHash<int,int> index;
				for (int i = 0; i < n; ++i) { index.insert(nodes[i], i); }
				QVector<quint16> C(n * n, CIntervalAlgebra::FULL_MASK);
				for (int i = 0; i < n; ++i) { C[i * n + i] = CIntervalAlgebra::bit(CIntervalAlgebra::EQUALS); }
				QList<int> timedActive;
				for (int u = 0; u < units.size(); ++u) {
					if (!active[u]) { continue; }
					const CUnit& unit = units.at(u);
					if (unit.mTimestamp) {
						timedActive.append(unit.mNode);
					} else {
						int i = index.value(unit.mSubject), j = index.value(unit.mObject);
						quint16 m = CIntervalAlgebra::maskOf(unit.mRelation);
						C[i * n + j] &= m;
						C[j * n + i] &= CIntervalAlgebra::inverseMask(m);
						if (C[i * n + j] == 0) {
							if (conflictI) { *conflictI = i; }
							if (conflictJ) { *conflictJ = j; }
							return false;
						}
					}
				}
				for (int a = 0; a < timedActive.size(); ++a) {
					for (int b = a + 1; b < timedActive.size(); ++b) {
						const CNode& x = mNodes.at(timedActive[a]);
						const CNode& y = mNodes.at(timedActive[b]);
						int i = index.value(timedActive[a]), j = index.value(timedActive[b]);
						quint16 m = CIntervalAlgebra::bit(CIntervalAlgebra::relation(x.mStart, x.mEnd, y.mStart, y.mEnd));
						C[i * n + j] &= m;
						C[j * n + i] &= CIntervalAlgebra::inverseMask(m);
						if (C[i * n + j] == 0) {
							if (conflictI) { *conflictI = i; }
							if (conflictJ) { *conflictJ = j; }
							return false;
						}
					}
				}
				bool consistent = CIntervalAlgebra::pathConsistency(n, C, conflictI, conflictJ);
				if (consistent && closure) {
					*closure = C;
				}
				return consistent;
			}


			QStringList CIntervalOntologyAnalyser::createReport() {
				QStringList out;
				mDerived.clear();
				mConflictCount = 0;

				scanAxioms();
				scanTriples();
				resolveInstants();

				// usable intervals
				int timedCount = 0;
				for (int i = 0; i < mNodes.size(); ++i) {
					CNode& node = mNodes[i];
					if (node.mHasStart && node.mHasEnd) {
						if (node.mStart < node.mEnd) {
							node.mTimed = true;
							++timedCount;
						} else {
							mIgnoredIntervals.append(QString("%1 [%2 .. %3]").arg(localName(node.mName)).arg(node.mStartText).arg(node.mEndText));
						}
					}
				}

				mTimedCount = timedCount;

				out.append("Ontology: " + mFileName);
				out.append(QString("Interval theory: %1 individuals with usable time stamps, %2 asserted interval relations (%3 object property assertions and %4 data property assertions read)")
						.arg(timedCount).arg(mAsserted.size()).arg(mObjectAssertionCount).arg(mDataAssertionCount));
				if (mUnreadableTimes > 0) {
					out.append(QString("[WARN] %1 time value(s) could not be read as a date/time or a number, for example: %2").arg(mUnreadableTimes).arg(mUnreadableExamples.join(" | ")));
				}
				for (const QString& ignored : mIgnoredIntervals) {
					out.append("[WARN] interval ignored (the end is not after the start): " + ignored);
				}

				// connected components of the asserted relations
				int N = mNodes.size();
				QVector<int> parent(N);
				for (int i = 0; i < N; ++i) { parent[i] = i; }
				std::function<int(int)> find = [&](int x) -> int { while (parent[x] != x) { parent[x] = parent[parent[x]]; x = parent[x]; } return x; };
				for (const CAssertedRelation& r : mAsserted) {
					int a = find(r.mSubject), b = find(r.mObject);
					if (a != b) { parent[a] = b; }
				}
				QHash<int, QVector<int> > components;
				QSet<int> inRelation;
				for (const CAssertedRelation& r : mAsserted) { inRelation.insert(r.mSubject); inRelation.insert(r.mObject); }
				for (int node : inRelation) { components[find(node)].append(node); }
				// timed nodes without an asserted relation that are not in any component are only used for the derivation from time stamps

				QSet<QPair<int,int> > assertedPairs;
				for (const CAssertedRelation& r : mAsserted) {
					assertedPairs.insert(qMakePair(r.mSubject, r.mObject));
				}

				QList<int> componentRoots = components.keys();
				std::sort(componentRoots.begin(), componentRoots.end());
				int analysed = 0, skipped = 0;
				for (int root : componentRoots) {
					QVector<int> nodes = components.value(root);
					std::sort(nodes.begin(), nodes.end(), [this](int a, int b) { return mNodes.at(a).mName < mNodes.at(b).mName; });
					QList<CUnit> units;
					for (const CAssertedRelation& r : mAsserted) {
						if (find(r.mSubject) == root) {
							CUnit u; u.mTimestamp = false; u.mNode = -1; u.mSubject = r.mSubject; u.mObject = r.mObject; u.mRelation = r.mRelation;
							units.append(u);
						}
					}
					for (int node : nodes) {
						if (mNodes.at(node).mTimed) {
							CUnit u; u.mTimestamp = true; u.mNode = node; u.mSubject = u.mObject = u.mRelation = -1;
							units.append(u);
						}
					}
					if (nodes.size() > mMaxComponentSize) {
						++skipped;
						out.append(QString("[WARN] a group of %1 individuals with interval relations is larger than the limit of %2 and was only checked against the time stamps pair by pair (option Konclude.CLI.Interval.MaxComponentSize)").arg(nodes.size()).arg(mMaxComponentSize));
						for (const CAssertedRelation& r : mAsserted) {
							if (find(r.mSubject) != root) { continue; }
							const CNode& x = mNodes.at(r.mSubject); const CNode& y = mNodes.at(r.mObject);
							if (x.mTimed && y.mTimed && (CIntervalAlgebra::maskOf(r.mRelation) & CIntervalAlgebra::bit(CIntervalAlgebra::relation(x.mStart, x.mEnd, y.mStart, y.mEnd))) == 0) {
								++mConflictCount;
								out.append(QString("[CONFLICT %1] asserted %2 %3 %4 contradicts the time stamps (%5 and %6)").arg(mConflictCount).arg(localName(x.mName)).arg(QString::fromLatin1(CIntervalAlgebra::name(r.mRelation))).arg(localName(y.mName)).arg(describeInterval(r.mSubject)).arg(describeInterval(r.mObject)));
							}
						}
						continue;
					}
					++analysed;
					QVector<bool> active(units.size(), true);
					QVector<quint16> closure;
					int ci = -1, cj = -1;
					if (analyseComponent(nodes, units, active, &closure, &ci, &cj)) {
						// derived relations of the composition: singleton relations of pairs without an asserted relation, at least one without time stamps
						int n = nodes.size();
						for (int i = 0; i < n; ++i) {
							for (int j = i + 1; j < n; ++j) {
								const CNode& x = mNodes.at(nodes[i]); const CNode& y = mNodes.at(nodes[j]);
								quint16 m = closure[i * n + j];
								if (m == 0 || (m & (m - 1)) != 0) { continue; }
								if (x.mTimed && y.mTimed) { continue; }
								if (assertedPairs.contains(qMakePair(nodes[i], nodes[j])) || assertedPairs.contains(qMakePair(nodes[j], nodes[i]))) { continue; }
								int rel = 0;
								while (!(m & CIntervalAlgebra::bit(rel))) { ++rel; }
								CDerived d; d.mSubject = nodes[i]; d.mObject = nodes[j]; d.mRelation = rel; d.mFromTimestamps = false;
								mDerived.append(d);
							}
						}
					} else {
						++mConflictCount;
						QStringList lines;
						if (nodes.size() <= mMaxMinimizationSize) {
							// deletion based minimisation: a unit that can be removed without making the network consistent is not needed
							for (int u = 0; u < units.size(); ++u) {
								active[u] = false;
								int a, b;
								if (analyseComponent(nodes, units, active, nullptr, &a, &b)) {
									active[u] = true;
								}
							}
							for (int u = 0; u < units.size(); ++u) {
								if (active[u]) { lines.append("       " + describeUnit(units.at(u))); }
							}
							out.append(QString("[CONFLICT %1] %2 facts contradict each other (a minimal set; the empty constraint arose between %3 and %4):").arg(mConflictCount).arg(lines.size()).arg(localName(mNodes.at(nodes[ci]).mName)).arg(localName(mNodes.at(nodes[cj]).mName)));
							out += lines;
						} else {
							out.append(QString("[CONFLICT %1] a group of %2 individuals is inconsistent (the empty constraint arose between %3 and %4); it is larger than the limit of %5 for the search of a minimal set")
									.arg(mConflictCount).arg(nodes.size()).arg(localName(mNodes.at(nodes[ci]).mName)).arg(localName(mNodes.at(nodes[cj]).mName)).arg(mMaxMinimizationSize));
						}
					}
				}

				// derivation from the time stamps
				qint64 fromTimestamps = 0;
				bool deriveNow = mDeriveFromTimestamps && (!mDeriveOnlyIfDeclared || mVocabularyDeclared);
				bool useGroups = !mGroupByProperty.isEmpty() || mGroupByConnectivity;
				if (deriveNow && useGroups && mGroupByProperty.isEmpty() && timedCount > 1) {
					scanAnyEdges();
				}
				if (deriveNow && timedCount > 1) {
					int N2 = mNodes.size();
					QVector<int> gparent(N2);
					for (int i = 0; i < N2; ++i) { gparent[i] = i; }
					std::function<int(int)> gfind = [&](int x) -> int { while (gparent[x] != x) { gparent[x] = gparent[gparent[x]]; x = gparent[x]; } return x; };
					if (useGroups) {
						for (const QPair<int,int>& e : mGroupEdges) {
							int a = gfind(e.first), b = gfind(e.second);
							if (a != b) { gparent[a] = b; }
						}
					}
					QHash<int, QVector<int> > groups;
					for (int i = 0; i < N2; ++i) {
						if (mNodes.at(i).mTimed) { groups[useGroups ? gfind(i) : 0].append(i); }
					}
					qint64 pairs = 0;
					for (QHash<int, QVector<int> >::const_iterator it = groups.constBegin(); it != groups.constEnd(); ++it) {
						qint64 k = it.value().size();
						pairs += k * (k - 1) / 2;
					}
					if (pairs > mMaxDerivedPairs) {
						out.append(QString("[WARN] %1 pairs of intervals with time stamps would have to be derived, more than the limit of %2: nothing derived from the time stamps (set Konclude.CLI.Interval.GroupByProperty to a property that links the intervals of one process, or raise Konclude.CLI.Interval.MaxDerivedPairs)").arg(pairs).arg(mMaxDerivedPairs));
					} else {
						for (QHash<int, QVector<int> >::const_iterator it = groups.constBegin(); it != groups.constEnd(); ++it) {
							QVector<int> members = it.value();
							std::sort(members.begin(), members.end(), [this](int a, int b) { return mNodes.at(a).mName < mNodes.at(b).mName; });
							for (int a = 0; a < members.size(); ++a) {
								for (int b = a + 1; b < members.size(); ++b) {
									const CNode& x = mNodes.at(members[a]); const CNode& y = mNodes.at(members[b]);
									if (assertedPairs.contains(qMakePair(members[a], members[b])) || assertedPairs.contains(qMakePair(members[b], members[a]))) { continue; }
									CDerived d; d.mSubject = members[a]; d.mObject = members[b]; d.mFromTimestamps = true;
									d.mRelation = CIntervalAlgebra::relation(x.mStart, x.mEnd, y.mStart, y.mEnd);
									mDerived.append(d);
									++fromTimestamps;
								}
							}
						}
					}
				}

				out.append(QString("[INFO] %1 group(s) of individuals with asserted interval relations checked (%2 too large for the full check); derived relations: %3 from time stamps, %4 from the composition of asserted relations")
						.arg(analysed).arg(skipped).arg(fromTimestamps).arg(mDerived.size() - fromTimestamps));
				if (mConflictCount == 0) {
					out.append("Interval result: consistent");
				} else {
					out.append(QString("Interval result: inconsistent (%1 conflict%2)").arg(mConflictCount).arg(mConflictCount == 1 ? "" : "s"));
				}
				return out;
			}


			QString CIntervalOntologyAnalyser::derivedTriples() const {
				QStringList lines;
				for (const CDerived& d : mDerived) {
					// the relation properties of the ontology are used if it has them (a property for the relation itself, else the one of its inverse with swapped individuals),
					// otherwise the configured vocabulary
					QString subject = mNodes.at(d.mSubject).mName, object = mNodes.at(d.mObject).mName, property;
					if (!mRelationIri[d.mRelation].isEmpty()) {
						property = mRelationIri[d.mRelation];
					} else if (!mRelationIri[CIntervalAlgebra::inverse(d.mRelation)].isEmpty()) {
						property = mRelationIri[CIntervalAlgebra::inverse(d.mRelation)];
						qSwap(subject, object);
					} else {
						property = mOutputNamespace + CIntervalAlgebra::localNameFor(d.mRelation, mOutputOwlTimeStyle);
					}
					lines.append(QString("<%1> <%2> <%3> .").arg(subject).arg(property).arg(object));
				}
				lines.sort();
				return lines.isEmpty() ? QString() : lines.join("\n") + "\n";
			}


		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
