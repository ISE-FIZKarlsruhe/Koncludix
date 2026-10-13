/*
 *		Copyright (C) 2013-2015, 2019 by the Konclude Developer Team.
 *
 *		This file is part of the reasoning system Konclude.
 *		For details and support, see <http://konclude.com/>.
 *
 *		Konclude is free software: you can redistribute it and/or modify
 *		it under the terms of version 3 of the GNU Lesser General Public
 *		License (LGPLv3) as published by the Free Software Foundation.
 *
 *		Konclude is distributed in the hope that it will be useful,
 *		but WITHOUT ANY WARRANTY; without even the implied warranty of
 *		MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *		GNU (Lesser) General Public License for more details.
 *
 *		You should have received a copy of the GNU (Lesser) General Public
 *		License along with Konclude. If not, see <http://www.gnu.org/licenses/>.
 *
 */

#include "CLintOntologyAnalyser.h"

#include <algorithm>

#include "Parser/CLintExpressionRenderer.h"

#include "Parser/Expressions/CClassExpression.h"
#include "Parser/Expressions/CObjectPropertyExpression.h"
#include "Parser/Expressions/CDataPropertyExpression.h"
#include "Parser/Expressions/CNamedIndividualExpression.h"
#include "Parser/Expressions/CAnonymousIndividualExpression.h"
#include "Parser/Expressions/CDatatypeExpression.h"
#include "Parser/Expressions/CDataLexicalValueExpression.h"
#include "Parser/Expressions/CDataLiteralExpression.h"
#include "Parser/Expressions/CInverseObjectPropertyOfExpression.h"
#include "Parser/Expressions/CObjectIntersectionOfExpression.h"
#include "Parser/Expressions/CObjectUnionOfExpression.h"
#include "Parser/Expressions/CObjectOneOfExpression.h"
#include "Parser/Expressions/CObjectCardinalityExpression.h"
#include "Parser/Expressions/CObjectHasSelfExpression.h"
#include "Parser/Expressions/CDataCardinalityExpression.h"
#include "Parser/Expressions/CSubClassOfExpression.h"
#include "Parser/Expressions/CEquivalentClassesExpression.h"
#include "Parser/Expressions/CDisjointUnionExpression.h"
#include "Parser/Expressions/CSubObjectPropertyOfExpression.h"
#include "Parser/Expressions/CInverseObjectPropertiesExpression.h"
#include "Parser/Expressions/CEquivalentObjectPropertiesExpression.h"
#include "Parser/Expressions/CTransetiveObjectPropertyExpression.h"
#include "Parser/Expressions/CSymmetricObjectPropertyExpression.h"
#include "Parser/Expressions/CClassAssertionExpression.h"
#include "Parser/Expressions/CObjectPropertyAssertionExpression.h"
#include "Parser/Expressions/CDataPropertyAssertionExpression.h"
#include "Parser/Expressions/CDeclarationAxiomExpression.h"
#include "Parser/Expressions/CSubExpressionVisitor.h"

#include "Reasoner/Triples/CTriplesData.h"
#include "Reasoner/Triples/CTriplesDataIterator.h"
#include "Reasoner/Ontology/COntologyTriplesData.h"
#include "Reasoner/Ontology/COntologyBuildData.h"

namespace Konclude {

	using namespace Reasoner::Triples;

	namespace Control {

		namespace Loader {

			namespace {

				static const QString OWL_NS = "http://www.w3.org/2002/07/owl#";
				static const QString RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
				static const QString RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#";
				static const QString RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
				static const QString OWL_THING = "http://www.w3.org/2002/07/owl#Thing";
				static const QString OWL_NAMED_INDIVIDUAL = "http://www.w3.org/2002/07/owl#NamedIndividual";

				bool isVocabulary(const QString& iri) {
					return iri.startsWith(OWL_NS) || iri.startsWith(RDF_NS) || iri.startsWith(RDFS_NS);
				}


				bool serialLess(CAxiomExpression* a, CAxiomExpression* b) {
					return a->getSerial() < b->getSerial();
				}


				bool isThingClass(CClassTermExpression* expression) {
					return expression && expression->getType() == CBuildExpression::BETCLASS && CLintExpressionRenderer::entityName(expression) == OWL_THING;
				}


				//! the conjuncts of an expression with nested intersections flattened (OWL API: asConjunctSet)
				void collectConjuncts(CClassTermExpression* expression, QList<CClassTermExpression*>& conjuncts) {
					if (expression->getType() == CBuildExpression::BETOBJECTINTERSECTION) {
						CObjectIntersectionOfExpression* intersection = dynamic_cast<CObjectIntersectionOfExpression*>(expression);
						if (intersection && intersection->getClassTermExpressionList()) {
							for (CClassTermExpression* operand : *intersection->getClassTermExpressionList()) {
								collectConjuncts(operand, conjuncts);
							}
							return;
						}
					}
					conjuncts.append(expression);
				}

			}



			CLintOntologyAnalyser::CLintOntologyAnalyser(CConcreteOntology* ontology, const QString& fileName) {
				mOntology = ontology;
				mFileName = fileName;
				mNominals = false;
				mInverse = false;
				mAnyCard = false;
				mMaxCard = 0;
				mAxiomCount = 0;
				mClassAssertions = 0;
				mObjectPropertyAssertions = 0;
				mDataPropertyAssertions = 0;
				mWarningCount = 0;
				mTriplesAbox = false;
				mKeepAllValues = false;
			}


			QString CLintOntologyAnalyser::shortName(const QString& iri) {
				int i = qMax(iri.lastIndexOf('#'), iri.lastIndexOf('/'));
				return i >= 0 ? iri.mid(i + 1) : iri;
			}


			QString CLintOntologyAnalyser::render(CBuildExpression* expression) {
				return CLintExpressionRenderer::render(expression);
			}


			void CLintOntologyAnalyser::warn(QStringList& out, const QString& title, const QString& detail, const QStringList& examples) {
				warn(out, title, detail, examples, QString());
			}

			void CLintOntologyAnalyser::warn(QStringList& out, const QString& title, const QString& detail, const QStringList& examples, const QString& suggestion) {
				++mWarningCount;
				out.append("[WARN] " + title);
				out.append("       " + detail);
				if (!suggestion.isEmpty()) {
					out.append("       Suggestion: " + suggestion);
				}
				int count = 0;
				CEntry entry;
				entry.mLevel = "warning";
				entry.mTitle = title;
				entry.mDetail = detail;
				entry.mSuggestion = suggestion;
				for (const QString& e : examples) {
					if (++count > 3) {
						break;
					}
					QString text = (e.length() > 170 ? e.left(170) + "..." : e);
					out.append("       e.g. " + text);
					entry.mExamples.append(text);
				}
				mEntries.append(entry);
			}

			void CLintOntologyAnalyser::info(QStringList& out, const QString& text) {
				out.append("[INFO] " + text);
				CEntry entry;
				entry.mLevel = "info";
				entry.mTitle = text;
				mEntries.append(entry);
			}

			QString CLintOntologyAnalyser::createJsonReport() {
				auto esc = [](const QString& text) -> QString {
					QString r;
					for (const QChar& c : text) {
						ushort u = c.unicode();
						if (u == '"') { r += "\\\""; }
						else if (u == '\\') { r += "\\\\"; }
						else if (u == '\n') { r += "\\n"; }
						else if (u == '\t') { r += "\\t"; }
						else if (u < 0x20) { r += QString("\\u%1").arg(u, 4, 16, QChar('0')); }
						else { r += c; }
					}
					return r;
				};
				QString json = "{\"ontology\":\"" + esc(mFileName) + "\",\"warnings\":" + QString::number(mWarningCount) + ",\"findings\":[";
				bool first = true;
				for (const CEntry& e : mEntries) {
					if (!first) { json += ","; }
					first = false;
					json += "\n  {\"level\":\"" + e.mLevel + "\",\"title\":\"" + esc(e.mTitle) + "\"";
					if (!e.mDetail.isEmpty()) { json += ",\"detail\":\"" + esc(e.mDetail) + "\""; }
					if (!e.mSuggestion.isEmpty()) { json += ",\"suggestion\":\"" + esc(e.mSuggestion) + "\""; }
					if (!e.mExamples.isEmpty()) {
						json += ",\"examples\":[";
						for (int i = 0; i < e.mExamples.size(); ++i) {
							json += (i ? ",\"" : "\"") + esc(e.mExamples.at(i)) + "\"";
						}
						json += "]";
					}
					json += "}";
				}
				json += "\n]}\n";
				return json;
			}


			// ------------------------------------------------------------------------------------------------
			// ABox model helpers
			// ------------------------------------------------------------------------------------------------

			int CLintOntologyAnalyser::individualId(const QString& name) {
				QHash<QString,int>::const_iterator it = mModel.individualIds.constFind(name);
				if (it != mModel.individualIds.constEnd()) {
					return it.value();
				}
				int id = mModel.individualNames.size();
				mModel.individualIds.insert(name, id);
				mModel.individualNames.append(name);
				mModel.types.append(QVector<int>());
				mModel.data.append(QVector<CAboxModel::CData>());
				return id;
			}

			int CLintOntologyAnalyser::typeId(const QString& name) {
				QHash<QString,int>::const_iterator it = mModel.typeIds.constFind(name);
				if (it != mModel.typeIds.constEnd()) {
					return it.value();
				}
				int id = mModel.typeIds.size();
				mModel.typeIds.insert(name, id);
				return id;
			}

			int CLintOntologyAnalyser::roleId(const QString& name) {
				QHash<QString,int>::const_iterator it = mModel.roleIds.constFind(name);
				if (it != mModel.roleIds.constEnd()) {
					return it.value();
				}
				int id = mModel.roleNames.size();
				mModel.roleIds.insert(name, id);
				mModel.roleNames.append(name);
				return id;
			}

			void CLintOntologyAnalyser::pinIndividual(const QString& name) {
				mPinnedNames.insert(name);
			}

			void CLintOntologyAnalyser::addEdge(const QString& source, const QString& role, const QString& target) {
				int s = individualId(source), o = individualId(target);
				CAboxModel::CEdge edge;
				edge.mSource = s;
				edge.mRole = roleId(role);
				edge.mTarget = o;
				mModel.edges.append(edge);
			}

			void CLintOntologyAnalyser::addTypeAssertion(const QString& individual, const QString& type) {
				int i = individualId(individual);
				int t = typeId(type);
				QVector<int>& types = mModel.types[i];
				if (!types.contains(t)) {
					types.append(t);
				}
			}

			void CLintOntologyAnalyser::addDataAssertion(const QString& individual, const QString& property, const QString& datatype, const QString& value) {
				int i = individualId(individual);
				CAboxModel::CData d;
				d.mProperty = mModel.dataPropertyIds.value(property, -1);
				if (d.mProperty < 0) {
					d.mProperty = mModel.dataPropertyIds.size();
					mModel.dataPropertyIds.insert(property, d.mProperty);
					mModel.dataPropertyNames.append(property);
				}
				QHash<QString,int>::const_iterator dtIt = mModel.datatypeIds.constFind(datatype);
				if (dtIt == mModel.datatypeIds.constEnd()) {
					dtIt = mModel.datatypeIds.insert(datatype, mModel.datatypeIds.size());
				}
				d.mDatatype = dtIt.value();
				d.mValue = -1;
				if (mKeepAllValues || mExactDataProperties.contains(property) || mKeyDataProperties.contains(property)) {
					QHash<QString,int>::const_iterator vIt = mModel.valueIds.constFind(value);
					if (vIt == mModel.valueIds.constEnd()) {
						vIt = mModel.valueIds.insert(value, mModel.valueIds.size());
					}
					d.mValue = vIt.value();
				}
				mModel.data[i].append(d);
			}


			QString CLintOntologyAnalyser::objectPropertyName(CObjectPropertyTermExpression* property, bool* inverse) {
				bool inv = false;
				CBuildExpression* current = property;
				while (current && current->getType() == CBuildExpression::BETINVERSEOBJECTPROPERTYOF) {
					CInverseObjectPropertyOfExpression* invExp = dynamic_cast<CInverseObjectPropertyOfExpression*>(current);
					inv = !inv;
					current = invExp ? invExp->getInverseOfExpression() : nullptr;
				}
				if (inverse) {
					*inverse = inv;
				}
				return CLintExpressionRenderer::entityName(current);
			}

			QString CLintOntologyAnalyser::individualName(CIndividualTermExpression* individual, bool* anonymous) {
				if (anonymous) {
					*anonymous = false;
				}
				if (!individual) {
					return QString();
				}
				if (individual->getType() == CBuildExpression::BETANONYMOUSINDIVIDUAL) {
					if (anonymous) {
						*anonymous = true;
					}
					return "_:" + CLintExpressionRenderer::entityName(individual);
				}
				return CLintExpressionRenderer::entityName(individual);
			}


			// ------------------------------------------------------------------------------------------------
			// scanning of the axioms
			// ------------------------------------------------------------------------------------------------

			void CLintOntologyAnalyser::scanAssertionAxiom(CAxiomExpression* axiom) {
				switch (axiom->getType()) {

					case CBuildExpression::BETCLASSASSERTION: {
						CClassAssertionExpression* ca = dynamic_cast<CClassAssertionExpression*>(axiom);
						if (!ca) { break; }
						++mClassAssertions;
						CClassTermExpression* classExp = ca->getClassTermExpression();
						bool anonymousIndividual = false;
						QString ind = individualName(ca->getIndividualTermExpression(), &anonymousIndividual);
						bool complexClass = !classExp || classExp->getType() != CBuildExpression::BETCLASS;
						QString typeName;
						if (!complexClass) {
							typeName = CLintExpressionRenderer::entityName(classExp);
							mClasses.insert(typeName);
						} else {
							typeName = "X:" + render(classExp);
							// the class expression of this class assertion is analysed like the ones in the TBox
							CLintAxiomWalker walker;
							walker.visit(classExp);
							for (const QString& n : walker.mClasses) { mClasses.insert(n); }
							for (const QString& n : walker.mObjectProperties) { mObjectProperties.insert(n); }
							for (const QString& n : walker.mDataProperties) { mDataProperties.insert(n); }
							for (const QString& n : walker.mIndividuals) { mIndividuals.insert(n); pinIndividual(n); }
							if (walker.mInverseUsed) { mInverse = true; }
							scanWalkedAxiom(axiom, walker);
						}
						if (!anonymousIndividual) {
							mIndividuals.insert(ind);
							addTypeAssertion(ind, typeName);
						}
						break;
					}

					case CBuildExpression::BETOBJECTPROPERTYASSERTION: {
						CObjectPropertyAssertionExpression* pa = dynamic_cast<CObjectPropertyAssertionExpression*>(axiom);
						if (!pa) { break; }
						++mObjectPropertyAssertions;
						bool inverse = false, anonymousS = false, anonymousO = false;
						QString role = objectPropertyName(pa->getObjectPropertyTermExpression(), &inverse);
						QString s = individualName(pa->getFirstIndividualTermExpression(), &anonymousS);
						QString o = individualName(pa->getSecondIndividualTermExpression(), &anonymousO);
						mObjectProperties.insert(role);
						if (!anonymousS) { mIndividuals.insert(s); }
						if (!anonymousO) { mIndividuals.insert(o); }
						if (anonymousS || anonymousO) {
							if (!anonymousS) { pinIndividual(s); }
							if (!anonymousO) { pinIndividual(o); }
							break;
						}
						if (inverse) {
							addEdge(o, role, s);
						} else {
							addEdge(s, role, o);
							if (mTransitive.contains(role)) {
								CNamedEdge e; e.mSource = s; e.mTarget = o;
								mEdgesByRole[role].append(e);
							}
						}
						break;
					}

					case CBuildExpression::BETDATAPROPERTYASSERTION: {
						CDataPropertyAssertionExpression* da = dynamic_cast<CDataPropertyAssertionExpression*>(axiom);
						if (!da) { break; }
						++mDataPropertyAssertions;
						bool anonymousS = false;
						QString s = individualName(da->getIndividualTermExpression(), &anonymousS);
						QString property = CLintExpressionRenderer::entityName(da->getDataPropertyTermExpression());
						mDataProperties.insert(property);
						if (anonymousS) { break; }
						mIndividuals.insert(s);
						QString datatype, lexical;
						CDataLiteralExpression* literal = dynamic_cast<CDataLiteralExpression*>(da->getDataLiteralTermExpression());
						if (literal) {
							datatype = CLintExpressionRenderer::entityName(literal->getDatatypeExpression());
							CDataLexicalValueExpression* lex = literal->getDataLexicalValueExpression();
							lexical = lex ? lex->getName() : QString();
						}
						addDataAssertion(s, property, datatype, lexical);
						break;
					}

					default: {
						// negative assertions, sameAs, differentFrom: only the signature and the pinning of the individuals matters
						CLintAxiomWalker walker;
						walker.visit(axiom);
						for (const QString& n : walker.mObjectProperties) { mObjectProperties.insert(n); }
						for (const QString& n : walker.mDataProperties) { mDataProperties.insert(n); }
						for (const QString& n : walker.mIndividuals) { mIndividuals.insert(n); pinIndividual(n); individualId(n); }
						break;
					}
				}
			}


			void CLintOntologyAnalyser::scanWalkedAxiom(CAxiomExpression* axiom, CLintAxiomWalker& walker) {
				QString axiomText;		// rendered only when needed
				bool rendered = false;
				for (CClassTermExpression* ce : walker.mClassExpressions) {
					switch (ce->getType()) {
						case CBuildExpression::BETOBJECTONEOF: {
							mNominals = true;
							CObjectOneOfExpression* oneOf = dynamic_cast<CObjectOneOfExpression*>(ce);
							int k = (oneOf && oneOf->getIndividualTermExpressionList()) ? oneOf->getIndividualTermExpressionList()->size() : 0;
							if (k >= 10) {
								if (!rendered) { axiomText = render(axiom); rendered = true; }
								mWarn.wideOneOf.append(QString::number(k) + " nominals in " + axiomText);
							}
							break;
						}
						case CBuildExpression::BETOBJECTHASVALUE: {
							mNominals = true;
							if (!rendered) { axiomText = render(axiom); rendered = true; }
							mWarn.hasValue.append(axiomText);
							break;
						}
						case CBuildExpression::BETOBJECTUNION: {
							CObjectUnionOfExpression* un = dynamic_cast<CObjectUnionOfExpression*>(ce);
							int k = (un && un->getClassTermExpressionList()) ? un->getClassTermExpressionList()->size() : 0;
							if (k >= 8) {
								if (!rendered) { axiomText = render(axiom); rendered = true; }
								mWarn.wideUnionExpressions.append(QString::number(k) + " disjuncts in " + axiomText);
							}
							break;
						}
						case CBuildExpression::BETOBJECTHASSELF: {
							if (!rendered) { axiomText = render(axiom); rendered = true; }
							mWarn.selfR.append(axiomText);
							break;
						}
						case CBuildExpression::BETOBJECTMINCARDINALITY:
						case CBuildExpression::BETOBJECTMAXCARDINALITY:
						case CBuildExpression::BETOBJECTEXACTLYCARDINALITY: {
							CObjectCardinalityExpression* cr = dynamic_cast<CObjectCardinalityExpression*>(ce);
							if (!cr) { break; }
							mAnyCard = true;
							qint64 card = cr->getCardinality();
							mMaxCard = qMax(mMaxCard, card);
							if (!rendered) { axiomText = render(axiom); rendered = true; }
							if (card >= 10) { mWarn.bigCard.append(QString::number(card) + " in " + axiomText); }
							if (!isThingClass(cr->getClassTermExpression())) { mWarn.qcard.append(axiomText); }
							if (ce->getType() == CBuildExpression::BETOBJECTMINCARDINALITY && card >= 2) { mWarn.minCardN.append(axiomText); }
							break;
						}
						default: break;
					}
				}
			}


			void CLintOntologyAnalyser::scanTBoxAxiom(CAxiomExpression* axiom) {
				CLintAxiomWalker walker;
				walker.visit(axiom);

				for (const QString& n : walker.mClasses) { mClasses.insert(n); }
				for (const QString& n : walker.mObjectProperties) { mObjectProperties.insert(n); }
				for (const QString& n : walker.mDataProperties) { mDataProperties.insert(n); }
				for (const QString& n : walker.mIndividuals) { mIndividuals.insert(n); individualId(n); pinIndividual(n); }
				if (walker.mInverseUsed) { mInverse = true; }

				// ---- first loop of the Java tool: axiom level checks ----
				switch (axiom->getType()) {
					case CBuildExpression::BETINVERSEFUNCTIONALPROPERTY:
						mWarn.ifp.append(render(axiom));
						for (const QString& n : walker.mObjectProperties) { mRoleSeeds.insert(n); }
						break;
					case CBuildExpression::BETTRANSITIVEOBJECTPROPERTY:
					case CBuildExpression::BETTRANSETIVEPROPERTYOF:
						if (!walker.mObjectProperties.isEmpty()) { mTransitive.insert(walker.mObjectProperties.first()); }
						for (const QString& n : walker.mObjectProperties) { mRoleSeeds.insert(n); }
						break;
					case CBuildExpression::BETSYMMETRICPROPERTY:
						if (!walker.mObjectProperties.isEmpty()) { mSymmetric.insert(walker.mObjectProperties.first()); }
						break;
					case CBuildExpression::BETINVERSEOBJECTPROPERTIES: {
						mInverse = true;
						QStringList names = walker.mObjectProperties;
						for (const QString& x : names) { for (const QString& y : names) { if (x != y) { mRoleRelation[x].insert(y); } } }
						break;
					}
					case CBuildExpression::BETEQUIVALENTOBJECTPROPERTIES: {
						QStringList names = walker.mObjectProperties;
						for (const QString& x : names) { for (const QString& y : names) { if (x != y) { mRoleRelation[x].insert(y); } } }
						break;
					}
					case CBuildExpression::BETFUNCTIONALOBJECTPROPERTY:
					case CBuildExpression::BETASYMMETRICPROPERTY:
					case CBuildExpression::BETIRREFLEXIVEPROPERTY:
					case CBuildExpression::BETREFLEXIVEPROPERTY:
					case CBuildExpression::BETDISJOINTOBJECTPROPERTIES:
						for (const QString& n : walker.mObjectProperties) { mRoleSeeds.insert(n); }
						break;
					case CBuildExpression::BETFUNCTIONALDATAPROPERTY:
						for (const QString& n : walker.mDataProperties) { mExactDataProperties.insert(n); }
						break;
					case CBuildExpression::BETDISJOINTUNION: {
						CDisjointUnionExpression* du = dynamic_cast<CDisjointUnionExpression*>(axiom);
						int k = (du && du->getClassTermExpressionList()) ? du->getClassTermExpressionList()->size() : 0;
						if (k >= 8) { mWarn.wideUnionAxioms.append(render(axiom)); }
						break;
					}
					case CBuildExpression::BETSUBOBJECTPROPERTYOF: {
						CSubObjectPropertyOfExpression* so = dynamic_cast<CSubObjectPropertyOfExpression*>(axiom);
						if (so && so->getSubObjectPropertyTermExpressionList() && so->getSubObjectPropertyTermExpressionList()->size() > 1) {
							// property chain
							for (const QString& n : walker.mObjectProperties) { mRoleSeeds.insert(n); }
						} else if (so && so->getSubObjectPropertyTermExpressionList() && !so->getSubObjectPropertyTermExpressionList()->isEmpty()) {
							QString sub = objectPropertyName(so->getSubObjectPropertyTermExpressionList()->first());
							QString sup = objectPropertyName(so->getSuperObjectPropertyTermExpression());
							mRoleRelation[sub].insert(sup);
							mRoleRelation[sup].insert(sub);
						}
						break;
					}
					case CBuildExpression::BETESUBCLASSOF: {
						CSubClassOfExpression* sc = dynamic_cast<CSubClassOfExpression*>(axiom);
						if (!sc) { break; }
						CClassTermExpression* sub = sc->getSubClassTermExpression();
						CClassTermExpression* sup = sc->getSuperClassTermExpression();
						if (sub && sub->getType() == CBuildExpression::BETCLASS && sup && sup->getType() == CBuildExpression::BETCLASS) {
							mSubClassOf[CLintExpressionRenderer::entityName(sup)].insert(CLintExpressionRenderer::entityName(sub));
						}
							if (sub && sub->getType() == CBuildExpression::BETCLASS && sup) {
								QString subName = CLintExpressionRenderer::entityName(sub);
								if (sup->getType() == CBuildExpression::BETCLASS) { mDirectSubs[CLintExpressionRenderer::entityName(sup)].insert(subName); }
								if (sup->getType() == CBuildExpression::BETOBJECTONEOF) {
									CObjectOneOfExpression* oneOf = dynamic_cast<CObjectOneOfExpression*>(sup);
									if (oneOf && oneOf->getIndividualTermExpressionList()) { mEnumSize.insert(subName, oneOf->getIndividualTermExpressionList()->size()); }
								}
								QList<CClassTermExpression*> parts;
								collectConjuncts(sup, parts);
								for (CClassTermExpression* part : parts) {
									CObjectCardinalityExpression* cr = part->getType() == CBuildExpression::BETOBJECTMINCARDINALITY ? dynamic_cast<CObjectCardinalityExpression*>(part) : nullptr;
									if (cr && cr->getCardinality() >= 2 && cr->getClassTermExpression() && cr->getClassTermExpression()->getType() == CBuildExpression::BETCLASS) {
										CMinCard mc;
										mc.mSubject = subName;
										mc.mRole = objectPropertyName(cr->getObjectPropertyTermExpression());
										mc.mFiller = CLintExpressionRenderer::entityName(cr->getClassTermExpression());
										mc.mCardinality = cr->getCardinality();
										mc.mText = render(axiom);
										mMinCards.append(mc);
									}
								}
							}
						if (sub && sub->getType() != CBuildExpression::BETCLASS) {
							// "complex" left-hand side: not a conjunction of named classes and existential restrictions, and not an existential restriction
							bool simple = false;
							if (sub->getType() == CBuildExpression::BETOBJECTINTERSECTION) {
								QList<CClassTermExpression*> conjuncts;
								collectConjuncts(sub, conjuncts);
								simple = true;
								for (CClassTermExpression* c : conjuncts) {
									if (c->getType() != CBuildExpression::BETCLASS && c->getType() != CBuildExpression::BETOBJECTSOMEVALUEFROM) { simple = false; }
								}
							}
							if (!simple && sub->getType() != CBuildExpression::BETOBJECTSOMEVALUEFROM) {
								mWarn.complexGci.append(render(axiom));
							}
						}
						break;
					}
					case CBuildExpression::BETEQUIVALENTCLASSES: {
							CEquivalentClassesExpression* ec = dynamic_cast<CEquivalentClassesExpression*>(axiom);
							if (ec && ec->getClassTermExpressionList()) {
								QStringList named;
								int enumerationSize = -1;
								for (CClassTermExpression* operand : *ec->getClassTermExpressionList()) {
									if (operand->getType() == CBuildExpression::BETCLASS) { named.append(CLintExpressionRenderer::entityName(operand)); }
									else if (operand->getType() == CBuildExpression::BETOBJECTONEOF) {
										CObjectOneOfExpression* oneOf = dynamic_cast<CObjectOneOfExpression*>(operand);
										if (oneOf && oneOf->getIndividualTermExpressionList()) { enumerationSize = oneOf->getIndividualTermExpressionList()->size(); }
									}
								}
								if (enumerationSize >= 0) { for (const QString& n : named) { mEnumSize.insert(n, enumerationSize); } }
								if (named.size() == ec->getClassTermExpressionList()->size()) {
									for (const QString& x : named) { for (const QString& y : named) { if (x != y) { mDirectSubs[x].insert(y); } } }
								}
								// named class equivalent to a conjunction with a minimum cardinality: its members need the successors as well
								for (CClassTermExpression* operand : *ec->getClassTermExpressionList()) {
									if (operand->getType() == CBuildExpression::BETCLASS || named.isEmpty()) { continue; }
									QList<CClassTermExpression*> parts;
									collectConjuncts(operand, parts);
									for (CClassTermExpression* part : parts) {
										CObjectCardinalityExpression* cr = part->getType() == CBuildExpression::BETOBJECTMINCARDINALITY ? dynamic_cast<CObjectCardinalityExpression*>(part) : nullptr;
										if (cr && cr->getCardinality() >= 2 && cr->getClassTermExpression() && cr->getClassTermExpression()->getType() == CBuildExpression::BETCLASS) {
											for (const QString& n : named) {
												CMinCard mc;
												mc.mSubject = n;
												mc.mRole = objectPropertyName(cr->getObjectPropertyTermExpression());
												mc.mFiller = CLintExpressionRenderer::entityName(cr->getClassTermExpression());
												mc.mCardinality = cr->getCardinality();
												mc.mText = render(axiom);
												// a defined class: the realisation has to test every individual of the named conjuncts of the definition as a possible member
												for (CClassTermExpression* conj : parts) {
													if (conj->getType() == CBuildExpression::BETCLASS) { mc.mCandidateClasses.append(CLintExpressionRenderer::entityName(conj)); }
												}
												mMinCards.append(mc);
											}
										}
									}
								}
							}
						for (const QString& c : walker.mClasses) { for (const QString& d : walker.mClasses) { if (c != d) { mSubClassOf[c].insert(d); } } }
						break;
					}
					default: break;
				}

				// ---- roles and data properties that make individuals "sensitive" for the ABox reduction ----
				for (CClassTermExpression* ce : walker.mClassExpressions) {
					switch (ce->getType()) {
						case CBuildExpression::BETOBJECTMINCARDINALITY:
						case CBuildExpression::BETOBJECTMAXCARDINALITY:
						case CBuildExpression::BETOBJECTEXACTLYCARDINALITY: {
							CObjectCardinalityExpression* cr = dynamic_cast<CObjectCardinalityExpression*>(ce);
							if (cr) { mRoleSeeds.insert(objectPropertyName(cr->getObjectPropertyTermExpression())); }
							break;
						}
						case CBuildExpression::BETOBJECTHASSELF: {
							CObjectHasSelfExpression* hs = dynamic_cast<CObjectHasSelfExpression*>(ce);
							if (hs) { mRoleSeeds.insert(objectPropertyName(hs->getObjectPropertyTermExpression())); }
							break;
						}
						case CBuildExpression::BETDATASOMEVALUEFROM:
						case CBuildExpression::BETDATAALLVALUEFROM:
						case CBuildExpression::BETDATAHASVALUE:
						case CBuildExpression::BETDATAMINCARDINALITY:
						case CBuildExpression::BETDATAMAXCARDINALITY:
						case CBuildExpression::BETDATAEXACTCARDINALITY: {
							CLintAxiomWalker w;
							w.visit(ce);
							for (const QString& n : w.mDataProperties) { mExactDataProperties.insert(n); }
							break;
						}
						default: break;
					}
				}

				// ---- second loop of the Java tool: nested class expressions ----
				scanWalkedAxiom(axiom, walker);
			}


			// ------------------------------------------------------------------------------------------------
			// ABox given as RDF triples (Turtle, RDF/XML, N-Triples): the simple assertions are not turned into axiom expressions
			// ------------------------------------------------------------------------------------------------

			void CLintOntologyAnalyser::scanTriples() {
				COntologyTriplesData* triplesData = mOntology->getOntologyTriplesData();
				if (!triplesData) {
					return;
				}
				QList<CTriplesData*>* triplesList = triplesData->getAllTriplesData();
				if (!triplesList || triplesList->isEmpty()) {
					return;
				}
				mTriplesAbox = true;
				mKeepAllValues = true;
				// owl:hasKey is not turned into an axiom by the RDF mapping; the lists of key properties are collected here
				QList<QPair<QString,QString> > hasKeyTriples;						// key class node, list node
				QHash<QString,QString> listFirst, listRest;
				QHash<QString,bool> listFirstIsLiteral;
				static const QString OWL_HAS_KEY = "http://www.w3.org/2002/07/owl#hasKey";
				static const QString RDF_FIRST = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
				static const QString RDF_REST = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
				static const QString RDF_NIL = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
				for (CTriplesData* data : *triplesList) {
					CTriplesDataIterator* it = data->getTriplesDataIterator();
					if (!it) {
						continue;
					}
					while (it->hasValue()) {
						if (it->getNodeType(1) == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE) {
							QString predicate = it->getNodeString(1);
							CTriplesDataIterator::TRIPLE_NODE_TYPE subjectType = it->getNodeType(0);
							CTriplesDataIterator::TRIPLE_NODE_TYPE objectType = it->getNodeType(2);
							bool subjectNamed = subjectType == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE;
							if (predicate == OWL_HAS_KEY) {
								hasKeyTriples.append(qMakePair((subjectNamed ? QString() : QString("_:")) + it->getNodeString(0), it->getNodeString(2)));
							} else if (predicate == RDF_FIRST) {
								listFirst.insert(it->getNodeString(0), it->getNodeString(2));
							} else if (predicate == RDF_REST) {
								listRest.insert(it->getNodeString(0), it->getNodeString(2));
							} else if (predicate == RDF_TYPE) {
								if (objectType == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE) {
									QString object = it->getNodeString(2);
									QString subject = it->getNodeString(0);
									if (object == OWL_NAMED_INDIVIDUAL) {
										if (subjectNamed) { mIndividuals.insert(subject); individualId(subject); ++mAxiomCount; }
									} else if (!isVocabulary(object) || object == OWL_THING) {
										++mClassAssertions;
										++mAxiomCount;
										mClasses.insert(object);
										if (subjectNamed) {
											mIndividuals.insert(subject);
											addTypeAssertion(subject, object);
										}
									}
								}
							} else if (mObjectProperties.contains(predicate) && objectType != CTriplesDataIterator::TRIPLE_LITERAL_NODE_TYPE) {
								QString subject = it->getNodeString(0), object = it->getNodeString(2);
								bool objectNamed = objectType == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE;
								++mObjectPropertyAssertions;
								++mAxiomCount;
								if (subjectNamed) { mIndividuals.insert(subject); }
								if (objectNamed) { mIndividuals.insert(object); }
								if (subjectNamed && objectNamed) {
									addEdge(subject, predicate, object);
									if (mTransitive.contains(predicate)) {
										CNamedEdge e; e.mSource = subject; e.mTarget = object;
										mEdgesByRole[predicate].append(e);
									}
								} else {
									if (subjectNamed) { pinIndividual(subject); }
									if (objectNamed) { pinIndividual(object); }
								}
							} else if (mDataProperties.contains(predicate) && objectType == CTriplesDataIterator::TRIPLE_LITERAL_NODE_TYPE) {
								QString subject = it->getNodeString(0);
								++mDataPropertyAssertions;
								++mAxiomCount;
								if (subjectNamed) {
									mIndividuals.insert(subject);
									addDataAssertion(subject, predicate, "literal", it->getNodeString(2));
								}
							}
						}
						it->moveNext();
					}
					delete it;
				}

				// resolve the key property lists
				for (const QPair<QString,QString>& keyTriple : hasKeyTriples) {
					CLintParseRecorder::CHasKeyRecord record;
					QStringList objectKeys, dataKeys;
					QString node = keyTriple.second;
					int guard = 0;
					while (!node.isEmpty() && node != RDF_NIL && listFirst.contains(node) && ++guard < 100000) {
						QString property = listFirst.value(node);
						if (mDataProperties.contains(property) && !mObjectProperties.contains(property)) { dataKeys.append(property); } else { objectKeys.append(property); }
						node = listRest.value(node);
					}
					QString classText = keyTriple.first.startsWith("_:") ? keyTriple.first : (keyTriple.first == OWL_THING ? QString("owl:Thing") : QString("<%1>").arg(keyTriple.first));
					QString text = QString("HasKey(%1 (").arg(classText);
					for (const QString& p : objectKeys) { text += QString("<%1> ").arg(p); }
					text += ") (";
					for (const QString& p : dataKeys) { text += QString("<%1> ").arg(p); }
					text += "))";
					record.mText = text;
					if (!keyTriple.first.startsWith("_:")) { record.mClassNames.append(keyTriple.first); }
					record.mObjectProperties = objectKeys;
					record.mDataProperties = dataKeys;
					mTripleHasKeys.append(record);
				}
			}


			// ------------------------------------------------------------------------------------------------
			// checks that look at the ABox
			// ------------------------------------------------------------------------------------------------

			void CLintOntologyAnalyser::checkEnumeratedMinCardinality(QStringList& out) {
				// Sub subClassOf (min n R F) where F is an enumeration of k nominals: every individual of Sub with fewer than n asserted
				// R-neighbours needs new successors, and each of them is a k-way choice. The estimate counts the asserted neighbours.
				QStringList examples, tooSmall;
				qint64 totalMembers = 0, totalDeficient = 0, totalMissing = 0, worstChoices = 0;
				for (const CMinCard& mc : mMinCards) {
					if (!mEnumSize.contains(mc.mFiller)) {
						continue;
					}
					int k = mEnumSize.value(mc.mFiller);
					if (k < mc.mCardinality) {
						tooSmall.append(mc.mText);
					}
					QSet<QString> classes;
					QStringList work;
					if (mc.mCandidateClasses.isEmpty()) { work.append(mc.mSubject); } else { work = mc.mCandidateClasses; }
					while (!work.isEmpty()) {
						QString c = work.takeLast();
						if (classes.contains(c)) { continue; }
						classes.insert(c);
						for (const QString& sub : mDirectSubs.value(c)) { work.append(sub); }
					}
					QSet<int> typeIds;
					for (const QString& c : classes) {
						QHash<QString,int>::const_iterator it = mModel.typeIds.constFind(c);
						if (it != mModel.typeIds.constEnd()) { typeIds.insert(it.value()); }
					}
					int role = mModel.roleIds.value(mc.mRole, -1);
					QVector<int> degree(mModel.individualNames.size(), 0);
					if (role >= 0) {
						for (const CAboxModel::CEdge& e : mModel.edges) {
							if (e.mRole == role) { ++degree[e.mSource]; }
						}
					}
					qint64 members = 0, deficient = 0, missing = 0;
					for (int i = 0; i < mModel.types.size(); ++i) {
						bool member = false;
						for (int t : mModel.types.at(i)) { if (typeIds.contains(t)) { member = true; break; } }
						if (!member) { continue; }
						++members;
						if (degree.at(i) < mc.mCardinality) { ++deficient; missing += mc.mCardinality - degree.at(i); }
					}
					totalMembers += members;
					totalDeficient += deficient;
					totalMissing += missing;
					worstChoices = qMax(worstChoices, (qint64)k);
					if (members > 0 || mModel.individualNames.isEmpty()) {
						examples.append(QString("%1  [filler is an enumeration of %2 nominals; %3 of %4 individuals have fewer than %5 asserted neighbours -> about %6 new successors]")
								.arg(mc.mText).arg(k).arg(deficient).arg(members).arg(mc.mCardinality).arg(missing));
					}
				}
				if (!tooSmall.isEmpty()) {
					warn(out, "Minimum cardinality larger than the enumeration it ranges over", "The filler enumerates fewer nominals than the minimum cardinality asks for, so every individual of the restricted class makes the ontology inconsistent.", tooSmall,
							"Lower the cardinality, add nominals to the enumeration, or check that the axiom was not meant for another class.");
				}
				if (!examples.isEmpty() && (totalDeficient > 0 || mModel.individualNames.isEmpty())) {
					warn(out, "Minimum cardinality over an enumerated class",
							(mModel.individualNames.isEmpty() ? QString("Individuals that have fewer asserted neighbours than the minimum need new successors, each a choice among the nominals of the enumeration (memory grows with the number of such individuals times the choices).")
							: QString("%1 of %2 individuals need new successors (about %3 nodes in total); each is a choice among up to %4 nominals and the choices stay on the search path (this is the pattern of the hobby example: it dominated memory and time in the tests).").arg(totalDeficient).arg(totalMembers).arg(totalMissing).arg(worstChoices)),
							examples,
							"Assert the missing neighbours in the data, or give the enumeration members as instances of an ordinary class (no choice is needed then), or lower the minimum; keep the enumeration only if the class must be closed.");
				}
			}


			void CLintOntologyAnalyser::checkTransitiveRoles(QStringList& out) {
				QStringList roles = mTransitive.values();
				std::sort(roles.begin(), roles.end());
				for (const QString& r : roles) {
					const QList<CNamedEdge>& es = mEdgesByRole.value(r);
					if (es.isEmpty()) {
						continue;
					}
					// union find over the individuals of the role's edges
					QHash<QString,QString> parent;
					auto find = [&parent](const QString& x) -> QString {
						if (!parent.contains(x)) { parent.insert(x, x); }
						QString p = x;
						while (parent.value(p) != p) { p = parent.value(p); }
						QString c = x;
						while (parent.value(c) != p) { QString n = parent.value(c); parent.insert(c, p); c = n; }
						return p;
					};
					bool selfLoop = false;
					for (const CNamedEdge& e : es) {
						QString x = find(e.mSource), y = find(e.mTarget);
						if (x != y) { parent.insert(x, y); }
						if (e.mSource == e.mTarget) { selfLoop = true; }
					}
					QHash<QString,int> sizes;
					QList<QString> keys = parent.keys();
					for (const QString& k : keys) { sizes[find(k)] += 1; }
					qint64 pairs = 0;
					for (QHash<QString,int>::const_iterator sIt = sizes.constBegin(); sIt != sizes.constEnd(); ++sIt) { pairs += (qint64)sIt.value() * sIt.value(); }
					bool sym = mSymmetric.contains(r);
					if (sym && pairs > 100000) {
						warn(out, "Transitive and symmetric role '" + shortName(r) + "' with a large ABox",
								"Its closure is quadratic in the size of each connected group: about " + QString::number(pairs) + " role assertions would be materialized (about 1 ms per 1000 assertions, output file size accordingly).",
								QStringList(shortName(r) + ": " + QString::number(es.size()) + " asserted edges in " + QString::number(sizes.size()) + " groups"));
					} else if (!sym && pairs > 1000000) {
						info(out, "Transitive role '" + shortName(r) + "': closure of up to about " + QString::number(pairs) + " assertions possible");
					}
					// cycles (directed) via self loops or mutual edges
					QSet<QString> seen;
					bool cycle = selfLoop;
					for (const CNamedEdge& e : es) {
						if (seen.contains(e.mTarget + ">" + e.mSource)) { cycle = true; } else { seen.insert(e.mSource + ">" + e.mTarget); }
					}
					if (cycle) {
						warn(out, "Cycle or self loop on transitive role '" + shortName(r) + "' in the ABox",
								"With this build, materialize can give non-deterministic output (an asserted triple of the cycle is sometimes missing) and in rare cases hangs.",
								QStringList(shortName(r) + (selfLoop ? " has a self loop" : " has mutual edges")));
					}
				}
			}


			//! Port of the colour refinement of tools/AboxReduce.java (report mode): how many individuals are interchangeable with another one
			bool CLintOntologyAnalyser::estimateReduction(qint64& removable, qint64& total) {
				CAboxModel& M = mModel;
				int n = M.individualNames.size();
				total = n;

				// sensitive roles: seeds and everything related to them by sub-property / inverse / equivalence
				QSet<QString> sensitive = mRoleSeeds;
				QList<QString> work = mRoleSeeds.values();
				while (!work.isEmpty()) {
					QString r = work.takeLast();
					for (const QString& q : mRoleRelation.value(r)) {
						if (!sensitive.contains(q)) { sensitive.insert(q); work.append(q); }
					}
				}
				QVector<bool> sensitiveRole(M.roleNames.size(), false);
				for (int r = 0; r < M.roleNames.size(); ++r) { sensitiveRole[r] = sensitive.contains(M.roleNames[r]); }

				QVector<bool> pinned(n, false);
				for (const QString& name : mPinnedNames) {
					QHash<QString,int>::const_iterator it = M.individualIds.constFind(name);
					if (it != M.individualIds.constEnd()) { pinned[it.value()] = true; }
				}

				// classes with a key (and their named subclasses): their instances are never merged unless the key values are unique
				QSet<QString> keyClasses;
				QSet<QString> keyDataProps;
				bool keyHasObjectProperties = false;
				QList<QString> classWork;
				for (const CLintParseRecorder::CHasKeyRecord& hk : mHasKeyRecords) {
					for (const QString& c : hk.mClassNames) { if (!keyClasses.contains(c)) { keyClasses.insert(c); classWork.append(c); } }
					for (const QString& p : hk.mDataProperties) { keyDataProps.insert(p); }
					if (!hk.mObjectProperties.isEmpty() || !hk.mClassObjectProperties.isEmpty()) { keyHasObjectProperties = true; }
				}
				while (!classWork.isEmpty()) {
					QString c = classWork.takeLast();
					for (const QString& s : mSubClassOf.value(c)) { if (!keyClasses.contains(s)) { keyClasses.insert(s); classWork.append(s); } }
				}
				if (!keyClasses.isEmpty()) {
					QVector<int> keyClassIds;
					QHash<int,bool> isKeyType;
					for (const QString& c : keyClasses) {
						QHash<QString,int>::const_iterator it = M.typeIds.constFind(c);
						if (it != M.typeIds.constEnd()) { isKeyType.insert(it.value(), true); }
					}
					QHash<int,QString> keyTuple;
					QHash<QString,int> keyTupleCount;
					for (int i = 0; i < n; ++i) {
						bool keyed = false;
						for (int t : M.types[i]) { if (isKeyType.contains(t)) { keyed = true; } }
						if (!keyed) { continue; }
						if (keyHasObjectProperties) { pinned[i] = true; continue; }
						QStringList vals;
						for (const CAboxModel::CData& d : M.data[i]) {
							if (keyDataProps.contains(M.dataPropertyNames[d.mProperty])) { vals.append(QString("%1=%2^%3").arg(d.mProperty).arg(d.mValue).arg(d.mDatatype)); }
						}
						vals.sort();
						QString tuple = vals.join(";");
						keyTuple.insert(i, tuple);
						keyTupleCount[tuple] += 1;
					}
					for (QHash<int,QString>::const_iterator it = keyTuple.constBegin(); it != keyTuple.constEnd(); ++it) {
						if (it.value().isEmpty() || keyTupleCount.value(it.value()) > 1) { pinned[it.key()] = true; }
					}
				}

				for (const CAboxModel::CEdge& e : M.edges) {
					if (sensitiveRole[e.mRole]) { pinned[e.mSource] = true; pinned[e.mTarget] = true; }
				}

				// colour refinement
				QHash<QByteArray,int> intern;
				QVector<int> color(n, 0);
				for (int i = 0; i < n; ++i) {
					QByteArray sig;
					if (pinned[i]) {
						sig = "P:" + QByteArray::number(i);
					} else {
						QVector<int> types = M.types[i];
						std::sort(types.begin(), types.end());
						sig = "T:";
						for (int t : types) { sig += QByteArray::number(t) + ","; }
						QMap<qint64,int> shape;
						QVector<QString> exact;
						for (const CAboxModel::CData& d : M.data[i]) {
							if (mExactDataProperties.contains(M.dataPropertyNames[d.mProperty])) {
								exact.append(QString("%1:%2:%3").arg(d.mProperty).arg(d.mDatatype).arg(d.mValue));
							} else {
								shape[((qint64)d.mProperty << 20) | d.mDatatype] += 1;
							}
						}
						std::sort(exact.begin(), exact.end());
						exact.erase(std::unique(exact.begin(), exact.end()), exact.end());
						QVector<QString> exactSorted = exact;
						sig += "#D:";
						for (QMap<qint64,int>::const_iterator sIt = shape.constBegin(); sIt != shape.constEnd(); ++sIt) { sig += QByteArray::number(sIt.key()) + "x" + QByteArray::number(qMin(2, sIt.value())) + ";"; }
						sig += "#E:";
						for (const QString& x : exactSorted) { sig += x.toLatin1() + ";"; }
					}
					QHash<QByteArray,int>::const_iterator it = intern.constFind(sig);
					if (it == intern.constEnd()) { it = intern.insert(sig, intern.size()); }
					color[i] = it.value();
				}
				int classes = 0;
				{
					QSet<int> distinct;
					for (int c : color) { distinct.insert(c); }
					classes = distinct.size();
				}
				QVector<QVector<int> > outEdges(n), inEdges(n);		// edge indices
				for (int k = 0; k < M.edges.size(); ++k) {
					outEdges[M.edges[k].mSource].append(k);
					inEdges[M.edges[k].mTarget].append(k);
				}
				for (int round = 0; round < 100; ++round) {
					QVector<int> next(n, 0);
					QHash<QByteArray,int> ids;
					QVector<quint64> nb;
					for (int i = 0; i < n; ++i) {
						nb.clear();
						for (int k : outEdges[i]) { const CAboxModel::CEdge& e = M.edges[k]; nb.append(((quint64)0 << 62) | ((quint64)e.mRole << 31) | (quint64)color[e.mTarget]); }
						for (int k : inEdges[i]) { const CAboxModel::CEdge& e = M.edges[k]; nb.append(((quint64)1 << 62) | ((quint64)e.mRole << 31) | (quint64)color[e.mSource]); }
						std::sort(nb.begin(), nb.end());
						nb.erase(std::unique(nb.begin(), nb.end()), nb.end());
						QByteArray sig;
						sig.reserve(4 + nb.size() * 8);
						int own = color[i];
						sig.append((const char*)&own, sizeof(int));
						sig.append((const char*)nb.constData(), nb.size() * (int)sizeof(quint64));
						QHash<QByteArray,int>::const_iterator it = ids.constFind(sig);
						if (it == ids.constEnd()) { it = ids.insert(sig, ids.size()); }
						next[i] = it.value();
					}
					color = next;
					int n2 = ids.size();
					if (n2 == classes) { break; }
					classes = n2;
				}

				QHash<int,int> sizes;
				for (int i = 0; i < n; ++i) { if (!pinned[i]) { sizes[color[i]] += 1; } }
				removable = 0;
				for (QHash<int,int>::const_iterator it = sizes.constBegin(); it != sizes.constEnd(); ++it) { removable += it.value() - 1; }
				return true;
			}


			// ------------------------------------------------------------------------------------------------
			// the report
			// ------------------------------------------------------------------------------------------------

			void CLintOntologyAnalyser::registerHasKey(const CLintParseRecorder::CHasKeyRecord& hk) {
				mWarn.hasKey.append(hk.mText);
				mHasKeyRecords.append(hk);
				for (const QString& c : hk.mClassNames) { mClasses.insert(c); }
				for (const QString& p : hk.mObjectProperties) { mObjectProperties.insert(p); mRoleSeeds.insert(p); }
				for (const QString& p : hk.mClassObjectProperties) { mObjectProperties.insert(p); mRoleSeeds.insert(p); }
				for (const QString& p : hk.mDataProperties) { mDataProperties.insert(p); mKeyDataProperties.insert(p); }
				++mAxiomCount;
			}


			QStringList CLintOntologyAnalyser::createReport() {
				QStringList out;

				// the hasKey axioms are skipped by the parsers and only reported to the recorder
				QList<CLintParseRecorder::CHasKeyRecord> hasKeys = CLintParseRecorder::getHasKeyRecords();
				for (const CLintParseRecorder::CHasKeyRecord& hk : hasKeys) {
					registerHasKey(hk);
				}

				// all axioms in the order in which they were created while parsing
				QList<CAxiomExpression*> tbox, abox;
				CONTOLOGYAXIOMSET<CAxiomExpression*>* tellAxioms = mOntology->getBuildData()->getTellAxiomSet();
				QList<CAxiomExpression*> declarations;
				if (tellAxioms) {
					for (CAxiomExpression* axiom : *tellAxioms) {
						++mAxiomCount;
						switch (axiom->getType()) {
							case CBuildExpression::BETDECLARATION: declarations.append(axiom); break;
							case CBuildExpression::BETCLASSASSERTION:
							case CBuildExpression::BETOBJECTPROPERTYASSERTION:
							case CBuildExpression::BETDATAPROPERTYASSERTION:
							case CBuildExpression::BETNEGATIVEOBJECTPROPERTYASSERTION:
							case CBuildExpression::BETNEGATIVEDATAPROPERTYASSERTION:
							case CBuildExpression::BETSAMEINDIVIDUAL:
							case CBuildExpression::BETDIFFERENTINDIVIDUAL:
								abox.append(axiom);
								break;
							default:
								tbox.append(axiom);
								break;
						}
					}
				}
				std::sort(tbox.begin(), tbox.end(), serialLess);
				std::sort(abox.begin(), abox.end(), serialLess);
				std::sort(declarations.begin(), declarations.end(), serialLess);

				// the signature also contains everything that is only declared
				for (CAxiomExpression* axiom : declarations) {
					CDeclarationAxiomExpression* decl = dynamic_cast<CDeclarationAxiomExpression*>(axiom);
					CBuildExpression* entity = decl ? dynamic_cast<CBuildExpression*>(decl->getEntity()) : nullptr;
					if (!entity) { continue; }
					QString name = CLintExpressionRenderer::entityName(entity);
					switch (entity->getType()) {
						case CBuildExpression::BETCLASS: mClasses.insert(name); break;
						case CBuildExpression::BETOBJECTPROPERTY: mObjectProperties.insert(name); break;
						case CBuildExpression::BETDATAPROPERTY: mDataProperties.insert(name); break;
						case CBuildExpression::BETNAMEDINDIVIDUAL: mIndividuals.insert(name); individualId(name); break;
						default: break;
					}
				}

				// the TBox/RBox first (so that transitive roles etc. are known when the ABox is read), then the ABox
				for (CAxiomExpression* axiom : tbox) {
					scanTBoxAxiom(axiom);
				}
				for (CAxiomExpression* axiom : abox) {
					scanAssertionAxiom(axiom);
				}
				scanTriples();
				// keys that are only found in RDF triples
				for (const CLintParseRecorder::CHasKeyRecord& hk : mTripleHasKeys) {
					registerHasKey(hk);
				}

				qint64 inds = mIndividuals.size();
				out.append("Ontology: " + mFileName);
				out.append(QString("Axioms %1 | classes %2, object properties %3, data properties %4 | individuals %5, class assertions %6, object property assertions %7, data property assertions %8")
						.arg(mAxiomCount).arg(mClasses.size()).arg(mObjectProperties.size()).arg(mDataProperties.size()).arg(inds).arg(mClassAssertions).arg(mObjectPropertyAssertions).arg(mDataPropertyAssertions));

				info(out, QString("Features: nominals %1, inverse roles %2, number restrictions %3%4, transitive roles %5, hasKey axioms %6")
						.arg(mNominals ? "true" : "false").arg(mInverse ? "true" : "false").arg(mAnyCard ? "true" : "false")
						.arg(mAnyCard ? QString(" (largest constant %1)").arg(mMaxCard) : QString()).arg(mTransitive.size()).arg(mWarn.hasKey.size()));

				QStringList wideUnion = mWarn.wideUnionAxioms + mWarn.wideUnionExpressions;
				if (!mWarn.hasKey.isEmpty()) {
					warn(out, "hasKey is not enforced", "Konclude (this build) does not derive sameAs or inconsistencies from owl:hasKey, so a violated key goes unnoticed (InferTest: the has-key test fails; checked with a small ontology that two individuals with equal keys are not merged and a key violation with differentFrom is not reported).", mWarn.hasKey, "Check the key in the data before loading (a uniqueness test on the key properties); if the key is one object property, declare it InverseFunctional, which this build enforces.");
				}
				if (!mWarn.ifp.isEmpty()) {
					warn(out, "InverseFunctionalProperty on a large ABox", "Inverse-functional properties are enforced (individuals with the same value are merged, a violation with differentFrom is detected), but every merge needs extra work and merged individuals are expanded together; with many asserted values this can make the reasoning slow.", mWarn.ifp, "If the data has many repeated values, check uniqueness in the data pipeline instead of letting the reasoner merge individuals.");
				}
				if (!mWarn.qcard.isEmpty()) {
					warn(out, "Qualified cardinality restrictions", "Qualified (filler other than owl:Thing) number restrictions are enforced on asserted individuals (fillers are merged or a clash is found), but they create successors for every individual and add a non-deterministic choice per individual (e.g. LeisureStudent = Student and at most one takesCourse.Course); on large ABoxes this is a main source of memory use.", mWarn.qcard, "If the filler only classifies the successors, an unqualified cardinality or a sub-property with a range is cheaper; the meaning changes unless all successors have the filler class anyway.");
				}
				if (!mWarn.wideOneOf.isEmpty()) {
					warn(out, "Large nominal enumerations", "Every node that must belong to an enumeration of n nominals is a n-way decision that stays live on the search path; with thousands of such nodes this dominates memory (OWL2Bench pattern: 28 interests).", mWarn.wideOneOf, "Use an ordinary class and assert the members as instances; keep the enumeration only if the class must be closed (no other members).");
				}
				if (!wideUnion.isEmpty()) {
					warn(out, "Wide disjunctions", "Disjunctions or disjoint unions with 8 or more operands create one task per alternative at every node they apply to.", wideUnion, "A class hierarchy with DisjointClasses is cheaper than a DisjointUnion or a long union: the covering axiom forces a choice per individual, so drop it if the cover need not be complete.");
				}
				if (!mWarn.bigCard.isEmpty()) {
					warn(out, "Large cardinality constants", "Minimum cardinality restrictions create that many distinct successors per individual (unless asserted neighbours already satisfy them).", mWarn.bigCard, "Lower the constant or assert the neighbours in the data; every unsatisfied minimum creates that many successors.");
				}
				if (!mWarn.complexGci.isEmpty()) {
					warn(out, "General concept inclusions with a complex left-hand side", QString::number(mWarn.complexGci.size()) + " axiom(s) that probably cannot be absorbed; each is applied to every individual as a disjunction.", mWarn.complexGci, "Give the complex left-hand side a name (N equivalent to C) and use N subClassOf D, or apply the rule in the data pipeline; axioms that cannot be absorbed are applied to every individual as a disjunction.");
				}
				if (!mWarn.selfR.isEmpty() || (!mWarn.hasValue.isEmpty() && !mWarn.hasKey.isEmpty())) {
					warn(out, "Combination of nominals/hasValue with hasKey/Self", "The verdict was observed to depend on the axiom order for such combinations; test with a second ordering if the result matters.", mWarn.hasValue + mWarn.selfR, "Test with a second axiom order, or avoid mixing keys or Self with nominals.");
				}

				// ABox based checks
				checkTransitiveRoles(out);
				checkEnumeratedMinCardinality(out);
				if (inds > 5000) {
					warn(out, "Large ABox (" + QString::number(inds) + " individuals)", "Use one worker (-w 1): with several workers idle workers explore sibling alternatives speculatively and memory grows several times (DL-5: 2.1 GB with -w 1, over 10 GB with -w 4). The minimum-cardinality optimisation (AtLeastBackendNeighbourSatisfaction) is on by default.", QStringList(), "Run with -w 1 and look at the ABox reduction estimate below.");
				}
				if (inds > 1000 && !mWarn.minCardN.isEmpty()) {
					info(out, "Minimum cardinality restrictions with n >= 2 on a large ABox: asserted neighbours are counted instead of creating new successors (option AtLeastBackendNeighbourSatisfaction, default on).");
				}
				if (inds > 1000) {
					qint64 removable = 0, total = 0;
					if (estimateReduction(removable, total)) {
						info(out, QString("ABox reduction: %1 of %2 individuals (%3 %) are interchangeable with another one").arg(removable).arg(total).arg(100 * removable / qMax((qint64)1, total))
								+ (removable * 5 >= total ? " -> tools/materialize-reduced.sh is worth trying." : " -> little to gain (run 'AboxReduce report' for the reasons)."));
					} else {
						info(out, "ABox reduction estimate not available");
					}
				}
				out.append(mWarningCount == 0 ? QString("No problems found.") : QString("%1 warning(s).").arg(mWarningCount));
					out.append("Lint judges from the ontology alone. To measure what really takes time and memory (or why a run does not finish): python3 tools/konclude_diagnose.py <Konclude> <ontology> --task consistency|classification|realization|materialize");
				return out;
			}


		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
