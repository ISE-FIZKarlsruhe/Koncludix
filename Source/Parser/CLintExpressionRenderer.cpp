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

#include "CLintExpressionRenderer.h"

#include "Expressions/CClassExpression.h"
#include "Expressions/CObjectPropertyExpression.h"
#include "Expressions/CDataPropertyExpression.h"
#include "Expressions/CNamedIndividualExpression.h"
#include "Expressions/CAnonymousIndividualExpression.h"
#include "Expressions/CDatatypeExpression.h"
#include "Expressions/CDataLexicalValueExpression.h"
#include "Expressions/CDataLiteralExpression.h"
#include "Expressions/CDataFacetExpression.h"
#include "Expressions/CDataFacetRestrictionExpression.h"
#include "Expressions/CInverseObjectPropertyOfExpression.h"
#include "Expressions/CObjectIntersectionOfExpression.h"
#include "Expressions/CObjectUnionOfExpression.h"
#include "Expressions/CObjectComplementOfExpression.h"
#include "Expressions/CObjectOneOfExpression.h"
#include "Expressions/CObjectSomeValuesFromExpression.h"
#include "Expressions/CObjectAllValuesFromExpression.h"
#include "Expressions/CObjectHasValueExpression.h"
#include "Expressions/CObjectHasSelfExpression.h"
#include "Expressions/CObjectMinCardinalityExpression.h"
#include "Expressions/CObjectMaxCardinalityExpression.h"
#include "Expressions/CObjectExactlyCardinalityExpression.h"
#include "Expressions/CDataSomeValuesFromExpression.h"
#include "Expressions/CDataAllValuesFromExpression.h"
#include "Expressions/CDataHasValueExpression.h"
#include "Expressions/CDataMinCardinalityExpression.h"
#include "Expressions/CDataMaxCardinalityExpression.h"
#include "Expressions/CDataExactCardinalityExpression.h"
#include "Expressions/CDataIntersectionOfExpression.h"
#include "Expressions/CDataUnionOfExpression.h"
#include "Expressions/CDataComplementOfExpression.h"
#include "Expressions/CDataOneOfExpression.h"
#include "Expressions/CDatatypeRestrictionExpression.h"
#include "Expressions/CSubClassOfExpression.h"
#include "Expressions/CEquivalentClassesExpression.h"
#include "Expressions/CDisjointClassesExpression.h"
#include "Expressions/CDisjointUnionExpression.h"
#include "Expressions/CSubObjectPropertyOfExpression.h"
#include "Expressions/CObjectPropertyChainExpression.h"
#include "Expressions/CObjectPropertyDomainExpression.h"
#include "Expressions/CObjectPropertyRangeExpression.h"
#include "Expressions/CEquivalentObjectPropertiesExpression.h"
#include "Expressions/CDisjointObjectPropertiesExpression.h"
#include "Expressions/CInverseObjectPropertiesExpression.h"
#include "Expressions/CFunctionalObjectPropertyExpression.h"
#include "Expressions/CInverseFunctionalObjectPropertyExpression.h"
#include "Expressions/CTransetiveObjectPropertyExpression.h"
#include "Expressions/CSymmetricObjectPropertyExpression.h"
#include "Expressions/CAsymmetricObjectPropertyExpression.h"
#include "Expressions/CReflexiveObjectPropertyExpression.h"
#include "Expressions/CIrreflexiveObjectPropertyExpression.h"
#include "Expressions/CDataPropertyDomainExpression.h"
#include "Expressions/CDataPropertyRangeExpression.h"
#include "Expressions/CSubDataPropertyOfExpression.h"
#include "Expressions/CEquivalentDataPropertiesExpression.h"
#include "Expressions/CDisjointDataPropertiesExpression.h"
#include "Expressions/CFunctionalDataPropertyExpression.h"
#include "Expressions/CClassAssertionExpression.h"
#include "Expressions/CObjectPropertyAssertionExpression.h"
#include "Expressions/CNegativeObjectPropertyAssertionExpression.h"
#include "Expressions/CDataPropertyAssertionExpression.h"
#include "Expressions/CNegativeDataPropertyAssertionExpression.h"
#include "Expressions/CSameIndividualExpression.h"
#include "Expressions/CDifferentIndividualsExpression.h"
#include "Expressions/CDeclarationAxiomExpression.h"


namespace Konclude {

	namespace Parser {

		namespace {

			static const char* OWL_THING = "http://www.w3.org/2002/07/owl#Thing";
			static const char* OWL_NOTHING = "http://www.w3.org/2002/07/owl#Nothing";
			static const char* OWL_TOP_OBJ = "http://www.w3.org/2002/07/owl#topObjectProperty";
			static const char* OWL_BOTTOM_OBJ = "http://www.w3.org/2002/07/owl#bottomObjectProperty";
			static const char* OWL_TOP_DATA = "http://www.w3.org/2002/07/owl#topDataProperty";
			static const char* OWL_BOTTOM_DATA = "http://www.w3.org/2002/07/owl#bottomDataProperty";

			QString iriText(const QString& iri) {
				if (iri == OWL_THING) { return "owl:Thing"; }
				if (iri == OWL_NOTHING) { return "owl:Nothing"; }
				if (iri == OWL_TOP_OBJ) { return "owl:topObjectProperty"; }
				if (iri == OWL_BOTTOM_OBJ) { return "owl:bottomObjectProperty"; }
				if (iri == OWL_TOP_DATA) { return "owl:topDataProperty"; }
				if (iri == OWL_BOTTOM_DATA) { return "owl:bottomDataProperty"; }
				return QString("<%1>").arg(iri);
			}

			//! collects the direct sub expressions and entities of an expression in order
			class CChildCollector : public CSubExpressionVisitor {
				public:
					QList<CBuildExpression*> children;
					virtual bool visitSubExpression(CBuildExpression* expression, CBuildExpression* subExpression, CSubExpressionVisitor* visitor) {
						if (subExpression) { children.append(subExpression); }
						return true;
					}
					virtual bool visitEntity(CBuildExpression* expression, CExpressionEntity* entity, CSubExpressionVisitor* visitor) {
						CBuildExpression* entityExp = dynamic_cast<CBuildExpression*>(entity);
						if (entityExp && entityExp != expression) { children.append(entityExp); }
						return true;
					}
			};

			//! collects named entities of a given type below an expression
			class CEntityNameCollector : public CSubExpressionVisitor {
				public:
					CEntityNameCollector(CBuildExpression::ExpressionType entityType, QStringList* names) : mType(entityType), mNames(names) {}
					CBuildExpression::ExpressionType mType;
					QStringList* mNames;
					QSet<CBuildExpression*> mVisited;
					void visitExpression(CBuildExpression* expression) {
						if (!expression || mVisited.contains(expression)) { return; }
						mVisited.insert(expression);
						if (expression->getType() == mType) {
							QString name = CLintExpressionRenderer::entityName(expression);
							if (!name.isEmpty() && !mNames->contains(name)) { mNames->append(name); }
						}
						expression->visitSubExpressions(this);
					}
					virtual bool visitSubExpression(CBuildExpression* expression, CBuildExpression* subExpression, CSubExpressionVisitor* visitor) {
						visitExpression(subExpression);
						return true;
					}
					virtual bool visitEntity(CBuildExpression* expression, CExpressionEntity* entity, CSubExpressionVisitor* visitor) {
						visitExpression(dynamic_cast<CBuildExpression*>(entity));
						return true;
					}
			};

		}



		QString CLintExpressionRenderer::entityName(CBuildExpression* expression) {
			if (!expression) { return QString(); }
			switch (expression->getType()) {
				case CBuildExpression::BETCLASS: { CClassExpression* e = dynamic_cast<CClassExpression*>(expression); return e ? e->getName() : QString(); }
				case CBuildExpression::BETOBJECTPROPERTY: { CObjectPropertyExpression* e = dynamic_cast<CObjectPropertyExpression*>(expression); return e ? e->getName() : QString(); }
				case CBuildExpression::BETDATAPROPERTY: { CDataPropertyExpression* e = dynamic_cast<CDataPropertyExpression*>(expression); return e ? e->getName() : QString(); }
				case CBuildExpression::BETNAMEDINDIVIDUAL: { CNamedIndividualExpression* e = dynamic_cast<CNamedIndividualExpression*>(expression); return e ? e->getName() : QString(); }
				case CBuildExpression::BETANONYMOUSINDIVIDUAL: { CAnonymousIndividualExpression* e = dynamic_cast<CAnonymousIndividualExpression*>(expression); return e ? e->getName() : QString(); }
				case CBuildExpression::BETDATATYPE: { CDatatypeExpression* e = dynamic_cast<CDatatypeExpression*>(expression); return e ? e->getName() : QString(); }
				default: break;
			}
			return QString();
		}


		void CLintExpressionRenderer::collectEntityNames(CBuildExpression* expression, CBuildExpression::ExpressionType entityType, QStringList& names) {
			CEntityNameCollector collector(entityType, &names);
			collector.visitExpression(expression);
		}


		QString CLintExpressionRenderer::renderList(const QList<CBuildExpression*>& list) {
			QStringList parts;
			for (CBuildExpression* e : list) { parts.append(render(e)); }
			return parts.join(" ");
		}

		// OWL API style: every element is followed by a blank (used for "(a b )" lists)
		QString CLintExpressionRenderer::renderSpaced(const QList<CBuildExpression*>& list) {
			QString text;
			for (CBuildExpression* e : list) { text += render(e) + " "; }
			return text;
		}


		QString CLintExpressionRenderer::renderGeneric(CBuildExpression* expression) {
			CChildCollector collector;
			expression->visitSubExpressions(&collector);
			return QString("Expression%1(%2)").arg((int)expression->getType()).arg(renderList(collector.children));
		}


		template<class T> static QList<CBuildExpression*> toBuildList(CEXPRESSIONLIST<T*>* list) {
			QList<CBuildExpression*> result;
			if (list) { for (T* e : *list) { result.append(e); } }
			return result;
		}


		QString CLintExpressionRenderer::render(CBuildExpression* expression) {
			if (!expression) { return QString("null"); }
			switch (expression->getType()) {
				case CBuildExpression::BETCLASS:
				case CBuildExpression::BETOBJECTPROPERTY:
				case CBuildExpression::BETDATAPROPERTY:
				case CBuildExpression::BETNAMEDINDIVIDUAL:
				case CBuildExpression::BETDATATYPE:
					return iriText(entityName(expression));
				case CBuildExpression::BETANONYMOUSINDIVIDUAL:
					return QString("_:%1").arg(entityName(expression));
				case CBuildExpression::BETDATALEXICALVALUE: {
					CDataLexicalValueExpression* e = dynamic_cast<CDataLexicalValueExpression*>(expression);
					return QString("\"%1\"").arg(e ? e->getName() : QString());
				}
				case CBuildExpression::BETDATALITERAL: {
					CDataLiteralExpression* e = dynamic_cast<CDataLiteralExpression*>(expression);
					if (e) { return render(e->getDataLexicalValueExpression()) + "^^" + render(e->getDatatypeExpression()); }
					break;
				}
				case CBuildExpression::BETDATAFACETIRI: {
					CDataFacetExpression* e = dynamic_cast<CDataFacetExpression*>(expression);
					return e ? iriText(e->getName()) : QString();
				}
				case CBuildExpression::BETDATAFACETRESTRICION: {
					CDataFacetRestrictionExpression* e = dynamic_cast<CDataFacetRestrictionExpression*>(expression);
					if (e) { return render(e->getDataFacetExpression()) + " " + render(e->getDataLiteralExpression()); }
					break;
				}
				case CBuildExpression::BETINVERSEOBJECTPROPERTYOF: {
					CInverseObjectPropertyOfExpression* e = dynamic_cast<CInverseObjectPropertyOfExpression*>(expression);
					if (e) { return QString("ObjectInverseOf(%1)").arg(render(e->getInverseOfExpression())); }
					break;
				}
				case CBuildExpression::BETOBJECTINTERSECTION: {
					CObjectIntersectionOfExpression* e = dynamic_cast<CObjectIntersectionOfExpression*>(expression);
					if (e) { return QString("ObjectIntersectionOf(%1)").arg(renderList(toBuildList(e->getClassTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETOBJECTUNION: {
					CObjectUnionOfExpression* e = dynamic_cast<CObjectUnionOfExpression*>(expression);
					if (e) { return QString("ObjectUnionOf(%1)").arg(renderList(toBuildList(e->getClassTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETOBJECTCOMPLEMENTOF: {
					CObjectComplementOfExpression* e = dynamic_cast<CObjectComplementOfExpression*>(expression);
					if (e) { return QString("ObjectComplementOf(%1)").arg(render(e->getComplementExpression())); }
					break;
				}
				case CBuildExpression::BETOBJECTONEOF: {
					CObjectOneOfExpression* e = dynamic_cast<CObjectOneOfExpression*>(expression);
					if (e) { return QString("ObjectOneOf(%1)").arg(renderList(toBuildList(e->getIndividualTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETOBJECTSOMEVALUEFROM: {
					CObjectSomeValuesFromExpression* e = dynamic_cast<CObjectSomeValuesFromExpression*>(expression);
					if (e) { return QString("ObjectSomeValuesFrom(%1 %2)").arg(render(e->getObjectPropertyTermExpression())).arg(render(e->getClassTermExpression())); }
					break;
				}
				case CBuildExpression::BETOBJECTALLVALUEFROM: {
					CObjectAllValuesFromExpression* e = dynamic_cast<CObjectAllValuesFromExpression*>(expression);
					if (e) { return QString("ObjectAllValuesFrom(%1 %2)").arg(render(e->getObjectPropertyTermExpression())).arg(render(e->getClassTermExpression())); }
					break;
				}
				case CBuildExpression::BETOBJECTHASVALUE: {
					CObjectHasValueExpression* e = dynamic_cast<CObjectHasValueExpression*>(expression);
					if (e) { return QString("ObjectHasValue(%1 %2)").arg(render(e->getObjectPropertyTermExpression())).arg(render(e->getIndividualTermExpression())); }
					break;
				}
				case CBuildExpression::BETOBJECTHASSELF: {
					CObjectHasSelfExpression* e = dynamic_cast<CObjectHasSelfExpression*>(expression);
					if (e) { return QString("ObjectHasSelf(%1)").arg(render(e->getObjectPropertyTermExpression())); }
					break;
				}
				case CBuildExpression::BETOBJECTMINCARDINALITY:
				case CBuildExpression::BETOBJECTMAXCARDINALITY:
				case CBuildExpression::BETOBJECTEXACTLYCARDINALITY: {
					CObjectCardinalityExpression* e = dynamic_cast<CObjectCardinalityExpression*>(expression);
					if (e) {
						const char* name = expression->getType() == CBuildExpression::BETOBJECTMINCARDINALITY ? "ObjectMinCardinality" : (expression->getType() == CBuildExpression::BETOBJECTMAXCARDINALITY ? "ObjectMaxCardinality" : "ObjectExactCardinality");
						return QString("%1(%2 %3 %4)").arg(name).arg(e->getCardinality()).arg(render(e->getObjectPropertyTermExpression())).arg(render(e->getClassTermExpression()));
					}
					break;
				}
				case CBuildExpression::BETDATASOMEVALUEFROM: {
					CDataSomeValuesFromExpression* e = dynamic_cast<CDataSomeValuesFromExpression*>(expression);
					if (e) { return QString("DataSomeValuesFrom(%1 %2)").arg(render(e->getDataPropertyTermExpression())).arg(render(e->getDataRangeTermExpression())); }
					break;
				}
				case CBuildExpression::BETDATAALLVALUEFROM: {
					CDataAllValuesFromExpression* e = dynamic_cast<CDataAllValuesFromExpression*>(expression);
					if (e) { return QString("DataAllValuesFrom(%1 %2)").arg(render(e->getDataPropertyTermExpression())).arg(render(e->getDataRangeTermExpression())); }
					break;
				}
				case CBuildExpression::BETDATAHASVALUE: {
					CDataHasValueExpression* e = dynamic_cast<CDataHasValueExpression*>(expression);
					if (e) { return QString("DataHasValue(%1 %2)").arg(render(e->getDataPropertyTermExpression())).arg(render(e->getDataRangeTermExpression())); }
					break;
				}
				case CBuildExpression::BETDATAMINCARDINALITY:
				case CBuildExpression::BETDATAMAXCARDINALITY:
				case CBuildExpression::BETDATAEXACTCARDINALITY: {
					CDataCardinalityExpression* e = dynamic_cast<CDataCardinalityExpression*>(expression);
					if (e) {
						const char* name = expression->getType() == CBuildExpression::BETDATAMINCARDINALITY ? "DataMinCardinality" : (expression->getType() == CBuildExpression::BETDATAMAXCARDINALITY ? "DataMaxCardinality" : "DataExactCardinality");
						return QString("%1(%2 %3 %4)").arg(name).arg(e->getCardinality()).arg(render(e->getDataPropertyTermExpression())).arg(render(e->getDataRangeTermExpression()));
					}
					break;
				}
				case CBuildExpression::BETDATAINTERSECTIONOF: {
					CDataIntersectionOfExpression* e = dynamic_cast<CDataIntersectionOfExpression*>(expression);
					if (e) { return QString("DataIntersectionOf(%1)").arg(renderList(toBuildList(e->getDataRangeTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETDATAUNIONOF: {
					CDataUnionOfExpression* e = dynamic_cast<CDataUnionOfExpression*>(expression);
					if (e) { return QString("DataUnionOf(%1)").arg(renderList(toBuildList(e->getDataRangeTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETDATACOMPLEMENTOF: {
					CDataComplementOfExpression* e = dynamic_cast<CDataComplementOfExpression*>(expression);
					if (e) { return QString("DataComplementOf(%1)").arg(render(e->getDataRangeTermExpression())); }
					break;
				}
				case CBuildExpression::BETDATAONEOF: {
					CDataOneOfExpression* e = dynamic_cast<CDataOneOfExpression*>(expression);
					if (e) { return QString("DataOneOf(%1)").arg(renderList(toBuildList(e->getDataRangeTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETDATATYPERESTRICTION: {
					CDatatypeRestrictionExpression* e = dynamic_cast<CDatatypeRestrictionExpression*>(expression);
					if (e) { return QString("DatatypeRestriction(%1 %2)").arg(render(e->getDatatypeExpression())).arg(renderList(toBuildList(e->getDataFacetRestrictionExpressionList()))); }
					break;
				}
				case CBuildExpression::BETOBJECTPROPERTYCHAIN: {
					CObjectPropertyChainExpression* e = dynamic_cast<CObjectPropertyChainExpression*>(expression);
					if (e) { return QString("ObjectPropertyChain(%1)").arg(renderList(toBuildList(e->getChainObjectPropertyTermExpressionList()))); }
					break;
				}

				// axioms
				case CBuildExpression::BETESUBCLASSOF: {
					CSubClassOfExpression* e = dynamic_cast<CSubClassOfExpression*>(expression);
					if (e) { return QString("SubClassOf(%1 %2)").arg(render(e->getSubClassTermExpression())).arg(render(e->getSuperClassTermExpression())); }
					break;
				}
				case CBuildExpression::BETEQUIVALENTCLASSES: {
					CEquivalentClassesExpression* e = dynamic_cast<CEquivalentClassesExpression*>(expression);
					if (e) { return QString("EquivalentClasses(%1)").arg(renderSpaced(toBuildList(e->getClassTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETDISJOINTCLASSES: {
					CDisjointClassesExpression* e = dynamic_cast<CDisjointClassesExpression*>(expression);
					if (e) { return QString("DisjointClasses(%1)").arg(renderSpaced(toBuildList(e->getClassTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETDISJOINTUNION: {
					CDisjointUnionExpression* e = dynamic_cast<CDisjointUnionExpression*>(expression);
					if (e) { return QString("DisjointUnion(%1 %2)").arg(render(e->getClassExpression())).arg(renderList(toBuildList(e->getClassTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETSUBOBJECTPROPERTYOF: {
					CSubObjectPropertyOfExpression* e = dynamic_cast<CSubObjectPropertyOfExpression*>(expression);
					if (e) {
						QList<CBuildExpression*> subList = toBuildList(e->getSubObjectPropertyTermExpressionList());
						QString sub = subList.size() == 1 ? render(subList.first()) : QString("ObjectPropertyChain(%1)").arg(renderList(subList));
						return QString("SubObjectPropertyOf(%1 %2)").arg(sub).arg(render(e->getSuperObjectPropertyTermExpression()));
					}
					break;
				}
				case CBuildExpression::BETOBJECTPROPERTYDOMAIN: {
					CObjectPropertyDomainExpression* e = dynamic_cast<CObjectPropertyDomainExpression*>(expression);
					if (e) { return QString("ObjectPropertyDomain(%1 %2)").arg(render(e->getObjectPropertyTermExpression())).arg(render(e->getClassTermExpression())); }
					break;
				}
				case CBuildExpression::BETOBJECTPROPERTYRANGE: {
					CObjectPropertyRangeExpression* e = dynamic_cast<CObjectPropertyRangeExpression*>(expression);
					if (e) { return QString("ObjectPropertyRange(%1 %2)").arg(render(e->getObjectPropertyTermExpression())).arg(render(e->getClassTermExpression())); }
					break;
				}
				case CBuildExpression::BETEQUIVALENTOBJECTPROPERTIES: {
					CEquivalentObjectPropertiesExpression* e = dynamic_cast<CEquivalentObjectPropertiesExpression*>(expression);
					if (e) { return QString("EquivalentObjectProperties(%1)").arg(renderSpaced(toBuildList(e->getObjectPropertyTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETDISJOINTOBJECTPROPERTIES: {
					CDisjointObjectPropertiesExpression* e = dynamic_cast<CDisjointObjectPropertiesExpression*>(expression);
					if (e) { return QString("DisjointObjectProperties(%1)").arg(renderSpaced(toBuildList(e->getObjectPropertyTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETINVERSEOBJECTPROPERTIES: {
					CInverseObjectPropertiesExpression* e = dynamic_cast<CInverseObjectPropertiesExpression*>(expression);
					if (e) { return QString("InverseObjectProperties(%1 %2)").arg(render(e->getFirstObjectPropertyTermExpression())).arg(render(e->getSecondObjectPropertyTermExpression())); }
					break;
				}
				case CBuildExpression::BETFUNCTIONALOBJECTPROPERTY: {
					CFunctionalObjectPropertyExpression* e = dynamic_cast<CFunctionalObjectPropertyExpression*>(expression);
					if (e) { return QString("FunctionalObjectProperty(%1)").arg(render(e->getFunctionalObjectPropertyExpression())); }
					break;
				}
				case CBuildExpression::BETINVERSEFUNCTIONALPROPERTY: {
					CInverseFunctionalObjectPropertyExpression* e = dynamic_cast<CInverseFunctionalObjectPropertyExpression*>(expression);
					if (e) { return QString("InverseFunctionalObjectProperty(%1)").arg(render(e->getInverseFunctionalObjectPropertyExpression())); }
					break;
				}
				case CBuildExpression::BETTRANSITIVEOBJECTPROPERTY:
				case CBuildExpression::BETTRANSETIVEPROPERTYOF: {
					CTransetiveObjectPropertyExpression* e = dynamic_cast<CTransetiveObjectPropertyExpression*>(expression);
					if (e) { return QString("TransitiveObjectProperty(%1)").arg(render(e->getTransetivePropertyExpression())); }
					break;
				}
				case CBuildExpression::BETSYMMETRICPROPERTY: {
					CSymmetricObjectPropertyExpression* e = dynamic_cast<CSymmetricObjectPropertyExpression*>(expression);
					if (e) { return QString("SymmetricObjectProperty(%1)").arg(render(e->getSymmetricObjectPropertyExpression())); }
					break;
				}
				case CBuildExpression::BETASYMMETRICPROPERTY: {
					CAsymmetricObjectPropertyExpression* e = dynamic_cast<CAsymmetricObjectPropertyExpression*>(expression);
					if (e) { return QString("AsymmetricObjectProperty(%1)").arg(render(e->getAsymmetricObjectPropertyExpression())); }
					break;
				}
				case CBuildExpression::BETREFLEXIVEPROPERTY: {
					CReflexiveObjectPropertyExpression* e = dynamic_cast<CReflexiveObjectPropertyExpression*>(expression);
					if (e) { return QString("ReflexiveObjectProperty(%1)").arg(render(e->getReflexiveObjectPropertyExpression())); }
					break;
				}
				case CBuildExpression::BETIRREFLEXIVEPROPERTY: {
					CIrreflexiveObjectPropertyExpression* e = dynamic_cast<CIrreflexiveObjectPropertyExpression*>(expression);
					if (e) { return QString("IrreflexiveObjectProperty(%1)").arg(render(e->getIreflexiveObjectPropertyExpression())); }
					break;
				}
				case CBuildExpression::BETDATAPROPERTYDOMAIN: {
					CDataPropertyDomainExpression* e = dynamic_cast<CDataPropertyDomainExpression*>(expression);
					if (e) { return QString("DataPropertyDomain(%1 %2)").arg(render(e->getDataPropertyTermExpression())).arg(render(e->getClassTermExpression())); }
					break;
				}
				case CBuildExpression::BETDATAPROPERTYRANGE: {
					CDataPropertyRangeExpression* e = dynamic_cast<CDataPropertyRangeExpression*>(expression);
					if (e) { return QString("DataPropertyRange(%1 %2)").arg(render(e->getDataPropertyTermExpression())).arg(render(e->getDataRangeTermExpression())); }
					break;
				}
				case CBuildExpression::BETSUBDATAPROPERTYOF: {
					CSubDataPropertyOfExpression* e = dynamic_cast<CSubDataPropertyOfExpression*>(expression);
					if (e) { return QString("SubDataPropertyOf(%1 %2)").arg(render(e->getSubDataPropertyTermExpression())).arg(render(e->getSuperDataPropertyTermExpression())); }
					break;
				}
				case CBuildExpression::BETEQUIVALENTDATAPROPERTIES: {
					CEquivalentDataPropertiesExpression* e = dynamic_cast<CEquivalentDataPropertiesExpression*>(expression);
					if (e) { return QString("EquivalentDataProperties(%1)").arg(renderSpaced(toBuildList(e->getDataPropertyTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETDISJOINTDATAPROPERTIES: {
					CDisjointDataPropertiesExpression* e = dynamic_cast<CDisjointDataPropertiesExpression*>(expression);
					if (e) { return QString("DisjointDataProperties(%1)").arg(renderSpaced(toBuildList(e->getDataPropertyTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETFUNCTIONALDATAPROPERTY: {
					CFunctionalDataPropertyExpression* e = dynamic_cast<CFunctionalDataPropertyExpression*>(expression);
					if (e) { return QString("FunctionalDataProperty(%1)").arg(render(e->getFunctionalDataPropertyExpression())); }
					break;
				}
				case CBuildExpression::BETCLASSASSERTION: {
					CClassAssertionExpression* e = dynamic_cast<CClassAssertionExpression*>(expression);
					if (e) { return QString("ClassAssertion(%1 %2)").arg(render(e->getClassTermExpression())).arg(render(e->getIndividualTermExpression())); }
					break;
				}
				case CBuildExpression::BETOBJECTPROPERTYASSERTION: {
					CObjectPropertyAssertionExpression* e = dynamic_cast<CObjectPropertyAssertionExpression*>(expression);
					if (e) { return QString("ObjectPropertyAssertion(%1 %2 %3)").arg(render(e->getObjectPropertyTermExpression())).arg(render(e->getFirstIndividualTermExpression())).arg(render(e->getSecondIndividualTermExpression())); }
					break;
				}
				case CBuildExpression::BETNEGATIVEOBJECTPROPERTYASSERTION: {
					CNegativeObjectPropertyAssertionExpression* e = dynamic_cast<CNegativeObjectPropertyAssertionExpression*>(expression);
					if (e) { return QString("NegativeObjectPropertyAssertion(%1 %2 %3)").arg(render(e->getObjectPropertyTermExpression())).arg(render(e->getFirstIndividualTermExpression())).arg(render(e->getSecondIndividualTermExpression())); }
					break;
				}
				case CBuildExpression::BETDATAPROPERTYASSERTION: {
					CDataPropertyAssertionExpression* e = dynamic_cast<CDataPropertyAssertionExpression*>(expression);
					if (e) { return QString("DataPropertyAssertion(%1 %2 %3)").arg(render(e->getDataPropertyTermExpression())).arg(render(e->getIndividualTermExpression())).arg(render(e->getDataLiteralTermExpression())); }
					break;
				}
				case CBuildExpression::BETNEGATIVEDATAPROPERTYASSERTION: {
					CNegativeDataPropertyAssertionExpression* e = dynamic_cast<CNegativeDataPropertyAssertionExpression*>(expression);
					if (e) { return QString("NegativeDataPropertyAssertion(%1 %2 %3)").arg(render(e->getDataPropertyTermExpression())).arg(render(e->getIndividualTermExpression())).arg(render(e->getDataLiteralTermExpression())); }
					break;
				}
				case CBuildExpression::BETSAMEINDIVIDUAL: {
					CSameIndividualExpression* e = dynamic_cast<CSameIndividualExpression*>(expression);
					if (e) { return QString("SameIndividual(%1)").arg(renderSpaced(toBuildList(e->getIndividualTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETDIFFERENTINDIVIDUAL: {
					CDifferentIndividualsExpression* e = dynamic_cast<CDifferentIndividualsExpression*>(expression);
					if (e) { return QString("DifferentIndividuals(%1)").arg(renderSpaced(toBuildList(e->getIndividualTermExpressionList()))); }
					break;
				}
				case CBuildExpression::BETDECLARATION: {
					CDeclarationAxiomExpression* e = dynamic_cast<CDeclarationAxiomExpression*>(expression);
					CBuildExpression* entity = e ? dynamic_cast<CBuildExpression*>(e->getEntity()) : nullptr;
					if (entity) {
						const char* kind = "Entity";
						switch (entity->getType()) {
							case CBuildExpression::BETCLASS: kind = "Class"; break;
							case CBuildExpression::BETOBJECTPROPERTY: kind = "ObjectProperty"; break;
							case CBuildExpression::BETDATAPROPERTY: kind = "DataProperty"; break;
							case CBuildExpression::BETNAMEDINDIVIDUAL: kind = "NamedIndividual"; break;
							case CBuildExpression::BETDATATYPE: kind = "Datatype"; break;
							default: break;
						}
						return QString("Declaration(%1(%2))").arg(kind).arg(render(entity));
					}
					break;
				}
				default: break;
			}
			return renderGeneric(expression);
		}


	}; // end namespace Parser

}; // end namespace Konclude
