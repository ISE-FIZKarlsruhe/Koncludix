/*
 *		Konclude explanation support (Koncludix).
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

#include "COntologyFunctionalSyntaxWriter.h"

#include <algorithm>

#include "Reasoner/Ontology/COntologyBuildData.h"
#include "Reasoner/Ontology/COntologyTriplesData.h"
#include "Reasoner/Triples/CTriplesData.h"
#include "Reasoner/Triples/CTriplesDataIterator.h"
#include "Reasoner/Generator/CStringRefStringHasher.h"

#include "Parser/Expressions/CSubClassOfExpression.h"
#include "Parser/Expressions/CClassExpression.h"
#include "Parser/Expressions/CObjectPropertyExpression.h"
#include "Parser/Expressions/CObjectIntersectionOfExpression.h"
#include "Parser/Expressions/CObjectUnionOfExpression.h"
#include "Parser/Expressions/CObjectComplementOfExpression.h"
#include "Parser/Expressions/CObjectSomeValuesFromExpression.h"
#include "Parser/Expressions/CObjectAllValuesFromExpression.h"
#include "Parser/Expressions/CObjectExactlyCardinalityExpression.h"
#include "Parser/Expressions/CObjectMinCardinalityExpression.h"
#include "Parser/Expressions/CObjectMaxCardinalityExpression.h"
#include "Parser/Expressions/CObjectHasSelfExpression.h"
#include "Parser/Expressions/CObjectHasValueExpression.h"
#include "Parser/Expressions/CObjectOneOfExpression.h"
#include "Parser/Expressions/CDataSomeValuesFromExpression.h"
#include "Parser/Expressions/CDataHasValueExpression.h"
#include "Parser/Expressions/CDataAllValuesFromExpression.h"
#include "Parser/Expressions/CDataMinCardinalityExpression.h"
#include "Parser/Expressions/CDataMaxCardinalityExpression.h"
#include "Parser/Expressions/CDataExactCardinalityExpression.h"
#include "Parser/Expressions/CEquivalentClassesExpression.h"
#include "Parser/Expressions/CDisjointUnionExpression.h"
#include "Parser/Expressions/CDisjointClassesExpression.h"
#include "Parser/Expressions/CSubObjectPropertyOfExpression.h"
#include "Parser/Expressions/CTransetiveObjectPropertyExpression.h"
#include "Parser/Expressions/CFunctionalObjectPropertyExpression.h"
#include "Parser/Expressions/CObjectPropertyDomainExpression.h"
#include "Parser/Expressions/CObjectPropertyRangeExpression.h"
#include "Parser/Expressions/CInverseObjectPropertiesExpression.h"
#include "Parser/Expressions/CEquivalentObjectPropertiesExpression.h"
#include "Parser/Expressions/CSymmetricObjectPropertyExpression.h"
#include "Parser/Expressions/CAsymmetricObjectPropertyExpression.h"
#include "Parser/Expressions/CDisjointObjectPropertiesExpression.h"
#include "Parser/Expressions/CReflexiveObjectPropertyExpression.h"
#include "Parser/Expressions/CIrreflexiveObjectPropertyExpression.h"
#include "Parser/Expressions/CInverseFunctionalObjectPropertyExpression.h"
#include "Parser/Expressions/CObjectPropertyChainExpression.h"
#include "Parser/Expressions/CInverseObjectPropertyOfExpression.h"
#include "Parser/Expressions/CDataPropertyRangeExpression.h"
#include "Parser/Expressions/CDataPropertyDomainExpression.h"
#include "Parser/Expressions/CDataPropertyExpression.h"
#include "Parser/Expressions/CSubDataPropertyOfExpression.h"
#include "Parser/Expressions/CDisjointDataPropertiesExpression.h"
#include "Parser/Expressions/CEquivalentDataPropertiesExpression.h"
#include "Parser/Expressions/CFunctionalDataPropertyExpression.h"
#include "Parser/Expressions/CDataPropertyAssertionExpression.h"
#include "Parser/Expressions/CNegativeDataPropertyAssertionExpression.h"
#include "Parser/Expressions/CObjectPropertyAssertionExpression.h"
#include "Parser/Expressions/CNegativeObjectPropertyAssertionExpression.h"
#include "Parser/Expressions/CClassAssertionExpression.h"
#include "Parser/Expressions/CSameIndividualExpression.h"
#include "Parser/Expressions/CDifferentIndividualsExpression.h"
#include "Parser/Expressions/CDataIntersectionOfExpression.h"
#include "Parser/Expressions/CDataUnionOfExpression.h"
#include "Parser/Expressions/CDataComplementOfExpression.h"
#include "Parser/Expressions/CDataOneOfExpression.h"
#include "Parser/Expressions/CDatatypeRestrictionExpression.h"
#include "Parser/Expressions/CDatatypeExpression.h"
#include "Parser/Expressions/CDataFacetRestrictionExpression.h"
#include "Parser/Expressions/CDataFacetExpression.h"
#include "Parser/Expressions/CDataLexicalValueExpression.h"
#include "Parser/Expressions/CDeclarationAxiomExpression.h"
#include "Parser/Expressions/CNamedIndividualExpression.h"
#include "Parser/Expressions/CAnonymousIndividualExpression.h"


namespace Konclude {

	using namespace Reasoner::Generator;
	using namespace Reasoner::Triples;

	namespace Renderer {


		COntologyFunctionalSyntaxWriter::COntologyFunctionalSyntaxWriter() {
		}


		const QString& COntologyFunctionalSyntaxWriter::getError() const {
			return mError;
		}


		bool COntologyFunctionalSyntaxWriter::failure(const QString& message) {
			if (mError.isEmpty()) {
				mError = message;
			}
			return false;
		}


		QString COntologyFunctionalSyntaxWriter::iri(const QString& name) {
			return QString("<") + name + QString(">");
		}


		QString COntologyFunctionalSyntaxWriter::literal(const QString& lexicalValue, const QString& datatypeIri) {
			// Konclude's functional syntax parser keeps the escape sequences of a quoted string (backslash followed by a quote or
			// a backslash) as part of the lexical value, the other parsers deliver the unescaped text. Existing escape sequences are
			// therefore copied unchanged, everything else that would end the string or start an escape sequence is escaped, so that
			// Konclude reads back exactly the same value.
			QString escaped;
			escaped.reserve(lexicalValue.size() + 8);
			for (int i = 0; i < lexicalValue.size(); ++i) {
				QChar c = lexicalValue.at(i);
				if (c == QChar('\\')) {
					QChar next = i + 1 < lexicalValue.size() ? lexicalValue.at(i + 1) : QChar();
					if (next == QChar('"') || next == QChar('\\') || next == QChar('\'')) {
						escaped += c;
						escaped += next;
						++i;
					} else {
						escaped += QString("\\\\");
					}
				} else if (c == QChar('"')) {
					escaped += QString("\\\"");
				} else {
					escaped += c;
				}
			}
			return QString("\"") + escaped + QString("\"^^") + iri(datatypeIri);
		}


		QString COntologyFunctionalSyntaxWriter::anonymousIndividual(const QString& nodeLabel) {
			QString label;
			for (QChar c : nodeLabel) {
				if (c.isLetterOrNumber() && c.unicode() < 128 || c == QChar('_') || c == QChar('-')) {
					label += c;
				} else {
					label += QString("_x%1_").arg((int)c.unicode(), 0, 16);
				}
			}
			if (label.startsWith("_:")) {
				label = label.mid(2);
			}
			return QString("_:") + label;
		}



		QString COntologyFunctionalSyntaxWriter::writeIndividual(CIndividualTermExpression* exp) {
			if (CAnonymousIndividualExpression* anon = dynamic_cast<CAnonymousIndividualExpression*>(exp)) {
				QString text = anonymousIndividual(anon->getNodeName());
				mCurrentIndividuals.append(text);
				return text;
			}
			if (CNamedIndividualExpression* named = dynamic_cast<CNamedIndividualExpression*>(exp)) {
				mCurrentIndividuals.append(named->getName());
				return iri(named->getName());
			}
			failure(QString("unsupported individual expression (type %1)").arg(exp ? (int)exp->getType() : -1));
			return QString();
		}


		QString COntologyFunctionalSyntaxWriter::writeIndividualList(CEXPRESSIONLIST<CIndividualTermExpression*>* list) {
			QStringList parts;
			for (CIndividualTermExpression* e : *list) {
				parts.append(writeIndividual(e));
			}
			return parts.join(" ");
		}

		QString COntologyFunctionalSyntaxWriter::writeClassList(CEXPRESSIONLIST<CClassTermExpression*>* list) {
			QStringList parts;
			for (CClassTermExpression* e : *list) {
				parts.append(writeClass(e));
			}
			return parts.join(" ");
		}

		QString COntologyFunctionalSyntaxWriter::writeObjectPropertyList(CEXPRESSIONLIST<CObjectPropertyTermExpression*>* list) {
			QStringList parts;
			for (CObjectPropertyTermExpression* e : *list) {
				parts.append(writeObjectProperty(e));
			}
			return parts.join(" ");
		}

		QString COntologyFunctionalSyntaxWriter::writeDataPropertyList(CEXPRESSIONLIST<CDataPropertyTermExpression*>* list) {
			QStringList parts;
			for (CDataPropertyTermExpression* e : *list) {
				parts.append(writeDataProperty(e));
			}
			return parts.join(" ");
		}

		QString COntologyFunctionalSyntaxWriter::writeDataRangeList(CEXPRESSIONLIST<CDataRangeTermExpression*>* list) {
			QStringList parts;
			for (CDataRangeTermExpression* e : *list) {
				parts.append(writeDataRange(e));
			}
			return parts.join(" ");
		}


		QString COntologyFunctionalSyntaxWriter::writeObjectProperty(CObjectPropertyTermExpression* exp) {
			if (CObjectPropertyExpression* prop = dynamic_cast<CObjectPropertyExpression*>(exp)) {
				return iri(prop->getName());
			}
			if (CInverseObjectPropertyOfExpression* inv = dynamic_cast<CInverseObjectPropertyOfExpression*>(exp)) {
				return QString("ObjectInverseOf(") + writeObjectProperty(inv->getInverseOfExpression()) + QString(")");
			}
			if (CObjectPropertyChainExpression* chain = dynamic_cast<CObjectPropertyChainExpression*>(exp)) {
				return QString("ObjectPropertyChain(") + writeObjectPropertyList(chain->getOrderedObjectPropertyTermExpressionList()) + QString(")");
			}
			failure(QString("unsupported object property expression (type %1)").arg(exp ? (int)exp->getType() : -1));
			return QString();
		}


		QString COntologyFunctionalSyntaxWriter::writeDataProperty(CDataPropertyTermExpression* exp) {
			if (CDataPropertyExpression* prop = dynamic_cast<CDataPropertyExpression*>(exp)) {
				return iri(prop->getName());
			}
			failure(QString("unsupported data property expression (type %1)").arg(exp ? (int)exp->getType() : -1));
			return QString();
		}


		QString COntologyFunctionalSyntaxWriter::writeLiteral(CDataLiteralExpression* exp) {
			CDataLexicalValueExpression* lexical = exp->getDataLexicalValueExpression();
			CDatatypeExpression* datatype = exp->getDatatypeExpression();
			if (!lexical || !datatype) {
				failure("data literal without lexical value or datatype");
				return QString();
			}
			return literal(lexical->getName(), datatype->getName());
		}


		QString COntologyFunctionalSyntaxWriter::writeDataRange(CDataRangeTermExpression* exp) {
			if (CDatatypeExpression* datatype = dynamic_cast<CDatatypeExpression*>(exp)) {
				return iri(datatype->getName());
			}
			if (CDataLiteralExpression* lit = dynamic_cast<CDataLiteralExpression*>(exp)) {
				return writeLiteral(lit);
			}
			if (CDataIntersectionOfExpression* e = dynamic_cast<CDataIntersectionOfExpression*>(exp)) {
				return QString("DataIntersectionOf(") + writeDataRangeList(e->getDataRangeTermExpressionList()) + QString(")");
			}
			if (CDataUnionOfExpression* e = dynamic_cast<CDataUnionOfExpression*>(exp)) {
				return QString("DataUnionOf(") + writeDataRangeList(e->getDataRangeTermExpressionList()) + QString(")");
			}
			if (CDataComplementOfExpression* e = dynamic_cast<CDataComplementOfExpression*>(exp)) {
				return QString("DataComplementOf(") + writeDataRange(e->getDataRangeTermExpression()) + QString(")");
			}
			if (CDataOneOfExpression* e = dynamic_cast<CDataOneOfExpression*>(exp)) {
				return QString("DataOneOf(") + writeDataRangeList(e->getDataRangeTermExpressionList()) + QString(")");
			}
			if (CDatatypeRestrictionExpression* e = dynamic_cast<CDatatypeRestrictionExpression*>(exp)) {
				QString text = QString("DatatypeRestriction(") + writeDataRange(e->getDatatypeExpression());
				for (CDataFacetRestrictionExpression* facet : *e->getDataFacetRestrictionExpressionList()) {
					text += QString(" ") + iri(facet->getDataFacetExpression()->getName()) + QString(" ") + writeLiteral(facet->getDataLiteralExpression());
				}
				return text + QString(")");
			}
			failure(QString("unsupported data range expression (type %1)").arg(exp ? (int)exp->getType() : -1));
			return QString();
		}


		QString COntologyFunctionalSyntaxWriter::writeClass(CClassTermExpression* exp) {
			if (!exp) {
				failure("missing class expression");
				return QString();
			}
			if (CClassExpression* e = dynamic_cast<CClassExpression*>(exp)) {
				return iri(e->getName());
			}
			if (CObjectIntersectionOfExpression* e = dynamic_cast<CObjectIntersectionOfExpression*>(exp)) {
				return QString("ObjectIntersectionOf(") + writeClassList(e->getClassTermExpressionList()) + QString(")");
			}
			if (CObjectUnionOfExpression* e = dynamic_cast<CObjectUnionOfExpression*>(exp)) {
				return QString("ObjectUnionOf(") + writeClassList(e->getClassTermExpressionList()) + QString(")");
			}
			if (CObjectComplementOfExpression* e = dynamic_cast<CObjectComplementOfExpression*>(exp)) {
				return QString("ObjectComplementOf(") + writeClass(e->getComplementExpression()) + QString(")");
			}
			if (CObjectSomeValuesFromExpression* e = dynamic_cast<CObjectSomeValuesFromExpression*>(exp)) {
				return QString("ObjectSomeValuesFrom(") + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(" ") + writeClass(e->getClassTermExpression()) + QString(")");
			}
			if (CObjectAllValuesFromExpression* e = dynamic_cast<CObjectAllValuesFromExpression*>(exp)) {
				return QString("ObjectAllValuesFrom(") + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(" ") + writeClass(e->getClassTermExpression()) + QString(")");
			}
			if (CObjectExactlyCardinalityExpression* e = dynamic_cast<CObjectExactlyCardinalityExpression*>(exp)) {
				return QString("ObjectExactCardinality(%1 ").arg((qlonglong)e->getCardinality()) + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(" ") + writeClass(e->getClassTermExpression()) + QString(")");
			}
			if (CObjectMinCardinalityExpression* e = dynamic_cast<CObjectMinCardinalityExpression*>(exp)) {
				return QString("ObjectMinCardinality(%1 ").arg((qlonglong)e->getCardinality()) + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(" ") + writeClass(e->getClassTermExpression()) + QString(")");
			}
			if (CObjectMaxCardinalityExpression* e = dynamic_cast<CObjectMaxCardinalityExpression*>(exp)) {
				return QString("ObjectMaxCardinality(%1 ").arg((qlonglong)e->getCardinality()) + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(" ") + writeClass(e->getClassTermExpression()) + QString(")");
			}
			if (CObjectHasSelfExpression* e = dynamic_cast<CObjectHasSelfExpression*>(exp)) {
				return QString("ObjectHasSelf(") + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(")");
			}
			if (CObjectHasValueExpression* e = dynamic_cast<CObjectHasValueExpression*>(exp)) {
				return QString("ObjectHasValue(") + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(" ") + writeIndividual(e->getIndividualTermExpression()) + QString(")");
			}
			if (CObjectOneOfExpression* e = dynamic_cast<CObjectOneOfExpression*>(exp)) {
				return QString("ObjectOneOf(") + writeIndividualList(e->getIndividualTermExpressionList()) + QString(")");
			}
			if (CDataSomeValuesFromExpression* e = dynamic_cast<CDataSomeValuesFromExpression*>(exp)) {
				return QString("DataSomeValuesFrom(") + writeDataProperty(e->getDataPropertyTermExpression()) + QString(" ") + writeDataRange(e->getDataRangeTermExpression()) + QString(")");
			}
			if (CDataAllValuesFromExpression* e = dynamic_cast<CDataAllValuesFromExpression*>(exp)) {
				return QString("DataAllValuesFrom(") + writeDataProperty(e->getDataPropertyTermExpression()) + QString(" ") + writeDataRange(e->getDataRangeTermExpression()) + QString(")");
			}
			if (CDataHasValueExpression* e = dynamic_cast<CDataHasValueExpression*>(exp)) {
				return QString("DataHasValue(") + writeDataProperty(e->getDataPropertyTermExpression()) + QString(" ") + writeDataRange(e->getDataRangeTermExpression()) + QString(")");
			}
			if (CDataExactCardinalityExpression* e = dynamic_cast<CDataExactCardinalityExpression*>(exp)) {
				return QString("DataExactCardinality(%1 ").arg((qlonglong)e->getCardinality()) + writeDataProperty(e->getDataPropertyTermExpression()) + QString(" ") + writeDataRange(e->getDataRangeTermExpression()) + QString(")");
			}
			if (CDataMinCardinalityExpression* e = dynamic_cast<CDataMinCardinalityExpression*>(exp)) {
				return QString("DataMinCardinality(%1 ").arg((qlonglong)e->getCardinality()) + writeDataProperty(e->getDataPropertyTermExpression()) + QString(" ") + writeDataRange(e->getDataRangeTermExpression()) + QString(")");
			}
			if (CDataMaxCardinalityExpression* e = dynamic_cast<CDataMaxCardinalityExpression*>(exp)) {
				return QString("DataMaxCardinality(%1 ").arg((qlonglong)e->getCardinality()) + writeDataProperty(e->getDataPropertyTermExpression()) + QString(" ") + writeDataRange(e->getDataRangeTermExpression()) + QString(")");
			}
			failure(QString("unsupported class expression (type %1)").arg((int)exp->getType()));
			return QString();
		}



		bool COntologyFunctionalSyntaxWriter::writeAxiom(CAxiomExpression* axiom, CWrittenAxiom& written) {
			mError.clear();
			mCurrentIndividuals.clear();
			written.mKind = AK_OTHER;
			QString text;

			if (CDeclarationAxiomExpression* e = dynamic_cast<CDeclarationAxiomExpression*>(axiom)) {
				written.mKind = AK_DECLARATION;
				CExpressionEntity* entity = e->getEntity();
				QString entityText;
				if (CNamedIndividualExpression* ent = dynamic_cast<CNamedIndividualExpression*>(entity)) {
					entityText = QString("NamedIndividual(") + iri(ent->getName()) + QString(")");
				} else if (dynamic_cast<CAnonymousIndividualExpression*>(entity)) {
					return failure("declaration of an anonymous individual");
				} else if (CClassExpression* ent = dynamic_cast<CClassExpression*>(entity)) {
					entityText = QString("Class(") + iri(ent->getName()) + QString(")");
				} else if (CObjectPropertyExpression* ent = dynamic_cast<CObjectPropertyExpression*>(entity)) {
					entityText = QString("ObjectProperty(") + iri(ent->getName()) + QString(")");
				} else if (CDataPropertyExpression* ent = dynamic_cast<CDataPropertyExpression*>(entity)) {
					entityText = QString("DataProperty(") + iri(ent->getName()) + QString(")");
				} else if (CDatatypeExpression* ent = dynamic_cast<CDatatypeExpression*>(entity)) {
					entityText = QString("Datatype(") + iri(ent->getName()) + QString(")");
				} else {
					return failure("declaration of an unsupported entity kind");
				}
				text = QString("Declaration(") + entityText + QString(")");

			} else if (CSubClassOfExpression* e = dynamic_cast<CSubClassOfExpression*>(axiom)) {
				text = QString("SubClassOf(") + writeClass(e->getSubClassTermExpression()) + QString(" ") + writeClass(e->getSuperClassTermExpression()) + QString(")");
			} else if (CEquivalentClassesExpression* e = dynamic_cast<CEquivalentClassesExpression*>(axiom)) {
				text = QString("EquivalentClasses(") + writeClassList(e->getClassTermExpressionList()) + QString(")");
			} else if (CDisjointClassesExpression* e = dynamic_cast<CDisjointClassesExpression*>(axiom)) {
				text = QString("DisjointClasses(") + writeClassList(e->getClassTermExpressionList()) + QString(")");
			} else if (CDisjointUnionExpression* e = dynamic_cast<CDisjointUnionExpression*>(axiom)) {
				text = QString("DisjointUnion(") + writeClass(e->getClassExpression()) + QString(" ") + writeClassList(e->getClassTermExpressionList()) + QString(")");

			} else if (CSubObjectPropertyOfExpression* e = dynamic_cast<CSubObjectPropertyOfExpression*>(axiom)) {
				CEXPRESSIONLIST<CObjectPropertyTermExpression*>* subList = e->getSubObjectPropertyTermExpressionList();
				QString sub;
				if (subList->size() == 1) {
					sub = writeObjectProperty(subList->front());
				} else {
					sub = QString("ObjectPropertyChain(") + writeObjectPropertyList(subList) + QString(")");
				}
				text = QString("SubObjectPropertyOf(") + sub + QString(" ") + writeObjectProperty(e->getSuperObjectPropertyTermExpression()) + QString(")");
			} else if (CEquivalentObjectPropertiesExpression* e = dynamic_cast<CEquivalentObjectPropertiesExpression*>(axiom)) {
				text = QString("EquivalentObjectProperties(") + writeObjectPropertyList(e->getObjectPropertyTermExpressionList()) + QString(")");
			} else if (CDisjointObjectPropertiesExpression* e = dynamic_cast<CDisjointObjectPropertiesExpression*>(axiom)) {
				text = QString("DisjointObjectProperties(") + writeObjectPropertyList(e->getObjectPropertyTermExpressionList()) + QString(")");
			} else if (CInverseObjectPropertiesExpression* e = dynamic_cast<CInverseObjectPropertiesExpression*>(axiom)) {
				text = QString("InverseObjectProperties(") + writeObjectProperty(e->getFirstObjectPropertyTermExpression()) + QString(" ") + writeObjectProperty(e->getSecondObjectPropertyTermExpression()) + QString(")");
			} else if (CObjectPropertyDomainExpression* e = dynamic_cast<CObjectPropertyDomainExpression*>(axiom)) {
				text = QString("ObjectPropertyDomain(") + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(" ") + writeClass(e->getClassTermExpression()) + QString(")");
			} else if (CObjectPropertyRangeExpression* e = dynamic_cast<CObjectPropertyRangeExpression*>(axiom)) {
				text = QString("ObjectPropertyRange(") + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(" ") + writeClass(e->getClassTermExpression()) + QString(")");
			} else if (CFunctionalObjectPropertyExpression* e = dynamic_cast<CFunctionalObjectPropertyExpression*>(axiom)) {
				text = QString("FunctionalObjectProperty(") + writeObjectProperty(e->getFunctionalObjectPropertyExpression()) + QString(")");
			} else if (CInverseFunctionalObjectPropertyExpression* e = dynamic_cast<CInverseFunctionalObjectPropertyExpression*>(axiom)) {
				text = QString("InverseFunctionalObjectProperty(") + writeObjectProperty(e->getInverseFunctionalObjectPropertyExpression()) + QString(")");
			} else if (CTransetiveObjectPropertyExpression* e = dynamic_cast<CTransetiveObjectPropertyExpression*>(axiom)) {
				text = QString("TransitiveObjectProperty(") + writeObjectProperty(e->getTransetivePropertyExpression()) + QString(")");
			} else if (CSymmetricObjectPropertyExpression* e = dynamic_cast<CSymmetricObjectPropertyExpression*>(axiom)) {
				text = QString("SymmetricObjectProperty(") + writeObjectProperty(e->getSymmetricObjectPropertyExpression()) + QString(")");
			} else if (CAsymmetricObjectPropertyExpression* e = dynamic_cast<CAsymmetricObjectPropertyExpression*>(axiom)) {
				text = QString("AsymmetricObjectProperty(") + writeObjectProperty(e->getAsymmetricObjectPropertyExpression()) + QString(")");
			} else if (CReflexiveObjectPropertyExpression* e = dynamic_cast<CReflexiveObjectPropertyExpression*>(axiom)) {
				text = QString("ReflexiveObjectProperty(") + writeObjectProperty(e->getReflexiveObjectPropertyExpression()) + QString(")");
			} else if (CIrreflexiveObjectPropertyExpression* e = dynamic_cast<CIrreflexiveObjectPropertyExpression*>(axiom)) {
				text = QString("IrreflexiveObjectProperty(") + writeObjectProperty(e->getIreflexiveObjectPropertyExpression()) + QString(")");

			} else if (CSubDataPropertyOfExpression* e = dynamic_cast<CSubDataPropertyOfExpression*>(axiom)) {
				text = QString("SubDataPropertyOf(") + writeDataProperty(e->getSubDataPropertyTermExpression()) + QString(" ") + writeDataProperty(e->getSuperDataPropertyTermExpression()) + QString(")");
			} else if (CEquivalentDataPropertiesExpression* e = dynamic_cast<CEquivalentDataPropertiesExpression*>(axiom)) {
				text = QString("EquivalentDataProperties(") + writeDataPropertyList(e->getDataPropertyTermExpressionList()) + QString(")");
			} else if (CDisjointDataPropertiesExpression* e = dynamic_cast<CDisjointDataPropertiesExpression*>(axiom)) {
				text = QString("DisjointDataProperties(") + writeDataPropertyList(e->getDataPropertyTermExpressionList()) + QString(")");
			} else if (CDataPropertyDomainExpression* e = dynamic_cast<CDataPropertyDomainExpression*>(axiom)) {
				text = QString("DataPropertyDomain(") + writeDataProperty(e->getDataPropertyTermExpression()) + QString(" ") + writeClass(e->getClassTermExpression()) + QString(")");
			} else if (CDataPropertyRangeExpression* e = dynamic_cast<CDataPropertyRangeExpression*>(axiom)) {
				text = QString("DataPropertyRange(") + writeDataProperty(e->getDataPropertyTermExpression()) + QString(" ") + writeDataRange(e->getDataRangeTermExpression()) + QString(")");
			} else if (CFunctionalDataPropertyExpression* e = dynamic_cast<CFunctionalDataPropertyExpression*>(axiom)) {
				text = QString("FunctionalDataProperty(") + writeDataProperty(e->getFunctionalDataPropertyExpression()) + QString(")");

			} else if (CClassAssertionExpression* e = dynamic_cast<CClassAssertionExpression*>(axiom)) {
				written.mKind = AK_ASSERTION;
				text = QString("ClassAssertion(") + writeClass(e->getClassTermExpression()) + QString(" ") + writeIndividual(e->getIndividualTermExpression()) + QString(")");
			} else if (CObjectPropertyAssertionExpression* e = dynamic_cast<CObjectPropertyAssertionExpression*>(axiom)) {
				written.mKind = AK_ASSERTION;
				text = QString("ObjectPropertyAssertion(") + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(" ") + writeIndividual(e->getFirstIndividualTermExpression()) + QString(" ") + writeIndividual(e->getSecondIndividualTermExpression()) + QString(")");
			} else if (CNegativeObjectPropertyAssertionExpression* e = dynamic_cast<CNegativeObjectPropertyAssertionExpression*>(axiom)) {
				written.mKind = AK_ASSERTION;
				text = QString("NegativeObjectPropertyAssertion(") + writeObjectProperty(e->getObjectPropertyTermExpression()) + QString(" ") + writeIndividual(e->getFirstIndividualTermExpression()) + QString(" ") + writeIndividual(e->getSecondIndividualTermExpression()) + QString(")");
			} else if (CDataPropertyAssertionExpression* e = dynamic_cast<CDataPropertyAssertionExpression*>(axiom)) {
				written.mKind = AK_ASSERTION;
				text = QString("DataPropertyAssertion(") + writeDataProperty(e->getDataPropertyTermExpression()) + QString(" ") + writeIndividual(e->getIndividualTermExpression()) + QString(" ") + writeDataRange(e->getDataLiteralTermExpression()) + QString(")");
			} else if (CNegativeDataPropertyAssertionExpression* e = dynamic_cast<CNegativeDataPropertyAssertionExpression*>(axiom)) {
				written.mKind = AK_ASSERTION;
				text = QString("NegativeDataPropertyAssertion(") + writeDataProperty(e->getDataPropertyTermExpression()) + QString(" ") + writeIndividual(e->getIndividualTermExpression()) + QString(" ") + writeDataRange(e->getDataLiteralTermExpression()) + QString(")");
			} else if (CSameIndividualExpression* e = dynamic_cast<CSameIndividualExpression*>(axiom)) {
				written.mKind = AK_ASSERTION;
				text = QString("SameIndividual(") + writeIndividualList(e->getIndividualTermExpressionList()) + QString(")");
			} else if (CDifferentIndividualsExpression* e = dynamic_cast<CDifferentIndividualsExpression*>(axiom)) {
				written.mKind = AK_ASSERTION;
				text = QString("DifferentIndividuals(") + writeIndividualList(e->getIndividualTermExpressionList()) + QString(")");

			} else {
				return failure(QString("axiom of unsupported kind (expression type %1)").arg(axiom ? (int)axiom->getType() : -1));
			}

			if (!mError.isEmpty()) {
				return false;
			}
			written.mText = text;
			written.mIndividuals = mCurrentIndividuals;
			return true;
		}



		bool COntologyFunctionalSyntaxWriter::writeOntology(CConcreteOntology* ontology, QList<CWrittenAxiom>& writtenAxioms) {
			mError.clear();
			COntologyBuildData* buildData = ontology->getBuildData();
			QList<CAxiomExpression*> axiomList;
			for (CAxiomExpression* axiom : *buildData->getTellAxiomSet()) {
				axiomList.append(axiom);
			}
			std::sort(axiomList.begin(), axiomList.end(), [](CAxiomExpression* a, CAxiomExpression* b) {
				return a->getSerial() < b->getSerial();
			});
			for (CAxiomExpression* axiom : axiomList) {
				CWrittenAxiom written;
				if (!writeAxiom(axiom, written)) {
					return false;
				}
				writtenAxioms.append(written);
			}
			return writeTriplesAssertions(ontology, writtenAxioms);
		}


		// Assertions of RDF/Turtle input are not turned into axioms but stay as triples that Konclude indexes later. They are
		// recognized in the same way as Konclude does it: rdf:type with a known class, a known object property with a resource
		// as object, a known data property with a literal as object. Everything else (annotations, vocabulary of the TBox, ...)
		// is not an assertion for the reasoner. If this reading differs from the one of Konclude, the check of the explain command
		// (answer for the input file against answer for the rewritten ontology) reports it.
		bool COntologyFunctionalSyntaxWriter::writeTriplesAssertions(CConcreteOntology* ontology, QList<CWrittenAxiom>& writtenAxioms) {
			COntologyTriplesData* triplesData = ontology->getOntologyTriplesData();
			if (!triplesData || !triplesData->getAllTriplesData()) {
				return true;
			}
			COntologyBuildData* buildData = ontology->getBuildData();
			CBUILDHASH<CStringRefStringHasher, CClassExpression*>* classHash = buildData->getClassEntityBuildHash();
			CBUILDHASH<CStringRefStringHasher, CObjectPropertyExpression*>* objectPropertyHash = buildData->getObjectPropertyEntityBuildHash();
			CBUILDHASH<CStringRefStringHasher, CDataPropertyExpression*>* dataPropertyHash = buildData->getDataPropertyEntityBuildHash();
			const QString rdfType(PREFIX_RDF_TYPE);
			const QString namedIndividualType(PREFIX_OWL_NAMED_INDIVIDUAL);

			for (CTriplesData* tripleData : *triplesData->getAllTriplesData()) {
				CTriplesDataIterator* it = tripleData->getTriplesDataIterator();
				if (!it) {
					continue;
				}
				while (it->hasValue()) {
					if (it->getNodeType(1) == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE && it->getNodeType(0) != CTriplesDataIterator::TRIPLE_LITERAL_NODE_TYPE) {
						QString predicate = it->getNodeString(1);
						CTriplesDataIterator::TRIPLE_NODE_TYPE objectType = it->getNodeType(2);
						QString text;
						QStringList individuals;
						bool isAssertion = false;
						if (predicate == rdfType) {
							if (objectType == CTriplesDataIterator::TRIPLE_URI_NODE_TYPE) {
								QString cls = it->getNodeString(2);
								if (cls != namedIndividualType && classHash->contains(cls)) {
									QString subject = it->getNodeType(0) == CTriplesDataIterator::TRIPLE_BLANK_NODE_TYPE ? anonymousIndividual(it->getNodeString(0)) : iri(it->getNodeString(0));
									text = QString("ClassAssertion(") + iri(cls) + QString(" ") + subject + QString(")");
									individuals.append(it->getNodeType(0) == CTriplesDataIterator::TRIPLE_BLANK_NODE_TYPE ? subject : it->getNodeString(0));
									isAssertion = true;
								}
							}
						} else if (objectPropertyHash->contains(predicate)) {
							if (objectType != CTriplesDataIterator::TRIPLE_LITERAL_NODE_TYPE) {
								bool subjectBlank = it->getNodeType(0) == CTriplesDataIterator::TRIPLE_BLANK_NODE_TYPE;
								bool objectBlank = objectType == CTriplesDataIterator::TRIPLE_BLANK_NODE_TYPE;
								QString subject = subjectBlank ? anonymousIndividual(it->getNodeString(0)) : iri(it->getNodeString(0));
								QString object = objectBlank ? anonymousIndividual(it->getNodeString(2)) : iri(it->getNodeString(2));
								text = QString("ObjectPropertyAssertion(") + iri(predicate) + QString(" ") + subject + QString(" ") + object + QString(")");
								individuals.append(subjectBlank ? subject : it->getNodeString(0));
								individuals.append(objectBlank ? object : it->getNodeString(2));
								isAssertion = true;
							}
						} else if (dataPropertyHash->contains(predicate)) {
							if (objectType == CTriplesDataIterator::TRIPLE_LITERAL_NODE_TYPE) {
								// the node string of a literal is "lexical value"^^datatype IRI
								QString literalString = it->getNodeString(2);
								int separator = literalString.lastIndexOf(QString("\"^^"));
								if (separator < 1 || !literalString.startsWith(QChar('"'))) {
									delete it;
									return failure(QString("cannot read the literal '%1' of a triple").arg(literalString));
								}
								QString lexical = literalString.mid(1, separator - 1);
								QString datatype = literalString.mid(separator + 3);
								bool subjectBlank = it->getNodeType(0) == CTriplesDataIterator::TRIPLE_BLANK_NODE_TYPE;
								QString subject = subjectBlank ? anonymousIndividual(it->getNodeString(0)) : iri(it->getNodeString(0));
								text = QString("DataPropertyAssertion(") + iri(predicate) + QString(" ") + subject + QString(" ") + literal(lexical, datatype) + QString(")");
								individuals.append(subjectBlank ? subject : it->getNodeString(0));
								isAssertion = true;
							}
						}
						if (isAssertion) {
							CWrittenAxiom written;
							written.mText = text;
							written.mKind = AK_ASSERTION;
							written.mIndividuals = individuals;
							writtenAxioms.append(written);
						}
					}
					it->moveNext();
				}
				delete it;
			}
			return true;
		}


	}; // end namespace Renderer

}; // end namespace Konclude
