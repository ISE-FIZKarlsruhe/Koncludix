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

#include "CWriteQueryFileOWL2XMLSerializer.h"


namespace Konclude {

	namespace Reasoner {

		namespace Query {


			CWriteQueryFileOWL2XMLSerializer::CWriteQueryFileOWL2XMLSerializer(const QString& fileString) : CWriteQueryFileSerializer(fileString) {
			}



			bool CWriteQueryFileOWL2XMLSerializer::startWritingOutput() {
				if (CWriteQueryFileSerializer::startWritingOutput()) {

					mCurrentOutputStreamWriter = new QXmlStreamWriter(mCurrentOutputFile);
					mCurrentOutputStreamWriter->setAutoFormatting(true);
					mCurrentOutputStreamWriter->writeStartDocument();
					return true;
				}
				return false;
			}



			bool CWriteQueryFileOWL2XMLSerializer::endWritingOutput() {
				if (mCurrentOutputFile) {
					mCurrentOutputStreamWriter->writeEndDocument();
					mCurrentOutputFile->close();
					delete mCurrentOutputStreamWriter;
					delete mCurrentOutputFile;
					return true;
				}
				return false;
			}



			void CWriteQueryFileOWL2XMLSerializer::writeObjectPropertyEquivalenceRelations(const QStringList& propertyNameList) {
				mCurrentOutputStreamWriter->writeStartElement("EquivalentObjectProperties");
				foreach (const QString& propertyName, propertyNameList) {
					mCurrentOutputStreamWriter->writeStartElement("ObjectProperty");
					mCurrentOutputStreamWriter->writeAttribute("IRI",propertyName);
					mCurrentOutputStreamWriter->writeEndElement();
				}
				mCurrentOutputStreamWriter->writeEndElement();
			}

			void CWriteQueryFileOWL2XMLSerializer::writeDataPropertyEquivalenceRelations(const QStringList& propertyNameList) {
				mCurrentOutputStreamWriter->writeStartElement("EquivalentDataProperties");
				foreach (const QString& propertyName, propertyNameList) {
					mCurrentOutputStreamWriter->writeStartElement("DataProperty");
					mCurrentOutputStreamWriter->writeAttribute("IRI",propertyName);
					mCurrentOutputStreamWriter->writeEndElement();
				}
				mCurrentOutputStreamWriter->writeEndElement();
			}




			void CWriteQueryFileOWL2XMLSerializer::writeClassEquivalenceRelations(const QStringList& classNameList) {
				mCurrentOutputStreamWriter->writeStartElement("EquivalentClasses");
				foreach (const QString& className, classNameList) {
					mCurrentOutputStreamWriter->writeStartElement("Class");
					mCurrentOutputStreamWriter->writeAttribute("IRI",className);
					mCurrentOutputStreamWriter->writeEndElement();
				}
				mCurrentOutputStreamWriter->writeEndElement();
			}





			void CWriteQueryFileOWL2XMLSerializer::writeClassDeclaration(const QString& className) {
				mCurrentOutputStreamWriter->writeStartElement("Declaration");
				mCurrentOutputStreamWriter->writeStartElement("Class");
				mCurrentOutputStreamWriter->writeAttribute("IRI",className);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeEndElement();
			}


			void CWriteQueryFileOWL2XMLSerializer::writeObjectPropertyDeclaration(const QString& propertyName) {
				mCurrentOutputStreamWriter->writeStartElement("Declaration");
				mCurrentOutputStreamWriter->writeStartElement("ObjectProperty");
				mCurrentOutputStreamWriter->writeAttribute("IRI",propertyName);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeEndElement();
			}

			void CWriteQueryFileOWL2XMLSerializer::writeDataPropertyDeclaration(const QString& propertyName) {
				mCurrentOutputStreamWriter->writeStartElement("Declaration");
				mCurrentOutputStreamWriter->writeStartElement("DataProperty");
				mCurrentOutputStreamWriter->writeAttribute("IRI",propertyName);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeEndElement();
			}




			void CWriteQueryFileOWL2XMLSerializer::writeOntologyStart() {
				mCurrentOutputStreamWriter->writeStartElement("Ontology");
				mCurrentOutputStreamWriter->writeAttribute("xmlns:rdfs","http://www.w3.org/2000/01/rdf-schema#");
				mCurrentOutputStreamWriter->writeAttribute("xmlns:rdf","http://www.w3.org/1999/02/22-rdf-syntax-ns#");
				mCurrentOutputStreamWriter->writeAttribute("xmlns","http://www.w3.org/2002/07/owl#");
				mCurrentOutputStreamWriter->writeAttribute("xmlns:xml","http://www.w3.org/XML/1998/namespace");
				mCurrentOutputStreamWriter->writeAttribute("xmlns:xsd","http://www.w3.org/2001/XMLSchema#");

				writeOntologyPrefix("","http://www.w3.org/2002/07/owl#");
				writeOntologyPrefix("owl","http://www.w3.org/2002/07/owl#");
				writeOntologyPrefix("rdf","http://www.w3.org/1999/02/22-rdf-syntax-ns#");
				writeOntologyPrefix("xml","http://www.w3.org/XML/1998/namespace");
				writeOntologyPrefix("xsd","http://www.w3.org/2001/XMLSchema#");
				writeOntologyPrefix("rdfs","http://www.w3.org/2000/01/rdf-schema#");
			}



			void CWriteQueryFileOWL2XMLSerializer::writeOntologyPrefix(const QString& prefixName, const QString& prefixIRI) {
				mCurrentOutputStreamWriter->writeStartElement("Prefix");
				mCurrentOutputStreamWriter->writeAttribute("name",prefixName);
				mCurrentOutputStreamWriter->writeAttribute("IRI",prefixIRI);
				mCurrentOutputStreamWriter->writeEndElement();
			}


			void CWriteQueryFileOWL2XMLSerializer::writeOntologyEnd() {
				mCurrentOutputStreamWriter->writeEndElement();
			}




			void CWriteQueryFileOWL2XMLSerializer::writeSubClassRelation(const QString& subClassName, const QString& superClassName) {
				mCurrentOutputStreamWriter->writeStartElement("SubClassOf");
				mCurrentOutputStreamWriter->writeStartElement("Class");
				mCurrentOutputStreamWriter->writeAttribute("IRI",subClassName);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeStartElement("Class");
				mCurrentOutputStreamWriter->writeAttribute("IRI",superClassName);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeEndElement();
			}

			void CWriteQueryFileOWL2XMLSerializer::writeSubObjectPropertyRelation(const QString& subPropertyName, const QString& superPropertyName) {
				mCurrentOutputStreamWriter->writeStartElement("SubObjectPropertyOf");
				mCurrentOutputStreamWriter->writeStartElement("ObjectProperty");
				mCurrentOutputStreamWriter->writeAttribute("IRI",subPropertyName);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeStartElement("ObjectProperty");
				mCurrentOutputStreamWriter->writeAttribute("IRI",superPropertyName);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeEndElement();
			}

			void CWriteQueryFileOWL2XMLSerializer::writeSubDataPropertyRelation(const QString& subPropertyName, const QString& superPropertyName) {
				mCurrentOutputStreamWriter->writeStartElement("SubDataPropertyOf");
				mCurrentOutputStreamWriter->writeStartElement("DataProperty");
				mCurrentOutputStreamWriter->writeAttribute("IRI",subPropertyName);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeStartElement("DataProperty");
				mCurrentOutputStreamWriter->writeAttribute("IRI",superPropertyName);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeEndElement();
			}


			void CWriteQueryFileOWL2XMLSerializer::writeNamedIndividualType(const QString& individualName, const QString& className) {
				mCurrentOutputStreamWriter->writeStartElement("ClassAssertion");
				mCurrentOutputStreamWriter->writeStartElement("Class");
				mCurrentOutputStreamWriter->writeAttribute("IRI",className);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeStartElement("NamedIndividual");
				mCurrentOutputStreamWriter->writeAttribute("IRI",individualName);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeEndElement();
			}


			void CWriteQueryFileOWL2XMLSerializer::writeAnonymousIndividualType(const QString& individualName, const QString& className) {
				mCurrentOutputStreamWriter->writeStartElement("ClassAssertion");
				mCurrentOutputStreamWriter->writeStartElement("Class");
				mCurrentOutputStreamWriter->writeAttribute("IRI", className);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeStartElement("AnonymousIndividual");
				mCurrentOutputStreamWriter->writeAttribute("nodeID", individualName);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeEndElement();
			}


			void CWriteQueryFileOWL2XMLSerializer::writeObjectPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& objectName, bool objectAnonymous) {
			mCurrentOutputStreamWriter->writeStartElement("ObjectPropertyAssertion");
			mCurrentOutputStreamWriter->writeStartElement("ObjectProperty");
			mCurrentOutputStreamWriter->writeAttribute("IRI",propertyName);
			mCurrentOutputStreamWriter->writeEndElement();
			if (subjectAnonymous) {
				mCurrentOutputStreamWriter->writeStartElement("AnonymousIndividual");
				mCurrentOutputStreamWriter->writeAttribute("nodeID",subjectName);
				mCurrentOutputStreamWriter->writeEndElement();
			} else {
				mCurrentOutputStreamWriter->writeStartElement("NamedIndividual");
				mCurrentOutputStreamWriter->writeAttribute("IRI",subjectName);
				mCurrentOutputStreamWriter->writeEndElement();
			}
			if (objectAnonymous) {
				mCurrentOutputStreamWriter->writeStartElement("AnonymousIndividual");
				mCurrentOutputStreamWriter->writeAttribute("nodeID",objectName);
				mCurrentOutputStreamWriter->writeEndElement();
			} else {
				mCurrentOutputStreamWriter->writeStartElement("NamedIndividual");
				mCurrentOutputStreamWriter->writeAttribute("IRI",objectName);
				mCurrentOutputStreamWriter->writeEndElement();
			}
			mCurrentOutputStreamWriter->writeEndElement();
			}

			void CWriteQueryFileOWL2XMLSerializer::writeDataPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& lexicalValue, const QString& datatypeIRI) {
			mCurrentOutputStreamWriter->writeStartElement("DataPropertyAssertion");
			mCurrentOutputStreamWriter->writeStartElement("DataProperty");
			mCurrentOutputStreamWriter->writeAttribute("IRI",propertyName);
			mCurrentOutputStreamWriter->writeEndElement();
			if (subjectAnonymous) {
				mCurrentOutputStreamWriter->writeStartElement("AnonymousIndividual");
				mCurrentOutputStreamWriter->writeAttribute("nodeID",subjectName);
				mCurrentOutputStreamWriter->writeEndElement();
			} else {
				mCurrentOutputStreamWriter->writeStartElement("NamedIndividual");
				mCurrentOutputStreamWriter->writeAttribute("IRI",subjectName);
				mCurrentOutputStreamWriter->writeEndElement();
			}
			mCurrentOutputStreamWriter->writeStartElement("Literal");
			mCurrentOutputStreamWriter->writeAttribute("datatypeIRI",datatypeIRI);
			mCurrentOutputStreamWriter->writeCharacters(lexicalValue);
			mCurrentOutputStreamWriter->writeEndElement();
			mCurrentOutputStreamWriter->writeEndElement();
			}

			void CWriteQueryFileOWL2XMLSerializer::writeIndividualEquivalenceRelations(const QStringList& individualNameList, const QList<bool>& individualAnonymousList) {
			mCurrentOutputStreamWriter->writeStartElement("SameIndividual");
			for (int i = 0; i < individualNameList.size(); ++i) {
				bool anonymous = i < individualAnonymousList.size() && individualAnonymousList.at(i);
				if (anonymous) {
					mCurrentOutputStreamWriter->writeStartElement("AnonymousIndividual");
					mCurrentOutputStreamWriter->writeAttribute("nodeID",individualNameList.at(i));
					mCurrentOutputStreamWriter->writeEndElement();
				} else {
					mCurrentOutputStreamWriter->writeStartElement("NamedIndividual");
					mCurrentOutputStreamWriter->writeAttribute("IRI",individualNameList.at(i));
					mCurrentOutputStreamWriter->writeEndElement();
				}
			}
			mCurrentOutputStreamWriter->writeEndElement();
			}

			void CWriteQueryFileOWL2XMLSerializer::writeNamedIndividualDeclaration(const QString& className) {
				mCurrentOutputStreamWriter->writeStartElement("Declaration");
				mCurrentOutputStreamWriter->writeStartElement("NamedIndividual");
				mCurrentOutputStreamWriter->writeAttribute("IRI",className);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeEndElement();
			}


			void CWriteQueryFileOWL2XMLSerializer::writeAnonymousIndividualDeclaration(const QString& className) {
				mCurrentOutputStreamWriter->writeStartElement("Declaration");
				mCurrentOutputStreamWriter->writeStartElement("AnonymousIndividual");
				mCurrentOutputStreamWriter->writeAttribute("nodeID", className);
				mCurrentOutputStreamWriter->writeEndElement();
				mCurrentOutputStreamWriter->writeEndElement();
			}

		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude
