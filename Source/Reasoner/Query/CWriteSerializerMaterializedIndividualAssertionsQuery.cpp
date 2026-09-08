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

#include "CWriteSerializerMaterializedIndividualAssertionsQuery.h"


namespace Konclude {

	namespace Reasoner {

		namespace Query {


			CWriteSerializerMaterializedIndividualAssertionsQuery::CWriteSerializerMaterializedIndividualAssertionsQuery(CConcreteOntology* ontology, CConfigurationBase *configuration, CWriteQuerySerializer* serializer, const QString& individualNameString, const QString &queryName)
					: CWriteMaterializedIndividualAssertionsQuery(ontology,configuration,serializer->getOutputName(),individualNameString,queryName) {

				mSerializer = serializer;
			}




			bool CWriteSerializerMaterializedIndividualAssertionsQuery::startWritingOutput() {
				return mSerializer->startWritingOutput();
			}



			bool CWriteSerializerMaterializedIndividualAssertionsQuery::endWritingOutput() {
				return mSerializer->endWritingOutput();
			}


			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeSubClassRelation(const QString& subClassName, const QString& superClassName) {
				mSerializer->writeSubClassRelation(subClassName,superClassName);
			}



			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeIndividualDeclaration(const QString& individualName, bool anonymous) {
				if (anonymous) {
					mSerializer->writeAnonymousIndividualDeclaration(individualName);
				} else {
					mSerializer->writeNamedIndividualDeclaration(individualName);
				}
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeNamedIndividualDeclaration(const QString& className) {
				mSerializer->writeNamedIndividualDeclaration(className);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeAnonymousIndividualDeclaration(const QString& className) {
				mSerializer->writeAnonymousIndividualDeclaration(className);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeClassDeclaration(const QString& className) {
				mSerializer->writeClassDeclaration(className);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeObjectPropertyDeclaration(const QString& propertyName) {
				mSerializer->writeObjectPropertyDeclaration(propertyName);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeDataPropertyDeclaration(const QString& propertyName) {
				mSerializer->writeDataPropertyDeclaration(propertyName);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeSubObjectPropertyRelation(const QString& subPropertyName, const QString& superPropertyName) {
				mSerializer->writeSubObjectPropertyRelation(subPropertyName,superPropertyName);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeSubDataPropertyRelation(const QString& subPropertyName, const QString& superPropertyName) {
				mSerializer->writeSubDataPropertyRelation(subPropertyName,superPropertyName);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeClassEquivalenceRelations(const QStringList& classNameList) {
				mSerializer->writeClassEquivalenceRelations(classNameList);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeObjectPropertyEquivalenceRelations(const QStringList& propertyNameList) {
				mSerializer->writeObjectPropertyEquivalenceRelations(propertyNameList);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeDataPropertyEquivalenceRelations(const QStringList& propertyNameList) {
				mSerializer->writeDataPropertyEquivalenceRelations(propertyNameList);
			}


			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeOntologyPrefix(const QString& prefixName, const QString& prefixIRI) {
				mSerializer->writeOntologyPrefix(prefixName,prefixIRI);
			}


			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeOntologyStart() {
				mSerializer->writeOntologyStart();
			}


			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeOntologyEnd() {
				mSerializer->writeOntologyEnd();
			}


			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeIndividualType(const QString& individualName, bool anonymous, const QString& className) {
				if (anonymous) {
					mSerializer->writeAnonymousIndividualType(individualName, className);
				} else {
					mSerializer->writeNamedIndividualType(individualName, className);
				}
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeNamedIndividualType(const QString& individualName, const QString& className) {
				mSerializer->writeNamedIndividualType(individualName, className);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeAnonymousIndividualType(const QString& individualName, const QString& className) {
				mSerializer->writeAnonymousIndividualType(individualName, className);
			}


			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeObjectPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& objectName, bool objectAnonymous) {
				mSerializer->writeObjectPropertyAssertion(subjectName, subjectAnonymous, propertyName, objectName, objectAnonymous);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeDataPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& lexicalValue, const QString& datatypeIRI) {
				mSerializer->writeDataPropertyAssertion(subjectName, subjectAnonymous, propertyName, lexicalValue, datatypeIRI);
			}

			void CWriteSerializerMaterializedIndividualAssertionsQuery::writeIndividualEquivalenceRelations(const QStringList& individualNameList, const QList<bool>& individualAnonymousList) {
				mSerializer->writeIndividualEquivalenceRelations(individualNameList, individualAnonymousList);
			}



		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude
