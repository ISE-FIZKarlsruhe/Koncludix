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

#ifndef KONCLUDE_REASONER_QUERY_CWRITESERIALIZERMATERIALIZEDINDIVIDUALASSERTIONSQUERY_H
#define KONCLUDE_REASONER_QUERY_CWRITESERIALIZERMATERIALIZEDINDIVIDUALASSERTIONSQUERY_H

// Libraries includes
#include <QString>
#include <QDir>
#include <QStringList>
#include <QXmlStreamWriter>


// Namespace includes
#include "CQuery.h"
#include "CWriteMaterializedIndividualAssertionsQuery.h"
#include "CWriteQuerySerializer.h"

// Other includes

// Logger includes
#include "Logger/CLogger.h"



namespace Konclude {

	namespace Reasoner {

		namespace Query {

			/*!
			 *
			 *		\class		CWriteSerializerMaterializedIndividualAssertionsQuery
			 *		\brief		Concrete materialized-ABox writer that forwards each
			 *					write primitive to a CWriteQuerySerializer (OWL2
			 *					Functional or OWL2 XML).
			 *
			 */
			class CWriteSerializerMaterializedIndividualAssertionsQuery : public CWriteMaterializedIndividualAssertionsQuery {
				// public methods
				public:
					//! Constructor
					CWriteSerializerMaterializedIndividualAssertionsQuery(CConcreteOntology* ontology, CConfigurationBase *configuration, CWriteQuerySerializer* serializer, const QString& individualNameString = QString(""), const QString &queryName = QString("UnnamedWriteMaterializedIndividualAssertionsQuery"));


				// protected methods
				protected:

					virtual void writeIndividualDeclaration(const QString& individualName, bool anonymous);
					virtual void writeNamedIndividualDeclaration(const QString& individualName);
					virtual void writeAnonymousIndividualDeclaration(const QString& individualName);
					virtual void writeClassDeclaration(const QString& className);
					virtual void writeObjectPropertyDeclaration(const QString& propertyName);
					virtual void writeDataPropertyDeclaration(const QString& propertyName);

					virtual void writeIndividualType(const QString& individualName, bool anonymous, const QString& className);
					virtual void writeNamedIndividualType(const QString& individualName, const QString& className);
					virtual void writeAnonymousIndividualType(const QString& individualName, const QString& className);
					virtual void writeSubClassRelation(const QString& subClassName, const QString& superClassName);
					virtual void writeSubObjectPropertyRelation(const QString& subPropertyName, const QString& superPropertyName);
					virtual void writeSubDataPropertyRelation(const QString& subPropertyName, const QString& superPropertyName);

					virtual void writeClassEquivalenceRelations(const QStringList& classNameList);
					virtual void writeObjectPropertyEquivalenceRelations(const QStringList& propertyNameList);
					virtual void writeDataPropertyEquivalenceRelations(const QStringList& propertyNameList);

					virtual void writeObjectPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& objectName, bool objectAnonymous);
					virtual void writeDataPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& lexicalValue, const QString& datatypeIRI);
					virtual void writeIndividualEquivalenceRelations(const QStringList& individualNameList, const QList<bool>& individualAnonymousList);

					virtual void writeOntologyStart();
					virtual void writeOntologyEnd();
					virtual void writeOntologyPrefix(const QString& prefixName, const QString& prefixIRI);


					virtual bool startWritingOutput();
					virtual bool endWritingOutput();

				// protected variables
				protected:
					CWriteQuerySerializer* mSerializer;



				// private methods
				private:

				// private variables
				private:

			};

		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude

#endif // KONCLUDE_REASONER_QUERY_CWRITESERIALIZERMATERIALIZEDINDIVIDUALASSERTIONSQUERY_H
