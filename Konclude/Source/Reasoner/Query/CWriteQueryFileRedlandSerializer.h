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

#ifndef KONCLUDE_REASONER_QUERY_CWRITEQUERYFILEREDLANDSERIALIZER_H
#define KONCLUDE_REASONER_QUERY_CWRITEQUERYFILEREDLANDSERIALIZER_H

// Only available in builds with Redland linked in (KONCLUDE_REDLAND_INTEGRATION,
// see KoncludeRedlandLinux.pro) -- our Windows build (KoncludeWithoutRedland.pro)
// never has Redland's headers available at all, so this whole file compiles
// to nothing there.
#ifdef KONCLUDE_REDLAND_INTEGRATION

// Libraries includes
#include <QString>
#include <QDir>
#include <QHash>

extern "C" {
#include <redland.h>
}

// Namespace includes
#include "QuerySettings.h"
#include "CWriteQuerySerializer.h"


// Logger includes
#include "Logger/CLogger.h"



namespace Konclude {

	namespace Reasoner {

		namespace Query {

			/*!
			 *
			 *		\class		CWriteQueryFileRedlandSerializer
			 *		\brief		Writes materialize's output as real RDF (Turtle by
			 *					default) via Redland's own serializer -- unlike
			 *					CWriteQueryFileOWL2XMLSerializer/
			 *					CWriteQueryFileOWL2FunctionalSerializer (which hand-write
			 *					OWL2-XML/Functional text directly), this builds an
			 *					in-memory librdf_model one triple at a time and lets
			 *					Redland serialize it, so downstream tools that only
			 *					understand plain RDF (rdflib, pyshacl, ...) can consume
			 *					the result with no separate conversion step, on either
			 *					end.
			 *
			 */
			class CWriteQueryFileRedlandSerializer : public CWriteQuerySerializer {
				// public methods
				public:
					//! Constructor
					CWriteQueryFileRedlandSerializer(const QString& fileString, const QString& syntaxName = QString("turtle"));

					//! Destructor
					virtual ~CWriteQueryFileRedlandSerializer();

					virtual const QString getOutputName();

					virtual void writeObjectPropertyDeclaration(const QString& propertyName);
					virtual void writeDataPropertyDeclaration(const QString& propertyName);
					virtual void writeSubObjectPropertyRelation(const QString& subPropertyName, const QString& superPropertyName);
					virtual void writeSubDataPropertyRelation(const QString& subPropertyName, const QString& superPropertyName);
					virtual void writeObjectPropertyEquivalenceRelations(const QStringList& propertyNameList);
					virtual void writeDataPropertyEquivalenceRelations(const QStringList& propertyNameList);

					virtual void writeClassDeclaration(const QString& className);
					virtual void writeSubClassRelation(const QString& subClassName, const QString& superClassName);
					virtual void writeClassEquivalenceRelations(const QStringList& classNameList);

					virtual void writeNamedIndividualDeclaration(const QString& individualName);
					virtual void writeAnonymousIndividualDeclaration(const QString& individualName);
					virtual void writeNamedIndividualType(const QString& individualName, const QString& className);
					virtual void writeAnonymousIndividualType(const QString& individualName, const QString& className);

					virtual void writeObjectPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& objectName, bool objectAnonymous);
					virtual void writeDataPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& lexicalValue, const QString& datatypeIRI);
					virtual void writeIndividualEquivalenceRelations(const QStringList& individualNameList, const QList<bool>& individualAnonymousList);

					virtual void writeOntologyPrefix(const QString& prefixName, const QString& prefixIRI);
					virtual void writeOntologyStart();
					virtual void writeOntologyEnd();


					virtual bool startWritingOutput();
					virtual bool endWritingOutput();


				// protected methods
				protected:
					librdf_node* createResourceNode(const QString& name, bool anonymous);
					void addTriple(librdf_node* subject, librdf_node* predicate, librdf_node* object);
					void addTypeTriple(const QString& subjectName, bool subjectAnonymous, const QString& classIRI);
					void addPairwiseChainRelation(const QStringList& nameList, const QString& relationIRI);


				// protected variables
				protected:
					QString mFileString;
					QString mSyntaxName;

					librdf_world* mWorld;
					librdf_storage* mStorage;
					librdf_model* mModel;

					// Redland's Turtle serializer writes each namespace prefix it's
					// told about, so IRIs it recognizes come out abbreviated
					// (foo:Bar) instead of always fully spelled out -- purely a
					// readability nicety for whoever reads the output, no semantic
					// effect.
					QHash<QString,QString> mPrefixIRIHash;

					bool mWritingError;

					// private methods
					private:

					// private variables
					private:

				};

		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude

#endif // KONCLUDE_REDLAND_INTEGRATION

#endif // KONCLUDE_REASONER_QUERY_CWRITEQUERYFILEREDLANDSERIALIZER_H
