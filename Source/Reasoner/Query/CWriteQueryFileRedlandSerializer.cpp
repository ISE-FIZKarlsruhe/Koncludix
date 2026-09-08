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

#include "CWriteQueryFileRedlandSerializer.h"

#ifdef KONCLUDE_REDLAND_INTEGRATION

namespace Konclude {

	namespace Reasoner {

		namespace Query {

			namespace {
				const char* const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
				const char* const RDFS_SUBCLASSOF = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
				const char* const RDFS_SUBPROPERTYOF = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf";
				const char* const OWL_CLASS = "http://www.w3.org/2002/07/owl#Class";
				const char* const OWL_OBJECTPROPERTY = "http://www.w3.org/2002/07/owl#ObjectProperty";
				const char* const OWL_DATATYPEPROPERTY = "http://www.w3.org/2002/07/owl#DatatypeProperty";
				const char* const OWL_NAMEDINDIVIDUAL = "http://www.w3.org/2002/07/owl#NamedIndividual";
				const char* const OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs";
				const char* const OWL_EQUIVALENTCLASS = "http://www.w3.org/2002/07/owl#equivalentClass";
				const char* const OWL_EQUIVALENTPROPERTY = "http://www.w3.org/2002/07/owl#equivalentProperty";
			}


			CWriteQueryFileRedlandSerializer::CWriteQueryFileRedlandSerializer(const QString& fileString, const QString& syntaxName) {
				mFileString = fileString;
				mSyntaxName = syntaxName;
				mWorld = nullptr;
				mStorage = nullptr;
				mModel = nullptr;
				mWritingError = false;
				mDirectTextMode = (syntaxName == "turtle" || syntaxName == "ntriples");
				mOutputFile = nullptr;
				mOutputStream = nullptr;
			}

			CWriteQueryFileRedlandSerializer::~CWriteQueryFileRedlandSerializer() {
				if (mOutputStream) {
					delete mOutputStream;
				}
				if (mOutputFile) {
					delete mOutputFile;
				}
				if (mModel) {
					librdf_free_model(mModel);
				}
				if (mStorage) {
					librdf_free_storage(mStorage);
				}
				if (mWorld) {
					librdf_free_world(mWorld);
				}
			}


			const QString CWriteQueryFileRedlandSerializer::getOutputName() {
				return mFileString;
			}


			bool CWriteQueryFileRedlandSerializer::startWritingOutput() {
				if (mDirectTextMode) {
					mOutputFile = new QFile(mFileString);
					if (!mOutputFile->open(QIODevice::WriteOnly | QIODevice::Text)) {
						mWritingError = true;
						return false;
					}
					mOutputStream = new QTextStream(mOutputFile);
					mOutputStream->setCodec("UTF-8");
					return true;
				}
				mWorld = librdf_new_world();
				if (!mWorld) {
					return false;
				}
				librdf_world_open(mWorld);
				mStorage = librdf_new_storage(mWorld, "memory", nullptr, nullptr);
				if (!mStorage) {
					return false;
				}
				mModel = librdf_new_model(mWorld, mStorage, nullptr);
				return mModel != nullptr;
			}

			bool CWriteQueryFileRedlandSerializer::endWritingOutput() {
				if (mDirectTextMode) {
					if (!mOutputStream) {
						return false;
					}
					mOutputStream->flush();
					mOutputFile->close();
					return !mWritingError;
				}
				if (!mModel) {
					return false;
				}
				librdf_serializer* serializer = librdf_new_serializer(mWorld, mSyntaxName.toUtf8().constData(), nullptr, nullptr);
				if (!serializer) {
					return false;
				}
				for (QHash<QString,QString>::const_iterator it = mPrefixIRIHash.constBegin(), itEnd = mPrefixIRIHash.constEnd(); it != itEnd; ++it) {
					if (!it.key().isEmpty()) {
						librdf_uri* nsUri = librdf_new_uri(mWorld, (const unsigned char*)it.value().toUtf8().constData());
						if (nsUri) {
							librdf_serializer_set_namespace(serializer, nsUri, it.key().toUtf8().constData());
							librdf_free_uri(nsUri);
						}
					}
				}
				int result = librdf_serializer_serialize_model_to_file(serializer, mFileString.toUtf8().constData(), nullptr, mModel);
				librdf_free_serializer(serializer);
				return result == 0 && !mWritingError;
			}


			QString CWriteQueryFileRedlandSerializer::escapeLiteralValue(const QString& value) {
				QString escaped = value;
				escaped.replace(QLatin1Char('\\'), QLatin1String("\\\\"));
				escaped.replace(QLatin1Char('"'), QLatin1String("\\\""));
				escaped.replace(QLatin1Char('\n'), QLatin1String("\\n"));
				escaped.replace(QLatin1Char('\r'), QLatin1String("\\r"));
				escaped.replace(QLatin1Char('\t'), QLatin1String("\\t"));
				return escaped;
			}

			QString CWriteQueryFileRedlandSerializer::formatResource(const QString& name, bool anonymous) {
				if (anonymous) {
					QString identifier = name;
					if (identifier.startsWith("_:")) {
						return identifier;
					}
					return QString("_:") + identifier;
				}
				return QLatin1Char('<') + name + QLatin1Char('>');
			}

			QString CWriteQueryFileRedlandSerializer::formatLiteral(const QString& lexicalValue, const QString& datatypeIRI) {
				QString literal = QLatin1Char('"') + escapeLiteralValue(lexicalValue) + QLatin1Char('"');
				if (!datatypeIRI.isEmpty()) {
					literal += QString("^^<%1>").arg(datatypeIRI);
				}
				return literal;
			}

			void CWriteQueryFileRedlandSerializer::writeTripleLine(const QString& subjectText, const QString& predicateText, const QString& objectText) {
				if (!mOutputStream) {
					mWritingError = true;
					return;
				}
				*mOutputStream << subjectText << ' ' << predicateText << ' ' << objectText << " .\n";
			}

			void CWriteQueryFileRedlandSerializer::addResourceTriple(const QString& subjectName, bool subjectAnonymous, const QString& predicateIRI, const QString& objectName, bool objectAnonymous) {
				if (mDirectTextMode) {
					writeTripleLine(formatResource(subjectName, subjectAnonymous), formatResource(predicateIRI, false), formatResource(objectName, objectAnonymous));
					return;
				}
				addTriple(createResourceNode(subjectName, subjectAnonymous),
						  librdf_new_node_from_uri_string(mWorld, (const unsigned char*)predicateIRI.toUtf8().constData()),
						  createResourceNode(objectName, objectAnonymous));
			}

			void CWriteQueryFileRedlandSerializer::addLiteralTriple(const QString& subjectName, bool subjectAnonymous, const QString& predicateIRI, const QString& lexicalValue, const QString& datatypeIRI) {
				if (mDirectTextMode) {
					writeTripleLine(formatResource(subjectName, subjectAnonymous), formatResource(predicateIRI, false), formatLiteral(lexicalValue, datatypeIRI));
					return;
				}
				librdf_uri* datatypeUri = datatypeIRI.isEmpty() ? nullptr : librdf_new_uri(mWorld, (const unsigned char*)datatypeIRI.toUtf8().constData());
				addTriple(createResourceNode(subjectName, subjectAnonymous),
						  librdf_new_node_from_uri_string(mWorld, (const unsigned char*)predicateIRI.toUtf8().constData()),
						  librdf_new_node_from_typed_literal(mWorld, (const unsigned char*)lexicalValue.toUtf8().constData(), nullptr, datatypeUri));
			}


			librdf_node* CWriteQueryFileRedlandSerializer::createResourceNode(const QString& name, bool anonymous) {
				if (anonymous) {
					QString identifier = name;
					if (identifier.startsWith("_:")) {
						identifier = identifier.mid(2);
					}
					return librdf_new_node_from_blank_identifier(mWorld, (const unsigned char*)identifier.toUtf8().constData());
				}
				return librdf_new_node_from_uri_string(mWorld, (const unsigned char*)name.toUtf8().constData());
			}

			void CWriteQueryFileRedlandSerializer::addTriple(librdf_node* subject, librdf_node* predicate, librdf_node* object) {
				if (!subject || !predicate || !object) {
					mWritingError = true;
					return;
				}
				if (librdf_model_add(mModel, subject, predicate, object) != 0) {
					mWritingError = true;
				}
			}

			void CWriteQueryFileRedlandSerializer::addTypeTriple(const QString& subjectName, bool subjectAnonymous, const QString& classIRI) {
				addResourceTriple(subjectName, subjectAnonymous, RDF_TYPE, classIRI, false);
			}

			void CWriteQueryFileRedlandSerializer::addPairwiseChainRelation(const QStringList& nameList, const QString& relationIRI) {
				// A chain (item[0]-item[1], item[1]-item[2], ...) is enough for a
				// symmetric+transitive relation like owl:sameAs/equivalentClass/
				// equivalentProperty -- any reasoner or SPARQL query treating it
				// that way will see the full equivalence class regardless, and it
				// keeps the output from growing quadratically for a large class.
				for (int i = 0; i + 1 < nameList.size(); ++i) {
					addResourceTriple(nameList.at(i), false, relationIRI, nameList.at(i+1), false);
				}
			}


			void CWriteQueryFileRedlandSerializer::writeObjectPropertyDeclaration(const QString& propertyName) {
				addTypeTriple(propertyName, false, OWL_OBJECTPROPERTY);
			}

			void CWriteQueryFileRedlandSerializer::writeDataPropertyDeclaration(const QString& propertyName) {
				addTypeTriple(propertyName, false, OWL_DATATYPEPROPERTY);
			}

			void CWriteQueryFileRedlandSerializer::writeSubObjectPropertyRelation(const QString& subPropertyName, const QString& superPropertyName) {
				addResourceTriple(subPropertyName, false, RDFS_SUBPROPERTYOF, superPropertyName, false);
			}

			void CWriteQueryFileRedlandSerializer::writeSubDataPropertyRelation(const QString& subPropertyName, const QString& superPropertyName) {
				writeSubObjectPropertyRelation(subPropertyName, superPropertyName);
			}

			void CWriteQueryFileRedlandSerializer::writeObjectPropertyEquivalenceRelations(const QStringList& propertyNameList) {
				addPairwiseChainRelation(propertyNameList, OWL_EQUIVALENTPROPERTY);
			}

			void CWriteQueryFileRedlandSerializer::writeDataPropertyEquivalenceRelations(const QStringList& propertyNameList) {
				addPairwiseChainRelation(propertyNameList, OWL_EQUIVALENTPROPERTY);
			}


			void CWriteQueryFileRedlandSerializer::writeClassDeclaration(const QString& className) {
				addTypeTriple(className, false, OWL_CLASS);
			}

			void CWriteQueryFileRedlandSerializer::writeSubClassRelation(const QString& subClassName, const QString& superClassName) {
				addResourceTriple(subClassName, false, RDFS_SUBCLASSOF, superClassName, false);
			}

			void CWriteQueryFileRedlandSerializer::writeClassEquivalenceRelations(const QStringList& classNameList) {
				addPairwiseChainRelation(classNameList, OWL_EQUIVALENTCLASS);
			}


			void CWriteQueryFileRedlandSerializer::writeNamedIndividualDeclaration(const QString& individualName) {
				addTypeTriple(individualName, false, OWL_NAMEDINDIVIDUAL);
			}

			void CWriteQueryFileRedlandSerializer::writeAnonymousIndividualDeclaration(const QString& individualName) {
				// Blank nodes don't need (and, per OWL 2, shouldn't get) an
				// owl:NamedIndividual declaration triple.
			}

			void CWriteQueryFileRedlandSerializer::writeNamedIndividualType(const QString& individualName, const QString& className) {
				addTypeTriple(individualName, false, className);
			}

			void CWriteQueryFileRedlandSerializer::writeAnonymousIndividualType(const QString& individualName, const QString& className) {
				addTypeTriple(individualName, true, className);
			}


			void CWriteQueryFileRedlandSerializer::writeObjectPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& objectName, bool objectAnonymous) {
				addResourceTriple(subjectName, subjectAnonymous, propertyName, objectName, objectAnonymous);
			}

			void CWriteQueryFileRedlandSerializer::writeDataPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& lexicalValue, const QString& datatypeIRI) {
				addLiteralTriple(subjectName, subjectAnonymous, propertyName, lexicalValue, datatypeIRI);
			}

			void CWriteQueryFileRedlandSerializer::writeIndividualEquivalenceRelations(const QStringList& individualNameList, const QList<bool>& individualAnonymousList) {
				for (int i = 0; i + 1 < individualNameList.size(); ++i) {
					bool anon1 = i < individualAnonymousList.size() && individualAnonymousList.at(i);
					bool anon2 = (i+1) < individualAnonymousList.size() && individualAnonymousList.at(i+1);
					addResourceTriple(individualNameList.at(i), anon1, OWL_SAMEAS, individualNameList.at(i+1), anon2);
				}
			}


			void CWriteQueryFileRedlandSerializer::writeOntologyPrefix(const QString& prefixName, const QString& prefixIRI) {
				mPrefixIRIHash.insert(prefixName, prefixIRI);
			}

			void CWriteQueryFileRedlandSerializer::writeOntologyStart() {
				writeOntologyPrefix("owl", "http://www.w3.org/2002/07/owl#");
				writeOntologyPrefix("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
				writeOntologyPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
				writeOntologyPrefix("xsd", "http://www.w3.org/2001/XMLSchema#");
			}

			void CWriteQueryFileRedlandSerializer::writeOntologyEnd() {
			}

		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude

#endif // KONCLUDE_REDLAND_INTEGRATION
