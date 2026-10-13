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

#ifndef KONCLUDE_CONTROL_LOADER_CLINTONTOLOGYANALYSER_H
#define KONCLUDE_CONTROL_LOADER_CLINTONTOLOGYANALYSER_H

// Libraries includes
#include <QString>
#include <QStringList>
#include <QList>
#include <QSet>
#include <QHash>
#include <QMap>
#include <QVector>

// Namespace includes
#include "LoaderSettings.h"

// Other includes
#include "Reasoner/Ontology/CConcreteOntology.h"
#include "Parser/Expressions/CBuildExpression.h"
#include "Parser/Expressions/CClassTermExpression.h"
#include "Parser/Expressions/CAxiomExpression.h"
#include "Parser/Expressions/CObjectPropertyTermExpression.h"
#include "Parser/Expressions/CIndividualTermExpression.h"
#include "Parser/Expressions/CSubExpressionVisitor.h"
#include "Parser/Expressions/CExpressionEntity.h"
#include "Parser/CLintParseRecorder.h"
#include "Parser/CLintExpressionRenderer.h"

namespace Konclude {

	using namespace Parser;
	using namespace Parser::Expression;
	using namespace Reasoner::Ontology;

	namespace Control {

		namespace Loader {

			//! walks over all sub expressions of one axiom and collects the nested class expressions and the signature
			class CLintAxiomWalker : public CSubExpressionVisitor {
				public:
					QSet<CBuildExpression*> mVisited;
					QList<CClassTermExpression*> mClassExpressions;
					QStringList mClasses, mObjectProperties, mDataProperties, mIndividuals;
					QSet<QString> mClassSet, mObjectPropertySet, mDataPropertySet, mIndividualSet;
					bool mInverseUsed;

					CLintAxiomWalker() : mInverseUsed(false) {}

					void visit(CBuildExpression* expression) {
						if (!expression || mVisited.contains(expression)) {
							return;
						}
						mVisited.insert(expression);
						switch (expression->getType()) {
							case CBuildExpression::BETCLASS: { QString n = CLintExpressionRenderer::entityName(expression); if (!mClassSet.contains(n)) { mClassSet.insert(n); mClasses.append(n); } break; }
							case CBuildExpression::BETOBJECTPROPERTY: { QString n = CLintExpressionRenderer::entityName(expression); if (!mObjectPropertySet.contains(n)) { mObjectPropertySet.insert(n); mObjectProperties.append(n); } break; }
							case CBuildExpression::BETDATAPROPERTY: { QString n = CLintExpressionRenderer::entityName(expression); if (!mDataPropertySet.contains(n)) { mDataPropertySet.insert(n); mDataProperties.append(n); } break; }
							case CBuildExpression::BETNAMEDINDIVIDUAL: { QString n = CLintExpressionRenderer::entityName(expression); if (!mIndividualSet.contains(n)) { mIndividualSet.insert(n); mIndividuals.append(n); } break; }
							case CBuildExpression::BETINVERSEOBJECTPROPERTYOF: mInverseUsed = true; break;
							default: break;
						}
						if (CClassTermExpression* classExp = dynamic_cast<CClassTermExpression*>(expression)) {
							mClassExpressions.append(classExp);
						}
						expression->visitSubExpressions(this);
					}

					virtual bool visitSubExpression(CBuildExpression* expression, CBuildExpression* subExpression, CSubExpressionVisitor* visitor) {
						visit(subExpression);
						return true;
					}
					virtual bool visitEntity(CBuildExpression* expression, CExpressionEntity* entity, CSubExpressionVisitor* visitor) {
						visit(dynamic_cast<CBuildExpression*>(entity));
						return true;
					}
			};


			/*!
			 *
			 *		\class		CLintOntologyAnalyser
			 *		\brief		Implementation of the "lint" command: looks at a loaded (but not reasoned over)
			 *					ontology and produces a report about constructs and data patterns that are
			 *					expensive for this build of Konclude or that it does not handle completely.
			 *					The report follows tools/KoncludeLint.java.
			 *
			 */
			class CLintOntologyAnalyser {
				// public methods
				public:
					CLintOntologyAnalyser(CConcreteOntology* ontology, const QString& fileName);

					//! creates the report; every list entry is one output line
					QStringList createReport();

					//! the findings of the last createReport() call as JSON (level, title, detail, suggestion, examples)
					QString createJsonReport();


				// protected types
				protected:

					struct CWarnings {
						QStringList hasKey, ifp, qcard, wideOneOf, wideUnionAxioms, wideUnionExpressions, bigCard, complexGci, minCardN, hasValue, selfR;
					};

					//! the ABox as far as the lint checks need it
					struct CAboxModel {
						QHash<QString,int> individualIds;
						QVector<QString> individualNames;
						QHash<QString,int> typeIds;						// type name or "X:<complex expression>" -> id
						QVector<QVector<int> > types;					// per individual
						struct CEdge { int mSource; int mRole; int mTarget; };
						QVector<CEdge> edges;							// all edges (inverse role assertions are swapped)
						QHash<QString,int> roleIds;
						QVector<QString> roleNames;
						struct CData { int mProperty; int mDatatype; int mValue; };		// value is -1 if the exact value is not needed
						QVector<QVector<CData> > data;					// per individual
						QHash<QString,int> datatypeIds;
						QHash<QString,int> valueIds;
						QHash<QString,int> dataPropertyIds;
						QVector<QString> dataPropertyNames;
					};

					//! one finding of the report (also written as JSON)
					struct CEntry {
						QString mLevel, mTitle, mDetail, mSuggestion;
						QStringList mExamples;
					};

					//! a restriction  Sub subClassOf (min n R F)  with named Sub and F, used for the estimate of the successors that must be created
					struct CMinCard {
						QString mSubject, mRole, mFiller, mText;
						QStringList mCandidateClasses;						// for a defined class (Sub equivalent to C and min n R F): the named conjuncts C
						qint64 mCardinality;
					};

					//! the named-edge view that is used for the transitive role checks
					struct CNamedEdge { QString mSource; QString mTarget; };


				// protected methods
				protected:

					static QString shortName(const QString& iri);
					static QString render(CBuildExpression* expression);

					void scanTBoxAxiom(CAxiomExpression* axiom);
					void scanWalkedAxiom(CAxiomExpression* axiom, CLintAxiomWalker& walker);
					void scanAssertionAxiom(CAxiomExpression* axiom);
					void scanTriples();
					void registerHasKey(const CLintParseRecorder::CHasKeyRecord& hasKey);

					int individualId(const QString& name);
					int typeId(const QString& name);
					int roleId(const QString& name);
					void pinIndividual(const QString& name);
					void addEdge(const QString& source, const QString& role, const QString& target);
					void addTypeAssertion(const QString& individual, const QString& type);
					void addDataAssertion(const QString& individual, const QString& property, const QString& datatype, const QString& value);

					QString objectPropertyName(CObjectPropertyTermExpression* property, bool* inverse = nullptr);
					QString individualName(CIndividualTermExpression* individual, bool* anonymous = nullptr);

					void warn(QStringList& out, const QString& title, const QString& detail, const QStringList& examples);
					void warn(QStringList& out, const QString& title, const QString& detail, const QStringList& examples, const QString& suggestion);
					void checkEnumeratedMinCardinality(QStringList& out);
					void info(QStringList& out, const QString& text);

					void checkTransitiveRoles(QStringList& out);
					bool estimateReduction(qint64& removable, qint64& total);


				// protected variables
				protected:
					CConcreteOntology* mOntology;
					QString mFileName;

					CWarnings mWarn;
					QSet<QString> mClasses, mObjectProperties, mDataProperties, mIndividuals;
					QSet<QString> mTransitive, mSymmetric;
					bool mNominals, mInverse, mAnyCard;
					qint64 mMaxCard;
					qint64 mAxiomCount;
					qint64 mClassAssertions, mObjectPropertyAssertions, mDataPropertyAssertions;
					qint64 mWarningCount;

					bool mTriplesAbox;
					bool mKeepAllValues;
					QList<CLintParseRecorder::CHasKeyRecord> mTripleHasKeys;
					QList<CLintParseRecorder::CHasKeyRecord> mHasKeyRecords;
					QHash<QString,QList<CNamedEdge> > mEdgesByRole;			// non-inverse assertions between named individuals

					// data for the ABox reduction estimate
					CAboxModel mModel;
					QSet<QString> mPinnedNames;
					QSet<QString> mExactDataProperties;
					QSet<QString> mKeyDataProperties;
					QMap<QString,QSet<QString> > mRoleRelation;
					QMap<QString,QSet<QString> > mSubClassOf;					// super -> subs (named classes)
					QSet<QString> mRoleSeeds;

					QList<CEntry> mEntries;
					QList<CMinCard> mMinCards;
					QMap<QString,QSet<QString> > mDirectSubs;				// super -> subs for SubClassOf / EquivalentClasses between named classes only (exact, unlike mSubClassOf)
					QHash<QString,int> mEnumSize;							// named class that is equivalent to / a subclass of an enumeration -> number of nominals
			};

		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_LOADER_CLINTONTOLOGYANALYSER_H
