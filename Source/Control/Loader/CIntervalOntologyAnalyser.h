/*
 *		Copyright (C) 2013, 2014 by the Konclude Developer Team.
 *
 *		This file is part of the reasoning system Konclude.
 *		For details and support, see <http://konclude.com/>.
 *
 *		Konclude is free software: you can redistribute it and/or modify it under
 *		the terms of version 3 of the GNU Lesser General Public License (LGPL3).
 */

#ifndef KONCLUDE_CONTROL_LOADER_CINTERVALONTOLOGYANALYSER_H
#define KONCLUDE_CONTROL_LOADER_CINTERVALONTOLOGYANALYSER_H

// Libraries includes
#include <QString>
#include <QStringList>
#include <QHash>
#include <QList>
#include <QVector>

// Namespace includes
#include "CIntervalAlgebra.h"

// Other includes
#include "Reasoner/Ontology/CConcreteOntology.h"
#include "Config/CConfiguration.h"

namespace Konclude {

	using namespace Reasoner::Ontology;

	namespace Control {

		namespace Loader {

			/*!
			 *
			 *		\class		CIntervalOntologyAnalyser
			 *		\brief		The interval theory of the "interval" command: reads the Allen relations and the time stamps of the individuals of a loaded ontology,
			 *					checks them for consistency (direct conflicts, and conflicts that only show through composition, by path consistency), explains
			 *					every conflict with a minimal set of facts, and derives the relations that follow (from the time stamps and from the composition).
			 *
			 *					Recognised input (by the local name of the IRI, in any namespace unless a namespace is given):
			 *					  - Allen relations between individuals: before, meets, overlaps, starts, during, finishes, equals, after, metBy, overlappedBy, startedBy,
			 *					    contains, finishedBy, and the OWL-Time names intervalBefore, intervalMeets, ... (object property assertions or RDF triples)
			 *					  - time stamps: PROV-O startedAtTime / endedAtTime (data properties of the individual), and OWL-Time hasBeginning / hasEnd to an instant
			 *					    with inXSDDateTimeStamp / inXSDDateTime / inXSDDate; xsd:dateTime values without time zone are read as UTC; plain integers are used as they are
			 *
			 */
			class CIntervalOntologyAnalyser {
				// public methods
				public:
					CIntervalOntologyAnalyser(CConcreteOntology* ontology, const QString& fileName);

					void setRelationNamespace(const QString& relationNamespace) { mRelationNamespace = relationNamespace; }
					void setGroupByProperty(const QString& groupByProperty) { mGroupByProperty = groupByProperty; }
					void setDeriveFromTimestamps(bool derive) { mDeriveFromTimestamps = derive; }
					void setMaxDerivedPairs(qint64 maxPairs) { mMaxDerivedPairs = maxPairs; }
					void setOutputVocabulary(bool owlTimeStyle, const QString& outputNamespace) { mOutputOwlTimeStyle = owlTimeStyle; mOutputNamespace = outputNamespace; }
					void setMaxComponentSize(int maxSize) { mMaxComponentSize = maxSize; }
					void setMaxMinimizationSize(int maxSize) { mMaxMinimizationSize = maxSize; }
					//! group the intervals for the derivation from time stamps by the connectivity of all object property assertions (instead of one group for all)
					void setGroupByConnectivity(bool connectivity) { mGroupByConnectivity = connectivity; }
					//! derive relations from the time stamps only if the ontology declares relation properties
					void setDeriveOnlyIfDeclared(bool onlyIfDeclared) { mDeriveOnlyIfDeclared = onlyIfDeclared; }

					//! reads all options Konclude.CLI.Interval.* from the configuration (of the knowledge base)
					void configure(Konclude::Config::CConfiguration* config);

					//! runs the analysis, every list entry is one output line
					QStringList createReport();

					//! the derived relations as N-Triples (after createReport())
					QString derivedTriples() const;

					bool hasConflicts() const { return mConflictCount > 0; }
					qint64 getDerivedCount() const { return mDerived.size(); }
					//! true if the ontology has interval relations or intervals with time stamps (after createReport())
					bool hasIntervalContent() const { return !mAsserted.isEmpty() || mTimedCount > 0; }
					int getTimedCount() const { return mTimedCount; }
					//! true if the ontology declares (or types as owl:ObjectProperty) a property that is named like an Allen relation: its axioms are meant to use the relations
					bool isRelationVocabularyDeclared() const { return mVocabularyDeclared; }
					int getAssertedRelationCount() const { return mAsserted.size(); }
					int getConflictCount() const { return mConflictCount; }


				// protected types
				protected:
					struct CNode {
						QString mName;
						bool mHasStart, mHasEnd;
						qint64 mStart, mEnd;
						QString mStartText, mEndText;
						bool mTimed;										// both end points known and start < end
						CNode() : mHasStart(false), mHasEnd(false), mStart(0), mEnd(0), mTimed(false) {}
					};

					struct CAssertedRelation {
						int mSubject, mObject, mRelation;
					};

					//! a fact that can take part in a conflict: an asserted relation, or the time stamps of one individual
					struct CUnit {
						bool mTimestamp;
						int mNode;											// for a time stamp unit
						int mSubject, mObject, mRelation;					// for an asserted relation
					};

					struct CDerived {
						int mSubject, mObject, mRelation;
						bool mFromTimestamps;
					};


				// protected methods
				protected:
					static QString localName(const QString& iri);
					static bool parseTime(const QString& lexical, qint64& value);
					static QString cleanLexical(const QString& lexical);

					int nodeId(const QString& name);
					void addRelationAssertion(const QString& subject, int relation, const QString& object, bool inverse);
					void addTimeValue(const QString& individual, bool isStart, const QString& lexical);
					void registerDeclaredProperty(const QString& iri);
					void scanAnyEdges();
					void scanAxioms();
					void scanTriples();
					void resolveInstants();

					bool analyseComponent(const QVector<int>& nodes, const QList<CUnit>& units, const QVector<bool>& active, QVector<quint16>* closure, int* conflictI, int* conflictJ);
					QString describeUnit(const CUnit& unit) const;
					QString describeInterval(int node) const;


				// protected variables
				protected:
					CConcreteOntology* mOntology;
					QString mFileName;

					QString mRelationNamespace, mGroupByProperty, mOutputNamespace;
					bool mDeriveFromTimestamps, mOutputOwlTimeStyle;
					qint64 mMaxDerivedPairs;
					int mMaxComponentSize, mMaxMinimizationSize;

					QHash<QString,int> mNodeIds;
					QVector<CNode> mNodes;
					QList<CAssertedRelation> mAsserted;
					QList< QPair<int,int> > mGroupEdges;
					QHash<QString,QString> mBeginningOf, mEndOf, mInstantTimeText;			// OWL-Time: interval -> instant, instant -> lexical time
					qint64 mObjectAssertionCount, mDataAssertionCount;
					QStringList mIgnoredIntervals;
					qint64 mUnreadableTimes;
					QStringList mUnreadableExamples;

					QList<CDerived> mDerived;
					int mConflictCount;
					int mTimedCount;
					QString mRelationIri[CIntervalAlgebra::ASSERTED_RELATION_COUNT];						// the property of the ontology for each relation (declared or asserted), empty if there is none
					bool mVocabularyDeclared;
					bool mGroupByConnectivity, mDeriveOnlyIfDeclared;
			};

		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_LOADER_CINTERVALONTOLOGYANALYSER_H
