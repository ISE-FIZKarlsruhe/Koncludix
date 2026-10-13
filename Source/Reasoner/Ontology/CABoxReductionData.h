/*
 *		Optional ABox reduction for the materialize command (Koncludix extension).
 *
 *		This file is part of the Konclude fork. It is distributed under the same terms as Konclude
 *		(GNU Lesser General Public License, version 3).
 */

#ifndef KONCLUDE_REASONER_ONTOLOGY_CABOXREDUCTIONDATA_H
#define KONCLUDE_REASONER_ONTOLOGY_CABOXREDUCTIONDATA_H

// Libraries includes
#include <QString>
#include <QStringList>
#include <QHash>
#include <QSet>
#include <QVector>
#include <QMutex>
#include <QSharedPointer>


namespace Konclude {

	namespace Reasoner {

		namespace Ontology {

			/*!
			 *
			 *		\class		CABoxReductionData
			 *		\brief		Result of the (optional) ABox reduction: individuals that are interchangeable (same asserted types, same
			 *					data shape and same neighbour classes, i.e. the same colour after colour refinement) are represented by
			 *					one individual in the ontology that is given to the reasoner. This class remembers which individuals were
			 *					removed, so that the writer of the materialization can copy the results of the representatives to them.
			 *
			 *					The data is attached to the COntologyLoadData of the ontology.
			 *
			 */
			class CABoxReductionData {
				// public types
				public:
					//! data property assertion of a removed individual (exactly the strings the ontology builder would have used)
					struct CDataAssertion {
						QString mPropertyIRI;
						QString mLexicalValue;
						QString mDatatypeIRI;
					};

				// public methods
				public:
					CABoxReductionData();

					// ---------- queries used by the writer ----------

					//! removed members of a (primary) representative, or nullptr if the individual does not represent removed individuals
					const QStringList* getRemovedMembers(const QString& representativeName) const;

					//! retained individual that belongs to a group in which individuals were removed or duplicated (colour class with internal edges)
					bool isAffected(const QString& individualName) const;

					//! individual that is never merged
					bool isPinned(const QString& individualName) const;

					//! facts between two individuals computed by the reasoner for the reduced ontology are not valid for the original ontology
					//! if at least one of the individuals is affected and the other one is not pinned (these facts are rebuilt from the asserted edges)
					bool isReasonerFactReplaced(const QString& subjectName, const QString& objectName) const;

					const QVector<CDataAssertion>* getDataAssertions(const QString& removedIndividualName) const;

					int getEdgeCount() const;
					//! subject/object individual names and the property IRI of an asserted edge between two individuals that are not pinned
					void getEdge(int index, QString& subject, QString& propertyIRI, QString& object) const;
					//! the edges are stored sorted by the unordered pair of their individuals, so that all edges between two individuals are neighbours
					int getEdgeSubjectIndex(int index) const;
					int getEdgeObjectIndex(int index) const;
					const QString& getIndividualNameOfEdgeIndex(int nameIndex) const;

					qint64 getOriginalIndividualCount() const;
					qint64 getRetainedIndividualCount() const;
					qint64 getRemovedIndividualCount() const;

					bool isReduced() const;

					// ---------- invalidation (the reduction turned out to be not applicable) ----------
					void setInvalid(const QString& reason);
					bool isInvalid() const;
					QString getInvalidReason() const;


					// ---------- construction (used by CABoxReducer) ----------
					QHash<QString, QStringList> mMembersOfRepresentative;
					QSet<QString> mAffectedRetained;
					QSet<QString> mPinnedRetained;
					//! individuals mentioned in told SameIndividual axioms (merges of other individuals are not expected, see the writer)
					QSet<QString> mToldSameIndividuals;
					QHash<QString, QVector<CDataAssertion> > mRemovedDataAssertions;

					QVector<QString> mEdgeIndividualNames;
					QVector<qint32> mEdgeSubject;
					QVector<qint32> mEdgeObject;
					QVector<qint32> mEdgeProperty;
					QVector<QString> mEdgePropertyIRIs;

					qint64 mOriginalIndividuals;
					qint64 mRetainedIndividuals;
					qint64 mRemovedIndividuals;

				// protected variables
				protected:
					volatile bool mInvalid;
					QString mInvalidReason;
					mutable QMutex mMutex;

			};


			/*!
			 *
			 *		\class		CABoxReductionControl
			 *		\brief		Process wide switches for the ABox reduction: the writer requests a fallback to the unreduced computation
			 *					if the reduced result cannot be expanded safely (e.g., a representative turned out to be merged with other
			 *					individuals or the reduced ontology is inconsistent), the materialize loader then starts again without reduction.
			 */
			class CABoxReductionControl {
				public:
					static void requestFallback(const QString& reason);
					static bool takeFallbackRequest(QString* reason = nullptr);

					static void setReductionSuppressed(bool suppressed);
					static bool isReductionSuppressed();
			};

		}; // end namespace Ontology

	}; // end namespace Reasoner

}; // end namespace Konclude

#endif // KONCLUDE_REASONER_ONTOLOGY_CABOXREDUCTIONDATA_H
