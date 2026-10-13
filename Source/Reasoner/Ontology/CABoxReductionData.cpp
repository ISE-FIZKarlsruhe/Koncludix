/*
 *		Optional ABox reduction for the materialize command (Koncludix extension).
 *
 *		This file is part of the Konclude fork. It is distributed under the same terms as Konclude
 *		(GNU Lesser General Public License, version 3).
 */

#include "CABoxReductionData.h"

#include <QMutexLocker>


namespace Konclude {

	namespace Reasoner {

		namespace Ontology {


			CABoxReductionData::CABoxReductionData() {
				mOriginalIndividuals = 0;
				mRetainedIndividuals = 0;
				mRemovedIndividuals = 0;
				mInvalid = false;
			}


			const QStringList* CABoxReductionData::getRemovedMembers(const QString& representativeName) const {
				QHash<QString, QStringList>::const_iterator it = mMembersOfRepresentative.constFind(representativeName);
				if (it == mMembersOfRepresentative.constEnd()) {
					return nullptr;
				}
				return &it.value();
			}


			bool CABoxReductionData::isAffected(const QString& individualName) const {
				return mAffectedRetained.contains(individualName);
			}


			bool CABoxReductionData::isPinned(const QString& individualName) const {
				return mPinnedRetained.contains(individualName) || individualName.startsWith(QLatin1String("_:"));
			}


			bool CABoxReductionData::isReasonerFactReplaced(const QString& subjectName, const QString& objectName) const {
				if (subjectName == objectName) {
					return false;
				}
				bool subjectAffected = mAffectedRetained.contains(subjectName);
				bool objectAffected = mAffectedRetained.contains(objectName);
				if (!subjectAffected && !objectAffected) {
					return false;
				}
				if (subjectAffected && !isPinned(objectName)) {
					return true;
				}
				if (objectAffected && !isPinned(subjectName)) {
					return true;
				}
				return false;
			}


			const QVector<CABoxReductionData::CDataAssertion>* CABoxReductionData::getDataAssertions(const QString& removedIndividualName) const {
				QHash<QString, QVector<CDataAssertion> >::const_iterator it = mRemovedDataAssertions.constFind(removedIndividualName);
				if (it == mRemovedDataAssertions.constEnd()) {
					return nullptr;
				}
				return &it.value();
			}


			int CABoxReductionData::getEdgeCount() const {
				return mEdgeSubject.size();
			}


			void CABoxReductionData::getEdge(int index, QString& subject, QString& propertyIRI, QString& object) const {
				subject = mEdgeIndividualNames.at(mEdgeSubject.at(index));
				propertyIRI = mEdgePropertyIRIs.at(mEdgeProperty.at(index));
				object = mEdgeIndividualNames.at(mEdgeObject.at(index));
			}


			int CABoxReductionData::getEdgeSubjectIndex(int index) const {
				return mEdgeSubject.at(index);
			}


			int CABoxReductionData::getEdgeObjectIndex(int index) const {
				return mEdgeObject.at(index);
			}


			const QString& CABoxReductionData::getIndividualNameOfEdgeIndex(int nameIndex) const {
				return mEdgeIndividualNames.at(nameIndex);
			}


			qint64 CABoxReductionData::getOriginalIndividualCount() const {
				return mOriginalIndividuals;
			}

			qint64 CABoxReductionData::getRetainedIndividualCount() const {
				return mRetainedIndividuals;
			}

			qint64 CABoxReductionData::getRemovedIndividualCount() const {
				return mRemovedIndividuals;
			}


			bool CABoxReductionData::isReduced() const {
				return mRemovedIndividuals > 0 || !mAffectedRetained.isEmpty();
			}


			void CABoxReductionData::setInvalid(const QString& reason) {
				QMutexLocker locker(&mMutex);
				mInvalid = true;
				mInvalidReason = reason;
			}

			bool CABoxReductionData::isInvalid() const {
				return mInvalid;
			}

			QString CABoxReductionData::getInvalidReason() const {
				QMutexLocker locker(&mMutex);
				return mInvalidReason;
			}



			namespace {
				QMutex sControlMutex;
				bool sFallbackRequested = false;
				QString sFallbackReason;
				bool sReductionSuppressed = false;
			}


			void CABoxReductionControl::requestFallback(const QString& reason) {
				QMutexLocker locker(&sControlMutex);
				sFallbackRequested = true;
				sFallbackReason = reason;
			}

			bool CABoxReductionControl::takeFallbackRequest(QString* reason) {
				QMutexLocker locker(&sControlMutex);
				bool requested = sFallbackRequested;
				if (requested && reason) {
					*reason = sFallbackReason;
				}
				sFallbackRequested = false;
				return requested;
			}

			void CABoxReductionControl::setReductionSuppressed(bool suppressed) {
				QMutexLocker locker(&sControlMutex);
				sReductionSuppressed = suppressed;
			}

			bool CABoxReductionControl::isReductionSuppressed() {
				QMutexLocker locker(&sControlMutex);
				return sReductionSuppressed;
			}


		}; // end namespace Ontology

	}; // end namespace Reasoner

}; // end namespace Konclude
