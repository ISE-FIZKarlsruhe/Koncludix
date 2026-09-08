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

#include "CForceSameIndividualsRealizationQuery.h"


namespace Konclude {

	namespace Reasoner {

		namespace Query {


			CForceSameIndividualsRealizationQuery::CForceSameIndividualsRealizationQuery(CConcreteOntology *ontology, CConfigurationBase *configuration, const QString &queryName)
					: CRealizationPremisingQuery(ontology,configuration) {
				mQueryName = queryName;
				mQueryString = QString("Force Complete Same-Individuals Realization");

				mResult = nullptr;

				mQueryConstructError = false;
				mRealizationCalcError = false;

				mDynamicRealisation = true;
			}


			CForceSameIndividualsRealizationQuery::~CForceSameIndividualsRealizationQuery() {
				delete mResult;
			}


			CQueryResult *CForceSameIndividualsRealizationQuery::getQueryResult() {
				return mResult;
			}


			CQueryResult *CForceSameIndividualsRealizationQuery::constructResult(CRealization *realization) {
				if (realization) {
					mResult = new CSucceedQueryResult();
				}
				return mResult;
			}


			COntologyProcessingDynamicRealizationRequirement* CForceSameIndividualsRealizationQuery::getDynamicRealizationRequirement() {
				// Both individual references left empty (their default) is
				// handled, in the dynamic-requirement dispatch, by calling
				// markInstantiatedItemForSameIndividualsRealization(procData,
				// CIndividualReference()) -- which, given an empty reference,
				// calls setAllSameIndividualsProcessing(true) and marks every
				// currently instantiated individual's "process possible same
				// individuals" flag, rather than looking any single individual
				// up (unlike the role-realization requirement's empty-role
				// case, there is no null-dereference hazard here -- the empty
				// branch is handled explicitly before any lookup).
				return new COntologyProcessingSameRealizationRequirement();
			}


			QString CForceSameIndividualsRealizationQuery::getQueryName() {
				return mQueryName;
			}

			QString CForceSameIndividualsRealizationQuery::getQueryString() {
				return mQueryString;
			}

			bool CForceSameIndividualsRealizationQuery::hasAnswer() {
				return mResult != nullptr;
			}

			QString CForceSameIndividualsRealizationQuery::getAnswerString() {
				if (mResult) {
					return QString("All same-individual sets realized");
				} else {
					return QString("Same-individuals realization not completed");
				}
			}


			bool CForceSameIndividualsRealizationQuery::hasError() {
				return mRealizationCalcError || mQueryConstructError || CQuery::hasError();
			}

		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude
