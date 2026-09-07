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

#include "CForceRoleRealizationQuery.h"


namespace Konclude {

	namespace Reasoner {

		namespace Query {


			CForceRoleRealizationQuery::CForceRoleRealizationQuery(CConcreteOntology *ontology, CConfigurationBase *configuration, const QString &queryName)
					: CRealizationPremisingQuery(ontology,configuration) {
				mQueryName = queryName;
				mQueryString = QString("Force Complete Role Realization");

				mResult = nullptr;

				mQueryConstructError = false;
				mRealizationCalcError = false;

				mDynamicRealisation = true;
			}


			CForceRoleRealizationQuery::~CForceRoleRealizationQuery() {
				delete mResult;
			}


			CQueryResult *CForceRoleRealizationQuery::getQueryResult() {
				return mResult;
			}


			CQueryResult *CForceRoleRealizationQuery::constructResult(CRealization *realization) {
				if (realization) {
					mResult = new CSucceedQueryResult();
				}
				return mResult;
			}


			COntologyProcessingDynamicRealizationRequirement* CForceRoleRealizationQuery::getDynamicRealizationRequirement() {
				// Both individual references left empty (their default) is
				// handled, in the dynamic-requirement dispatch, by calling
				// markIntanceItemForRoleRealization(procData, role) -- i.e. "every
				// individual for this role". That function (unlike
				// queueRoleFillerInstanceRealization, used when an individual
				// reference *is* given) does NOT itself substitute a null role
				// with the top object role -- it looks the role straight up in
				// mRedirectedRoleInstancesItemHash and dereferences the result
				// unconditionally, so role=nullptr segfaults there. Passing the
				// top object role explicitly is what triggers its "realize
				// everything" special case (the role==getTopObjectRole() check
				// at the top of markIntanceItemForRoleRealization calls
				// setAllRoleInstancesProcessing(true)), and subroleRealizationRequired
				// defaults to true, so this one requirement cascades to every
				// actual object role transitively via topRole's successor items.
				return new COntologyProcessingRoleRealizationRequirement(mOntology->getRBox()->getTopObjectRole());
			}


			QString CForceRoleRealizationQuery::getQueryName() {
				return mQueryName;
			}

			QString CForceRoleRealizationQuery::getQueryString() {
				return mQueryString;
			}

			bool CForceRoleRealizationQuery::hasAnswer() {
				return mResult != nullptr;
			}

			QString CForceRoleRealizationQuery::getAnswerString() {
				if (mResult) {
					return QString("All object role instances realized");
				} else {
					return QString("Role realization not completed");
				}
			}


			bool CForceRoleRealizationQuery::hasError() {
				return mRealizationCalcError || mQueryConstructError || CQuery::hasError();
			}

		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude
