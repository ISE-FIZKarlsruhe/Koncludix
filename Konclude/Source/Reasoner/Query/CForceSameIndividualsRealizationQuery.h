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

#ifndef KONCLUDE_REASONER_QUERY_CFORCESAMEINDIVIDUALSREALIZATIONQUERY_H
#define KONCLUDE_REASONER_QUERY_CFORCESAMEINDIVIDUALSREALIZATIONQUERY_H

// Libraries includes
#include <QString>


// Namespace includes
#include "CQuery.h"
#include "CSucceedQueryResult.h"
#include "CRealizationPremisingQuery.h"

#include "Config/CConfigDataReader.h"

// Other includes
#include "Reasoner/Ontology/COntologyProcessingSameRealizationRequirement.h"

// Logger includes
#include "Logger/CLogger.h"



namespace Konclude {

	using namespace Config;

	namespace Reasoner {

		namespace Query {

			/*!
			 *
			 *		\class		CForceSameIndividualsRealizationQuery
			 *		\brief		A no-output query whose only purpose is to force
			 *					*complete* same-individual realization (every named
			 *					individual's set of tableau-merged/nominal-equal
			 *					individuals) via the dynamic-realization-requirement
			 *					mechanism, and properly block until it is done, before
			 *					any subsequent query reads same-individual data. Mirrors
			 *					CForceRoleRealizationQuery: passing an empty/empty
			 *					source/destination individual reference pair to
			 *					COntologyProcessingSameRealizationRequirement resolves,
			 *					in the dynamic-requirement dispatch, to
			 *					setAllSameIndividualsProcessing(true) plus marking every
			 *					individual's "process possible same individuals" flag --
			 *					i.e. "every individual" -- rather than relying on the
			 *					coarse CRealizationPremisingQuery::mRequiresSameIndividualRealisation
			 *					flag, which (like the analogous role-realization flag)
			 *					does not by itself guarantee every individual's merge set
			 *					has actually been computed by the time a write query reads
			 *					it.
			 *
			 */
			class CForceSameIndividualsRealizationQuery : public CRealizationPremisingQuery {
				// public methods
				public:
					//! Constructor
					CForceSameIndividualsRealizationQuery(CConcreteOntology *ontology, CConfigurationBase *configuration, const QString &queryName = QString("UnnamedForceSameIndividualsRealizationQuery"));

					//! Destructor
					virtual ~CForceSameIndividualsRealizationQuery();

					virtual CQueryResult *constructResult(CRealization *realization);

					virtual QString getQueryName();
					virtual QString getQueryString();
					virtual bool hasAnswer();
					virtual QString getAnswerString();

					virtual CQueryResult *getQueryResult();

					virtual bool hasError();

					virtual COntologyProcessingDynamicRealizationRequirement* getDynamicRealizationRequirement();

				// protected methods
				protected:

				// protected variables
				protected:
					QString mQueryName;
					QString mQueryString;

					CSucceedQueryResult* mResult;

					bool mQueryConstructError;
					bool mRealizationCalcError;


				// private methods
				private:

				// private variables
				private:

			};

		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude

#endif // KONCLUDE_REASONER_QUERY_CFORCESAMEINDIVIDUALSREALIZATIONQUERY_H
