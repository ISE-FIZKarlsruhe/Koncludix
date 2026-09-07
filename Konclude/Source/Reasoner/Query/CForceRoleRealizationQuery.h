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

#ifndef KONCLUDE_REASONER_QUERY_CFORCEROLEREALIZATIONQUERY_H
#define KONCLUDE_REASONER_QUERY_CFORCEROLEREALIZATIONQUERY_H

// Libraries includes
#include <QString>


// Namespace includes
#include "CQuery.h"
#include "CSucceedQueryResult.h"
#include "CRealizationPremisingQuery.h"

#include "Config/CConfigDataReader.h"

// Other includes
#include "Reasoner/Ontology/COntologyProcessingRoleRealizationRequirement.h"

// Logger includes
#include "Logger/CLogger.h"



namespace Konclude {

	using namespace Config;

	namespace Reasoner {

		namespace Query {

			/*!
			 *
			 *		\class		CForceRoleRealizationQuery
			 *		\brief		A no-output query whose only purpose is to force
			 *					*complete* role realization (every object property,
			 *					every individual) via the dynamic-realization-requirement
			 *					mechanism, and properly block until it is done, before any
			 *					subsequent query reads role instance data. Unlike the
			 *					coarse "role realization step complete" flag
			 *					(CRealizationPremisingQuery::mRequiresRoleRealisation),
			 *					this goes through the same
			 *					COntologyProcessingRoleRealizationRequirement +
			 *					getDynamicRealizationRequirement() path the Answerer uses
			 *					to safely resolve SPARQL/complex queries over role
			 *					instances, which properly waits for the specific
			 *					requirement to be confirmed rather than relying on a
			 *					single coarse "processing step complete" signal.
			 *
			 */
			class CForceRoleRealizationQuery : public CRealizationPremisingQuery {
				// public methods
				public:
					//! Constructor
					CForceRoleRealizationQuery(CConcreteOntology *ontology, CConfigurationBase *configuration, const QString &queryName = QString("UnnamedForceRoleRealizationQuery"));

					//! Destructor
					virtual ~CForceRoleRealizationQuery();

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

#endif // KONCLUDE_REASONER_QUERY_CFORCEROLEREALIZATIONQUERY_H
