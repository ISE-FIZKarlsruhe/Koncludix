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

#ifndef KONCLUDE_CONTROL_COMMAND_CFORCESAMEINDIVIDUALSREALIZATIONQUERYCOMMAND_H
#define KONCLUDE_CONTROL_COMMAND_CFORCESAMEINDIVIDUALSREALIZATIONQUERYCOMMAND_H

// Libraries includes
#include <QString>
#include <QDomElement>

// Namespace includes
#include "CKnowledgeBaseQueryCommand.h"

// Other includes
#include "Control/Command/CommandSettings.h"
#include "Control/Command/CCommand.h"
#include "Control/Command/CPreconditionCommand.h"


// Logger includes
#include "Logger/CLogger.h"

using namespace Konclude::Logger;


namespace Konclude {

	namespace Control {

		namespace Command {

			namespace Instructions {

				/*!
				 *
				 *		\class		CForceSameIndividualsRealizationQueryCommand
				 *		\brief		Forces complete same-individual realization (every
				 *					individual's tableau-merged/nominal-equal set) for a
				 *					knowledge base -- see
				 *					CForceSameIndividualsRealizationQuery for why this
				 *					exists as its own step rather than relying on the
				 *					usual realization command.
				 *
				 */
				class CForceSameIndividualsRealizationQueryCommand : public CKnowledgeBaseQueryCommand {
					// public methods
					public:
						//! Constructor
						CForceSameIndividualsRealizationQueryCommand(const QString& knowledgeBaseNameString, CCommand *parentSuperCommand = 0);

						//! Destructor
						virtual ~CForceSameIndividualsRealizationQueryCommand();

						virtual qint64 getCommandTag();

						virtual QString getBriefCommandDescription();

					// protected methods
					protected:

					// protected variables
					protected:

					// private methods
					private:

					// private variables
					private:

				};

			}; // end namespace Instructions

		}; // end namespace Command

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_COMMAND_CFORCESAMEINDIVIDUALSREALIZATIONQUERYCOMMAND_H
