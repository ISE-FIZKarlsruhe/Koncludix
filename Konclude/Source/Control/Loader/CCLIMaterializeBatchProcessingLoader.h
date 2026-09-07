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

#ifndef KONCLUDE_CONTROL_LOADER_CCLIMATERIALIZEBATCHPROCESSINGLOADER_H
#define KONCLUDE_CONTROL_LOADER_CCLIMATERIALIZEBATCHPROCESSINGLOADER_H

// Libraries includes
#include <QString>
#include <QRegExp>
#include <QTime>
#include <QDir>

// Namespace includes
#include "LoaderSettings.h"
#include "CCLIBatchProcessingLoader.h"

// Other includes

// Logger includes
#include "Logger/CLogger.h"
#include "Logger/CLogIdentifier.h"


#include <stdio.h>
#include <iostream>


namespace Konclude {

	using namespace Logger;

	namespace Control {

		namespace Loader {

			/*!
			 *
			 *		\class		CCLIMaterializeBatchProcessingLoader
			 *		\brief		Loads an ontology, realizes it, and writes the full
			 *					materialized ABox (class assertions and object
			 *					property assertions) computed by Konclude's native
			 *					realizer -- the "materialize" CLI command.
			 *
			 */
			class CCLIMaterializeBatchProcessingLoader : public CCLIBatchProcessingLoader {
				// public methods
				public:
					//! Constructor
					CCLIMaterializeBatchProcessingLoader();

					//! Destructor
					virtual ~CCLIMaterializeBatchProcessingLoader();


				// protected methods
				protected:

					void createMaterializeTestingCommands();
					virtual void createTestingCommands();


				// protected variables
				protected:

				// private methods
				private:

				// private variables
				private:

			};

		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_LOADER_CCLIMATERIALIZEBATCHPROCESSINGLOADER_H
