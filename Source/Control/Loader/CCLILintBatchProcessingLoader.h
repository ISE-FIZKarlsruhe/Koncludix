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

#ifndef KONCLUDE_CONTROL_LOADER_CCLILINTBATCHPROCESSINGLOADER_H
#define KONCLUDE_CONTROL_LOADER_CCLILINTBATCHPROCESSINGLOADER_H

// Libraries includes
#include <QString>
#include <QStringList>

// Namespace includes
#include "LoaderSettings.h"
#include "CCLIBatchProcessingLoader.h"

// Other includes

// Logger includes
#include "Logger/CLogger.h"
#include "Logger/CLogIdentifier.h"


namespace Konclude {

	using namespace Logger;

	namespace Control {

		namespace Loader {

			/*!
			 *
			 *		\class		CCLILintBatchProcessingLoader
			 *		\brief		Loads an ontology (parsing only, no reasoning) and prints a lint report about
			 *					constructs and data patterns that are expensive for this build of Konclude or that
			 *					it does not handle completely -- the "lint" CLI command.
			 *
			 */
			class CCLILintBatchProcessingLoader : public CCLIBatchProcessingLoader {
				// public methods
				public:
					//! Constructor
					CCLILintBatchProcessingLoader();

					//! Destructor
					virtual ~CCLILintBatchProcessingLoader();


				// protected methods
				protected:
					virtual void createTestingCommands();
					virtual void writeCommandOutput(const QString& outputFileName, CCommand* processedCommand);

					void printReport(const QStringList& reportLines);

				// protected variables
				protected:
					QString mLintKnowledgeBase;

			};

		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_LOADER_CCLILINTBATCHPROCESSINGLOADER_H
