/*
 *		Copyright (C) 2013, 2014 by the Konclude Developer Team.
 *
 *		This file is part of the reasoning system Konclude.
 *		For details and support, see <http://konclude.com/>.
 *
 *		Konclude is free software: you can redistribute it and/or modify it under
 *		the terms of version 3 of the GNU Lesser General Public License (LGPL3).
 */

#ifndef KONCLUDE_CONTROL_LOADER_CCLIINTERVALBATCHPROCESSINGLOADER_H
#define KONCLUDE_CONTROL_LOADER_CCLIINTERVALBATCHPROCESSINGLOADER_H

// Libraries includes
#include <QString>
#include <QStringList>

// Namespace includes
#include "LoaderSettings.h"
#include "CCLILintBatchProcessingLoader.h"

namespace Konclude {

	using namespace Logger;

	namespace Control {

		namespace Loader {

			/*!
			 *
			 *		\class		CCLIIntervalBatchProcessingLoader
			 *		\brief		The "interval" CLI command: loads an ontology (parsing only, no tableau reasoning) and runs the interval theory
			 *					(CIntervalOntologyAnalyser) on its Allen relations and time stamps. The report goes to the standard output, the derived
			 *					relations are written as N-Triples to the file given with -o.
			 *
			 */
			class CCLIIntervalBatchProcessingLoader : public CCLILintBatchProcessingLoader {
				// public methods
				public:
					CCLIIntervalBatchProcessingLoader();
					virtual ~CCLIIntervalBatchProcessingLoader();

				// protected methods
				protected:
					virtual void createTestingCommands();
					virtual void writeCommandOutput(const QString& outputFileName, CCommand* processedCommand);

			};

		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_LOADER_CCLIINTERVALBATCHPROCESSINGLOADER_H
