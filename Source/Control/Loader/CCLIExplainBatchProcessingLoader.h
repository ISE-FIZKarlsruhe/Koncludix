/*
 *		Konclude explanation support (Koncludix).
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

#ifndef KONCLUDE_CONTROL_LOADER_CCLIEXPLAINBATCHPROCESSINGLOADER_H
#define KONCLUDE_CONTROL_LOADER_CCLIEXPLAINBATCHPROCESSINGLOADER_H

// Libraries includes
#include <QString>
#include <QStringList>
#include <QList>
#include <QVector>
#include <QByteArray>

// Namespace includes
#include "LoaderSettings.h"
#include "CCLIBatchProcessingLoader.h"
#include "CExplainJustificationSearch.h"

// Other includes
#include "Renderer/COntologyFunctionalSyntaxWriter.h"

// Logger includes
#include "Logger/CLogger.h"
#include "Logger/CLogIdentifier.h"


namespace Konclude {

	using namespace Logger;
	using namespace Renderer;

	namespace Control {

		namespace Loader {

			/*!
			 *
			 *		\class		CCLIExplainBatchProcessingLoader
			 *		\brief		Implements the CLI command "explain": computes a minimal set of axioms (a 1-minimal justification) that
			 *					makes the ontology inconsistent or that entails a subclass statement / a class assertion.
			 *					The ontology is parsed once in this process and written as one OWL functional syntax axiom per entry; the
			 *					shrinking then calls this same executable ("consistency") on temporary files, one process per test, so that
			 *					every test has its own timeout and cannot influence the next one.
			 *
			 */
			class CCLIExplainBatchProcessingLoader : public CCLIBatchProcessingLoader {
				// public methods
				public:
					//! Constructor
					CCLIExplainBatchProcessingLoader();

					//! Destructor
					virtual ~CCLIExplainBatchProcessingLoader();

					virtual CLoader* init(CLoaderFactory* loaderFactory = 0, CConfiguration* config = 0);

					virtual CCommandRecorder* recordData(CCommandRecordData* recData);

					//! exit code for the process: 0 explanation found, 1 nothing to explain, 2 error
					static int getProcessExitCode();


				// protected methods
				protected:
					virtual void createTestingCommands();
					virtual void writeCommandOutput(const QString& outputFileName, CCommand* processedCommand);
					virtual void finishProcessing();

					void extractAxioms(CCommand* revisionCommand);
					bool runExplanation();
					void printLine(const QString& text);
					static QString decodeOptionValue(const QString& value);
					static QString normalizeExpression(const QString& text);

				// protected variables
				protected:
					QString mKBName;

					// options
					QString mMode;					// "none" (inconsistency), "subclass", "type"
					QString mArgument1;
					QString mArgument2;
					int mTimeoutSeconds;
					int mCallWorkers;
					bool mSlicing;
					bool mVerify;
					QString mFlattenFile;

					// extracted ontology
					bool mExtractionDone;
					bool mLoadErrorSeen;
					QString mFailure;
					QList<COntologyFunctionalSyntaxWriter::CWrittenAxiom> mWrittenAxioms;

					static int sProcessExitCode;
			};

		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_LOADER_CCLIEXPLAINBATCHPROCESSINGLOADER_H
