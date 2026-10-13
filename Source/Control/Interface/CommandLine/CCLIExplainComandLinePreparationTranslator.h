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

#ifndef KONCLUDE_CONTROL_INTERFACE_COMMANDLINE_CCLIEXPLAINCOMANDLINEPREPARATIONTRANSLATOR_H
#define KONCLUDE_CONTROL_INTERFACE_COMMANDLINE_CCLIEXPLAINCOMANDLINEPREPARATIONTRANSLATOR_H

// Libraries includes
#include <QString>

// Namespace includes
#include "CCLIComandLinePreparationTranslator.h"

// Other includes
#include "Control/Loader/CLoaderFactory.h"

// Logger includes
#include "Logger/CLogger.h"

using namespace Konclude::Logger;

namespace Konclude {

	namespace Control {

		using namespace Loader;

		namespace Interface {

			namespace CommandLine {

				/*!
				 *
				 *		\class		CCLIExplainComandLinePreparationTranslator
				 *		\brief		Recognizes the "explain" CLI command:
				 *						explain -i ontology [--subclass SUB SUPER | --type INDIVIDUAL CLASS] [--timeout SECONDS]
				 *								[--call-workers N] [--no-slicing] [--no-verify] [--flatten FILE] [-o FILE]
				 *					The options are handed to the loader as "Load.CLI.Explain*" configuration entries.
				 *
				 */
				class CCLIExplainComandLinePreparationTranslator : public CCLIComandLinePreparationTranslator {
					// public methods
					public:
						//! Constructor
						CCLIExplainComandLinePreparationTranslator(CLoaderFactory *loaderFactory);

						//! Destructor
						virtual ~CCLIExplainComandLinePreparationTranslator();

						virtual bool canTranslate(const QString& string);
						virtual bool hasNeccessaryParameters();

					// protected methods
					protected:
						virtual bool translate(const QString& argumentText, QStringList& remainingArgList);

						QString encodeValue(const QString& value);
						bool takeValues(const QString& optionName, int count, QStringList& remainingArgList, QStringList& values);

					// protected variables
					protected:
						bool mOptionError;
						bool mHasMode;
				};

			}; // end namespace CommandLine

		}; // end namespace Interface

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_INTERFACE_COMMANDLINE_CCLIEXPLAINCOMANDLINEPREPARATIONTRANSLATOR_H
