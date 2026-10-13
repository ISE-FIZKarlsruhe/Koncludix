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

#ifndef KONCLUDE_CONTROL_INTERFACE_COMMANDLINE_CCLILINTCOMANDLINEPREPARATIONTRANSLATOR_H
#define KONCLUDE_CONTROL_INTERFACE_COMMANDLINE_CCLILINTCOMANDLINEPREPARATIONTRANSLATOR_H

// Libraries includes
#include <QString>
#include <QHash>

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
				 *		\class		CCLILintComandLinePreparationTranslator
				 *		\brief		Recognizes the "lint" CLI command, which prints a report about constructs
				 *					and data patterns of the ontology specified with -i that are expensive for
				 *					this build of Konclude or that it does not handle completely.
				 *
				 */
				class CCLILintComandLinePreparationTranslator : public CCLIComandLinePreparationTranslator {
					// public methods
					public:
						//! Constructor
						CCLILintComandLinePreparationTranslator(CLoaderFactory *loaderFactory);

						//! Destructor
						virtual ~CCLILintComandLinePreparationTranslator();

						virtual bool canTranslate(const QString& string);

						virtual bool hasNeccessaryParameters();

					// protected methods
					protected:
						virtual bool translate(const QString& argumentText, QStringList& remainingArgList);

					// protected variables
					protected:
						CLoaderFactory *loaderFac;

				};

			}; // end namespace CommandLine

		}; // end namespace Interface

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_INTERFACE_COMMANDLINE_CCLILINTCOMANDLINEPREPARATIONTRANSLATOR_H
