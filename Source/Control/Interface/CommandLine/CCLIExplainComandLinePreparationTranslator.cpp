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

#include "CCLIExplainComandLinePreparationTranslator.h"


namespace Konclude {

	namespace Control {

		namespace Interface {

			namespace CommandLine {

				CCLIExplainComandLinePreparationTranslator::CCLIExplainComandLinePreparationTranslator(CLoaderFactory *loaderFactory) : CCLIComandLinePreparationTranslator(loaderFactory) {
					mOperationTypeString = QString("Explain");
					mMinNecessaryArguments = true;
					mFirstArgumentTranslation = false;
					mLoaderTypeString = QString("-CLIExplainBatchProcessingLoader");
					mOptionError = false;
					mHasMode = false;
				}


				CCLIExplainComandLinePreparationTranslator::~CCLIExplainComandLinePreparationTranslator() {
				}


				bool CCLIExplainComandLinePreparationTranslator::hasNeccessaryParameters() {
					bool minNecessaryArguments = true;
					if (!mFirstArgumentTranslation) {
						minNecessaryArguments = false;
					}
					if (!mRequestFileTranslated) {
						LOG(ERROR,getLogDomain(),logTr("%1 request file not specified.").arg(mOperationTypeString),this);
						minNecessaryArguments = false;
					}
					if (mOptionError) {
						minNecessaryArguments = false;
					}
					return minNecessaryArguments;
				}


				// the command line loader reads "+name=value" entries with a regular expression: '=' and '+' inside values would break it
				QString CCLIExplainComandLinePreparationTranslator::encodeValue(const QString& value) {
					QString encoded = value.trimmed();
					encoded.replace("%", "%25");
					encoded.replace("=", "%3D");
					encoded.replace("+", "%2B");
					return encoded;
				}


				bool CCLIExplainComandLinePreparationTranslator::takeValues(const QString& optionName, int count, QStringList& remainingArgList, QStringList& values) {
					for (int i = 0; i < count; ++i) {
						if (remainingArgList.isEmpty() || remainingArgList.first().trimmed().startsWith("--")) {
							LOG(ERROR,getLogDomain(),logTr("Option '%1' needs %2 value(s).").arg(optionName).arg(count),this);
							mOptionError = true;
							return false;
						}
						values.append(remainingArgList.takeFirst());
					}
					return true;
				}


				bool CCLIExplainComandLinePreparationTranslator::translate(const QString& argumentText, QStringList& remainingArgList) {

					bool argumentTranslated = false;

					QString modArgText = argumentText.trimmed();
					if (!mSkipTranslation) {
						// the options of this command are checked first, because the general translator treats any argument that
						// starts with a known letter (e.g. -t...) as one of its own short options
						QStringList values;
						if (modArgText == "--subclass" || modArgText == "--type") {
							argumentTranslated = true;
							if (takeValues(modArgText, 2, remainingArgList, values)) {
								if (mHasMode) {
									LOG(ERROR,getLogDomain(),logTr("Only one of --subclass and --type can be given."),this);
									mOptionError = true;
								}
								mHasMode = true;
								mRequestConfList.append(QString("+Load.CLI.ExplainMode=%1").arg(modArgText == "--subclass" ? "subclass" : "type"));
								mRequestConfList.append(QString("+Load.CLI.ExplainArgument1=%1").arg(encodeValue(values.at(0))));
								mRequestConfList.append(QString("+Load.CLI.ExplainArgument2=%1").arg(encodeValue(values.at(1))));
							}
						} else if (modArgText == "--timeout") {
							argumentTranslated = true;
							if (takeValues(modArgText, 1, remainingArgList, values)) {
								bool ok = false;
								int seconds = values.at(0).trimmed().toInt(&ok);
								if (!ok || seconds < 0) {
									LOG(ERROR,getLogDomain(),logTr("Option '--timeout' needs a number of seconds (0 = no timeout)."),this);
									mOptionError = true;
								} else {
									mRequestConfList.append(QString("+Load.CLI.ExplainTimeout=%1").arg(seconds));
								}
							}
						} else if (modArgText == "--call-workers") {
							argumentTranslated = true;
							if (takeValues(modArgText, 1, remainingArgList, values)) {
								bool ok = false;
								int workers = values.at(0).trimmed().toInt(&ok);
								if (!ok || workers < 1) {
									LOG(ERROR,getLogDomain(),logTr("Option '--call-workers' needs a number greater than 0."),this);
									mOptionError = true;
								} else {
									mRequestConfList.append(QString("+Load.CLI.ExplainWorkers=%1").arg(workers));
								}
							}
						} else if (modArgText == "--flatten") {
							argumentTranslated = true;
							if (takeValues(modArgText, 1, remainingArgList, values)) {
								mRequestConfList.append(QString("+Load.CLI.ExplainFlatten=%1").arg(encodeValue(values.at(0))));
							}
						} else if (modArgText == "--no-slicing") {
							argumentTranslated = true;
							mRequestConfList.append(QString("+Load.CLI.ExplainSlicing=false"));
						} else if (modArgText == "--no-verify") {
							argumentTranslated = true;
							mRequestConfList.append(QString("+Load.CLI.ExplainVerify=false"));

						} else if (CCommandLinePreparationTranslator::translate(argumentText,remainingArgList)) {
							argumentTranslated = true;
						} else {
							if (modArgText.indexOf("-") == 0) {
							} else if (modArgText == "explain" || modArgText == "explanation") {
								argumentTranslated = true;
								mFirstArgumentTranslation = true;
							}
						}
					}
					return argumentTranslated;
				}


				bool CCLIExplainComandLinePreparationTranslator::canTranslate(const QString& string) {
					if (QString::compare(string,"explain",Qt::CaseInsensitive) == 0 || QString::compare(string,"explanation",Qt::CaseInsensitive) == 0) {
						return true;
					}
					return false;
				}


			}; // end namespace CommandLine

		}; // end namespace Interface

	}; // end namespace Control

}; // end namespace Konclude
