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

#include "CCLILintBatchProcessingLoader.h"

#include <QFile>
#include <QTextStream>

#include "CLintOntologyAnalyser.h"
#include "Parser/CLintParseRecorder.h"

#include "Reasoner/Revision/COntologyRevision.h"
#include "Reasoner/Ontology/CConcreteOntology.h"
#include "Reasoner/Ontology/COntologyLoadData.h"


namespace Konclude {

	namespace Control {

		namespace Loader {


			CCLILintBatchProcessingLoader::CCLILintBatchProcessingLoader() {
			}


			CCLILintBatchProcessingLoader::~CCLILintBatchProcessingLoader() {
			}


			void CCLILintBatchProcessingLoader::createTestingCommands() {
				logOutputMessage(QString("Starting lint analysis for ontology '%1'.").arg(mRequestFileString));

				// the parsers report the axioms that they skip (owl:hasKey) while recording is on
				Parser::CLintParseRecorder::clearRecords();
				Parser::CLintParseRecorder::setRecording(true);

				mLintKnowledgeBase = QString("http://konclude.com/test/kb");
				CCreateKnowledgeBaseCommand* createKBCommand = new CCreateKnowledgeBaseCommand(mLintKnowledgeBase);
				QStringList ontoIRIList;
				ontoIRIList.append(mRequestFileString);
				CLoadKnowledgeBaseOWLAutoOntologyCommand* loadKBCommand = new CLoadKnowledgeBaseOWLAutoOntologyCommand(mLintKnowledgeBase, ontoIRIList);
				CGetCurrentKnowledgeBaseRevisionCommand* getRevisionCommand = new CGetCurrentKnowledgeBaseRevisionCommand(mLintKnowledgeBase);
				addProcessingCommand(createKBCommand);
				addProcessingCommand(loadKBCommand);
				// the revision is analysed in writeCommandOutput() as soon as it is available
				addProcessingCommand(getRevisionCommand, false, QString(), true, QString("lint"));
				processNextCommand();
			}


			void CCLILintBatchProcessingLoader::writeCommandOutput(const QString& outputFileName, CCommand* processedCommand) {
				CGetCurrentKnowledgeBaseRevisionCommand* getRevisionCommand = dynamic_cast<CGetCurrentKnowledgeBaseRevisionCommand*>(processedCommand);
				if (!getRevisionCommand) {
					return;
				}
				Parser::CLintParseRecorder::setRecording(false);
				COntologyRevision* revision = getRevisionCommand->getOntologyRevision();
				CConcreteOntology* ontology = revision ? revision->getOntology() : nullptr;
				if (!ontology || !ontology->getLoadData() || ontology->getLoadData()->getLoadedOntologyIRIs().isEmpty()) {
					logOutputError(QString("The ontology '%1' could not be loaded, no lint report is available.").arg(mRequestFileString));
					return;
				}
				CLintOntologyAnalyser analyser(ontology, mRequestFileString);
				QStringList report = analyser.createReport();
				if (mResponseFileString.endsWith(".json", Qt::CaseInsensitive) || !qgetenv("KONCLUDE_LINT_JSON").isEmpty()) {
					// machine readable form: level, title, detail, suggestion and examples of every finding
					report = QStringList(analyser.createJsonReport());
				}
				printReport(report);
			}


			void CCLILintBatchProcessingLoader::printReport(const QStringList& reportLines) {
				// the report is written to the standard output, after the pending log messages have been printed
				CLogger::getInstance()->waitSynchronization();
				QString text = reportLines.join("\n") + "\n";
				fputs(text.toUtf8().constData(), stdout);
				fflush(stdout);
				if (!mResponseFileString.isEmpty()) {
					forcedPathCreated(mResponseFileString);
					QFile file(mResponseFileString);
					if (file.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
						file.write(text.toUtf8());
						file.close();
					} else {
						logOutputError(QString("The lint report could not be written to '%1'.").arg(mResponseFileString));
					}
				}
			}


		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
