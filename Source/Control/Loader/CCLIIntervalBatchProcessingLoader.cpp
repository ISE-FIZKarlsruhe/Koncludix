/*
 *		Copyright (C) 2013, 2014 by the Konclude Developer Team.
 *
 *		This file is part of the reasoning system Konclude.
 *		For details and support, see <http://konclude.com/>.
 *
 *		Konclude is free software: you can redistribute it and/or modify it under
 *		the terms of version 3 of the GNU Lesser General Public License (LGPL3).
 */

#include "CCLIIntervalBatchProcessingLoader.h"

#include <QFile>

#include "CIntervalOntologyAnalyser.h"

#include "Reasoner/Revision/COntologyRevision.h"
#include "Reasoner/Ontology/CConcreteOntology.h"
#include "Reasoner/Ontology/COntologyLoadData.h"


namespace Konclude {

	namespace Control {

		namespace Loader {


			CCLIIntervalBatchProcessingLoader::CCLIIntervalBatchProcessingLoader() {
			}


			CCLIIntervalBatchProcessingLoader::~CCLIIntervalBatchProcessingLoader() {
			}


			void CCLIIntervalBatchProcessingLoader::createTestingCommands() {
				logOutputMessage(QString("Starting interval analysis for ontology '%1'.").arg(mRequestFileString));

				mLintKnowledgeBase = QString("http://konclude.com/test/kb");
				CCreateKnowledgeBaseCommand* createKBCommand = new CCreateKnowledgeBaseCommand(mLintKnowledgeBase);
				QStringList ontoIRIList;
				ontoIRIList.append(mRequestFileString);
				CLoadKnowledgeBaseOWLAutoOntologyCommand* loadKBCommand = new CLoadKnowledgeBaseOWLAutoOntologyCommand(mLintKnowledgeBase, ontoIRIList);
				CGetCurrentKnowledgeBaseRevisionCommand* getRevisionCommand = new CGetCurrentKnowledgeBaseRevisionCommand(mLintKnowledgeBase);
				addProcessingCommand(createKBCommand);
				addProcessingCommand(loadKBCommand);
				// the revision is analysed in writeCommandOutput() as soon as it is available
				addProcessingCommand(getRevisionCommand, false, QString(), true, QString("interval"));
				processNextCommand();
			}


			void CCLIIntervalBatchProcessingLoader::writeCommandOutput(const QString& outputFileName, CCommand* processedCommand) {
				CGetCurrentKnowledgeBaseRevisionCommand* getRevisionCommand = dynamic_cast<CGetCurrentKnowledgeBaseRevisionCommand*>(processedCommand);
				if (!getRevisionCommand) {
					return;
				}
				COntologyRevision* revision = getRevisionCommand->getOntologyRevision();
				CConcreteOntology* ontology = revision ? revision->getOntology() : nullptr;
				if (!ontology || !ontology->getLoadData() || ontology->getLoadData()->getLoadedOntologyIRIs().isEmpty()) {
					logOutputError(QString("The ontology '%1' could not be loaded, no interval report is available.").arg(mRequestFileString));
					return;
				}

				// the options come from the configuration of the knowledge base (the Set commands of a request file given with -c), else from the loader configuration
				CConfiguration* config = ontology->getConfiguration() ? (CConfiguration*)ontology->getConfiguration() : mLoaderConfig;

				CIntervalOntologyAnalyser analyser(ontology, mRequestFileString);
				analyser.configure(config);

				QStringList report = analyser.createReport();

				// the report is written to the standard output, after the pending log messages have been printed
				CLogger::getInstance()->waitSynchronization();
				QString text = report.join("\n") + "\n";
				fputs(text.toUtf8().constData(), stdout);
				fflush(stdout);

				if (!mResponseFileString.isEmpty()) {
					forcedPathCreated(mResponseFileString);
					QFile file(mResponseFileString);
					if (file.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
						file.write(analyser.derivedTriples().toUtf8());
						file.close();
					} else {
						logOutputError(QString("The derived interval relations could not be written to '%1'.").arg(mResponseFileString));
					}
				}
			}


		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
