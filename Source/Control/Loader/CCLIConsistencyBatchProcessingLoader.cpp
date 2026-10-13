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

#include "CCLIConsistencyBatchProcessingLoader.h"

#include <QFile>
#include <QTemporaryFile>
#include <QDir>

#include "CIntervalOntologyAnalyser.h"
#include "Config/CConfigDataReader.h"

#include "Reasoner/Revision/COntologyRevision.h"
#include "Reasoner/Ontology/CConcreteOntology.h"
#include "Reasoner/Ontology/COntologyLoadData.h"


namespace Konclude {

	namespace Control {

		namespace Loader {


			CCLIConsistencyBatchProcessingLoader::CCLIConsistencyBatchProcessingLoader() {
				mConsistencyKBCommand = nullptr;
				mTriviallyConsistencyKBCommand = nullptr;
				mIntervalRevisionCommand = nullptr;
			}



			CCLIConsistencyBatchProcessingLoader::~CCLIConsistencyBatchProcessingLoader() {
				if (!mDerivedRelationsFile.isEmpty()) {
					QFile::remove(mDerivedRelationsFile);
				}
			}


			
			
			void CCLIConsistencyBatchProcessingLoader::createTestingCommands() {
				createConsistencyTestingCommands();
			}


			void CCLIConsistencyBatchProcessingLoader::createConsistencyTestingCommands() {
				logOutputMessage(QString("Starting consistency checking for '%1'.").arg(mRequestFileString));
				mTestKB = QString("http://konclude.com/test/kb");
				CCreateKnowledgeBaseCommand* createKBCommand = new CCreateKnowledgeBaseCommand(mTestKB);
				QStringList ontoIRIList;
				ontoIRIList.append(mRequestFileString);
				CLoadKnowledgeBaseOWLAutoOntologyCommand* loadKBCommand = new CLoadKnowledgeBaseOWLAutoOntologyCommand(mTestKB,ontoIRIList);
				// the interval theory (interval relations and time stamps of the individuals) looks at the loaded ontology before the reasoning starts
				mIntervalRevisionCommand = new CGetCurrentKnowledgeBaseRevisionCommand(mTestKB);
				addProcessingCommand(createKBCommand);
				addProcessingCommand(loadKBCommand);
				addProcessingCommand(mIntervalRevisionCommand,false,QString(),true,QString("interval"));
				processNextCommand();
			}


			void CCLIConsistencyBatchProcessingLoader::startConsistencyReasoning(const QString& knowledgeBase) {
				mConsistencyKBCommand = new CIsConsistentQueryCommand(knowledgeBase);
				mTriviallyConsistencyKBCommand = new CIsTriviallyConsistentQueryCommand(knowledgeBase);
				addProcessingCommand(mTriviallyConsistencyKBCommand,false,"",true,mResponseFileString);
			}


			// The interval theory in the consistency check: the interval relations and time stamps of the individuals are checked for conflicts (Allen's path consistency, also
			// conflicts that only show through composition); a conflict makes the ontology inconsistent, without any tableau reasoning. Optionally the relations that follow from
			// the time stamps and the composition are added to the ontology (as object property assertions) so that the axioms of the ontology can use them in the reasoning.
			// Konclude.CLI.Interval.Consistency = auto (default: only if the ontology has interval relations or time stamps) | on | off
			void CCLIConsistencyBatchProcessingLoader::handleIntervalStage(CGetCurrentKnowledgeBaseRevisionCommand* revisionCommand, const QString& outputFileName) {
				COntologyRevision* revision = revisionCommand->getOntologyRevision();
				CConcreteOntology* ontology = revision ? revision->getOntology() : nullptr;
				if (!ontology || !ontology->getLoadData() || ontology->getLoadData()->getLoadedOntologyIRIs().isEmpty()) {
					// loading failed: the consistency commands report the problem as before
					startConsistencyReasoning(mTestKB);
					return;
				}
				CConfiguration* config = ontology->getConfiguration() ? (CConfiguration*)ontology->getConfiguration() : mLoaderConfig;
				QString mode = CConfigDataReader::readConfigString(config, "Konclude.CLI.Interval.Consistency", QString("auto")).trimmed().toLower();
				if (mode == "off") {
					startConsistencyReasoning(mTestKB);
					return;
				}

				// Konclude.CLI.Interval.AddDerivedRelations = auto (default: if the ontology declares properties named like Allen relations, i.e. its axioms are meant to use them) | true | false
				QString addMode = CConfigDataReader::readConfigString(config, "Konclude.CLI.Interval.AddDerivedRelations", QString("auto")).trimmed().toLower();
				bool addAlways = (addMode == "true" || addMode == "on" || addMode == "yes");
				bool addAuto = (addMode == "auto");
				CIntervalOntologyAnalyser analyser(ontology, mRequestFileString);
				analyser.configure(config);
				analyser.setGroupByConnectivity(true);
				analyser.setDeriveOnlyIfDeclared(addAuto);
				QStringList report = analyser.createReport();
				if (!analyser.hasIntervalContent() && mode != "on") {
					startConsistencyReasoning(mTestKB);
					return;
				}

				if (analyser.hasConflicts()) {
					// the report with the minimal sets of conflicting facts goes to the standard output, then the usual answer
					CLogger::getInstance()->waitSynchronization();
					QString text = report.join("\n") + "\n";
					fputs(text.toUtf8().constData(), stdout);
					fflush(stdout);
					logOutputMessage(QString("Ontology '%1' is inconsistent.").arg(mRequestFileString));
					if (!outputFileName.isEmpty() && outputFileName != QString("interval")) {
						forcedPathCreated(outputFileName);
						QFile outputFile(outputFileName);
						if (outputFile.open(QIODevice::WriteOnly)) {
							outputFile.write(QString("false\n").toUtf8());
							outputFile.close();
						}
					} else if (!mResponseFileString.isEmpty()) {
						forcedPathCreated(mResponseFileString);
						QFile outputFile(mResponseFileString);
						if (outputFile.open(QIODevice::WriteOnly)) {
							outputFile.write(QString("false\n").toUtf8());
							outputFile.close();
						}
					}
					return;
				}

				logOutputMessage(QString("Interval theory: %1 individuals with time stamps and %2 interval relations, no conflict.").arg(analyser.getTimedCount()).arg(analyser.getAssertedRelationCount()));
				bool addDerived = addAlways || (addAuto && analyser.isRelationVocabularyDeclared());
				if (addDerived && analyser.getDerivedCount() > 0) {
					QTemporaryFile derivedFile(QDir::tempPath() + "/konclude_interval_XXXXXX.ttl");
					derivedFile.setAutoRemove(false);
					if (derivedFile.open()) {
						derivedFile.write(analyser.derivedTriples().toUtf8());
						mDerivedRelationsFile = derivedFile.fileName();
						derivedFile.close();
						logOutputMessage(QString("Interval theory: %1 derived relations are added to the ontology.").arg(analyser.getDerivedCount()));
						QString derivedKB = QString("http://konclude.com/test/kb_interval");
						QStringList ontoIRIList;
						ontoIRIList.append(mRequestFileString);
						ontoIRIList.append(mDerivedRelationsFile);
						addProcessingCommand(new CCreateKnowledgeBaseCommand(derivedKB));
						addProcessingCommand(new CLoadKnowledgeBaseOWLAutoOntologyCommand(derivedKB, ontoIRIList));
						startConsistencyReasoning(derivedKB);
						return;
					}
					logOutputError("The derived interval relations could not be written to a temporary file, they are not added.");
				}
				startConsistencyReasoning(mTestKB);
			}


void CCLIConsistencyBatchProcessingLoader::writeCommandOutput(const QString& outputFileName, CCommand* processedCommand) {
				if (processedCommand == mIntervalRevisionCommand && mIntervalRevisionCommand) {
					handleIntervalStage(mIntervalRevisionCommand, outputFileName);
					return;
				}
				bool outputWritten = false;
				bool requiresDetailedConsistencyChecking = true;
				CKnowledgeBaseQueryCommand* kbQueryCommand = dynamic_cast<CKnowledgeBaseQueryCommand*>(processedCommand);
				if (kbQueryCommand) {
					CQuery* query = kbQueryCommand->getCalculateQueryCommand()->getQuery();
					if (query) {
						CQueryResult* queryResult = query->getQueryResult();
						if (queryResult) {
							CBooleanQueryResult* boolQueryResult = dynamic_cast<CBooleanQueryResult*>(queryResult);
							if (boolQueryResult) {
								bool writeOutput = false;
								if (boolQueryResult->getResult() == true) {
									requiresDetailedConsistencyChecking = false;
									writeOutput = true;
									logOutputMessage(QString("Ontology '%1' is consistent.").arg(mRequestFileString));
								} else {
									if (processedCommand == mConsistencyKBCommand) {
										requiresDetailedConsistencyChecking = false;
										writeOutput = true;
										logOutputMessage(QString("Ontology '%1' is inconsistent.").arg(mRequestFileString));
									}
								}

								if (writeOutput) {
									outputWritten = true;
									if (!outputFileName.isEmpty()) {
										forcedPathCreated(outputFileName);
										QFile outputFile(outputFileName);
										if (outputFile.open(QIODevice::WriteOnly)) {
											QString outputData;
											if (boolQueryResult->getResult() == true) {
												outputData = QString("true\n");
											} else {
												outputData = QString("false\n");
											}
											outputFile.write(outputData.toUtf8());
											outputFile.close();
										} else {
											logOutputError(QString("Failed writing output to file '%1'.").arg(outputFileName));
										}
									}
								}
							}
						}
					}
				}
				if (requiresDetailedConsistencyChecking) {
					addProcessingCommand(mConsistencyKBCommand,false,"",true,mResponseFileString);
				} else if (!outputWritten) {
					logOutputError("Consistency checking failed.");
				}
			}



		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
