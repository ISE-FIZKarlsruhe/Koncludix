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

#include "CCLIExplainBatchProcessingLoader.h"

#include <QCoreApplication>
#include <QDir>
#include <QElapsedTimer>
#include <QFile>
#include <QHash>
#include <QProcess>
#include <QRegExp>
#include <QSet>
#include <QTemporaryDir>

#include <iostream>
#include <algorithm>


namespace Konclude {

	namespace Control {

		namespace Loader {

			int CCLIExplainBatchProcessingLoader::sProcessExitCode = 0;

			namespace {

				static const char* const EXPLAIN_FRESH_INDIVIDUAL = "urn:konclude:explain#x";


				/*!
				 *		Runs "<this executable> consistency -w N -i <temporary file>" for every test.
				 */
				class CExplainKoncludeOracle : public CExplainOracle {
					public:
						CExplainKoncludeOracle(const QString& directory, const QByteArray& declarations, const QVector<QByteArray>& axioms, const QByteArray& extraAxioms, int timeoutSeconds, int workers)
								: mDirectory(directory), mDeclarations(declarations), mAxioms(axioms), mExtra(extraAxioms), mTimeoutSeconds(timeoutSeconds), mWorkers(workers) {
							mRuns = 0;
							mTimeouts = 0;
							mErrors = 0;
							mExecutable = QCoreApplication::applicationFilePath();
						}

						virtual EVerdict test(const QVector<int>& sortedAxiomIndices) {
							return run(sortedAxiomIndices, true);
						}

						EVerdict run(const QVector<int>& sortedAxiomIndices, bool withExtra) {
							QString fileName = mDirectory + QString("/test.ofn");
							QFile file(fileName);
							if (!file.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
								mLastProblem = QString("cannot write the temporary file '%1'").arg(fileName);
								++mErrors;
								return UNKNOWN;
							}
							file.write("Ontology(\n");
							file.write(mDeclarations);
							for (int index : sortedAxiomIndices) {
								file.write(mAxioms[index]);
								file.write("\n");
							}
							if (withExtra) {
								file.write(mExtra);
							}
							file.write(")\n");
							file.close();
							return runFile(fileName);
						}

						EVerdict runFile(const QString& fileName) {
							++mRuns;
							QProcess process;
							process.setProcessChannelMode(QProcess::MergedChannels);
							QStringList arguments;
							arguments << "consistency" << "-w" << QString::number(mWorkers) << "-i" << fileName;
							process.start(mExecutable, arguments);
							if (!process.waitForStarted(20000)) {
								mLastProblem = QString("cannot start '%1'").arg(mExecutable);
								++mErrors;
								return UNKNOWN;
							}
							int timeoutMs = mTimeoutSeconds > 0 ? mTimeoutSeconds * 1000 : -1;
							if (!process.waitForFinished(timeoutMs)) {
								process.kill();
								process.waitForFinished(10000);
								++mTimeouts;
								mLastProblem = QString("timeout after %1 s").arg(mTimeoutSeconds);
								return UNKNOWN;
							}
							QString output = QString::fromUtf8(process.readAll());
							mLastOutput = output;
							if (output.contains("is inconsistent")) {
								return ENTAILED;
							}
							if (output.contains("is consistent")) {
								// Konclude answers "consistent" for an ontology that could not be read at all (and ignores single axioms it
								// cannot build with a message only), so a failure to read the whole file must not count as an answer.
								if (output.contains("All parsers failed") || output.contains("{catastrophic}") || QRegExp("File '.*' not found").indexIn(output) >= 0) {
									QStringList problems;
									for (const QString& line : output.split('\n')) {
										if (line.contains("{error}") || line.contains("{catastrophic}")) {
											problems.append(line.trimmed());
										}
									}
									mLastProblem = problems.mid(0, 6).join("\n");
									++mErrors;
									return UNKNOWN;
								}
								return NOT_ENTAILED;
							}
							mLastProblem = QString("no consistency answer in the output (exit status %1): %2").arg(process.exitCode()).arg(output.right(500).trimmed());
							++mErrors;
							return UNKNOWN;
						}

					public:
						QString mDirectory;
						QByteArray mDeclarations;
						QVector<QByteArray> mAxioms;
						QByteArray mExtra;
						int mTimeoutSeconds;
						int mWorkers;
						QString mExecutable;
						int mRuns;
						int mTimeouts;
						int mErrors;
						QString mLastProblem;
						QString mLastOutput;
				};

			}


			CCLIExplainBatchProcessingLoader::CCLIExplainBatchProcessingLoader() {
				mMode = "none";
				mTimeoutSeconds = 60;
				mCallWorkers = 1;
				mSlicing = true;
				mVerify = true;
				mExtractionDone = false;
				mLoadErrorSeen = false;
				mKBName = QString("http://konclude.com/test/kb");
			}


			CCLIExplainBatchProcessingLoader::~CCLIExplainBatchProcessingLoader() {
			}


			int CCLIExplainBatchProcessingLoader::getProcessExitCode() {
				return sProcessExitCode;
			}


			QString CCLIExplainBatchProcessingLoader::decodeOptionValue(const QString& value) {
				QString decoded = value;
				decoded.replace("%3D", "=");
				decoded.replace("%2B", "+");
				decoded.replace("%25", "%");
				return decoded;
			}


			// an IRI written without angle brackets is accepted for convenience; everything else is passed on as functional syntax
			QString CCLIExplainBatchProcessingLoader::normalizeExpression(const QString& text) {
				QString trimmed = text.trimmed();
				QRegExp bareIri("[A-Za-z][A-Za-z0-9+.-]*:[^\\s()<>\"]*");
				if (bareIri.exactMatch(trimmed)) {
					return QString("<") + trimmed + QString(">");
				}
				return trimmed;
			}


			CLoader* CCLIExplainBatchProcessingLoader::init(CLoaderFactory* loaderFactory, CConfiguration* config) {
				CCLIBatchProcessingLoader::init(loaderFactory, config);
				mMode = CConfigDataReader::readConfigString(config, "Load.CLI.ExplainMode", "none");
				mArgument1 = decodeOptionValue(CConfigDataReader::readConfigString(config, "Load.CLI.ExplainArgument1", ""));
				mArgument2 = decodeOptionValue(CConfigDataReader::readConfigString(config, "Load.CLI.ExplainArgument2", ""));
				mTimeoutSeconds = CConfigDataReader::readConfigString(config, "Load.CLI.ExplainTimeout", "60").toInt();
				mCallWorkers = CConfigDataReader::readConfigString(config, "Load.CLI.ExplainWorkers", "1").toInt();
				mSlicing = CConfigDataReader::readConfigString(config, "Load.CLI.ExplainSlicing", "true") == "true";
				mVerify = CConfigDataReader::readConfigString(config, "Load.CLI.ExplainVerify", "true") == "true";
				mFlattenFile = decodeOptionValue(CConfigDataReader::readConfigString(config, "Load.CLI.ExplainFlatten", ""));
				if (mCallWorkers < 1) {
					mCallWorkers = 1;
				}
				return this;
			}


			CCommandRecorder* CCLIExplainBatchProcessingLoader::recordData(CCommandRecordData* recData) {
				if (recData->getRecordErrorLevel() >= 70) {
					mLoadErrorSeen = true;
				}
				return CCLIBatchProcessingLoader::recordData(recData);
			}


			void CCLIExplainBatchProcessingLoader::createTestingCommands() {
				logOutputMessage(QString("Starting explanation for '%1'.").arg(mRequestFileString));
				CCreateKnowledgeBaseCommand* createKBCommand = new CCreateKnowledgeBaseCommand(mKBName);
				QStringList ontoIRIList;
				ontoIRIList.append(mRequestFileString);
				CLoadKnowledgeBaseOWLAutoOntologyCommand* loadKBCommand = new CLoadKnowledgeBaseOWLAutoOntologyCommand(mKBName, ontoIRIList);
				CGetCurrentKnowledgeBaseRevisionCommand* revisionCommand = new CGetCurrentKnowledgeBaseRevisionCommand(mKBName);
				CReleaseKnowledgeBaseCommand* releaseKBCommand = new CReleaseKnowledgeBaseCommand(mKBName);
				addProcessingCommand(createKBCommand);
				addProcessingCommand(loadKBCommand);
				addProcessingCommand(revisionCommand, false, "", true, mResponseFileString);
				addProcessingCommand(releaseKBCommand);
				processNextCommand();
			}


			void CCLIExplainBatchProcessingLoader::writeCommandOutput(const QString& outputFileName, CCommand* processedCommand) {
				CGetCurrentKnowledgeBaseRevisionCommand* revisionCommand = dynamic_cast<CGetCurrentKnowledgeBaseRevisionCommand*>(processedCommand);
				if (revisionCommand) {
					extractAxioms(revisionCommand);
				}
			}


			void CCLIExplainBatchProcessingLoader::extractAxioms(CCommand* revisionCommand) {
				mExtractionDone = true;
				COntologyRevision* revision = ((CGetCurrentKnowledgeBaseRevisionCommand*)revisionCommand)->getOntologyRevision();
				if (!revision || !revision->getOntology()) {
					mFailure = QString("the ontology '%1' could not be loaded").arg(mRequestFileString);
					return;
				}
				COntologyFunctionalSyntaxWriter writer;
				if (!writer.writeOntology(revision->getOntology(), mWrittenAxioms)) {
					mFailure = QString("the ontology contains something that cannot be written as functional syntax: %1").arg(writer.getError());
					mWrittenAxioms.clear();
				}
			}


			void CCLIExplainBatchProcessingLoader::printLine(const QString& text) {
				CLogger::getInstance()->waitSynchronization();
				std::cout << text.toUtf8().constData() << std::endl;
			}


			void CCLIExplainBatchProcessingLoader::finishProcessing() {
				bool success = false;
				if (!mExtractionDone || !mFailure.isEmpty()) {
					if (mFailure.isEmpty()) {
						mFailure = QString("the ontology '%1' could not be loaded").arg(mRequestFileString);
					}
					CLogger::getInstance()->waitSynchronization();
					printLine(QString("Error: %1.").arg(mFailure));
					sProcessExitCode = 2;
				} else {
					success = runExplanation();
				}
				(void)success;
			}


			bool CCLIExplainBatchProcessingLoader::runExplanation() {
				QElapsedTimer totalTimer;
				totalTimer.start();

				// declarations are kept in every test ontology, the other axioms are the candidates of the justification
				QByteArray declarations;
				QVector<QByteArray> axioms;
				QVector<bool> isAssertion;
				QList<QStringList> axiomIndividuals;
				QStringList axiomTexts;
				QSet<QString> seen;
				int declarationCount = 0;
				int duplicateCount = 0;
				for (const COntologyFunctionalSyntaxWriter::CWrittenAxiom& written : mWrittenAxioms) {
					if (seen.contains(written.mText)) {
						++duplicateCount;
						continue;
					}
					seen.insert(written.mText);
					if (written.mKind == COntologyFunctionalSyntaxWriter::AK_DECLARATION) {
						declarations.append(written.mText.toUtf8());
						declarations.append('\n');
						++declarationCount;
					} else {
						axioms.append(written.mText.toUtf8());
						axiomTexts.append(written.mText);
						isAssertion.append(written.mKind == COntologyFunctionalSyntaxWriter::AK_ASSERTION);
						axiomIndividuals.append(written.mIndividuals);
					}
				}
				mWrittenAxioms.clear();

				if (!mFlattenFile.isEmpty()) {
					QFile flat(mFlattenFile);
					if (!flat.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
						printLine(QString("Error: cannot write '%1'.").arg(mFlattenFile));
						sProcessExitCode = 2;
						return false;
					}
					flat.write("Ontology(\n");
					flat.write(declarations);
					for (const QByteArray& axiom : axioms) {
						flat.write(axiom);
						flat.write("\n");
					}
					flat.write(")\n");
					flat.close();
					printLine(QString("Wrote %1 axiom(s) and %2 declaration(s) to '%3'.").arg(axioms.size()).arg(declarationCount).arg(mFlattenFile));
					sProcessExitCode = 0;
					return true;
				}

				if (axioms.isEmpty() && mLoadErrorSeen) {
					printLine(QString("Error: the ontology '%1' contains no axioms (loading reported errors).").arg(mRequestFileString));
					sProcessExitCode = 2;
					return false;
				}

				// statement that is tested for entailment
				QStringList extraAxioms;
				QString statementText;
				if (mMode == "subclass") {
					QString sub = normalizeExpression(mArgument1);
					QString super = normalizeExpression(mArgument2);
					extraAxioms << QString("ClassAssertion(%1 <%2>)").arg(sub).arg(EXPLAIN_FRESH_INDIVIDUAL)
							<< QString("ClassAssertion(ObjectComplementOf(%1) <%2>)").arg(super).arg(EXPLAIN_FRESH_INDIVIDUAL);
					statementText = QString("SubClassOf(%1 %2)").arg(sub).arg(super);
				} else if (mMode == "type") {
					QString individual = normalizeExpression(mArgument1);
					QString cls = normalizeExpression(mArgument2);
					extraAxioms << QString("ClassAssertion(ObjectComplementOf(%1) %2)").arg(cls).arg(individual);
					statementText = QString("ClassAssertion(%1 %2)").arg(cls).arg(individual);
				}
				QByteArray extra;
				for (const QString& e : extraAxioms) {
					extra.append(e.toUtf8());
					extra.append('\n');
				}
				bool entailmentMode = !extraAxioms.isEmpty();

				QTemporaryDir tempDir(QDir::tempPath() + QString("/konclude-explain-XXXXXX"));
				if (!tempDir.isValid()) {
					printLine("Error: cannot create a temporary directory.");
					sProcessExitCode = 2;
					return false;
				}

				CExplainKoncludeOracle oracle(tempDir.path(), declarations, axioms, extra, mTimeoutSeconds, mCallWorkers);

				logOutputMessage(QString("Loaded %1 axiom(s) (%2 declaration(s), %3 duplicate(s) merged); every test calls '%4 consistency' with a timeout of %5 s.")
						.arg(axioms.size()).arg(declarationCount).arg(duplicateCount).arg(oracle.mExecutable).arg(mTimeoutSeconds > 0 ? QString::number(mTimeoutSeconds) : QString("infinite")));

				QVector<int> allIndices(axioms.size());
				for (int i = 0; i < allIndices.size(); ++i) {
					allIndices[i] = i;
				}

				// check that the written copy of the ontology behaves like the original file
				CExplainOracle::EVerdict plainVerdict = CExplainOracle::UNKNOWN;
				if (mVerify || entailmentMode) {
					plainVerdict = oracle.run(allIndices, false);
				}
				if (mVerify) {
					CExplainOracle::EVerdict originalVerdict = oracle.runFile(mRequestFileString);
					if (originalVerdict == CExplainOracle::UNKNOWN || plainVerdict == CExplainOracle::UNKNOWN) {
						logOutputMessage(QString("Warning: the check that the rewritten ontology agrees with '%1' gave no answer (%2).").arg(mRequestFileString).arg(oracle.mLastProblem.split('\n').first()));
					} else if (originalVerdict != plainVerdict) {
						printLine(QString("Error: Konclude says that the ontology '%1' is %2, but the rewritten copy used for explaining is %3. "
								"Some axiom was not written faithfully; refusing to explain (use --no-verify to skip this check).")
								.arg(mRequestFileString).arg(originalVerdict == CExplainOracle::ENTAILED ? "inconsistent" : "consistent").arg(plainVerdict == CExplainOracle::ENTAILED ? "inconsistent" : "consistent"));
						sProcessExitCode = 2;
						return false;
					}
				}

				CExplainJustificationSearch search(&oracle, [&](const QString& message) {
					logOutputMessage(message);
				});

				CExplainOracle::EVerdict fullVerdict = search.test(allIndices);
				if (fullVerdict != CExplainOracle::ENTAILED) {
					QString what = entailmentMode ? "does not entail the statement" : "is not inconsistent";
					if (fullVerdict == CExplainOracle::UNKNOWN) {
						printLine(QString("Nothing to explain: Konclude gave no answer for the full ontology%1.").arg(oracle.mTimeouts > 0 ? QString(" within the timeout of %1 s (see --timeout)").arg(mTimeoutSeconds) : QString()));
						if (!oracle.mLastProblem.isEmpty() && oracle.mTimeouts == 0) {
							printLine(oracle.mLastProblem);
						}
						sProcessExitCode = 2;
					} else {
						printLine(QString("Nothing to explain: the ontology %1.").arg(what));
						sProcessExitCode = 1;
					}
					return false;
				}

				// connected components of the ABox (heuristic; every step that uses them is verified by the oracle)
				QVector<QVector<int> > components;
				if (mSlicing) {
					QHash<QString, int> individualId;
					QVector<int> parent;
					std::function<int(int)> find = [&](int x) -> int {
						while (parent[x] != x) {
							parent[x] = parent[parent[x]];
							x = parent[x];
						}
						return x;
					};
					for (int i = 0; i < axioms.size(); ++i) {
						if (!isAssertion[i]) {
							continue;
						}
						int first = -1;
						for (const QString& name : axiomIndividuals[i]) {
							int id;
							QHash<QString, int>::const_iterator it = individualId.constFind(name);
							if (it == individualId.constEnd()) {
								id = parent.size();
								parent.append(id);
								individualId.insert(name, id);
							} else {
								id = it.value();
							}
							if (first < 0) {
								first = id;
							} else {
								int a = find(first), b = find(id);
								if (a != b) {
									parent[a] = b;
								}
							}
						}
					}
					QHash<int, int> componentOfRoot;
					for (int i = 0; i < axioms.size(); ++i) {
						if (!isAssertion[i]) {
							continue;
						}
						if (axiomIndividuals[i].isEmpty()) {
							components.append(QVector<int>(1, i));
							continue;
						}
						int root = find(individualId.value(axiomIndividuals[i].first()));
						QHash<int, int>::const_iterator it = componentOfRoot.constFind(root);
						if (it == componentOfRoot.constEnd()) {
							componentOfRoot.insert(root, components.size());
							components.append(QVector<int>(1, i));
						} else {
							components[it.value()].append(i);
						}
					}
				}

				QVector<int> result = search.search(axioms.size(), isAssertion, components);

				bool trivial = false;
				if (entailmentMode) {
					// are the added test axioms needed at all?
					trivial = (oracle.run(result, false) == CExplainOracle::ENTAILED);
				}

				QString what = entailmentMode ? "entails the statement" : "is inconsistent";
				int searchCalls = search.getOracleCallCount() + (entailmentMode ? 1 : 0);
				printLine(QString("The ontology %1 because of these %2 axiom(s) (minimal; %3 Konclude calls):").arg(what).arg(result.size()).arg(searchCalls));
				QStringList lines;
				for (int index : result) {
					lines.append(axiomTexts[index]);
					printLine(QString("  ") + axiomTexts[index]);
				}
				if (entailmentMode) {
					printLine(QString("(statement: %1; test axioms added for the check: %2)").arg(statementText).arg(extraAxioms.join("; ")));
					if (trivial) {
						printLine("Note: these axioms are already inconsistent without the test axioms, so the ontology entails every statement.");
					}
				}
				if (oracle.mTimeouts > 0 || oracle.mErrors > 0) {
					printLine(QString("Warning: %1 reasoner call(s) ran into the timeout of %2 s and %3 call(s) failed; they were treated as \"not entailed\". "
							"The axioms above are a verified justification, but minimality is only guaranteed with respect to the calls that answered.").arg(oracle.mTimeouts).arg(mTimeoutSeconds).arg(oracle.mErrors));
				}
				logOutputMessage(QString("Explanation finished in %1 s.").arg(totalTimer.elapsed() / 1000.0, 0, 'f', 1));

				if (!mResponseFileString.isEmpty()) {
					forcedPathCreated(mResponseFileString);
					QFile out(mResponseFileString);
					if (out.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
						out.write(lines.join("\n").toUtf8());
						out.write("\n");
						out.close();
					} else {
						logOutputError(QString("Failed writing output to file '%1'.").arg(mResponseFileString));
					}
				}
				sProcessExitCode = 0;
				return true;
			}


		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
