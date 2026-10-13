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
 *		MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *		GNU Lesser General Public License (LGPLv3) for more details.
 *
 *		You should have received a copy of the GNU Lesser General Public
 *		License (LGPLv3) along with Konclude. If not, see
 *		<http://www.gnu.org/licenses/>.
 *
 */

#include "CRunSupervisor.h"

#include <QtGlobal>

#ifdef Q_OS_LINUX

#include <stdio.h>
#include <stdlib.h>
#include <QCoreApplication>
#include <QProcess>
#include <QProcessEnvironment>
#include <QFile>
#include <QFileInfo>
#include <QStringList>
#include <QDateTime>
#include <QElapsedTimer>

#endif

namespace Konclude {

	namespace Control {

		namespace Loader {

#ifdef Q_OS_LINUX

			static void supervisorLog(const QString& text) {
				QByteArray line = QString("{info} %1 >> [supervisor] %2\n").arg(QDateTime::currentDateTime().toString("hh:mm:ss:zzz")).arg(text).toLocal8Bit();
				fwrite(line.constData(), 1, line.size(), stdout);
				fflush(stdout);
			}

			// Files in /proc report the size 0, so QFile::atEnd() / readLine() cannot be used: the whole content is read at once.
			static qint64 readProcFileValueKB(const QString& fileName, const QByteArray& key) {
				QFile procFile(fileName);
				if (procFile.open(QIODevice::ReadOnly)) {
					QByteArray content = procFile.readAll();
					foreach (QByteArray line, content.split('\n')) {
						if (line.startsWith(key)) {
							return line.mid(key.size()).trimmed().split(' ').first().toLongLong();
						}
					}
				}
				return -1;
			}

			static qint64 readMemoryAvailableMB() {
				qint64 kb = readProcFileValueKB("/proc/meminfo", "MemAvailable:");
				return kb < 0 ? -1 : kb / 1024;
			}

			static qint64 readProcessRssMB(qint64 pid) {
				qint64 kb = readProcFileValueKB(QString("/proc/%1/status").arg(pid), "VmRSS:");
				return kb < 0 ? -1 : kb / 1024;
			}


			bool CRunSupervisor::runSupervised(int argc, char* argv[], int& exitCode) {
				if (getenv("KONCLUDE_SUPERVISED") || argc < 2) {
					return false;
				}
				QString command = QString::fromLocal8Bit(argv[1]).toLower();
				if (command != "consistency" && command != "classification" && command != "realization" && command != "materialize" && command != "satisfiability") {
					return false;
				}

				// separate the supervisor option from the arguments of the command
				QStringList childArgs;
				QString memoryOption;
				QString inputFile;
				for (int i = 1; i < argc; ++i) {
					QString arg = QString::fromLocal8Bit(argv[i]);
					if (i > 1 && (arg == "-m" || arg == "-M") && i + 1 < argc) {
						memoryOption = QString::fromLocal8Bit(argv[++i]);
						continue;
					}
					if (i > 1 && (arg == "-i" || arg == "-I") && i + 1 < argc) {
						inputFile = QString::fromLocal8Bit(argv[i + 1]);
					}
					childArgs.append(arg);
				}

				qint64 limitMB = 0;
				qint64 availableMB = readMemoryAvailableMB();
				if (memoryOption.isEmpty()) {
					if (inputFile.isEmpty() || QFileInfo(inputFile).size() <= 4*1024*1024 || availableMB <= 0) {
						return false;
					}
					memoryOption = "AUTO";
				}
				if (memoryOption.compare("OFF", Qt::CaseInsensitive) == 0 || memoryOption == "0") {
					return false;
				} else if (memoryOption.compare("AUTO", Qt::CaseInsensitive) == 0) {
					if (availableMB <= 0) {
						return false;
					}
					limitMB = qMax((qint64)1024, availableMB * 8 / 10);
				} else {
					bool ok = false;
					limitMB = memoryOption.toLongLong(&ok);
					if (!ok || limitMB <= 0) {
						fprintf(stdout, "{error} >> [supervisor] The value '%s' of -m is not a number of megabytes, AUTO or OFF.\n", memoryOption.toLocal8Bit().constData());
						exitCode = 2;
						return true;
					}
				}

				int maxAttempts = 6;
				if (getenv("KONCLUDE_SUPERVISOR_ATTEMPTS")) {
					maxAttempts = qMax(1, atoi(getenv("KONCLUDE_SUPERVISOR_ATTEMPTS")));
				}
				QList<int> seeds;
				seeds << 2 << 3 << 6 << 11 << 13 << 17 << 19 << 23 << 29 << 31;
				if (getenv("KONCLUDE_QHASH_SEED")) {
					seeds.prepend(atoi(getenv("KONCLUDE_QHASH_SEED")));
				}

				QCoreApplication supervisorApp(argc, argv);
				QString program = QFile::symLinkTarget("/proc/self/exe");
				if (program.isEmpty()) {
					program = QCoreApplication::applicationFilePath();
				}

				supervisorLog(QString("Running '%1' with a memory limit of %2 MB (available now: %3 MB, up to %4 attempts with different hash seeds; change with -m MB|AUTO|OFF).").arg(command).arg(limitMB).arg(availableMB).arg(maxAttempts));

				for (int attempt = 0; attempt < maxAttempts && attempt < seeds.size(); ++attempt) {
					int seed = seeds.at(attempt);
					QProcess child;
					QProcessEnvironment env = QProcessEnvironment::systemEnvironment();
					env.insert("KONCLUDE_SUPERVISED", "1");
					env.insert("KONCLUDE_QHASH_SEED", QString::number(seed));
					child.setProcessEnvironment(env);
					child.setProcessChannelMode(QProcess::MergedChannels);
					child.start(program, childArgs);
					if (!child.waitForStarted(10000)) {
						fprintf(stdout, "{error} >> [supervisor] The command could not be started.\n");
						exitCode = 2;
						return true;
					}
					supervisorLog(QString("Attempt %1 with hash seed %2.").arg(attempt + 1).arg(seed));

					bool classificationFinished = false;
					bool killedForMemory = false;
					qint64 peakMB = 0;
					QElapsedTimer pollTimer;
					pollTimer.start();
					while (child.state() != QProcess::NotRunning) {
						child.waitForReadyRead(200);
						QByteArray data = child.readAllStandardOutput();
						if (!data.isEmpty()) {
							fwrite(data.constData(), 1, data.size(), stdout);
							fflush(stdout);
							if (data.contains("Finished class classification")) {
								classificationFinished = true;
							}
						}
						if (pollTimer.elapsed() >= 500) {
							pollTimer.restart();
							qint64 rssMB = readProcessRssMB(child.processId());
							peakMB = qMax(peakMB, rssMB);
							if (rssMB > limitMB && child.state() != QProcess::NotRunning) {
								child.kill();
								killedForMemory = true;
								break;
							}
						}
					}
					child.waitForFinished(10000);
					QByteArray rest = child.readAllStandardOutput();
					if (!rest.isEmpty()) {
						fwrite(rest.constData(), 1, rest.size(), stdout);
						fflush(stdout);
					}

					if (!killedForMemory) {
						exitCode = (child.exitStatus() == QProcess::NormalExit) ? child.exitCode() : 3;
						if (child.exitStatus() != QProcess::NormalExit) {
							supervisorLog(QString("The command crashed (attempt %1, hash seed %2, peak %3 MB).").arg(attempt + 1).arg(seed).arg(peakMB));
						}
						return true;
					}
					if (classificationFinished) {
						supervisorLog(QString("The command passed the memory limit of %1 MB after the class hierarchy was computed (peak %2 MB); a restart with another seed would not help much here. Give it more memory or raise the limit with -m MB.").arg(limitMB).arg(peakMB));
						exitCode = 3;
						return true;
					}
					supervisorLog(QString("Stopped attempt %1 (hash seed %2): more than %3 MB used, restarting with another seed. The search order of the reasoner depends on the seed, the result does not.").arg(attempt + 1).arg(seed).arg(limitMB));
				}
				supervisorLog(QString("Giving up: no attempt finished within %1 MB. Use a machine with more memory or raise the limit with -m MB.").arg(limitMB));
				exitCode = 3;
				return true;
			}

#else

			bool CRunSupervisor::runSupervised(int argc, char* argv[], int& exitCode) {
				Q_UNUSED(argc);
				Q_UNUSED(argv);
				Q_UNUSED(exitCode);
				return false;
			}

#endif

		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
