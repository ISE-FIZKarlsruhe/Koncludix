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

#ifndef KONCLUDE_CONTROL_LOADER_CRUNSUPERVISOR_H
#define KONCLUDE_CONTROL_LOADER_CRUNSUPERVISOR_H

namespace Konclude {

	namespace Control {

		namespace Loader {

			/*!
			 *		\class		CRunSupervisor
			 *		\author		Konclude Developer Team
			 *		\brief		Runs the reasoning command in a child process that is watched for its memory use (Linux only).
			 *
			 *		The search order of the tableau procedure depends on the (hash) order of the loaded ontology. For some large ABoxes (e.g. OWL2Bench DL-5)
			 *		one order needs a few GB for the class classification while another order needs more than 10 GB. The supervisor starts the command as a child
			 *		process with a fixed hash seed, kills it when it passes the memory limit before the class hierarchy is finished and starts it again with
			 *		another seed. The result of a finished run does not depend on the seed.
			 *
			 *		Option: -m MB | AUTO | OFF  (AUTO = 80 % of the available memory). Without -m the supervisor is only used for input files larger than 4 MB.
			 *		Environment: KONCLUDE_SUPERVISOR_ATTEMPTS (default 6).
			 */
			class CRunSupervisor {
				public:
					//! Returns true if the command was run (or failed) under supervision, the process exit code is then written to exitCode. Returns false if the caller has to run the command itself.
					static bool runSupervised(int argc, char* argv[], int& exitCode);
			};

		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_LOADER_CRUNSUPERVISOR_H
