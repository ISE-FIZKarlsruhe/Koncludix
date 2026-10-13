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

#include "CCommandLineTranlatorHelpLoader.h"


namespace Konclude {

	namespace Control {

		namespace Loader {


			CCommandLineTranlatorHelpLoader::CCommandLineTranlatorHelpLoader() {
			}



			CCommandLineTranlatorHelpLoader::~CCommandLineTranlatorHelpLoader() {
			}


			CLoader *CCommandLineTranlatorHelpLoader::init(CLoaderFactory *loaderFactory, CConfiguration *config) {

				return this;
			}




			CLoader *CCommandLineTranlatorHelpLoader::load() {
				
				CLogger::getInstance()->waitSynchronization();
				LOG(INFO, "::Konclude::Loader::OWLlinkTranslationHelpLoader", logTr(
						"Usage: 'Konclude' <command> [parameters]\n"
						"\n"
						"Available commands:\n"
						"\towllinkfile : Processes an OWLlink request file, which has to be specified with the parameter -i.\n"
						"\towllinkserver : Listening on the specified port (default 8080) for OWLlink requests sent over HTTP.\n"
						"\tclassification : Classifies the ontology that is specified with the parameter -i.\n"
						"\tconsistency : Checks the consistency of the ontology that is specified with the parameter -i.\n"
						"\tsatisfiability : Checks the satisfiability of the class that is specified with -x for the ontology specified with -i.\n"
						"\trealization : Realizes the ontology that is specified with -i (or only a certain individual that is specified with -x).\n"
						"\tmaterialize : Writes the full materialized ontology (transitively closed class/object-/data-property hierarchies,\n"
						"\t\t\tclass assertions, and object property assertions) for the ontology specified with -i.\n"
						"\texplain : Computes a minimal set of axioms (a justification) that makes the ontology specified with -i inconsistent, or with\n"
						"\t\t\t--subclass SUB SUPER / --type INDIVIDUAL CLASS that entails the subclass statement / class assertion\n"
						"\t\t\t(class expressions in OWL functional syntax with full IRIs, e.g. \"<http://ex.org/A>\"). The axioms are printed in functional\n"
						"\t\t\tsyntax, one per line (and written to the file given with -o). Needs no other tools: the shrinking calls\n"
						"\t\t\tthis same executable once per test.\n"
						"\tlint : Prints a report about constructs and data patterns of the ontology specified with -i that are expensive for this build\n"
						"\t\t\tor that it does not handle completely (the ontology is only parsed, not reasoned over). The report is written to the standard\n"
						"\t\t\toutput and, if given, also to the file specified with -o.\n"
						"\tinterval : Checks the interval relations and time stamps of the individuals of the ontology specified with -i (Allen relations such as before, meets,\n"
						"\t\t\toverlaps, also with the OWL-Time names; PROV-O startedAtTime / endedAtTime and OWL-Time instants as time stamps): finds contradictions,\n"
						"\t\t\texplains each with a minimal set of facts, and derives the relations that follow. The report goes to the standard output, the derived\n"
						"\t\t\trelations are written as N-Triples to the file given with -o. Options: Konclude.CLI.Interval.* (see the documentation).\n"
						"\tsparqlfile : Processes a SPARQL request file, which has to be specified with the parameter -s, results are written to the file specified by -o.\n"
						"\t\t\t(an ontology can be given with -i or via a SPARQL Update LOAD command in the SPARQL file)\n"
						"\tsparqlserver : Listening on the specified port (default 8080) for SPARQL requests sent via HTTP POST.\n"
						"\n"
						"Configurable parameter:\n"
						"\t-w X : Configures the number of processing threads. X has to be an integer greater than 0 or 'AUTO' for\n"
						"\t\t\tscaling the number of processing threads to the number of computer cores. Without -w Konclude decides itself ('SMART'):\n"
						"\t\t\tup to 4 threads for small inputs and 1 for large ABoxes (several threads need several times the memory there).\n"
						"\t-m X : (Linux) Memory limit for the run: X is a number of megabytes, 'AUTO' (80 % of the available memory) or 'OFF'. The command is run in a\n"
						"\t\t\tchild process that is restarted with another hash seed when it passes the limit before the class hierarchy is computed (the search\n"
						"\t\t\torder depends on the seed, the result does not). Without -m this is only done for input files larger than 4 MB.\n"
						"\t-c FILEPATH : Path and file name of the config file.\n"
						"\t-i FILEPATH : Path and file name of the ontology input file / OWLlink request file.\n"
						"\t-o FILEPATH : Path and file name of the output / (SPARQL) result / (OWLlink) response file.\n"
						"\t-s FILEPATH : Path and file name of the SPARQL request/query file.\n"
						"\t-x IRI : IRI of the input entity.\n"
						"\t--subclass SUB SUPER : (explain) explains why SUB is a subclass of SUPER.\n"
						"\t--type INDIVIDUAL CLASS : (explain) explains why INDIVIDUAL is an instance of CLASS.\n"
						"\t--timeout SECONDS : (explain) timeout for every single reasoner call, default 60, 0 = none.\n"
						"\t--call-workers N : (explain) number of processing threads of every reasoner call, default 1.\n"
						"\t--no-slicing : (explain) do not first shrink the ABox by connected components of individuals.\n"
						"\t--no-verify : (explain) skip the check that the rewritten ontology gives the same answer as the input file.\n"
						"\t--flatten FILE : (explain) only write the ontology as functional syntax (one axiom per line) to FILE.\n"
						"\t-p PORT: Specifies PORT as the listening port for the OWLlink/SPARQL server.\n"
						"\t-a : Periodically prints the progress of the current activities.\n"
						"\t-v : Shows loading and processing times in more detail.\n"
						"\t-h : Shows this help (overwrites all other arguments/parameters).\n"
						"\n"
						"Examples:\n"
						"\tKonclude owllinkfile -i Tests/test-request.xml -o test-response.xml\n"
						"\tKonclude owllinkfile -c Configs/default-config.xml -i Tests/test-request.xml -o test-response.xml\n"
						"\tKonclude owllinkserver -w 2 -c Configs/default-config.xml -p 8080\n"
						"\tKonclude satisfiability -i Tests/galen.owl.xml -x http://ex.test/galen#ExternalIliacVein\n"
						"\tKonclude classification -w AUTO -i Tests/roberts-family-full-D.owl.xml -o Tests/roberts-family-full-D-class.owl.xml\n"
						"\tKonclude realization -w AUTO -i Tests/roberts-family-full-D.owl.xml -o Tests/roberts-family-full-D-real.owl.xml\n"
						"\tKonclude materialize -w AUTO -i Tests/roberts-family-full-D.owl.xml -o Tests/roberts-family-full-D-materialized.owl.xml\n"
						"\tKonclude explain -i Tests/inconsistent.owl\n"
						"\tKonclude explain -i Tests/ontology.owl --subclass \"<http://ex.org/A>\" \"<http://ex.org/B>\" -o justification.txt\n"
						"\tKonclude explain -i Tests/ontology.owl --type \"<http://ex.org/i>\" \"<http://ex.org/C>\" --timeout 120\n"
						"\tKonclude lint -i Tests/roberts-family-full-D.owl.xml\n"
						"\tKonclude interval -i timeline.ttl -o derived-relations.nt\n"
						"\tKonclude sparqlfile -s Tests/lubm-univ-bench-sparql-load-and-query-test.sparql -o Tests/query-answers.xml\n"
						"\tKonclude sparqlfile -s Tests/roberts-family-full-sparql-existential-variables-query-test.sparql -i Tests/roberts-family-full-D.owl.xml\n"
						"\tKonclude sparqlserver -p 8080 -c Configs/querying-config.xml\n"
						"\n"
						),this);

				CLogger::getInstance()->waitSynchronization();
				QCoreApplication::exit();

				return this;
			}

			CLoader *CCommandLineTranlatorHelpLoader::exit() {
				return this;
			}



		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
