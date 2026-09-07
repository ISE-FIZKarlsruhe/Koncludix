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

#include "CCLIMaterializeBatchProcessingLoader.h"


namespace Konclude {

	namespace Control {

		namespace Loader {


			CCLIMaterializeBatchProcessingLoader::CCLIMaterializeBatchProcessingLoader() {
			}



			CCLIMaterializeBatchProcessingLoader::~CCLIMaterializeBatchProcessingLoader() {
			}



			void CCLIMaterializeBatchProcessingLoader::createTestingCommands() {
				createMaterializeTestingCommands();
			}


			void CCLIMaterializeBatchProcessingLoader::createMaterializeTestingCommands() {
				if (mIRINameString.isEmpty()) {
					logOutputMessage(QString("Starting materialization processing for ontology '%1'.").arg(mRequestFileString));
				} else {
					logOutputMessage(QString("Starting materialization processing of individual '%1' for ontology '%2'.").arg(mIRINameString).arg(mRequestFileString));
				}
				QString testKB = QString("http://konclude.com/test/kb");
				CCreateKnowledgeBaseCommand* createKBCommand = new CCreateKnowledgeBaseCommand(testKB);
				QStringList ontoIRIList;
				ontoIRIList.append(mRequestFileString);
				CLoadKnowledgeBaseOWLAutoOntologyCommand* loadKBCommand = new CLoadKnowledgeBaseOWLAutoOntologyCommand(testKB,ontoIRIList);
				CRealizeQueryCommand* realizeKBCommand = new CRealizeQueryCommand(testKB);
				addProcessingCommand(createKBCommand);
				addProcessingCommand(loadKBCommand);
				addProcessingCommand(realizeKBCommand);
				// Plain realization only precomputes concept (class) instances by
				// default -- role instances are left for lazy, on-demand
				// confirmation the first time something actually asks for them
				// (see CRealizeQuery's "RealizePrecomputation.RoleInstances" config,
				// which defaults to false). Reading role instances directly via
				// CRoleRealization::visitTargetIndividuals (as the write step below
				// does) without going through the proper per-pair
				// "requiresSourceIndividualRolesRealization -> request -> wait"
				// protocol the SPARQL/complex-query Answerer uses is exactly what
				// produced non-deterministic, occasionally-incomplete
				// ObjectPropertyAssertion output under load. CForceRoleRealizationQuery
				// uses the dynamic-realization-requirement mechanism (the same
				// mechanism CFlattenedObjectPropertyTargetsQuery uses for a single
				// (individual, role) pair) with a wildcard request -- role=nullptr,
				// individual references left empty -- which the realizer's
				// dynamic-requirement dispatch resolves as "every object role, every
				// individual" (see queueRoleFillerInstanceRealization's role==nullptr
				// handling and the empty/empty branch that calls
				// markIntanceItemForRoleRealization). This properly, synchronously
				// blocks until that is complete before the write step below reads
				// anything.
				CForceRoleRealizationQueryCommand* forceRoleRealizationCommand = new CForceRoleRealizationQueryCommand(testKB);
				addProcessingCommand(forceRoleRealizationCommand);
				// Same-individual merges (owl:FunctionalProperty, cardinality
				// restrictions, hasKey, ...) are, like role instances above,
				// only ever lazily/partially computed by plain realization --
				// CForceSameIndividualsRealizationQuery uses the analogous
				// dynamic-realization-requirement mechanism (wildcard: both
				// individual references left empty) to force every
				// individual's tableau-merged/nominal-equal set to be
				// completely known before the write step below reads it via
				// CRealization::getSameRealization().
				CForceSameIndividualsRealizationQueryCommand* forceSameIndividualsRealizationCommand = new CForceSameIndividualsRealizationQueryCommand(testKB);
				addProcessingCommand(forceSameIndividualsRealizationCommand);
				if (!mResponseFileString.isEmpty()) {
					// Object/data property (sub-)classification is not a realization
					// premise, so it is never computed as a side effect of the
					// CRealizeQueryCommand above -- unlike the concept taxonomy, which
					// realization always needs and which the classifier therefore
					// caches on the ontology unconditionally. Priming these two
					// classification-premising write commands first forces the role
					// hierarchies to be computed and cached on the ontology's shared
					// CClassification object, so the combined materialization command
					// below can read them back via CConcreteOntology::getClassification().
					// Their own output is immediately superseded by the real
					// materialization write below, so it is discarded to the same path.
					CWriteCustomQueryCommand* primeObjectPropertyHierarchyCommand = new CWriteCustomQueryCommand(testKB,CWriteQuery::WRITESUBOBJECTPROPERTYHIERARCHY,new CWriteQueryFileOWL2XMLSerializer(mResponseFileString));
					CWriteCustomQueryCommand* primeDataPropertyHierarchyCommand = new CWriteCustomQueryCommand(testKB,CWriteQuery::WRITESUBDATAPROPERTYHIERARCHY,new CWriteQueryFileOWL2XMLSerializer(mResponseFileString));
					addProcessingCommand(primeObjectPropertyHierarchyCommand);
					addProcessingCommand(primeDataPropertyHierarchyCommand);

					CWriteCustomQueryCommand* writeAssertionsCommand = new CWriteCustomQueryCommand(testKB,CWriteQuery::WRITEMATERIALIZEDINDIVIDUALASSERTIONS,new CWriteQueryFileOWL2XMLSerializer(mResponseFileString));
					addProcessingCommand(writeAssertionsCommand);
				}
				processNextCommand();
			}




		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
