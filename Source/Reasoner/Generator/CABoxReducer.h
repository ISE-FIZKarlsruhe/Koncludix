/*
 *		Optional ABox reduction for the materialize command (Koncludix extension).
 *
 *		This file is part of the Konclude fork. It is distributed under the same terms as Konclude
 *		(GNU Lesser General Public License, version 3).
 */

#ifndef KONCLUDE_REASONER_GENERATOR_CABOXREDUCER_H
#define KONCLUDE_REASONER_GENERATOR_CABOXREDUCER_H

// Libraries includes
#include <QString>
#include <QList>

// Namespace includes
#include "CConcreteOntologyUpdateCollectorBuilder.h"

// Other includes
#include "Config/CConfiguration.h"
#include "Config/CConfigDataReader.h"

#include "Reasoner/Ontology/CConcreteOntology.h"
#include "Reasoner/Ontology/CABoxReductionData.h"

// Logger includes
#include "Logger/CLogger.h"


namespace Konclude {

	using namespace Config;

	namespace Reasoner {

		using namespace Ontology;

		namespace Generator {

			/*!
			 *
			 *		\class		CABoxReducer
			 *		\brief		Optional ABox reduction for the materialize command.
			 *
			 *					Individuals that are interchangeable (same asserted types, same data shape and the same neighbour classes, i.e.
			 *					the same colour after colour refinement) are represented by one individual; the materialization is computed for
			 *					the reduced ABox and the writer (CWriteMaterializedIndividualAssertionsQuery) copies the results to all individuals
			 *					that were left out.
			 *
			 *					Individuals that are never merged (pinned): individuals that are mentioned in TBox/RBox axioms (nominals), in
			 *					sameAs/differentFrom/negative assertions, in complex class assertions, anonymous individuals and their neighbours,
			 *					individuals with an asserted self-loop and individuals that are incident to an asserted edge of a role with
			 *					cardinality restrictions, functional, inverse functional, reflexive, irreflexive, asymmetric, disjoint, transitive or
			 *					chain axioms (including all roles related by sub-property/inverse/equivalence axioms).
			 *
			 *					The reduction is applied on the told axioms after parsing and before the reasoner's ontology is built, it is configured by
			 *					Konclude.CLI.Materialize.ReduceABox (off | on | auto).
			 */
			class CABoxReducer {
				// public methods
				public:
					enum REDUCTIONMODE { REDUCTIONOFF, REDUCTIONON, REDUCTIONAUTO };

					//! reads Konclude.CLI.Materialize.ReduceABox
					static REDUCTIONMODE readReductionMode(CConfiguration* config);

					/*! Analyses the told axioms of the ontology and, if requested (and worthwhile), removes the axioms of the interchangeable individuals
					 *  from the (still open) parsing session of the builder. Returns true if individuals were removed.
					 *  The information needed to expand the results is attached to the load data of the ontology.
					 */
					static bool reduceOntology(CConcreteOntology* ontology, CConcreteOntologyUpdateCollectorBuilder* builder, CConfiguration* config, const QList<QString>& pendingImportList);
			};

		}; // end namespace Generator

	}; // end namespace Reasoner

}; // end namespace Konclude

#endif // KONCLUDE_REASONER_GENERATOR_CABOXREDUCER_H
