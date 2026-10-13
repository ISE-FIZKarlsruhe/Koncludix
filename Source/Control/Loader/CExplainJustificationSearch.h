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

#ifndef KONCLUDE_CONTROL_LOADER_CEXPLAINJUSTIFICATIONSEARCH_H
#define KONCLUDE_CONTROL_LOADER_CEXPLAINJUSTIFICATIONSEARCH_H

// Libraries includes
#include <QString>
#include <QVector>
#include <QHash>
#include <QByteArray>

#include <functional>


namespace Konclude {

	namespace Control {

		namespace Loader {

			/*!
			 *
			 *		\class		CExplainOracle
			 *		\brief		Answers "does this subset of the axioms (with the fixed rest of the test ontology) lead to the
			 *					inconsistency?". Axioms are identified by their index; the index vector is sorted.
			 *					Must be monotone for the minimality guarantee (OWL 2 DL is monotone).
			 *
			 */
			class CExplainOracle {
				public:
					enum EVerdict { ENTAILED, NOT_ENTAILED, UNKNOWN };
					virtual ~CExplainOracle() {}
					virtual EVerdict test(const QVector<int>& sortedAxiomIndices) = 0;
			};


			/*!
			 *
			 *		\class		CExplainJustificationSearch
			 *		\brief		Delta debugging (divide-and-conquer shrinking, same loop as tools/explain.py) that shrinks a set of axioms
			 *					to a 1-minimal one: every accepted shrinking step has been confirmed by the oracle, and the last stage
			 *					tests the removal of every single remaining axiom. An answer UNKNOWN (timeout, error) counts as
			 *					"not entailed", i.e. the axiom stays; this keeps the result a (verified) justification.
			 *
			 *					Stages (every step is verified by the oracle):
			 *					1. optional speed-up: units = connected components of the ABox axioms (TBox/RBox axioms fixed),
			 *					2. units = all remaining axioms, nothing fixed: this stage gives the 1-minimality.
			 *					(Separate stages for ABox axioms with fixed TBox and the other way round were tried; they were slower, because
			 *					a consistent test with the full TBox needs the full reasoning, while mixed subsets lose TBox axioms early.)
			 */
			class CExplainJustificationSearch {
				public:
					CExplainJustificationSearch(CExplainOracle* oracle, std::function<void(const QString&)> progress = std::function<void(const QString&)>());

					//! axiomCount axioms 0..axiomCount-1, isAssertion[i] tells whether axiom i is an ABox axiom, aboxComponents groups the ABox axioms
					//! (may be empty to skip stage 1). The caller has to check beforehand that all axioms entail.
					QVector<int> search(int axiomCount, const QVector<bool>& isAssertion, const QVector<QVector<int> >& aboxComponents);

					//! Number of oracle calls that were really made (answers from the cache are not counted)
					int getOracleCallCount() const;

					//! Verdict for the given set, memoized
					CExplainOracle::EVerdict test(const QVector<int>& sortedAxiomIndices);

				protected:
					QVector<int> shrink(const QVector<int>& base, const QVector<QVector<int> >& units, const QString& stageName);
					CExplainOracle::EVerdict testUnits(const QVector<int>& base, const QVector<QVector<int> >& units, const QVector<int>& chosenUnits);

				protected:
					CExplainOracle* mOracle;
					std::function<void(const QString&)> mProgress;
					QHash<QByteArray, int> mCache;
					int mCalls;
			};

		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_LOADER_CEXPLAINJUSTIFICATIONSEARCH_H
