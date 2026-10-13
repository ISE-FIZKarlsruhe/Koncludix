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

#include "CExplainJustificationSearch.h"

#include <QCryptographicHash>

#include <algorithm>


namespace Konclude {

	namespace Control {

		namespace Loader {


			CExplainJustificationSearch::CExplainJustificationSearch(CExplainOracle* oracle, std::function<void(const QString&)> progress) {
				mOracle = oracle;
				mProgress = progress;
				mCalls = 0;
			}


			int CExplainJustificationSearch::getOracleCallCount() const {
				return mCalls;
			}


			CExplainOracle::EVerdict CExplainJustificationSearch::test(const QVector<int>& sortedAxiomIndices) {
				QByteArray key = QCryptographicHash::hash(QByteArray((const char*)sortedAxiomIndices.constData(), sortedAxiomIndices.size() * (int)sizeof(int)), QCryptographicHash::Sha1);
				key.append(QByteArray::number(sortedAxiomIndices.size()));
				QHash<QByteArray, int>::const_iterator it = mCache.constFind(key);
				if (it != mCache.constEnd()) {
					return (CExplainOracle::EVerdict)it.value();
				}
				++mCalls;
				CExplainOracle::EVerdict verdict = mOracle->test(sortedAxiomIndices);
				mCache.insert(key, (int)verdict);
				if (mProgress && mCalls % 10 == 0) {
					mProgress(QString("%1 reasoner calls so far (last test: %2 axioms, %3)").arg(mCalls).arg(sortedAxiomIndices.size())
							.arg(verdict == CExplainOracle::ENTAILED ? "entailed" : (verdict == CExplainOracle::NOT_ENTAILED ? "not entailed" : "no answer")));
				}
				return verdict;
			}


			CExplainOracle::EVerdict CExplainJustificationSearch::testUnits(const QVector<int>& base, const QVector<QVector<int> >& units, const QVector<int>& chosenUnits) {
				QVector<int> indices = base;
				for (int u : chosenUnits) {
					indices += units[u];
				}
				std::sort(indices.begin(), indices.end());
				return test(indices);
			}


			// Same loop as in tools/explain.py (complement testing with growing granularity); returns the indices of the surviving units
			QVector<int> CExplainJustificationSearch::shrink(const QVector<int>& base, const QVector<QVector<int> >& units, const QString& stageName) {
				QVector<int> current(units.size());
				for (int i = 0; i < current.size(); ++i) {
					current[i] = i;
				}
				int startCalls = mCalls;
				int startSize = current.size();
				int n = 2;
				while (current.size() >= 2) {
					int chunk = std::max(1, (int)current.size() / n);
					QVector<QVector<int> > subsets;
					for (int i = 0; i < current.size(); i += chunk) {
						subsets.append(current.mid(i, chunk));
					}
					bool reduced = false;
					for (int i = 0; i < subsets.size() && !reduced; ++i) {
						QVector<int> complement;
						for (int j = 0; j < subsets.size(); ++j) {
							if (j != i) {
								complement += subsets[j];
							}
						}
						if (!complement.isEmpty() && testUnits(base, units, complement) == CExplainOracle::ENTAILED) {
							current = complement;
							n = std::max(n - 1, 2);
							reduced = true;
						}
					}
					if (!reduced) {
						if (n >= current.size()) {
							break;
						}
						n = std::min((int)current.size(), n * 2);
					}
				}
				if (current.size() == 1 && testUnits(base, units, QVector<int>()) == CExplainOracle::ENTAILED) {
					current.clear();
				}
				if (mProgress && startSize > 0) {
					mProgress(QString("%1: %2 -> %3 unit(s) (%4 reasoner calls so far)").arg(stageName).arg(startSize).arg(current.size()).arg(mCalls));
				}
				(void)startCalls;
				return current;
			}


			QVector<int> CExplainJustificationSearch::search(int axiomCount, const QVector<bool>& isAssertion, const QVector<QVector<int> >& aboxComponents) {
				QVector<int> abox;
				QVector<int> other;
				for (int i = 0; i < axiomCount; ++i) {
					if (isAssertion[i]) {
						abox.append(i);
					} else {
						other.append(i);
					}
				}

				// stage 1: connected components of the ABox
				if (aboxComponents.size() >= 2) {
					QVector<QVector<int> > units = aboxComponents;
					QVector<int> kept = shrink(other, units, "stage 1 (ABox components)");
					QVector<int> newAbox;
					for (int u : kept) {
						newAbox += units[u];
					}
					std::sort(newAbox.begin(), newAbox.end());
					abox = newAbox;
				}

				// stage 2: every axiom that is left as its own unit, nothing fixed; this stage gives the 1-minimality
				QVector<int> all = other + abox;
				std::sort(all.begin(), all.end());
				QVector<QVector<int> > units;
				for (int a : all) {
					units.append(QVector<int>(1, a));
				}
				QVector<int> kept = shrink(QVector<int>(), units, "stage 2 (all remaining axioms)");
				QVector<int> result;
				for (int u : kept) {
					result.append(all[u]);
				}
				std::sort(result.begin(), result.end());
				return result;
			}


		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
