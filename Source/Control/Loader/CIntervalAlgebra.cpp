/*
 *		Copyright (C) 2013, 2014 by the Konclude Developer Team.
 *
 *		This file is part of the reasoning system Konclude.
 *		For details and support, see <http://konclude.com/>.
 *
 *		Konclude is free software: you can redistribute it and/or modify it under
 *		the terms of version 3 of the GNU Lesser General Public License (LGPL3).
 */

#include "CIntervalAlgebra.h"

namespace Konclude {

	namespace Control {

		namespace Loader {

			namespace {

				const char* ALLEN_NAMES[13] = { "before", "meets", "overlaps", "starts", "during", "finishes", "equals", "after", "metBy", "overlappedBy", "startedBy", "contains", "finishedBy" };
				const char* OWLTIME_NAMES[13] = { "intervalBefore", "intervalMeets", "intervalOverlaps", "intervalStarts", "intervalDuring", "intervalFinishes", "intervalEquals",
												  "intervalAfter", "intervalMetBy", "intervalOverlappedBy", "intervalStartedBy", "intervalContains", "intervalFinishedBy" };
				const int INVERSE[13] = { CIntervalAlgebra::AFTER, CIntervalAlgebra::MET_BY, CIntervalAlgebra::OVERLAPPED_BY, CIntervalAlgebra::STARTED_BY, CIntervalAlgebra::CONTAINS,
										  CIntervalAlgebra::FINISHED_BY, CIntervalAlgebra::EQUALS, CIntervalAlgebra::BEFORE, CIntervalAlgebra::MEETS, CIntervalAlgebra::OVERLAPS,
										  CIntervalAlgebra::STARTS, CIntervalAlgebra::DURING, CIntervalAlgebra::FINISHES };

				struct CCompositionTable {
					quint16 mTable[13][13];
					CCompositionTable() {
						for (int i = 0; i < 13; ++i) { for (int j = 0; j < 13; ++j) { mTable[i][j] = 0; } }
						// all intervals over the six points 0..5, all triples
						QVector<int> starts, ends;
						for (int a = 0; a < 6; ++a) { for (int b = a + 1; b < 6; ++b) { starts.append(a); ends.append(b); } }
						int n = starts.size();
						for (int x = 0; x < n; ++x) {
							for (int y = 0; y < n; ++y) {
								int r1 = CIntervalAlgebra::relation(starts[x], ends[x], starts[y], ends[y]);
								for (int z = 0; z < n; ++z) {
									int r2 = CIntervalAlgebra::relation(starts[y], ends[y], starts[z], ends[z]);
									int r3 = CIntervalAlgebra::relation(starts[x], ends[x], starts[z], ends[z]);
									mTable[r1][r2] |= CIntervalAlgebra::bit(r3);
								}
							}
						}
					}
				};

				const CCompositionTable& compositionTable() {
					static CCompositionTable table;
					return table;
				}

			}


			int CIntervalAlgebra::relation(qint64 a, qint64 b, qint64 c, qint64 d) {
				if (b < c) { return BEFORE; }
				if (b == c) { return MEETS; }
				if (d < a) { return AFTER; }
				if (d == a) { return MET_BY; }
				if (a == c && b == d) { return EQUALS; }
				if (a == c) { return b < d ? STARTS : STARTED_BY; }
				if (b == d) { return a > c ? FINISHES : FINISHED_BY; }
				if (c < a && b < d) { return DURING; }
				if (a < c && d < b) { return CONTAINS; }
				if (a < c && c < b && b < d) { return OVERLAPS; }
				return OVERLAPPED_BY;
			}


			int CIntervalAlgebra::inverse(int relation) {
			if (relation == PRECEDES) { return PRECEDED_BY; }
			if (relation == PRECEDED_BY) { return PRECEDES; }
			return INVERSE[relation];
			}


			quint16 CIntervalAlgebra::maskOf(int relation) {
			if (relation == PRECEDES) { return bit(BEFORE) | bit(MEETS); }
			if (relation == PRECEDED_BY) { return bit(AFTER) | bit(MET_BY); }
			return bit(relation);
			}


			quint16 CIntervalAlgebra::inverseMask(quint16 mask) {
				quint16 result = 0;
				for (int i = 0; i < 13; ++i) {
					if (mask & bit(i)) { result |= bit(INVERSE[i]); }
				}
				return result;
			}


			quint16 CIntervalAlgebra::compose(quint16 mask1, quint16 mask2) {
				const CCompositionTable& table = compositionTable();
				quint16 result = 0;
				for (int i = 0; i < 13; ++i) {
					if (mask1 & bit(i)) {
						for (int j = 0; j < 13; ++j) {
							if (mask2 & bit(j)) { result |= table.mTable[i][j]; }
						}
					}
				}
				return result;
			}


			const char* CIntervalAlgebra::name(int relation) {
			if (relation == PRECEDES) { return "precedes"; }
			if (relation == PRECEDED_BY) { return "precededBy"; }
			return ALLEN_NAMES[relation];
			}


			int CIntervalAlgebra::relationFromLocalName(const QString& localName) {
				for (int i = 0; i < 13; ++i) {
					if (localName == QLatin1String(ALLEN_NAMES[i]) || localName == QLatin1String(OWLTIME_NAMES[i])) { return i; }
					}
					if (localName == QLatin1String("precedes") || localName == QLatin1String("BFO_0000063")) { return PRECEDES; }
					if (localName == QLatin1String("precededBy") || localName == QLatin1String("preceded_by") || localName == QLatin1String("BFO_0000062")) { return PRECEDED_BY; }
					return -1;
			}


			QString CIntervalAlgebra::localNameFor(int relation, bool owlTimeStyle) {
				return QString::fromLatin1(owlTimeStyle ? OWLTIME_NAMES[relation] : ALLEN_NAMES[relation]);
			}


			bool CIntervalAlgebra::pathConsistency(int n, QVector<quint16>& C, int* conflictI, int* conflictJ) {
				bool changed = true;
				while (changed) {
					changed = false;
					for (int i = 0; i < n; ++i) {
						for (int j = 0; j < n; ++j) {
							if (i == j) { continue; }
							for (int k = 0; k < n; ++k) {
								if (k == i || k == j) { continue; }
								quint16 current = C[i * n + j];
								quint16 refined = current & compose(C[i * n + k], C[k * n + j]);
								if (refined != current) {
									if (refined == 0) {
										if (conflictI) { *conflictI = i; }
										if (conflictJ) { *conflictJ = j; }
										return false;
									}
									C[i * n + j] = refined;
									C[j * n + i] = inverseMask(refined);
									changed = true;
								}
							}
						}
					}
				}
				return true;
			}


		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude
