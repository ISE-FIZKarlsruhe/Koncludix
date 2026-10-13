/*
 *		Copyright (C) 2013, 2014 by the Konclude Developer Team.
 *
 *		This file is part of the reasoning system Konclude.
 *		For details and support, see <http://konclude.com/>.
 *
 *		Konclude is free software: you can redistribute it and/or modify it under
 *		the terms of version 3 of the GNU Lesser General Public License (LGPL3).
 */

#ifndef KONCLUDE_CONTROL_LOADER_CINTERVALALGEBRA_H
#define KONCLUDE_CONTROL_LOADER_CINTERVALALGEBRA_H

// Libraries includes
#include <QtGlobal>
#include <QVector>
#include <QString>

// Namespace includes

namespace Konclude {

	namespace Control {

		namespace Loader {

			/*!
			 *
			 *		\class		CIntervalAlgebra
			 *		\brief		Allen's interval algebra: the 13 basic relations, their inverses, the composition table and path consistency.
			 *
			 *					Sets of relations are 13 bit masks (bit i = relation i). The composition table is not typed in: it is derived once from all triples of
			 *					intervals over six points (three intervals have at most six distinct end points, so every qualitative configuration occurs).
			 *					Path consistency is sound (an empty set proves that a network is inconsistent) but not complete for general networks.
			 *
			 */
			class CIntervalAlgebra {
				// public types and constants
				public:
					enum Relation { BEFORE = 0, MEETS, OVERLAPS, STARTS, DURING, FINISHES, EQUALS, AFTER, MET_BY, OVERLAPPED_BY, STARTED_BY, CONTAINS, FINISHED_BY, RELATION_COUNT };
					//! two further asserted relations that are sets of basic relations (the BFO relations): precedes = before or meets (the end of x is not after the start of y), preceded by = after or met by
					enum SetRelation { PRECEDES = 13, PRECEDED_BY = 14, ASSERTED_RELATION_COUNT = 15 };
					static const quint16 FULL_MASK = (1 << 13) - 1;

				// public methods
				public:
					//! the relation r with r(i, j) for i = [a,b], j = [c,d] (a < b, c < d)
					static int relation(qint64 a, qint64 b, qint64 c, qint64 d);

					static quint16 bit(int relation) { return (quint16)(1 << relation); }
					//! the set of basic relations of an asserted relation (a basic relation, PRECEDES or PRECEDED_BY)
					static quint16 maskOf(int relation);
					static int inverse(int relation);
					static quint16 inverseMask(quint16 mask);
					static quint16 compose(quint16 mask1, quint16 mask2);

					static const char* name(int relation);
					//! the relation named like the Allen relations ("before", "meets", ...), like OWL-Time ("intervalBefore", ...) or like the BFO relations ("precedes", "preceded by", BFO_0000063, BFO_0000062), -1 if the name is no relation
					static int relationFromLocalName(const QString& localName);
					//! the local name used for the relation in the output: OWL-Time style ("intervalBefore") or Allen style ("before")
					static QString localNameFor(int relation, bool owlTimeStyle);

					//! path consistency on a network with n nodes; C is the n*n matrix of constraint masks (C[i*n+j]); returns false and the pair if a constraint becomes empty
					static bool pathConsistency(int n, QVector<quint16>& C, int* conflictI = nullptr, int* conflictJ = nullptr);

			};

		}; // end namespace Loader

	}; // end namespace Control

}; // end namespace Konclude

#endif // KONCLUDE_CONTROL_LOADER_CINTERVALALGEBRA_H
