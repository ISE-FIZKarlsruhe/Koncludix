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

#ifndef KONCLUDE_PARSER_CLINTPARSERECORDER_H
#define KONCLUDE_PARSER_CLINTPARSERECORDER_H

// Libraries includes
#include <QString>
#include <QStringList>
#include <QList>
#include <QMutex>

// Namespace includes
#include "Expressions/CBuildExpression.h"

namespace Konclude {

	namespace Parser {

		using namespace Expression;

		/*!
		 *
		 *		\class		CLintParseRecorder
		 *		\brief		The parsers skip owl:hasKey axioms (they are not part of the internal ontology
		 *					representation). While the "lint" command is running, the parsers report the keys
		 *					they skip to this recorder, so that lint can still warn about them.
		 *					Outside of "lint" recording is switched off and the parsers only evaluate a flag.
		 *
		 */
		class CLintParseRecorder {
			public:
				struct CHasKeyRecord {
					QString mText;					// functional syntax text of the axiom
					QStringList mClassNames;		// named classes occurring in the key class expression
					QStringList mObjectProperties;	// key object properties
					QStringList mDataProperties;	// key data properties
					QStringList mClassObjectProperties;	// object properties occurring in the key class expression
				};

				static void setRecording(bool recording);
				static bool isRecording();

				//! called by the parsers for every parsed (and skipped) HasKey axiom; the expressions may be null
				static void recordHasKey(CBuildExpression* classExpression, const QList<CBuildExpression*>& objectKeyProperties, const QList<CBuildExpression*>& dataKeyProperties);

				//! called by parsers that cannot tell object from data properties: they are sorted by their expression type
				static void recordHasKeyMixedProperties(CBuildExpression* classExpression, const QList<CBuildExpression*>& keyProperties);

				static QList<CHasKeyRecord> getHasKeyRecords();
				static void clearRecords();
		};

	}; // end namespace Parser

}; // end namespace Konclude

#endif // KONCLUDE_PARSER_CLINTPARSERECORDER_H
