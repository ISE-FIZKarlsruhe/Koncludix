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

#ifndef KONCLUDE_PARSER_CLINTEXPRESSIONRENDERER_H
#define KONCLUDE_PARSER_CLINTEXPRESSIONRENDERER_H

// Libraries includes
#include <QString>
#include <QStringList>
#include <QList>

// Namespace includes
#include "Expressions/CBuildExpression.h"

namespace Konclude {

	namespace Parser {

		using namespace Expression;

		/*!
		 *
		 *		\class		CLintExpressionRenderer
		 *		\brief		Writes parsed expressions and axioms as text in OWL 2 functional syntax
		 *					(IRIs in angle brackets, like the OWL API's toString), and collects
		 *					the named entities occurring inside an expression.
		 *					Only used by the "lint" command to print example axioms.
		 *
		 */
		class CLintExpressionRenderer {
			public:
				//! renders any expression or axiom
				static QString render(CBuildExpression* expression);

				//! the IRI/name of a named entity expression (class, property, individual, datatype); empty string for other expressions
				static QString entityName(CBuildExpression* expression);

				//! collects the names of all named entities of the given type that occur in the expression (including the expression itself)
				static void collectEntityNames(CBuildExpression* expression, CBuildExpression::ExpressionType entityType, QStringList& names);

			protected:
				static QString renderList(const QList<CBuildExpression*>& list);
				static QString renderSpaced(const QList<CBuildExpression*>& list);
				static QString renderGeneric(CBuildExpression* expression);
		};

	}; // end namespace Parser

}; // end namespace Konclude

#endif // KONCLUDE_PARSER_CLINTEXPRESSIONRENDERER_H
