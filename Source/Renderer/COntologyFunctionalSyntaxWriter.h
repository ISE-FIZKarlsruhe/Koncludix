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

#ifndef KONCLUDE_RENDERER_CONTOLOGYFUNCTIONALSYNTAXWRITER_H
#define KONCLUDE_RENDERER_CONTOLOGYFUNCTIONALSYNTAXWRITER_H

// Libraries includes
#include <QString>
#include <QStringList>
#include <QList>

// Namespace includes
#include "RendererSettings.h"

// Other includes
#include "Reasoner/Ontology/CConcreteOntology.h"

#include "Parser/Expressions/CAxiomExpression.h"
#include "Parser/Expressions/CClassTermExpression.h"
#include "Parser/Expressions/CObjectPropertyTermExpression.h"
#include "Parser/Expressions/CDataPropertyTermExpression.h"
#include "Parser/Expressions/CDataRangeTermExpression.h"
#include "Parser/Expressions/CIndividualTermExpression.h"
#include "Parser/Expressions/CDataLiteralExpression.h"

// Logger includes
#include "Logger/CLogger.h"

using namespace Konclude::Logger;

namespace Konclude {

	using namespace Reasoner::Ontology;
	using namespace Parser::Expression;

	namespace Renderer {

		/*!
		 *
		 *		\class		COntologyFunctionalSyntaxWriter
		 *		\brief		Writes the axiom expressions of a loaded ontology as OWL 2 functional syntax,
		 *					one axiom per string. IRIs are always written in full (<...>), so the output does
		 *					not depend on prefix declarations. In contrast to the generic renderers in this
		 *					directory, every axiom/expression kind that cannot be written makes the writer
		 *					fail with an error message (nothing is silently dropped).
		 *
		 */
		class COntologyFunctionalSyntaxWriter {
			// public methods
			public:
				//! Kind of a written axiom
				enum AxiomKind { AK_DECLARATION, AK_ASSERTION, AK_OTHER };

				//! One written axiom
				struct CWrittenAxiom {
					QString mText;
					AxiomKind mKind;
					QStringList mIndividuals;		// names (IRI or _:label) of all individuals occurring in the axiom
				};

				//! Constructor
				COntologyFunctionalSyntaxWriter();

				//! Writes one axiom, returns false (and sets the error text) if it cannot be written
				bool writeAxiom(CAxiomExpression* axiom, CWrittenAxiom& written);

				//! Writes all told axioms (ordered by creation serial) and all assertions that are stored as triples
				bool writeOntology(CConcreteOntology* ontology, QList<CWrittenAxiom>& writtenAxioms);

				const QString& getError() const;

				static QString iri(const QString& name);
				static QString literal(const QString& lexicalValue, const QString& datatypeIri);
				static QString anonymousIndividual(const QString& nodeLabel);

			// protected methods
			protected:
				QString writeClass(CClassTermExpression* exp);
				QString writeObjectProperty(CObjectPropertyTermExpression* exp);
				QString writeDataProperty(CDataPropertyTermExpression* exp);
				QString writeDataRange(CDataRangeTermExpression* exp);
				QString writeLiteral(CDataLiteralExpression* exp);
				QString writeIndividual(CIndividualTermExpression* exp);

				QString writeClassList(CEXPRESSIONLIST<CClassTermExpression*>* list);
				QString writeObjectPropertyList(CEXPRESSIONLIST<CObjectPropertyTermExpression*>* list);
				QString writeDataPropertyList(CEXPRESSIONLIST<CDataPropertyTermExpression*>* list);
				QString writeIndividualList(CEXPRESSIONLIST<CIndividualTermExpression*>* list);
				QString writeDataRangeList(CEXPRESSIONLIST<CDataRangeTermExpression*>* list);

				bool failure(const QString& message);

				bool writeTriplesAssertions(CConcreteOntology* ontology, QList<CWrittenAxiom>& writtenAxioms);

			// protected variables
			protected:
				QString mError;
				QStringList mCurrentIndividuals;
		};

	}; // end namespace Renderer

}; // end namespace Konclude

#endif // KONCLUDE_RENDERER_CONTOLOGYFUNCTIONALSYNTAXWRITER_H
