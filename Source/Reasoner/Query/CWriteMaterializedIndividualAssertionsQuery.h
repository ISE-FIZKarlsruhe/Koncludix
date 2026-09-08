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

#ifndef KONCLUDE_REASONER_QUERY_CWRITEMATERIALIZEDINDIVIDUALASSERTIONSQUERY_H
#define KONCLUDE_REASONER_QUERY_CWRITEMATERIALIZEDINDIVIDUALASSERTIONSQUERY_H

// Libraries includes
#include <QString>
#include <QDir>
#include <QStringList>
#include <QSet>
#include <QList>
#include <QHash>
#include <QXmlStreamWriter>


// Namespace includes
#include "CQuery.h"
#include "CRealizationPremisingQuery.h"
#include "CQueryUnspecifiedStringError.h"
#include "CQueryInconsitentOntologyError.h"
#include "CWriteQuery.h"

#include "Reasoner/Realization/CConceptRealizationInstantiatedToConceptVisitor.h"
#include "Reasoner/Realization/CRoleRealizationInstanceVisitor.h"
#include "Reasoner/Realization/CRoleRealization.h"
#include "Reasoner/Realization/CSameRealization.h"
#include "Reasoner/Realization/CSameRealizationIndividualVisitor.h"
#include "Reasoner/Ontology/CRole.h"
#include "Reasoner/Ontology/CRBox.h"
#include "Reasoner/Ontology/CRoleChain.h"
#include "Reasoner/Ontology/CDataAssertionLinker.h"
#include "Reasoner/Ontology/CDataLiteral.h"
#include "Reasoner/Ontology/CDatatype.h"
#include "Reasoner/Ontology/CIndividual.h"

#include "Reasoner/Taxonomy/CTaxonomy.h"
#include "Reasoner/Taxonomy/CHierarchyNode.h"
#include "Reasoner/Classification/CClassification.h"
#include "Reasoner/Classification/CPropertyRoleClassification.h"
#include "Reasoner/Taxonomy/CRolePropertiesHierarchy.h"
#include "Reasoner/Taxonomy/CRolePropertiesHierarchyNode.h"

#include "Config/CConfigDataReader.h"

// Logger includes
#include "Logger/CLogger.h"



namespace Konclude {

	using namespace Config;

	namespace Reasoner {

		using namespace Realization;
		using namespace Ontology;
		using namespace Taxonomy;
		using namespace Classification;

		namespace Query {

			/*!
			 *
			 *		\class		CWriteMaterializedIndividualAssertionsQuery
			 *		\brief		Writes both the flattened class assertions (as computed
			 *					during realisation) and the entailed object property
			 *					assertions (as computed by the role realizer) for every
			 *					individual into a single output ontology -- i.e. a real,
			 *					materialized ABox produced entirely by Konclude's native
			 *					tableau reasoning, without going through the SPARQL/Rasqal
			 *					query layer.
			 *
			 */
			class CWriteMaterializedIndividualAssertionsQuery : public CRealizationPremisingQuery, public CConceptRealizationInstantiatedToConceptVisitor, public CRoleRealizationInstanceVisitor, public CSameRealizationIndividualVisitor, public CWriteQuery {
				// public methods
				public:
					//! Constructor
					CWriteMaterializedIndividualAssertionsQuery(CConcreteOntology* ontology, CConfigurationBase *configuration, const QString& outputFileString, const QString& individualNameString = QString(""), const QString &queryName = QString("UnnamedWriteMaterializedIndividualAssertionsQuery"));

					virtual CQueryResult* constructResult(CRealization* realization);

					virtual WRITEQUERYTYPE getWriteQueryType();

					virtual QString getQueryName();
					virtual QString getQueryString();
					virtual bool hasAnswer();
					virtual QString getAnswerString();

					virtual CQueryResult *getQueryResult();

					virtual bool hasError();

					virtual CQuery* addQueryError(CQueryError* queryError);

				// protected methods
				protected:
					bool writeInconsistentIndividualAssertions();
					bool writeMaterializedAssertionsResult(CRealization* realization);

					virtual void writeIndividualDeclaration(const QString& individualName, bool anonymous) = 0;
					virtual void writeNamedIndividualDeclaration(const QString& individualName) = 0;
					virtual void writeAnonymousIndividualDeclaration(const QString& className) = 0;
					virtual void writeClassDeclaration(const QString& className) = 0;
					virtual void writeObjectPropertyDeclaration(const QString& propertyName) = 0;
					virtual void writeDataPropertyDeclaration(const QString& propertyName) = 0;

					virtual void writeIndividualType(const QString& individualName, bool anonymous, const QString& className) = 0;
					virtual void writeNamedIndividualType(const QString& individualName, const QString& className) = 0;
					virtual void writeAnonymousIndividualType(const QString& individualName, const QString& className) = 0;
					virtual void writeSubClassRelation(const QString& subClassName, const QString& superClassName) = 0;
					virtual void writeSubObjectPropertyRelation(const QString& subPropertyName, const QString& superPropertyName) = 0;
					virtual void writeSubDataPropertyRelation(const QString& subPropertyName, const QString& superPropertyName) = 0;

					virtual void writeClassEquivalenceRelations(const QStringList& classNameList) = 0;
					virtual void writeObjectPropertyEquivalenceRelations(const QStringList& propertyNameList) = 0;
					virtual void writeDataPropertyEquivalenceRelations(const QStringList& propertyNameList) = 0;

					virtual void writeObjectPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& objectName, bool objectAnonymous) = 0;
					virtual void writeDataPropertyAssertion(const QString& subjectName, bool subjectAnonymous, const QString& propertyName, const QString& lexicalValue, const QString& datatypeIRI) = 0;

					// Writes one SameIndividual(...) equivalence class -- every
					// individual name in individualNameList (parallel-indexed
					// with individualAnonymousList for named-vs-anonymous
					// serialization) is asserted pairwise-same. Called once per
					// non-trivial (size > 1) equivalence class discovered via
					// CSameRealization -- see visitIndividual() below and its
					// use in writeMaterializedAssertionsResult().
					virtual void writeIndividualEquivalenceRelations(const QStringList& individualNameList, const QList<bool>& individualAnonymousList) = 0;

					void writeTransitiveClassHierarchy();
					void writeTransitivePropertyHierarchy(bool dataProperties);

					// Propagates every collected (subject, role, target) fact in
					// mGlobalSubjectRoleTargets to every transitive super-role of
					// role, in place. Safe to call more than once (idempotent).
					void propagateSubProperties();

					// For every collected (subject, role, target) fact whose role has
					// a declared inverse role that is itself one of our active object
					// properties, adds (target, inverseRole, subject) into
					// mGlobalSubjectRoleTargets. Computed from a snapshot and applied
					// afterward, so it never sees its own newly-added facts within one
					// call -- call propagateSubProperties() again afterward so an
					// inverse-derived fact still rolls up to its own super-roles.
					void propagateInverseRoles();

					// For every active object-property role Q that one or more
					// SubObjectPropertyOf(P1 o P2 o ... o Pn, Q) property-chain axioms
					// entail (Q->getRoleChainSuperSharingLinker()), walks each chain as
					// a path composition over the currently collected facts: for every
					// subject x with x -P1-> y1 -P2-> y2 -> ... -Pn-> z, adds (x, Q, z).
					// Uses whatever propagateSubProperties()/propagateInverseRoles()
					// have already unioned into each step role, so call this after
					// those (and call them again afterward so a chain-derived fact
					// still rolls up to Q's own super-roles / gets its own inverse).
					void propagatePropertyChains();

					virtual void writeOntologyStart() = 0;
					virtual void writeOntologyEnd() = 0;
					virtual void writeOntologyPrefix(const QString& prefixName, const QString& prefixIRI) = 0;



					virtual bool visitConcept(CConcept* concept, CConceptRealization* conRealization);
					virtual bool visitRoleInstance(const CRealizationIndividualInstanceItemReference& indiRealItemRef, CRoleRealization* roleRealization);
					virtual bool visitIndividual(const CIndividualReference& indiRef, CSameRealization* sameRealization);

					// An individual loaded through Konclude's Redland-based RDF
					// parser (as opposed to its native OWL2-XML/Functional parser)
					// can have isAnonymous() report false for what is, in every
					// output-relevant sense, still a blank node -- its resolved
					// "name" is instead a synthesized identifier string that
					// itself starts with "_:" (not a valid absolute IRI; nothing
					// with a real IRI ever starts that way). Writing that string
					// as if it were a named resource produces genuinely invalid
					// output (e.g. Turtle's <_:...> is a malformed IRIREF, not a
					// blank node). Every anonymity check in this class goes
					// through here instead of using CIndividualNameResolver's
					// isAnonymous() result directly, so this one place decides
					// what "anonymous" means for OUTPUT purposes, regardless of
					// which parser loaded the individual.
					static bool isEffectivelyAnonymous(bool resolverAnonymous, const QString& name);


					virtual bool startWritingOutput() = 0;
					virtual bool endWritingOutput() = 0;


					bool visitIndividuals(function<bool(const CIndividualReference& indiRef)> visitFunc);


				// protected variables
				protected:
					QString mQueryName;
					QString mQueryString;

					QString mIndividualNameString;


					QString mOutputFileNameString;
					QString mBottomClassNameString;
					QString mTopClassNameString;

					QSet<CConcept*> mDeclaratedConceptSet;
					QString mCurrentIndividualName;
					bool mCurrentIndividualAnonymous;

					QString mCurrentPropertyName;
					CRole* mCurrentRole;

					QList<CRole*> mObjectPropertyRoleList;
					QHash<CRole*,QString> mObjectPropertyNameHash;

					// Every proper transitive super-role of a given (active, object)
					// role -- built once from the same predecessor-set data
					// writeTransitivePropertyHierarchy() already walks. Konclude's own
					// role realizer can, for a small number of individuals, fail to
					// roll a sub-property's instances up to a broad super-property
					// several hierarchy levels above it (observed concretely: with
					// SubObjectPropertyOf(isBloodRelationOf, isRelationOf) asserted,
					// some isBloodRelationOf pairs were reproducibly missing from
					// isRelationOf's own realized instances, while every other
					// property -- including isBloodRelationOf itself -- was exactly
					// stable across repeated runs). Rather than trust the realizer's
					// roll-up for broad properties, each individual's role assertions
					// are collected per-role first and then propagated to every
					// transitive super-role here in code we control, unioned with
					// whatever the realizer already found directly for that
					// super-role (never subtracted from).
					QHash<CRole*, QSet<CRole*> > mObjectPropertySuperRolesHash;

					// Data properties are simpler: OWL 2 has no inverse or chain
					// axioms for datatype properties, and Konclude exposes each
					// individual's *asserted* data property values directly via
					// CIndividual::getAssertionDataLinker() (a plain linked list of
					// (CRole*, CDataLiteral*) pairs) -- no realizer query needed at
					// all. The only propagation still required by hand is the same
					// sub-property roll-up as for object properties, using this
					// analogous transitive super-role map.
					QList<CRole*> mDataPropertyRoleList;
					QHash<CRole*,QString> mDataPropertyNameHash;
					QHash<CRole*, QSet<CRole*> > mDataPropertySuperRolesHash;

					// Global, cross-individual accumulator: subject individual name ->
					// role -> {target individual name -> is target anonymous}. Filled
					// by visitRoleInstance() (keyed by mCurrentIndividualName /
					// mCurrentRole) while every individual is visited for its direct
					// role assertions; only propagated and flushed to
					// writeObjectPropertyAssertion() once that whole pass is done (see
					// writeMaterializedAssertionsResult(), propagateSubProperties(),
					// propagateInverseRoles()). This has to be global rather than
					// per-individual because inverse-role propagation adds a fact
					// whose subject is a *different* individual than the one currently
					// being visited (if X R Y is found while visiting X, and R has
					// declared inverse invR, the derived fact is Y invR X).
					QHash<QString, QHash<CRole*, QHash<QString,bool> > > mGlobalSubjectRoleTargets;

					// Every individual name encountered as either a subject or a
					// target above, mapped to whether it is anonymous -- needed at
					// emission time (and by inverse propagation, which turns a target
					// into a subject) since mGlobalSubjectRoleTargets by then no
					// longer carries anonymity for its own keys.
					QHash<QString, bool> mIndividualAnonymousHash;

					// Scratch space for one visitSameIndividuals() call (see
					// visitIndividual() and its use in
					// writeMaterializedAssertionsResult()): the queried
					// individual itself plus every other individual currently
					// known to be tableau-merged/nominal-equal with it, in
					// visitation order, with mCurrentSameIndividualAnonymousList
					// parallel-indexed to it. A size of 1 after one call means
					// that individual has no (currently known) merge partners.
					QStringList mCurrentSameIndividualNameList;
					QList<bool> mCurrentSameIndividualAnonymousList;

					// Every individual name already written as part of some
					// equivalence class, across the whole writeMaterializedAssertionsResult()
					// pass -- so a class of N mutually-same individuals is
					// written exactly once (when the first of its members is
					// visited) rather than once per member.
					QSet<QString> mSameIndividualEmittedSet;

					bool mUseAbbreviatedIRIs;
					bool mWriteDeclarations;
					bool mWriteOnlyDirectTypes;
					bool mWriteSubClassOfInconsistency;
					bool mWriteAnonymousIndividuals;

					bool mQueryAnswered;
					bool mQueryConstructError;
					bool mRealizationCalcError;


				// private methods
				private:

				// private variables
				private:

			};

		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude

#endif // KONCLUDE_REASONER_QUERY_CWRITEMATERIALIZEDINDIVIDUALASSERTIONSQUERY_H
