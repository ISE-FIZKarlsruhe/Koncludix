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

#include "CWriteMaterializedIndividualAssertionsQuery.h"


namespace Konclude {

	namespace Reasoner {

		namespace Query {


			CWriteMaterializedIndividualAssertionsQuery::CWriteMaterializedIndividualAssertionsQuery(CConcreteOntology* ontology, CConfigurationBase *configuration, const QString& outputFileString, const QString& individualNameString, const QString &queryName)
					: CRealizationPremisingQuery(ontology,configuration) {
				mQueryName = queryName;
				mIndividualNameString = individualNameString;
				if (mIndividualNameString.isEmpty()) {
					mQueryString = QString("Write Materialized Individual Assertions");
				} else {
					mQueryString = QString("Write Materialized Individual Assertions for '%1'").arg(mIndividualNameString);
				}

				mUseAbbreviatedIRIs = CConfigDataReader::readConfigBoolean(configuration,"Konclude.CLI.Output.AbbreviatedIRIs",false);
				mWriteDeclarations = CConfigDataReader::readConfigBoolean(configuration,"Konclude.CLI.Output.WriteDeclarations",false);
				mWriteOnlyDirectTypes = CConfigDataReader::readConfigBoolean(configuration,"Konclude.CLI.Output.WriteOnlyDirectTypes",false);
				mWriteSubClassOfInconsistency = CConfigDataReader::readConfigBoolean(configuration, "Konclude.CLI.Output.WriteReducedInconsistency", false);
				mWriteAnonymousIndividuals = CConfigDataReader::readConfigBoolean(configuration, "Konclude.CLI.Output.WriteAnonymousIndividualResults", false);

				mRealizationCalcError = false;
				mQueryConstructError = false;

				mBottomClassNameString = QString("http://www.w3.org/2002/07/owl#Nothing");
				mTopClassNameString = QString("http://www.w3.org/2002/07/owl#Thing");
				mOutputFileNameString = outputFileString;
				mQueryAnswered = false;

				mRequiresSameIndividualRealisation = true;
				mRequiresConceptRealisation = true;
				mRequiresRoleRealisation = true;
			}


			CQueryResult *CWriteMaterializedIndividualAssertionsQuery::getQueryResult() {
				return nullptr;
			}


			CQuery* CWriteMaterializedIndividualAssertionsQuery::addQueryError(CQueryError* queryError) {
				if (CQueryInconsitentOntologyError::hasInconsistentOntologyError(queryError)) {
					// write inconsistency
					if (!writeInconsistentIndividualAssertions()) {
						addQueryError(new CQueryUnspecifiedStringError(QString("Could not write materialized individual assertions to file '%1'.").arg(mOutputFileNameString)));
					}
				}
				CQuery::addQueryError(queryError);
				return this;
			}


			bool CWriteMaterializedIndividualAssertionsQuery::visitIndividuals(function<bool(const CIndividualReference& indiRef)> visitFunc) {
				CBOXSET<CIndividual*>* activeIndiSet = mOntology->getABox()->getActiveIndividualSet();
				CIndividualVector* indiVec = mOntology->getABox()->getIndividualVector(false);
				cint64 indiCount = 0;
				if (indiVec) {
					indiCount = indiVec->getItemCount();
				}

				bool visited = false;
				bool continueVisiting = true;
				cint64 maxTriplesIndexedIndiId = 0;
				cint64 maxABoxIndiId = 0;
				COntologyTriplesAssertionsAccessor* triplesAccessor = mOntology->getOntologyTriplesData()->getTripleAssertionAccessor();
				if (triplesAccessor) {
					maxTriplesIndexedIndiId = mOntology->getOntologyTriplesData()->getTripleAssertionAccessor()->getMaxIndexedIndividualId();
				}
				if (indiVec) {
					cint64 indiCount = indiVec->getItemCount();
					for (cint64 idx = 0; idx < indiCount && continueVisiting; ++idx) {
						CIndividual* indi = indiVec->getData(idx);
						if (indi && activeIndiSet->contains(indi)) {
							visited = true;
							continueVisiting = visitFunc(CIndividualReference(indi));
						} else if (idx <= maxTriplesIndexedIndiId) {
							visited = true;
							continueVisiting = visitFunc(CIndividualReference(idx));
						}
						maxABoxIndiId = qMax(idx, maxABoxIndiId);
					}
				}
				return visited;
			}


			bool CWriteMaterializedIndividualAssertionsQuery::writeInconsistentIndividualAssertions() {
				if (startWritingOutput()) {
					writeOntologyStart();

					CConcept* bottomConcept = mOntology->getTBox()->getBottomConcept();

					QString bottomClassName = CIRIName::getRecentIRIName(bottomConcept->getClassNameLinker());
					if (mUseAbbreviatedIRIs) {
						bottomClassName = CAbbreviatedIRIName::getRecentAbbreviatedPrefixWithAbbreviatedIRIName(bottomConcept->getClassNameLinker());
					}

					if (mWriteDeclarations) {
						writeClassDeclaration(bottomClassName);
					}

					if (mWriteSubClassOfInconsistency) {

						CConcept* topConcept = mOntology->getTBox()->getTopConcept();
						QString topClassName = CIRIName::getRecentIRIName(topConcept->getClassNameLinker());
						if (mUseAbbreviatedIRIs) {
							topClassName = CAbbreviatedIRIName::getRecentAbbreviatedPrefixWithAbbreviatedIRIName(topConcept->getClassNameLinker());
						}
						if (mWriteDeclarations) {
							writeClassDeclaration(topClassName);
						}
						writeSubClassRelation(topClassName,bottomClassName);

					} else {
						if (mIndividualNameString.isEmpty()) {

							visitIndividuals([&](const CIndividualReference& indiRef)->bool {
								bool anonymous = mOntology->getIndividualNameResolver()->isAnonymous(indiRef);
								if (mWriteAnonymousIndividuals || !anonymous) {
									QString individualName = mOntology->getIndividualNameResolver()->getIndividualName(indiRef, mUseAbbreviatedIRIs);
									if (mWriteDeclarations) {
										writeIndividualDeclaration(individualName, anonymous);
									}
									writeIndividualType(individualName, anonymous, bottomClassName);
								}
								return true;
							});

						} else {
							if (mWriteDeclarations) {
								writeNamedIndividualDeclaration(mIndividualNameString);
							}
							writeIndividualType(mIndividualNameString, false, bottomClassName);
						}
					}

					writeOntologyEnd();

					return endWritingOutput();
				}
				return false;
			}




			bool CWriteMaterializedIndividualAssertionsQuery::visitConcept(CConcept* concept, CConceptRealization* conRealization) {
				QString conceptString;
				if (mUseAbbreviatedIRIs) {
					conceptString = CAbbreviatedIRIName::getRecentAbbreviatedPrefixWithAbbreviatedIRIName(concept->getClassNameLinker());
				}
				if (conceptString.isEmpty()) {
					conceptString = CIRIName::getRecentIRIName(concept->getClassNameLinker());
				}
				if (!conceptString.isEmpty()) {
					if (mWriteDeclarations && !mDeclaratedConceptSet.contains(concept)) {
						mDeclaratedConceptSet.insert(concept);
						writeClassDeclaration(conceptString);
					}
					// Anonymity is checked here (at the point of writing),
					// not by skipping visitation of an anonymous individual
					// altogether -- see visitRoleInstance()'s comment for why
					// that distinction matters.
					if (mWriteAnonymousIndividuals || !mCurrentIndividualAnonymous) {
						writeIndividualType(mCurrentIndividualName, mCurrentIndividualAnonymous, conceptString);
					}
				}
				return true;
			}


			bool CWriteMaterializedIndividualAssertionsQuery::visitIndividual(const CIndividualReference& indiRef, CSameRealization* sameRealization) {
				bool anonymous = mOntology->getIndividualNameResolver()->isAnonymous(indiRef);
				if (mWriteAnonymousIndividuals || !anonymous) {
					QString individualName = mOntology->getIndividualNameResolver()->getIndividualName(indiRef, mUseAbbreviatedIRIs);
					mCurrentSameIndividualNameList.append(individualName);
					mCurrentSameIndividualAnonymousList.append(anonymous);
				}
				return true;
			}


			bool CWriteMaterializedIndividualAssertionsQuery::visitRoleInstance(const CRealizationIndividualInstanceItemReference& indiRealItemRef, CRoleRealization* roleRealization) {
				// Always collect, regardless of mWriteAnonymousIndividuals --
				// this fact feeds propagateSubProperties()/propagateInverseRoles()/
				// propagatePropertyChains() below, which can derive a fact
				// between two NAMED individuals via an intermediate role hop
				// that happens to land on an anonymous individual (e.g. a
				// property-chain composition A->_:b->C, where only the final
				// A->C fact is ever written). Silently dropping the A->_:b
				// hop here purely because _:b is anonymous would make that
				// derivation impossible to reach even though nothing
				// anonymous ever appears in the final written fact. Whether
				// an individual (named or anonymous) actually appears as a
				// subject/target in the WRITTEN output is decided later, at
				// each write call site, not here at collection time.
				bool targetAnonymous = mOntology->getIndividualNameResolver()->isAnonymous(indiRealItemRef);
				QString targetName = mOntology->getIndividualNameResolver()->getIndividualName(indiRealItemRef, mUseAbbreviatedIRIs);
				mGlobalSubjectRoleTargets[mCurrentIndividualName][mCurrentRole].insert(targetName, targetAnonymous);
				mIndividualAnonymousHash.insert(targetName, targetAnonymous);
				return true;
			}


			void CWriteMaterializedIndividualAssertionsQuery::propagateSubProperties() {
				QList<QString> subjectList(mGlobalSubjectRoleTargets.keys());
				foreach (const QString& subject, subjectList) {
					QHash<CRole*, QHash<QString,bool> >& roleMap = mGlobalSubjectRoleTargets[subject];
					QList<CRole*> roleList(roleMap.keys());
					foreach (CRole* role, roleList) {
						const QHash<QString,bool> targets(roleMap.value(role));
						const QSet<CRole*>& superRoles = mObjectPropertySuperRolesHash.value(role);
						foreach (CRole* superRole, superRoles) {
							QHash<QString,bool>& superTargets = roleMap[superRole];
							for (QHash<QString,bool>::const_iterator it = targets.constBegin(), itEnd = targets.constEnd(); it != itEnd; ++it) {
								superTargets.insert(it.key(), it.value());
							}
						}
					}
				}
			}


			void CWriteMaterializedIndividualAssertionsQuery::propagateInverseRoles() {
				struct TInverseAddition {
					QString subject;
					bool subjectAnonymous;
					CRole* role;
					QString target;
					bool targetAnonymous;
				};
				QList<TInverseAddition> additions;

				for (QHash<QString, QHash<CRole*, QHash<QString,bool> > >::const_iterator subjIt = mGlobalSubjectRoleTargets.constBegin(), subjItEnd = mGlobalSubjectRoleTargets.constEnd(); subjIt != subjItEnd; ++subjIt) {
					const QString& subjectName = subjIt.key();
					bool subjectAnonymous = mIndividualAnonymousHash.value(subjectName, false);
					const QHash<CRole*, QHash<QString,bool> >& roleMap = subjIt.value();
					for (QHash<CRole*, QHash<QString,bool> >::const_iterator roleIt = roleMap.constBegin(), roleItEnd = roleMap.constEnd(); roleIt != roleItEnd; ++roleIt) {
						CRole* role = roleIt.key();
						CRole* invRole = role->getInverseRole();
						if (!invRole || !mObjectPropertyNameHash.contains(invRole)) {
							continue;
						}
						const QHash<QString,bool>& targets = roleIt.value();
						for (QHash<QString,bool>::const_iterator it = targets.constBegin(), itEnd = targets.constEnd(); it != itEnd; ++it) {
							TInverseAddition addition;
							addition.subject = it.key();
							addition.subjectAnonymous = it.value();
							addition.role = invRole;
							addition.target = subjectName;
							addition.targetAnonymous = subjectAnonymous;
							additions.append(addition);
						}
					}
				}

				foreach (const TInverseAddition& addition, additions) {
					mGlobalSubjectRoleTargets[addition.subject][addition.role].insert(addition.target, addition.targetAnonymous);
					mIndividualAnonymousHash.insert(addition.subject, addition.subjectAnonymous);
				}
			}


			void CWriteMaterializedIndividualAssertionsQuery::propagatePropertyChains() {
				foreach (CRole* impliedRole, mObjectPropertyRoleList) {
					CXLinker<CRoleChain*>* chainLinker = impliedRole->getRoleChainSuperSharingLinker();
					for (; chainLinker; chainLinker = chainLinker->getNext()) {
						CRoleChain* chain = chainLinker->getData();
						if (!chain) {
							continue;
						}
						QList<CRole*> chainRoleList;
						for (CXLinker<CRole*>* roleLink = chain->getRoleChainLinker(); roleLink; roleLink = roleLink->getNext()) {
							chainRoleList.append(roleLink->getData());
						}
						if (chainRoleList.isEmpty()) {
							continue;
						}

						// frontier: original chain-start subject name -> set of
						// individual names reached so far by walking the chain's
						// role sequence from that subject. Looked up via find()
						// and the iterator's value() throughout (never the QHash
						// container's value(key), which returns its value type by
						// copy -- expensive here since that type is itself a nested
						// QHash). Seeded from the first step role's own targets, so
						// subjects that never participate in this particular chain
						// at all are never touched further.
						CRole* firstStepRole = chainRoleList.first();
						QHash<QString, QSet<QString> > frontier;
						for (QHash<QString, QHash<CRole*, QHash<QString,bool> > >::const_iterator it = mGlobalSubjectRoleTargets.constBegin(), itEnd = mGlobalSubjectRoleTargets.constEnd(); it != itEnd; ++it) {
							QHash<CRole*, QHash<QString,bool> >::const_iterator roleIt = it.value().find(firstStepRole);
							if (roleIt == it.value().constEnd() || roleIt.value().isEmpty()) {
								continue;
							}
							QSet<QString> targetSet;
							for (QHash<QString,bool>::const_iterator tIt = roleIt.value().constBegin(), tItEnd = roleIt.value().constEnd(); tIt != tItEnd; ++tIt) {
								targetSet.insert(tIt.key());
							}
							frontier.insert(it.key(), targetSet);
						}

						for (int stepIdx = 1; stepIdx < chainRoleList.size() && !frontier.isEmpty(); ++stepIdx) {
							CRole* stepRole = chainRoleList.at(stepIdx);
							QHash<QString, QSet<QString> > nextFrontier;
							for (QHash<QString, QSet<QString> >::const_iterator it = frontier.constBegin(), itEnd = frontier.constEnd(); it != itEnd; ++it) {
								QSet<QString> nextSet;
								foreach (const QString& current, it.value()) {
									QHash<QString, QHash<CRole*, QHash<QString,bool> > >::const_iterator subjIt = mGlobalSubjectRoleTargets.find(current);
									if (subjIt == mGlobalSubjectRoleTargets.constEnd()) {
										continue;
									}
									QHash<CRole*, QHash<QString,bool> >::const_iterator roleIt = subjIt.value().find(stepRole);
									if (roleIt == subjIt.value().constEnd()) {
										continue;
									}
									for (QHash<QString,bool>::const_iterator tIt = roleIt.value().constBegin(), tItEnd = roleIt.value().constEnd(); tIt != tItEnd; ++tIt) {
										nextSet.insert(tIt.key());
									}
								}
								if (!nextSet.isEmpty()) {
									nextFrontier.insert(it.key(), nextSet);
								}
							}
							frontier = nextFrontier;
						}

						for (QHash<QString, QSet<QString> >::const_iterator it = frontier.constBegin(), itEnd = frontier.constEnd(); it != itEnd; ++it) {
							const QString& subjectName = it.key();
							foreach (const QString& target, it.value()) {
								bool targetAnonymous = mIndividualAnonymousHash.value(target, false);
								mGlobalSubjectRoleTargets[subjectName][impliedRole].insert(target, targetAnonymous);
							}
						}
					}
				}
			}




			CQueryResult *CWriteMaterializedIndividualAssertionsQuery::constructResult(CRealization* realization) {
				mQueryAnswered = true;
				if (!writeMaterializedAssertionsResult(realization)) {
					addQueryError(new CQueryUnspecifiedStringError(QString("Could not write materialized individual assertions to file '%1'.").arg(mOutputFileNameString)));
				}
				return nullptr;
			}




			void CWriteMaterializedIndividualAssertionsQuery::writeTransitiveClassHierarchy() {
				CTaxonomy* taxonomy = mOntology->getConceptTaxonomy();
				if (!taxonomy) {
					return;
				}
				CHierarchyNode* topNode = taxonomy->getTopHierarchyNode();
				CHierarchyNode* bottomNode = taxonomy->getBottomHierarchyNode();
				if (!topNode) {
					return;
				}
				QSet<CHierarchyNode*> visitedNodeSet;
				QList<CHierarchyNode*> processNodeList;
				visitedNodeSet.insert(topNode);
				processNodeList.append(topNode);
				while (!processNodeList.isEmpty()) {
					CHierarchyNode* node = processNodeList.takeFirst();
					const QStringList& nodeNameList(node->getEquivalentConceptStringList(mUseAbbreviatedIRIs));
					if (mWriteDeclarations) {
						foreach (const QString& className, nodeNameList) {
							writeClassDeclaration(className);
						}
					}
					if (node->getEquivalentConceptCount() > 1) {
						writeClassEquivalenceRelations(nodeNameList);
					}
					if (!nodeNameList.isEmpty() && node != bottomNode) {
						const QString nodeName(nodeNameList.first());
						QSet<CHierarchyNode*>* predSet = node->getPredecessorNodeSet();
						foreach (CHierarchyNode* predNode, *predSet) {
							if (predNode != node) {
								const QStringList& predNameList(predNode->getEquivalentConceptStringList(mUseAbbreviatedIRIs));
								if (!predNameList.isEmpty()) {
									writeSubClassRelation(nodeName,predNameList.first());
								}
							}
						}
					}
					QSet<CHierarchyNode*>* childNodeSet(node->getChildNodeSet());
					foreach (CHierarchyNode* childNode, *childNodeSet) {
						if (!visitedNodeSet.contains(childNode)) {
							visitedNodeSet.insert(childNode);
							processNodeList.append(childNode);
						}
					}
				}
			}


			void CWriteMaterializedIndividualAssertionsQuery::writeTransitivePropertyHierarchy(bool dataProperties) {
				CClassification* classification = mOntology->getClassification();
				if (!classification) {
					return;
				}
				CPropertyRoleClassification* roleClassification = dataProperties ? classification->getDataPropertyRoleClassification() : classification->getObjectPropertyRoleClassification();
				if (!roleClassification) {
					return;
				}
				CRolePropertiesHierarchy* hierarchy = roleClassification->getRolePropertiesHierarchy();
				if (!hierarchy) {
					return;
				}
				CRolePropertiesHierarchyNode* topNode = hierarchy->getTopHierarchyNode();
				CRolePropertiesHierarchyNode* bottomNode = hierarchy->getBottomHierarchyNode();
				if (!topNode) {
					return;
				}
				if (dataProperties) {
					mDataPropertySuperRolesHash.clear();
				} else {
					mObjectPropertySuperRolesHash.clear();
				}
				QSet<CRolePropertiesHierarchyNode*> visitedNodeSet;
				QList<CRolePropertiesHierarchyNode*> processNodeList;
				visitedNodeSet.insert(topNode);
				processNodeList.append(topNode);
				while (!processNodeList.isEmpty()) {
					CRolePropertiesHierarchyNode* node = processNodeList.takeFirst();
					const QStringList& nodeNameList(node->getEquivalentRoleStringList(mUseAbbreviatedIRIs));
					if (mWriteDeclarations) {
						foreach (const QString& propertyName, nodeNameList) {
							if (dataProperties) {
								writeDataPropertyDeclaration(propertyName);
							} else {
								writeObjectPropertyDeclaration(propertyName);
							}
						}
					}
					if (node->getEquivalentRoleCount() > 1) {
						if (dataProperties) {
							writeDataPropertyEquivalenceRelations(nodeNameList);
						} else {
							writeObjectPropertyEquivalenceRelations(nodeNameList);
						}
					}
					if (!nodeNameList.isEmpty() && node != bottomNode) {
						const QString nodeName(nodeNameList.first());
						QSet<CRolePropertiesHierarchyNode*>* predSet = node->getPredecessorNodeSet();
						foreach (CRolePropertiesHierarchyNode* predNode, *predSet) {
							if (predNode != node) {
								const QStringList& predNameList(predNode->getEquivalentRoleStringList(mUseAbbreviatedIRIs));
								if (!predNameList.isEmpty()) {
									if (dataProperties) {
										writeSubDataPropertyRelation(nodeName,predNameList.first());
									} else {
										writeSubObjectPropertyRelation(nodeName,predNameList.first());
									}
								}
							}
						}
						QHash<CRole*, QSet<CRole*> >& superRolesHash = dataProperties ? mDataPropertySuperRolesHash : mObjectPropertySuperRolesHash;
						QList<CRole*>* nodeRoleList = node->getEquivalentRoleList();
						foreach (CRolePropertiesHierarchyNode* predNode, *predSet) {
							if (predNode != node) {
								QList<CRole*>* predRoleList = predNode->getEquivalentRoleList();
								foreach (CRole* nodeRole, *nodeRoleList) {
									QSet<CRole*>& superRoleSet = superRolesHash[nodeRole];
									foreach (CRole* predRole, *predRoleList) {
										superRoleSet.insert(predRole);
									}
								}
							}
						}
					}
					QSet<CRolePropertiesHierarchyNode*>* childNodeSet(node->getChildNodeSet());
					foreach (CRolePropertiesHierarchyNode* childNode, *childNodeSet) {
						if (!visitedNodeSet.contains(childNode)) {
							visitedNodeSet.insert(childNode);
							processNodeList.append(childNode);
						}
					}
				}
			}




			bool CWriteMaterializedIndividualAssertionsQuery::writeMaterializedAssertionsResult(CRealization* realization) {
				if (startWritingOutput()) {

					CConceptRealization* conRealization = realization->getConceptRealization();
					CRoleRealization* roleRealization = realization->getRoleRealization();

					writeOntologyStart();

					writeTransitiveClassHierarchy();
					writeTransitivePropertyHierarchy(false);
					writeTransitivePropertyHierarchy(true);

					mObjectPropertyRoleList.clear();
					mObjectPropertyNameHash.clear();

					CBOXSET<CRole*>* activeRoleSet = mOntology->getRBox()->getActivePropertyRoleSet();
					if (activeRoleSet) {
						CRole* topObjRole = mOntology->getRBox()->getTopObjectRole();
						CRole* bottomObjRole = mOntology->getRBox()->getBottomObjectRole();
						for (CBOXSET<CRole*>::const_iterator it = activeRoleSet->constBegin(), itEnd = activeRoleSet->constEnd(); it != itEnd; ++it) {
							CRole* role(*it);
							if (role && role->isObjectRole() && role != topObjRole && role != bottomObjRole) {
								QString propertyName;
								if (mUseAbbreviatedIRIs) {
									propertyName = CAbbreviatedIRIName::getRecentAbbreviatedPrefixWithAbbreviatedIRIName(role->getPropertyNameLinker());
								}
								if (propertyName.isEmpty()) {
									propertyName = CIRIName::getRecentIRIName(role->getPropertyNameLinker());
								}
								if (!propertyName.isEmpty()) {
									mObjectPropertyRoleList.append(role);
									mObjectPropertyNameHash.insert(role,propertyName);
									if (mWriteDeclarations) {
										writeObjectPropertyDeclaration(propertyName);
									}
								}
							}
						}
					}

					mDataPropertyRoleList.clear();
					mDataPropertyNameHash.clear();

					if (activeRoleSet) {
						CRole* topDataRole = mOntology->getRBox()->getTopDataRole();
						CRole* bottomDataRole = mOntology->getRBox()->getBottomDataRole();
						for (CBOXSET<CRole*>::const_iterator it = activeRoleSet->constBegin(), itEnd = activeRoleSet->constEnd(); it != itEnd; ++it) {
							CRole* role(*it);
							if (role && role->isDataRole() && role != topDataRole && role != bottomDataRole) {
								QString propertyName;
								if (mUseAbbreviatedIRIs) {
									propertyName = CAbbreviatedIRIName::getRecentAbbreviatedPrefixWithAbbreviatedIRIName(role->getPropertyNameLinker());
								}
								if (propertyName.isEmpty()) {
									propertyName = CIRIName::getRecentIRIName(role->getPropertyNameLinker());
								}
								if (!propertyName.isEmpty()) {
									mDataPropertyRoleList.append(role);
									mDataPropertyNameHash.insert(role,propertyName);
								}
							}
						}
					}

					mGlobalSubjectRoleTargets.clear();
					mIndividualAnonymousHash.clear();

					visitIndividuals([&](const CIndividualReference& indiRef)->bool {
						// Every individual is always visited here -- named or
						// anonymous -- regardless of mWriteAnonymousIndividuals.
						// That flag only controls what ends up WRITTEN to the
						// output (checked at each write call site below, and
						// inside visitConcept()/writeObjectPropertyAssertion's
						// caller), not whether an anonymous individual's own
						// outgoing facts get queried and collected at all.
						// Skipping collection entirely for an anonymous
						// individual would silently break propagateSubProperties()/
						// propagateInverseRoles()/propagatePropertyChains()
						// whenever a derivation's intermediate hop happens to
						// pass through one, even if the final derived fact
						// only involves named individuals on both ends (e.g.
						// a property chain A --P--> _:b --P--> C entailing
						// A --Q--> C, composed via SubObjectPropertyOf(P o P,
						// Q): if A--P-->_:b were never collected because _:b
						// is anonymous, that composition could never be
						// found, even though _:b never appears in the final
						// A--Q-->C fact that IS supposed to be written).
						bool anonymous = mOntology->getIndividualNameResolver()->isAnonymous(indiRef);
						mCurrentIndividualName = mOntology->getIndividualNameResolver()->getIndividualName(indiRef, mUseAbbreviatedIRIs);
						mCurrentIndividualAnonymous = anonymous;
						mIndividualAnonymousHash.insert(mCurrentIndividualName, anonymous);
						if (mWriteDeclarations && (mWriteAnonymousIndividuals || !anonymous)) {
							writeIndividualDeclaration(mCurrentIndividualName, anonymous);
						}
						if (conRealization) {
							conRealization->visitTypes(indiRef, mWriteOnlyDirectTypes, this);
						}
						if (roleRealization) {
							foreach (CRole* role, mObjectPropertyRoleList) {
								mCurrentRole = role;
								mCurrentPropertyName = mObjectPropertyNameHash.value(role);
								roleRealization->visitTargetIndividuals(indiRef, role, this);
							}
						}

						// Data properties: read each individual's *asserted* data
						// property values directly off the ABox (no realizer query
						// -- see mDataPropertySuperRolesHash's comment), then
						// propagate each one to its transitive super-data-properties
						// using the very same hierarchy-closure mechanism as
						// mObjectPropertySuperRolesHash, and write immediately (no
						// cross-individual propagation is needed for data
						// properties, so unlike object properties this does not need
						// to go through the global accumulator). Collection always
						// happens; only the final write is gated on
						// mWriteAnonymousIndividuals, for the same reason as above
						// (data properties have no cross-individual composition, but
						// keeping collect/write separated here anyway for
						// consistency and in case that ever changes).
						CIndividual* individual = indiRef.getIndividual();
						if (individual) {
							QHash<CRole*, QSet<QPair<QString,QString> > > collectedDataAssertions;
							for (CDataAssertionLinker* link = individual->getAssertionDataLinker(); link; link = link->getNext()) {
								CRole* dataRole = link->getRole();
								CDataLiteral* literal = link->getDataLiteral();
								if (dataRole && literal && mDataPropertyNameHash.contains(dataRole)) {
									CDatatype* datatype = literal->getDatatype();
									QString datatypeIRI = datatype ? datatype->getDatatypeIRI() : QString();
									collectedDataAssertions[dataRole].insert(qMakePair(literal->getLexicalDataLiteralValueString(), datatypeIRI));
								}
							}
							QList<CRole*> collectedDataRoleList(collectedDataAssertions.keys());
							foreach (CRole* role, collectedDataRoleList) {
								const QSet<QPair<QString,QString> > literals(collectedDataAssertions.value(role));
								const QSet<CRole*>& superRoles = mDataPropertySuperRolesHash.value(role);
								foreach (CRole* superRole, superRoles) {
									collectedDataAssertions[superRole].unite(literals);
								}
							}
							if (mWriteAnonymousIndividuals || !anonymous) {
								for (QHash<CRole*, QSet<QPair<QString,QString> > >::const_iterator dpIt = collectedDataAssertions.constBegin(), dpItEnd = collectedDataAssertions.constEnd(); dpIt != dpItEnd; ++dpIt) {
									const QString& propertyName = mDataPropertyNameHash.value(dpIt.key());
									if (propertyName.isEmpty()) {
										continue;
									}
									for (QSet<QPair<QString,QString> >::const_iterator litIt = dpIt.value().constBegin(), litItEnd = dpIt.value().constEnd(); litIt != litItEnd; ++litIt) {
										writeDataPropertyAssertion(mCurrentIndividualName, mCurrentIndividualAnonymous, propertyName, litIt->first, litIt->second);
									}
								}
							}
						}
						return true;
					});

					if (roleRealization) {
						// Every fact is now collected in mGlobalSubjectRoleTargets, across
						// every individual. Propagate sub-properties up to their
						// transitive super-roles, then propagate declared inverse roles
						// (which can add a fact under a *different* subject than the one
						// it was found under -- see propagateInverseRoles()), then
						// propagate sub-properties once more so an inverse-derived fact
						// still rolls up to its own super-roles. Then walk property
						// chains (composition axioms, e.g. partOf o hasParticipant =>
						// hasParticipant) over the now sub-property/inverse-complete
						// data, and repeat inverse+sub-property propagation once more
						// so a chain-derived fact gets its own inverse and rolls up to
						// its own super-roles too. This is a bounded, not a fixed-point,
						// sequence -- it covers one round of each interaction, which is
						// what every case found in practice needed.
						propagateSubProperties();
						propagateInverseRoles();
						propagateSubProperties();
						propagatePropertyChains();
						propagateInverseRoles();
						propagateSubProperties();

						for (QHash<QString, QHash<CRole*, QHash<QString,bool> > >::const_iterator subjIt = mGlobalSubjectRoleTargets.constBegin(), subjItEnd = mGlobalSubjectRoleTargets.constEnd(); subjIt != subjItEnd; ++subjIt) {
							const QString& subjectName = subjIt.key();
							bool subjectAnonymous = mIndividualAnonymousHash.value(subjectName, false);
							const QHash<CRole*, QHash<QString,bool> >& roleMap = subjIt.value();
							for (QHash<CRole*, QHash<QString,bool> >::const_iterator roleIt = roleMap.constBegin(), roleItEnd = roleMap.constEnd(); roleIt != roleItEnd; ++roleIt) {
								const QString& propertyName = mObjectPropertyNameHash.value(roleIt.key());
								if (propertyName.isEmpty()) {
									continue;
								}
								const QHash<QString,bool>& targets = roleIt.value();
								for (QHash<QString,bool>::const_iterator it = targets.constBegin(), itEnd = targets.constEnd(); it != itEnd; ++it) {
									// Collection (visitRoleInstance()) is unconditional so
									// that a fact touching an anonymous individual can still
									// feed chain/inverse/sub-property propagation -- so the
									// anonymity check that used to happen at collection time
									// has to happen here instead, at the actual point of
									// writing, on both ends of the (possibly propagated)
									// fact.
									bool targetAnonymous = it.value();
									if (!mWriteAnonymousIndividuals && (subjectAnonymous || targetAnonymous)) {
										continue;
									}
									writeObjectPropertyAssertion(subjectName, subjectAnonymous, propertyName, it.key(), targetAnonymous);
								}
							}
						}
					}

					// Same-individual equivalences: functional/inverse-functional
					// properties, (qualified) cardinality restrictions and
					// hasKey axioms can force two distinct *named* individuals
					// to be tableau-merged even though nothing ever asserted
					// owl:sameAs between them directly. CForceSameIndividualsRealizationQuery
					// (run earlier in the materialize CLI pipeline) has already
					// forced every individual's merge set to be fully known, so
					// this is a single pass over all individuals: for each not
					// yet covered by some previously-written equivalence class,
					// ask CSameRealization for its full known-same set (which
					// always includes the individual itself -- see
					// COptimizedRepresentativeKPSetOntologyRealizingItem::visitSameIndividuals),
					// and if that set has more than one member, write it as one
					// equivalence class and mark every member of it as covered.
					CSameRealization* sameRealization = realization->getSameRealization();
					if (sameRealization) {
						mSameIndividualEmittedSet.clear();
						visitIndividuals([&](const CIndividualReference& indiRef)->bool {
							bool anonymous = mOntology->getIndividualNameResolver()->isAnonymous(indiRef);
							if (!(mWriteAnonymousIndividuals || !anonymous)) {
								return true;
							}
							QString individualName = mOntology->getIndividualNameResolver()->getIndividualName(indiRef, mUseAbbreviatedIRIs);
							if (mSameIndividualEmittedSet.contains(individualName)) {
								return true;
							}
							mCurrentSameIndividualNameList.clear();
							mCurrentSameIndividualAnonymousList.clear();
							sameRealization->visitSameIndividuals(indiRef, this);
							if (mCurrentSameIndividualNameList.size() > 1) {
								foreach (const QString& sameName, mCurrentSameIndividualNameList) {
									mSameIndividualEmittedSet.insert(sameName);
								}
								writeIndividualEquivalenceRelations(mCurrentSameIndividualNameList, mCurrentSameIndividualAnonymousList);
							} else {
								mSameIndividualEmittedSet.insert(individualName);
							}
							return true;
						});
					}


					writeOntologyEnd();

					return endWritingOutput();
				}
				return false;
			}




			CWriteQuery::WRITEQUERYTYPE CWriteMaterializedIndividualAssertionsQuery::getWriteQueryType() {
				return CWriteQuery::WRITEMATERIALIZEDINDIVIDUALASSERTIONS;
			}


			QString CWriteMaterializedIndividualAssertionsQuery::getQueryName() {
				return mQueryName;
			}

			QString CWriteMaterializedIndividualAssertionsQuery::getQueryString() {
				return mQueryString;
			}

			bool CWriteMaterializedIndividualAssertionsQuery::hasAnswer() {
				return mQueryAnswered;
			}

			QString CWriteMaterializedIndividualAssertionsQuery::getAnswerString() {
				if (mIndividualNameString.isEmpty()) {
					return QString("Materialized individual assertions written to file '%1'").arg(mOutputFileNameString);
				} else {
					return QString("Materialized individual assertions for '%1' written to file '%2'").arg(mIndividualNameString).arg(mOutputFileNameString);
				}
			}


			bool CWriteMaterializedIndividualAssertionsQuery::hasError() {
				return mRealizationCalcError || mQueryConstructError || CQuery::hasError();
			}

		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude
