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

#include "CWriteIndividualFlattenedTypesQuery.h"


namespace Konclude {

	namespace Reasoner {

		namespace Query {


			CWriteIndividualFlattenedTypesQuery::CWriteIndividualFlattenedTypesQuery(CConcreteOntology* ontology, CConfigurationBase *configuration, const QString& outputFileString, const QString& individualNameString, const QString &subClassHierarchyQueryName)
					: CRealizationPremisingQuery(ontology,configuration) {
				mQueryName = subClassHierarchyQueryName;
				mIndividualNameString = individualNameString;
				if (mIndividualNameString.isEmpty()) {
					mQueryString = QString("Write Individual Types");
				} else {
					mQueryString = QString("Write Individual Types for '%1'").arg(mIndividualNameString);
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

				mReduction = nullptr;
				mCurrentIndividualMembers = nullptr;
				if (ontology && mIndividualNameString.isEmpty() && ontology->getLoadData()) {
					CABoxReductionData* reduction = ontology->getLoadData()->getABoxReductionData();
					if (reduction && reduction->isReduced() && !reduction->isInvalid()) {
						mReduction = reduction;
					}
				}
			}


			bool CWriteIndividualFlattenedTypesQuery::visitIndividual(const CIndividualReference& indiRef, CSameRealization* sameRealization) {
				mCheckSameIndividualNameList.append(mOntology->getIndividualNameResolver()->getIndividualName(indiRef, false));
				return true;
			}


			bool CWriteIndividualFlattenedTypesQuery::checkABoxReductionApplicable(CRealization* realization, QString& problem) {
				CSameRealization* sameRealization = realization->getSameRealization();
				if (!sameRealization) {
					problem = QString("the same individuals are not available");
					return false;
				}
				bool applicable = true;
				visitIndividuals([&](const CIndividualReference& indiRef)->bool {
					QString individualName = mOntology->getIndividualNameResolver()->getIndividualName(indiRef, false);
					mCheckSameIndividualNameList.clear();
					sameRealization->visitSameIndividuals(indiRef, this);
					if (mCheckSameIndividualNameList.size() > 1) {
						// merges of individuals that are not told to be the same depend on the ABox, the reduction is only used if there are none
						foreach (const QString& sameName, mCheckSameIndividualNameList) {
							if (!mReduction->mToldSameIndividuals.contains(sameName)) {
								problem = QString("the reasoner derived that '%1' is the same as other individuals (e.g., '%2')").arg(individualName).arg(sameName);
								applicable = false;
								return false;
							}
						}
					}
					return true;
				});
				return applicable;
			}


			CQueryResult *CWriteIndividualFlattenedTypesQuery::getQueryResult() {
				return nullptr;
			}


			CQuery* CWriteIndividualFlattenedTypesQuery::addQueryError(CQueryError* queryError) {
				if (CQueryInconsitentOntologyError::hasInconsistentOntologyError(queryError)) {
					if (mReduction) {
						// the individuals that were removed by the ABox reduction would have to be written as well, the computation is repeated without the reduction
						QString reason = QString("the ontology with the reduced ABox is inconsistent");
						mReduction->setInvalid(reason);
						CABoxReductionControl::requestFallback(reason);
					} else {
						// write inconsistency
						if (!writeInconsistentIndividualTypes()) {
							addQueryError(new CQueryUnspecifiedStringError(QString("Could not write individual types to file '%1'.").arg(mOutputFileNameString)));
						}
					}
				}
				CQuery::addQueryError(queryError);
				return this;
			}


			bool CWriteIndividualFlattenedTypesQuery::visitIndividuals(function<bool(const CIndividualReference& indiRef)> visitFunc) {
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


			bool CWriteIndividualFlattenedTypesQuery::writeInconsistentIndividualTypes() {
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
							QStringList incClassNameList;

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




			bool CWriteIndividualFlattenedTypesQuery::visitConcept(CConcept* concept, CConceptRealization* conRealization) {
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
					writeIndividualType(mCurrentIndividualName, mCurrentIndividualAnonymous, conceptString);
					if (mCurrentIndividualMembers) {
						// individuals that were removed by the ABox reduction have the same types as their representative
						foreach (const QString& memberName, *mCurrentIndividualMembers) {
							writeIndividualType(memberName, false, conceptString);
						}
					}
				}
				return true;
			}





			CQueryResult *CWriteIndividualFlattenedTypesQuery::constructResult(CRealization* realization) {
				mQueryAnswered = true;
				if (!writeIndividualTypesResult(realization)) {
					addQueryError(new CQueryUnspecifiedStringError(QString("Could not write individual types to file '%1'.").arg(mOutputFileNameString)));
				}
				return nullptr;
			}








			bool CWriteIndividualFlattenedTypesQuery::writeIndividualTypesResult(CRealization* realization) {
				if (mReduction) {
					QString problem;
					if (!checkABoxReductionApplicable(realization, problem)) {
						// nothing is written, the realization is computed again without the reduction
						mReduction->setInvalid(problem);
						CABoxReductionControl::requestFallback(problem);
						return true;
					}
				}
				if (startWritingOutput()) {

					CConceptRealization* conRealization = realization->getConceptRealization();

					writeOntologyStart();


					visitIndividuals([&](const CIndividualReference& indiRef)->bool {
						bool anonymous = mOntology->getIndividualNameResolver()->isAnonymous(indiRef);
						if (mWriteAnonymousIndividuals || !anonymous) {
							mCurrentIndividualName = mOntology->getIndividualNameResolver()->getIndividualName(indiRef, mUseAbbreviatedIRIs);
							mCurrentIndividualAnonymous = anonymous;
							if (mWriteDeclarations) {
								writeIndividualDeclaration(mCurrentIndividualName, anonymous);
							}
							mCurrentIndividualMembers = mReduction ? mReduction->getRemovedMembers(mCurrentIndividualName) : nullptr;
							if (mWriteDeclarations && mCurrentIndividualMembers) {
								foreach (const QString& memberName, *mCurrentIndividualMembers) {
									writeIndividualDeclaration(memberName, false);
								}
							}
							conRealization->visitTypes(indiRef, mWriteOnlyDirectTypes, this);
							mCurrentIndividualMembers = nullptr;
						}
						return true;
					});


					writeOntologyEnd();
					
					return endWritingOutput();
				}
				return false;
			}




			CWriteQuery::WRITEQUERYTYPE CWriteIndividualFlattenedTypesQuery::getWriteQueryType() {
				return CWriteQuery::WRITEINDIVIDUALFLATTENEDTYPES;
			}


			QString CWriteIndividualFlattenedTypesQuery::getQueryName() {
				return mQueryName;
			}

			QString CWriteIndividualFlattenedTypesQuery::getQueryString() {
				return mQueryString;
			}

			bool CWriteIndividualFlattenedTypesQuery::hasAnswer() {
				return mQueryAnswered;
			}

			QString CWriteIndividualFlattenedTypesQuery::getAnswerString() {
				if (mIndividualNameString.isEmpty()) {
					return QString("Individual Types written to file '%1'").arg(mOutputFileNameString);
					mQueryString = QString("Write Individual Types");
				} else {
					return QString("Individual Types for '%1' written to file '%2'").arg(mIndividualNameString).arg(mOutputFileNameString);
				}
			}


			bool CWriteIndividualFlattenedTypesQuery::hasError() {
				return mRealizationCalcError || mQueryConstructError || CQuery::hasError();
			}

		}; // end namespace Query

	}; // end namespace Reasoner

}; // end namespace Konclude
