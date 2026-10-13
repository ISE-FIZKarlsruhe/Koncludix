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

#include "CLintParseRecorder.h"
#include "CLintExpressionRenderer.h"

#include <atomic>


namespace Konclude {

	namespace Parser {

		namespace {
			std::atomic<bool> sRecording(false);
			QMutex sMutex;
			QList<CLintParseRecorder::CHasKeyRecord> sHasKeyRecords;
			QSet<QString> sKnownTexts;
		}


		void CLintParseRecorder::setRecording(bool recording) {
			sRecording = recording;
		}

		bool CLintParseRecorder::isRecording() {
			return sRecording;
		}


		void CLintParseRecorder::recordHasKey(CBuildExpression* classExpression, const QList<CBuildExpression*>& objectKeyProperties, const QList<CBuildExpression*>& dataKeyProperties) {
			if (!sRecording) {
				return;
			}
			CHasKeyRecord record;
			QString text = QString("HasKey(%1 (").arg(CLintExpressionRenderer::render(classExpression));
			for (CBuildExpression* p : objectKeyProperties) {
				text += CLintExpressionRenderer::render(p) + " ";
				QString name = CLintExpressionRenderer::entityName(p);
				if (!name.isEmpty() && !record.mObjectProperties.contains(name)) { record.mObjectProperties.append(name); }
			}
			text += ") (";
			for (CBuildExpression* p : dataKeyProperties) {
				text += CLintExpressionRenderer::render(p) + " ";
				QString name = CLintExpressionRenderer::entityName(p);
				if (!name.isEmpty() && !record.mDataProperties.contains(name)) { record.mDataProperties.append(name); }
			}
			text += "))";
			record.mText = text;
			CLintExpressionRenderer::collectEntityNames(classExpression, CBuildExpression::BETCLASS, record.mClassNames);
			CLintExpressionRenderer::collectEntityNames(classExpression, CBuildExpression::BETOBJECTPROPERTY, record.mClassObjectProperties);
			QMutexLocker locker(&sMutex);
			// the same axiom is reported again if a second parser is tried after a first one failed or the ontology is parsed twice
			if (!sKnownTexts.contains(text)) {
				sKnownTexts.insert(text);
				sHasKeyRecords.append(record);
			}
		}


		void CLintParseRecorder::recordHasKeyMixedProperties(CBuildExpression* classExpression, const QList<CBuildExpression*>& keyProperties) {
			if (!sRecording) {
				return;
			}
			QList<CBuildExpression*> objectProperties, dataProperties;
			for (CBuildExpression* p : keyProperties) {
				if (p && p->getType() == CBuildExpression::BETDATAPROPERTY) {
					dataProperties.append(p);
				} else {
					objectProperties.append(p);
				}
			}
			recordHasKey(classExpression, objectProperties, dataProperties);
		}


		QList<CLintParseRecorder::CHasKeyRecord> CLintParseRecorder::getHasKeyRecords() {
			QMutexLocker locker(&sMutex);
			return sHasKeyRecords;
		}


		void CLintParseRecorder::clearRecords() {
			QMutexLocker locker(&sMutex);
			sHasKeyRecords.clear();
			sKnownTexts.clear();
		}


	}; // end namespace Parser

}; // end namespace Konclude
