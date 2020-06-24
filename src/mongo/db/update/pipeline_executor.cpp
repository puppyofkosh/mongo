/**
 *    Copyright (C) 2019-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/update/pipeline_executor.h"

#include "mongo/db/bson/dotted_path_support.h"
#include "mongo/db/pipeline/document_source_queue.h"
#include "mongo/db/pipeline/lite_parsed_pipeline.h"
#include "mongo/db/query/query_knobs_gen.h"
#include "mongo/db/update/object_replace_executor.h"
#include "mongo/db/update/oplog_delta_calculator.h"
#include "mongo/db/update/storage_validation.h"

namespace mongo {

namespace {
constexpr StringData kIdFieldName = "_id"_sd;
}  // namespace

PipelineExecutor::PipelineExecutor(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                   const std::vector<BSONObj>& pipeline,
                                   boost::optional<BSONObj> constants)
    : _expCtx(expCtx) {
    // "Resolve" involved namespaces into a map. We have to populate this map so that any
    // $lookups, etc. will not fail instantiation. They will not be used for execution as these
    // stages are not allowed within an update context.
    LiteParsedPipeline liteParsedPipeline(NamespaceString("dummy.namespace"), pipeline);
    StringMap<ExpressionContext::ResolvedNamespace> resolvedNamespaces;
    for (auto&& nss : liteParsedPipeline.getInvolvedNamespaces()) {
        resolvedNamespaces.try_emplace(nss.coll(), nss, std::vector<BSONObj>{});
    }

    if (constants) {
        for (auto&& constElem : *constants) {
            const auto constName = constElem.fieldNameStringData();
            Variables::validateNameForUserRead(constName);

            auto varId = _expCtx->variablesParseState.defineVariable(constName);
            _expCtx->variables.setConstantValue(varId, Value(constElem));
        }
    }

    _expCtx->setResolvedNamespaces(resolvedNamespaces);
    _pipeline = Pipeline::parse(pipeline, _expCtx);

    // Validate the update pipeline.
    for (auto&& stage : _pipeline->getSources()) {
        auto stageConstraints = stage->constraints();
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << stage->getSourceName()
                              << " is not allowed to be used within an update",
                stageConstraints.isAllowedWithinUpdatePipeline);

        invariant(stageConstraints.requiredPosition ==
                  StageConstraints::PositionRequirement::kNone);
        invariant(!stageConstraints.isIndependentOfAnyCollection);
    }

    _pipeline->addInitialSource(DocumentSourceQueue::create(expCtx));
}

UpdateExecutor::ApplyResult PipelineExecutor::applyUpdate(ApplyParams applyParams) const {
    const auto originalDoc = applyParams.element.getDocument().getObject();

    DocumentSourceQueue* queueStage = static_cast<DocumentSourceQueue*>(_pipeline->peekFront());
    queueStage->emplace_back(Document{originalDoc});

    const auto transformedDoc = _pipeline->getNext()->toBson();
    const auto transformedDocHasIdField = transformedDoc.hasField(kIdFieldName);

    std::cout << "ian: In applyUpdate() " << std::endl;
    // TODO: feature flag.
    invariant(!applyParams.logBuilder);
    if (applyParams.simpleLogBuilder) {
        const auto diff = doc_diff::computeDiff(originalDoc, transformedDoc);
        if (diff) {
            std::cout << "Generating diff entry\n";
            applyParams.simpleLogBuilder->setDelta(*diff);
        } else {
            std::cout << "Generating replacement diff " << std::endl;
            applyParams.simpleLogBuilder->setReplacement(transformedDoc);
        }

        // Run the object replace executor, but run it without the logBuilder, indicating
        // that it should not also generate an oplog entry.
        invariant(applyParams.logBuilder == nullptr);

        SimpleLogBuilder* tempLogBuilder = nullptr;
        std::swap(applyParams.simpleLogBuilder, tempLogBuilder);
        applyParams.simpleLogBuilder = nullptr;
        const auto ret = ObjectReplaceExecutor::applyReplacementUpdate(
            applyParams, transformedDoc, transformedDocHasIdField);
        std::swap(applyParams.simpleLogBuilder, tempLogBuilder);
        return ret;
    }

    return ObjectReplaceExecutor::applyReplacementUpdate(
        applyParams, transformedDoc, transformedDocHasIdField);
}

Value PipelineExecutor::serialize() const {
    std::vector<Value> valueArray;
    for (const auto& stage : _pipeline->getSources()) {
        // The queue stage we add to adapt the pull-based '_pipeline' to our use case should not
        // be serialized out. Firstly, this was not part of the user's pipeline and is just an
        // implementation detail. It wouldn't have much value in exposing. Secondly, supporting
        // a serialization that we can later re-parse is non trivial. See the comment in
        // DocumentSourceQueue for more details.
        if (typeid(*stage) == typeid(DocumentSourceQueue)) {
            continue;
        }

        stage->serializeToArray(valueArray);
    }

    return Value(valueArray);
}

}  // namespace mongo
