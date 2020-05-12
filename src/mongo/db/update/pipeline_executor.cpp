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

#include <iomanip>

#include "mongo/db/update/pipeline_executor.h"

#include "mongo/db/bson/dotted_path_support.h"
#include "mongo/db/pipeline/document_source_queue.h"
#include "mongo/db/pipeline/lite_parsed_pipeline.h"
#include "mongo/db/update/document_differ.h"
#include "mongo/db/update/object_replace_executor.h"
#include "mongo/db/update/storage_validation.h"

namespace mongo {

namespace {
constexpr StringData kIdFieldName = "_id"_sd;

void initPipeline(const boost::intrusive_ptr<ExpressionContext>& expCtx, Pipeline* pipeline) {
    // Validate the update pipeline.
    for (auto&& stage : pipeline->getSources()) {
        auto stageConstraints = stage->constraints();
        uassert(ErrorCodes::InvalidOptions,
                str::stream() << stage->getSourceName()
                              << " is not allowed to be used within an update",
                stageConstraints.isAllowedWithinUpdatePipeline);

        invariant(stageConstraints.requiredPosition ==
                  StageConstraints::PositionRequirement::kNone);
        invariant(!stageConstraints.isIndependentOfAnyCollection);
    }
    pipeline->addInitialSource(DocumentSourceQueue::create(expCtx));
}

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
            Variables::uassertValidNameForUserRead(constName);

            auto varId = _expCtx->variablesParseState.defineVariable(constName);
            _expCtx->variables.setConstantValue(varId, Value(constElem));
        }
    }

    _expCtx->setResolvedNamespaces(resolvedNamespaces);
    _pipeline = Pipeline::parse(pipeline, _expCtx);
    initPipeline(expCtx, _pipeline.get());
}

PipelineExecutor::PipelineExecutor(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                   std::unique_ptr<Pipeline, PipelineDeleter> pipeline)
    : _expCtx(expCtx), _pipeline(std::move(pipeline)) {
    initPipeline(expCtx, _pipeline.get());
}

UpdateExecutor::ApplyResult PipelineExecutor::applyUpdate(ApplyParams applyParams) const {
    DocumentSourceQueue* queueStage = static_cast<DocumentSourceQueue*>(_pipeline->peekFront());
    queueStage->emplace_back(Document{applyParams.element.getDocument().getObject()});
    auto transformedDoc = _pipeline->getNext()->toBson();
    auto transformedDocHasIdField = transformedDoc.hasField(kIdFieldName);

    auto originalDoc = applyParams.element.getDocument().getObject();

    if (applyParams.logBuilder) {
        std::vector<std::string> fieldsRemoved;
        {
            auto replacementDoc = transformedDoc;
            auto originalDoc = applyParams.element.getDocument().getObject();

            // Check for noop.
            if (originalDoc.binaryEqual(replacementDoc)) {
                return ApplyResult::noopResult();
            }

            // Remove the contents of the provided document.
            auto current = applyParams.element.leftChild();
            while (current.ok()) {
                // Keep the _id if the replacement document does not have one.
                if (!transformedDocHasIdField && current.getFieldName() == kIdFieldName) {
                    current = current.rightSibling();
                    continue;
                }
                fieldsRemoved.push_back(current.getFieldName().toString());

                auto toRemove = current;
                current = current.rightSibling();
                invariant(toRemove.remove());
            }

            // Insert the provided contents instead.
            for (auto&& elem : replacementDoc) {
                invariant(applyParams.element.appendElement(elem));
            }

            // Validate for storage.
            if (applyParams.validateForStorage) {
                storage_validation::storageValid(applyParams.element.getDocument());
            }

            // Check immutable paths.
            for (auto path = applyParams.immutablePaths.begin();
                 path != applyParams.immutablePaths.end();
                 ++path) {
                // TODO: ian review this in more depth. It's copy-pasted.

                // Find the updated field in the updated document.
                auto newElem = applyParams.element;
                for (size_t i = 0; i < (*path)->numParts(); ++i) {
                    newElem = newElem[(*path)->getPart(i)];
                    if (!newElem.ok()) {
                        break;
                    }
                    uassert(
                        ErrorCodes::NotSingleValueField,
                        str::stream()
                            << "After applying the update to the document, the (immutable) field '"
                            << (*path)->dottedField()
                            << "' was found to be an array or array descendant.",
                        newElem.getType() != BSONType::Array);
                }

                auto oldElem =
                    dotted_path_support::extractElementAtPath(originalDoc, (*path)->dottedField());

                uassert(ErrorCodes::ImmutableField,
                        str::stream()
                            << "After applying the update, the '" << (*path)->dottedField()
                            << "' (required and immutable) field was "
                               "found to have been removed --"
                            << originalDoc,
                        newElem.ok() || !oldElem.ok());
                if (newElem.ok() && oldElem.ok()) {
                    uassert(ErrorCodes::ImmutableField,
                            str::stream()
                                << "After applying the update, the (immutable) field '"
                                << (*path)->dottedField() << "' was found to have been altered to "
                                << newElem.toString(),
                            newElem.compareWithBSONElement(oldElem, nullptr, false) == 0);
                }
            }
        }

        auto diff = doc_diff::computeDiff(originalDoc, transformedDoc);

        std::cout << "ian: diff is ";
        for (size_t i = 0; i < diff.len(); ++i) {
            const unsigned char next = static_cast<unsigned char>(diff.raw()[i]);
            std::cout << std::setfill('0') << std::setw(2) << std::hex << static_cast<int>(next) << std::dec << " ";
        }
        std::cout << std::dec << std::endl;
        
        auto dbg = doc_diff::diffToDebugBSON(diff);
        std::cout << "ian: dbg " << dbg << std::endl;

        // TODO: Re-enable this branch.
        if (diff.len() < static_cast<size_t>(transformedDoc.objsize())) {
            invariant(applyParams.logBuilder->setDeltaBin(diff.raw(), diff.len()));
            //if (diff.computeApproxSize() * 2 < static_cast<size_t>(transformedDoc.objsize()) && false) {
            // TODO: Set logBuilder's diff field.
            // for (auto&& [fieldRef, elt] : diff.toUpsert()) {
            //     invariant(
            //         applyParams.logBuilder->addToSetsWithNewFieldName(fieldRef.toString(), elt));
            // }

            // for (auto&& fieldRef : diff.toDelete()) {
            //     invariant(applyParams.logBuilder->addToUnsets(fieldRef.toString()));
            // }

            // for (auto&& [fieldRef, elt] : diff.toInsert()) {
            //     invariant(applyParams.logBuilder->addToCreates(fieldRef.toString(), elt));
            // }

            // for (auto&& [fieldRef, newSize] : diff.toResize()) {
            //     invariant(applyParams.logBuilder->addToResizes(fieldRef.toString(), newSize));
            // }

            invariant(applyParams.logBuilder->setUpdateSemantics(UpdateSemantics::kPipeline));
        } else {
            auto replacementObject = applyParams.logBuilder->getDocument().end();
            invariant(applyParams.logBuilder->getReplacementObject(&replacementObject));
            for (auto current = applyParams.element.leftChild(); current.ok();
                 current = current.rightSibling()) {
                invariant(replacementObject.appendElement(current.getValue()));
            }
        }
        return ApplyResult();
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
