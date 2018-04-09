/**
 *    Copyright (C) 2017 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/db/pipeline/document_source_change_stream_transform.h"

#include "mongo/bson/simple_bsonelement_comparator.h"
#include "mongo/db/bson/bson_helper.h"
#include "mongo/db/catalog/uuid_catalog.h"
#include "mongo/db/commands/feature_compatibility_version_documentation.h"
#include "mongo/db/logical_clock.h"
#include "mongo/db/pipeline/change_stream_constants.h"
#include "mongo/db/pipeline/document_path_support.h"
#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/db/pipeline/document_source_check_resume_token.h"
#include "mongo/db/pipeline/document_source_limit.h"
#include "mongo/db/pipeline/document_source_lookup_change_post_image.h"
#include "mongo/db/pipeline/document_source_sort.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/lite_parsed_document_source.h"
#include "mongo/db/pipeline/resume_token.h"
#include "mongo/db/repl/oplog_entry.h"
#include "mongo/db/repl/oplog_entry_gen.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/s/catalog_cache.h"
#include "mongo/s/grid.h"
#include "mongo/util/log.h"

namespace mongo {

using boost::intrusive_ptr;
using boost::optional;
using std::list;
using std::string;
using std::vector;

namespace {
constexpr auto checkValueType = &DocumentSourceChangeStream::checkValueType;
}

DocumentSourceOplogTransformation::DocumentSourceOplogTransformation(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, BSONObj changeStreamSpec)
    : DocumentSource(expCtx), _changeStreamSpec(changeStreamSpec.getOwned()) {

    if (expCtx->ns.isCollectionlessAggregateNS()) {
        _nsRegex.emplace(DocumentSourceChangeStream::buildNsRegex(expCtx->ns));
    }
}

Document DocumentSourceOplogTransformation::applyTransformation(const Document& input,
                                                                bool isApplyOpsEntry) {
    // If we're executing a change stream pipeline that was forwarded from mongos, then we expect it
    // to "need merge"---we expect to be executing the shards part of a split pipeline. It is never
    // correct for mongos to pass through the change stream without splitting into into a merging
    // part executed on mongos and a shards part.
    //
    // This is necessary so that mongos can correctly handle "invalidate" and "retryNeeded" change
    // notifications. See SERVER-31978 for an example of why the pipeline must be split.
    //
    // We have to check this invariant at run-time of the change stream rather than parse time,
    // since a mongos may forward a change stream in an invalid position (e.g. in a nested $lookup
    // or $facet pipeline). In this case, mongod is responsible for parsing the pipeline and
    // throwing an error without ever executing the change stream.
    if (pExpCtx->fromMongos) {
        invariant(pExpCtx->needsMerge);
    }
    log() << "ian: processing... " << input;

    MutableDocument doc;

    // Extract the fields we need.
    checkValueType(input[repl::OplogEntry::kOpTypeFieldName],
                   repl::OplogEntry::kOpTypeFieldName,
                   BSONType::String);
    string op = input[repl::OplogEntry::kOpTypeFieldName].getString();
    Value ts = input[repl::OplogEntry::kTimestampFieldName];
    Value ns = input[repl::OplogEntry::kNamespaceFieldName];
    checkValueType(ns, repl::OplogEntry::kNamespaceFieldName, BSONType::String);
    Value uuid = input[repl::OplogEntry::kUuidFieldName];
    if (!uuid.missing()) {
        checkValueType(uuid, repl::OplogEntry::kUuidFieldName, BSONType::BinData);
        // We need to retrieve the document key fields if our cached copy has not been populated. If
        // the collection was unsharded but has now transitioned to a sharded state, we must update
        // the documentKey fields to include the shard key. We only need to re-check the documentKey
        // while the collection is unsharded; if the collection is or becomes sharded, then the
        // documentKey is final and will not change.
        if (!_documentKeyFieldsSharded) {
            // If this is not a shard server, 'catalogCache' will be nullptr and we will skip the
            // routing table check.
            auto catalogCache = Grid::get(pExpCtx->opCtx)->catalogCache();
            const bool collectionIsSharded = catalogCache && [catalogCache, this]() {
                auto routingInfo =
                    catalogCache->getCollectionRoutingInfo(pExpCtx->opCtx, pExpCtx->ns);
                return routingInfo.isOK() && routingInfo.getValue().cm();
            }();
            if (_documentKeyFields.empty() || collectionIsSharded) {
                _documentKeyFields = pExpCtx->mongoProcessInterface->collectDocumentKeyFields(
                    pExpCtx->opCtx, pExpCtx->ns, uuid.getUuid());
                _documentKeyFieldsSharded = collectionIsSharded;
            }
        }
    }
    NamespaceString nss(ns.getString());
    Value id = input.getNestedField("o._id");
    // Non-replace updates have the _id in field "o2".
    StringData operationType;
    Value fullDocument;
    Value updateDescription;
    Value documentKey;

    // Deal with CRUD operations and commands.
    auto opType = repl::OpType_parse(IDLParserErrorContext("ChangeStreamEntry.op"), op);
    switch (opType) {
        case repl::OpTypeEnum::kInsert: {
            operationType = DocumentSourceChangeStream::kInsertOpType;
            fullDocument = input[repl::OplogEntry::kObjectFieldName];
            documentKey = Value(document_path_support::extractDocumentKeyFromDoc(
                fullDocument.getDocument(), _documentKeyFields));
            break;
        }
        case repl::OpTypeEnum::kDelete: {
            operationType = DocumentSourceChangeStream::kDeleteOpType;
            documentKey = input[repl::OplogEntry::kObjectFieldName];
            break;
        }
        case repl::OpTypeEnum::kUpdate: {
            if (id.missing()) {
                operationType = DocumentSourceChangeStream::kUpdateOpType;
                checkValueType(input[repl::OplogEntry::kObjectFieldName],
                               repl::OplogEntry::kObjectFieldName,
                               BSONType::Object);
                Document opObject = input[repl::OplogEntry::kObjectFieldName].getDocument();
                Value updatedFields = opObject["$set"];
                Value removedFields = opObject["$unset"];

                // Extract the field names of $unset document.
                vector<Value> removedFieldsVector;
                if (removedFields.getType() == BSONType::Object) {
                    auto iter = removedFields.getDocument().fieldIterator();
                    while (iter.more()) {
                        removedFieldsVector.push_back(Value(iter.next().first));
                    }
                }
                updateDescription = Value(Document{
                    {"updatedFields", updatedFields.missing() ? Value(Document()) : updatedFields},
                    {"removedFields", removedFieldsVector}});
            } else {
                operationType = DocumentSourceChangeStream::kReplaceOpType;
                fullDocument = input[repl::OplogEntry::kObjectFieldName];
            }
            documentKey = input[repl::OplogEntry::kObject2FieldName];
            break;
        }
        case repl::OpTypeEnum::kCommand: {
            // Any command that makes it through our filter is an invalidating command such as a
            // drop.
            log() << "ian: Found a command: " << input;
            if (!input.getNestedField("o.applyOps").missing()) {
                log() << "ian: found an applyOps. Saving it...";

                // We should never see an applyOps inside of an applyOps that made it past the
                // filter. This prevents more than one level of recursion.
                invariant(!isApplyOpsEntry);
                invariant(_currentApplyOps.empty());
                invariant(_applyOpsIndex == 0);

                // TODO: Switch to using BSONObjIterator
                _currentApplyOps = input.getNestedField("o.applyOps").getArray();
                invariant(!_currentApplyOps.empty());
                // TODO: Where are the string constants for these?
                Value lsid = input["lsid"];
                checkValueType(lsid, "lsid", BSONType::Object);
                _lsid = lsid.getDocument();

                Value txnNumber = input["txnNumber"];
                checkValueType(txnNumber, "txnNumber", BSONType::NumberLong);
                _txnNumber = txnNumber.getLong();

                // Now call applyTransformation on the first relevant entry in the applyOps.
                Document nextDoc = extractNextApplyOpsEntry();
                invariant(!nextDoc.empty());
                return applyTransformation(nextDoc, true);
            }

            log() << "ian: found an invalidating command";
            operationType = DocumentSourceChangeStream::kInvalidateOpType;
            // Make sure the result doesn't have a document key.
            documentKey = Value();
            break;
        }
        case repl::OpTypeEnum::kNoop: {
            operationType = DocumentSourceChangeStream::kNewShardDetectedOpType;
            // Generate a fake document Id for NewShardDetected operation so that we can resume
            // after this operation.
            documentKey = Value(Document{{DocumentSourceChangeStream::kIdField,
                                          input[repl::OplogEntry::kObject2FieldName]}});
            break;
        }
        default: { MONGO_UNREACHABLE; }
    }

    // UUID should always be present except for invalidate entries.  It will not be under
    // FCV 3.4, so we should close the stream as invalid.
    if (operationType != DocumentSourceChangeStream::kInvalidateOpType && uuid.missing()) {
        warning() << "Saw a CRUD op without a UUID.  Did Feature Compatibility Version get "
                     "downgraded after opening the stream?";
        operationType = DocumentSourceChangeStream::kInvalidateOpType;
        fullDocument = Value();
        updateDescription = Value();
        documentKey = Value();
    }

    // Note that 'documentKey' and/or 'uuid' might be missing, in which case the missing fields will
    // not appear in the output.
    ResumeTokenData resumeTokenData;
    if (isApplyOpsEntry) {
        // Do something with resume token

        // TODO:!
        // For now we return an empty resumeToken.

    } else {
        // do something else
        resumeTokenData.clusterTime = ts.getTimestamp();
        resumeTokenData.documentKey = documentKey;
        if (!uuid.missing())
            resumeTokenData.uuid = uuid.getUuid();
    }

    if (isApplyOpsEntry) {
        invariant(_txnNumber);
        invariant(_lsid);
        doc.addField(DocumentSourceChangeStream::kTxnNumberField,
                     Value(static_cast<long long>(_txnNumber.get())));
        doc.addField(DocumentSourceChangeStream::kLsidField, Value(_lsid.get()));
    }

    doc.addField(DocumentSourceChangeStream::kIdField,
                 Value(ResumeToken(resumeTokenData).toDocument()));
    doc.addField(DocumentSourceChangeStream::kOperationTypeField, Value(operationType));

    // If we're in a sharded environment, we'll need to merge the results by their sort key, so add
    // that as metadata.
    if (pExpCtx->needsMerge) {
        doc.setSortKeyMetaField(BSON("" << ts << "" << uuid << "" << documentKey));
    }

    // "invalidate" and "newShardDetected" entries have fewer fields.
    if (operationType == DocumentSourceChangeStream::kInvalidateOpType ||
        operationType == DocumentSourceChangeStream::kNewShardDetectedOpType) {
        return doc.freeze();
    }

    doc.addField(DocumentSourceChangeStream::kFullDocumentField, fullDocument);
    doc.addField(DocumentSourceChangeStream::kNamespaceField,
                 Value(Document{{"db", nss.db()}, {"coll", nss.coll()}}));
    doc.addField(DocumentSourceChangeStream::kDocumentKeyField, documentKey);

    // Note that 'updateDescription' might be the 'missing' value, in which case it will not be
    // serialized.
    doc.addField("updateDescription", updateDescription);
    return doc.freeze();
}

Document DocumentSourceOplogTransformation::serializeStageOptions(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    Document changeStreamOptions(_changeStreamSpec);
    // If we're on a mongos and no other start time is specified, we want to start at the current
    // cluster time on the mongos.  This ensures all shards use the same start time.
    if (pExpCtx->inMongos &&
        changeStreamOptions[DocumentSourceChangeStreamSpec::kResumeAfterFieldName].missing() &&
        changeStreamOptions
            [DocumentSourceChangeStreamSpec::kResumeAfterClusterTimeDeprecatedFieldName]
                .missing() &&
        changeStreamOptions[DocumentSourceChangeStreamSpec::kStartAtClusterTimeFieldName]
            .missing()) {
        MutableDocument newChangeStreamOptions(changeStreamOptions);

        // Use the current cluster time plus 1 tick since the oplog query will include all
        // operations/commands equal to or greater than the 'startAtClusterTime' timestamp. In
        // particular, avoid including the last operation that went through mongos in an attempt to
        // match the behavior of a replica set more closely.
        auto clusterTime = LogicalClock::get(pExpCtx->opCtx)->getClusterTime();
        clusterTime.addTicks(1);
        newChangeStreamOptions[DocumentSourceChangeStreamSpec::kStartAtClusterTimeFieldName]
                              [ResumeTokenClusterTime::kTimestampFieldName] =
                                  Value(clusterTime.asTimestamp());
        changeStreamOptions = newChangeStreamOptions.freeze();
    }
    return changeStreamOptions;
}

DocumentSource::GetDepsReturn DocumentSourceOplogTransformation::getDependencies(
    DepsTracker* deps) const {
    deps->fields.insert(repl::OplogEntry::kOpTypeFieldName.toString());
    deps->fields.insert(repl::OplogEntry::kTimestampFieldName.toString());
    deps->fields.insert(repl::OplogEntry::kNamespaceFieldName.toString());
    deps->fields.insert(repl::OplogEntry::kUuidFieldName.toString());
    deps->fields.insert(repl::OplogEntry::kObjectFieldName.toString());
    deps->fields.insert(repl::OplogEntry::kObject2FieldName.toString());
    return DocumentSource::GetDepsReturn::EXHAUSTIVE_ALL;
}

DocumentSource::GetModPathsReturn DocumentSourceOplogTransformation::getModifiedPaths() const {
    // All paths are modified.
    return {DocumentSource::GetModPathsReturn::Type::kAllPaths, std::set<string>{}, {}};
}

Value DocumentSourceOplogTransformation::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(Document{{getSourceName(), serializeStageOptions(explain)}});
}

DocumentSource::StageConstraints DocumentSourceOplogTransformation::constraints(
    Pipeline::SplitState pipeState) const {
    StageConstraints constraints(StreamType::kStreaming,
                                 PositionRequirement::kNone,
                                 HostTypeRequirement::kNone,
                                 DiskUseRequirement::kNoDiskUse,
                                 FacetRequirement::kNotAllowed,
                                 TransactionRequirement::kNotAllowed,
                                 ChangeStreamRequirement::kChangeStreamStage);

    constraints.canSwapWithMatch = true;
    constraints.canSwapWithLimit = true;
    return constraints;
}

DocumentSource::GetNextResult DocumentSourceOplogTransformation::getNext() {
    pExpCtx->checkForInterrupt();

    Document next = extractNextApplyOpsEntry();
    if (!next.empty()) {
        log() << "ian: non empty _applyOps: ";
        log() << "Extracted: " << next;
        return applyTransformation(next, true);
    }

    // Get the next input document.
    auto input = pSource->getNext();
    if (!input.isAdvanced()) {
        return input;
    }

    // Apply and return the document with added fields.
    return applyTransformation(input.releaseDocument(), false);
}

bool isOpTypeRelevant(const Document& d) {
    // TODO: talk to Charlie about just using this instead of having two implementations of the
    // same thing.

    Value op = d["op"];
    invariant(!op.missing());

    if (op.getString() != "n") {
        return true;
    }

    Value type = d.getNestedField("o2.type");
    if (!type.missing() && type.getString() == "migrateChunkToNewShard") {
        return true;
    }

    return false;
}

bool DocumentSourceOplogTransformation::isDocumentRelevant(const Document& d) {
    if (!isOpTypeRelevant(d)) {
        return false;
    }

    Value nsField = d["ns"];
    invariant(!nsField.missing());

    if (_nsRegex) {
        // Match all namespaces that start with db name, followed by ".", then not followed by
        // '$' or 'system.'
        return _nsRegex->PartialMatch(nsField.getString());
    }

    return nsField.getString() == pExpCtx->ns.ns();
}

/*
 * Gets the next relevant applyOps entry that should be returned. If there is none, returns empty
 * document.
 */
Document DocumentSourceOplogTransformation::extractNextApplyOpsEntry() {

    while (_applyOpsIndex < _currentApplyOps.size()) {
        Document d = _currentApplyOps[_applyOpsIndex++].getDocument();
        log() << "Found applyOps subDocument " << d;
        if (!isDocumentRelevant(d)) {
            log() << "Document is not relevant";
            continue;
        }

        return d;
    }

    // We ran out of stuff in the applyOps entry. Clear out _currentApplyOps.
    _currentApplyOps.clear();
    _applyOpsIndex = 0;

    return Document();
}
}
