/**
 *    Copyright (C) 2018-present MongoDB, Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/db/exec/projection.h"

#include "boost/optional.hpp"

#include "mongo/db/exec/plan_stage.h"
#include "mongo/db/exec/scoped_timer.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/exec/working_set_computed_data.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/pipeline/parsed_inclusion_projection.h"
#include "mongo/db/query/find_projection_ast.h"
#include "mongo/db/record_id.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/str.h"

namespace mongo {

namespace fpast = find_projection_ast;

static const char* kIdField = "_id";

namespace {

BSONObj indexKey(const WorkingSetMember& member) {
    return static_cast<const IndexKeyComputedData*>(member.getComputed(WSM_INDEX_KEY))->getKey();
}

BSONObj sortKey(const WorkingSetMember& member) {
    return static_cast<const SortKeyComputedData*>(member.getComputed(WSM_SORT_KEY))->getSortKey();
}

double geoDistance(const WorkingSetMember& member) {
    return static_cast<const GeoDistanceComputedData*>(
               member.getComputed(WSM_COMPUTED_GEO_DISTANCE))
        ->getDist();
}

BSONObj geoPoint(const WorkingSetMember& member) {
    return static_cast<const GeoNearPointComputedData*>(member.getComputed(WSM_GEO_NEAR_POINT))
        ->getPoint();
}

double textScore(const WorkingSetMember& member) {
    if (member.hasComputed(WSM_COMPUTED_TEXT_SCORE))
        return static_cast<const TextScoreComputedData*>(
                   member.getComputed(WSM_COMPUTED_TEXT_SCORE))
            ->getScore();
    // It is permitted to request a text score when none has been computed. Zero is returned as an
    // empty value in this case.
    else
        return 0.0;
}

void transitionMemberToOwnedObj(const BSONObj& bo, WorkingSetMember* member) {
    member->keyData.clear();
    member->recordId = RecordId();
    member->obj = Snapshotted<BSONObj>(SnapshotId(), bo);
    member->transitionToOwnedObj();
}

}  // namespace

ProjectionStage::ProjectionStage(OperationContext* opCtx,
                                 const BSONObj& projObj,
                                 WorkingSet* ws,
                                 std::unique_ptr<PlanStage> child,
                                 const char* stageType)
    : PlanStage(opCtx, std::move(child), stageType), _projObj(projObj), _ws(*ws) {}

// static
void ProjectionStage::getSimpleInclusionFields(const BSONObj& projObj, FieldSet* includedFields) {
    // The _id is included by default.
    bool includeId = true;

    // Figure out what fields are in the projection.  TODO: we can get this from the
    // ParsedProjection...modify that to have this type instead of a vector.
    BSONObjIterator projObjIt(projObj);
    while (projObjIt.more()) {
        BSONElement elt = projObjIt.next();
        // Must deal with the _id case separately as there is an implicit _id: 1 in the
        // projection.
        if ((elt.fieldNameStringData() == kIdField) && !elt.trueValue()) {
            includeId = false;
            continue;
        }
        (*includedFields)[elt.fieldNameStringData()] = true;
    }

    if (includeId) {
        (*includedFields)[kIdField] = true;
    }
}

bool ProjectionStage::isEOF() {
    return child()->isEOF();
}

PlanStage::StageState ProjectionStage::doWork(WorkingSetID* out) {
    WorkingSetID id = WorkingSet::INVALID_ID;
    StageState status = child()->work(&id);

    // Note that we don't do the normal if isEOF() return EOF thing here.  Our child might be a
    // tailable cursor and isEOF() would be true even if it had more data...
    if (PlanStage::ADVANCED == status) {
        WorkingSetMember* member = _ws.get(id);
        // Punt to our specific projection impl.
        Status projStatus = transform(member);
        if (!projStatus.isOK()) {
            warning() << "Couldn't execute projection, status = " << redact(projStatus);
            *out = WorkingSetCommon::allocateStatusMember(&_ws, projStatus);
            return PlanStage::FAILURE;
        }

        *out = id;
    } else if (PlanStage::FAILURE == status) {
        // The stage which produces a failure is responsible for allocating a working set member
        // with error details.
        invariant(WorkingSet::INVALID_ID != id);
        *out = id;
    } else if (PlanStage::NEED_YIELD == status) {
        *out = id;
    }

    return status;
}

std::unique_ptr<PlanStageStats> ProjectionStage::getStats() {
    _commonStats.isEOF = isEOF();
    auto ret = std::make_unique<PlanStageStats>(_commonStats, stageType());

    auto projStats = std::make_unique<ProjectionStats>(_specificStats);
    projStats->projObj = _projObj;
    ret->specific = std::move(projStats);

    ret->children.emplace_back(child()->getStats());
    return ret;
}

ProjectionStageReturnKey::ProjectionStageReturnKey(OperationContext* opCtx,
                                                   const LogicalProjection& lp,
                                                   WorkingSet* ws,
                                                   std::unique_ptr<PlanStage> child,
                                                   const MatchExpression& fullExpression,
                                                   const CollatorInterface* collator)
    : ProjectionStage(opCtx, lp.getProjObj(), ws, std::move(child), "PROJECTION_RETURN_KEY"),
      _logicalProjection(lp) {}

StatusWith<BSONObj> ProjectionStageReturnKey::computeReturnKeyProjection(
    const BSONObj& indexKey, const BSONObj& sortKey) const {
    BSONObjBuilder bob;

    if (!indexKey.isEmpty()) {
        bob.appendElements(indexKey);
    }

    // Must be possible to do both returnKey meta-projection and sortKey meta-projection so that
    // mongos can support returnKey.
    for (auto fieldName : _logicalProjection.sortKeyMetaFields())
        bob.append(fieldName, sortKey);

    return bob.obj();
}

Status ProjectionStageReturnKey::transform(WorkingSetMember* member) const {
    if (_logicalProjection.needsSortKey() && !member->hasComputed(WSM_SORT_KEY))
        return Status(ErrorCodes::InternalError,
                      "sortKey meta-projection requested but no data available");

    auto keys = computeReturnKeyProjection(
        member->hasComputed(WSM_INDEX_KEY) ? indexKey(*member) : BSONObj(),
        _logicalProjection.needsSortKey() ? sortKey(*member) : BSONObj());
    if (!keys.isOK())
        return keys.getStatus();

    transitionMemberToOwnedObj(keys.getValue(), member);
    return Status::OK();
}

ProjectionStageDefault::ProjectionStageDefault(OperationContext* opCtx,
                                               const LogicalProjection& logicalProjection,
                                               WorkingSet* ws,
                                               std::unique_ptr<PlanStage> child,
                                               const MatchExpression& fullExpression,
                                               const CollatorInterface* collator)
    : ProjectionStage(
          opCtx, logicalProjection.getProjObj(), ws, std::move(child), "PROJECTION_DEFAULT"),
      _logicalProjection(logicalProjection),
      _expCtx(new ExpressionContext(opCtx, collator)),
      _originalMatchExpression(&fullExpression) {

    _projExec = parsed_aggregation_projection::ParsedAggregationProjection::create(
        _expCtx, &logicalProjection, ProjectionPolicies{}, &fullExpression);
}

namespace {
void appendMetadata(WorkingSetMember* member, MutableDocument* md, const LogicalProjection& lp) {
    invariant(member);
    invariant(md);

    if (lp.wantGeoNearDistance()) {
        md->setGeoNearDistance(geoDistance(*member));
    }

    if (lp.wantGeoNearPoint()) {
        md->setGeoNearPoint(Value(geoPoint(*member)));
    }

    log() << "member has recordid " << member->hasRecordId();
    if (member->hasRecordId()) {
        md->setRecordId(member->recordId);
    }
}

void doSlicing(MutableDocument* outputDoc, const fpast::SliceInfo& args, size_t indexIntoPath) {
    if (indexIntoPath + 1 == args.path.getPathLength()) {

        std::string fieldName = args.path.getFieldName(indexIntoPath).toString();

        Document d(outputDoc->peek());
        Value v = d.getField(fieldName);

        if (v.getType() != BSONType::Array) {
            // Nothing to slice
            return;
        }

        std::vector<Value> arr = v.getArray();
        // Not supporting {$slice: <anything less than 0>}
        invariant(args.limit > 0);
        if (static_cast<size_t>(args.limit) < arr.size()) {
            arr.resize(args.limit);
        }

        outputDoc->setField(fieldName, Value(arr));

        return;
    }

    std::string fieldName = args.path.getFieldName(indexIntoPath).toString();
    Value f = outputDoc->peek().getField(fieldName);

    if (f.getType() == BSONType::Object) {
        Document subDoc(f.getDocument());
        MutableDocument mutSubDoc(subDoc);

        doSlicing(&mutSubDoc, args, indexIntoPath + 1);

        outputDoc->setField(fieldName, Value(mutSubDoc.freeze()));
    } else if (f.getType() == BSONType::Array) {
        const std::vector<Value> arr = f.getArray();
        std::vector<Value> results;
        for (auto&& elem : arr) {
            if (elem.getType() != BSONType::Object) {
                results.push_back(elem);
                continue;
            }

            Document subDoc = elem.getDocument();
            MutableDocument md(subDoc);
            doSlicing(&md, args, indexIntoPath + 1);
            results.push_back(Value(md.freeze()));
        }
        outputDoc->setField(fieldName, Value(results));
    }
}
}

Document ProjectionStageDefault::doProjectionTransformation(Document input) const {
    Document out = _projExec->applyTransformation(input);

    auto positionalProjectionPath = _logicalProjection.getPositionalProjection();
    if (positionalProjectionPath) {
        std::cout << "ian: applying positional projection w path "
                  << positionalProjectionPath->path.fullPath() << std::endl;

        // Apply the match expression
        MatchDetails details;
        details.requestElemMatchKey();

        invariant(_originalMatchExpression);
        invariant(_originalMatchExpression->matchesBSON(input.toBson(), &details));

        uassert(ErrorCodes::BadValue,
                "positional operator '.$' requires correspoding field in query specifier",
                details.hasElemMatchKey());

        boost::optional<size_t> optIndex = str::parseUnsignedBase10Integer(details.elemMatchKey());
        invariant(optIndex);

        // Find the first array in the document, and trim it to just have the element from optIndex.
        MutableDocument outputDoc(out);
        FieldPath fp(positionalProjectionPath->path);
        for (size_t i = 0; i < fp.getPathLength(); ++i) {
            FieldPath subPath = fp.getSubpath(i);
            Value v = outputDoc.peek().getNestedField(subPath);
            if (v.getType() == BSONType::Array) {
                log() << "ian: found array at component " << subPath.fullPath();
                std::vector<Value> arr = v.getArray();

                uassert(
                    ErrorCodes::BadValue, "positional operator mismatch", *optIndex < arr.size());

                outputDoc.setNestedField(subPath, Value(std::vector<Value>{arr[*optIndex]}));
                break;
            }
        }

        out = outputDoc.freeze();
    }

    auto sliceArgs = _logicalProjection.getSliceArgs();
    if (sliceArgs) {
        MutableDocument outputDoc(out);
        std::cout << "ian: applying slice to path " << sliceArgs->path.fullPath() << std::endl;

        doSlicing(&outputDoc, *sliceArgs, 0);

        out = outputDoc.freeze();
    }

    return out;
}

Status ProjectionStageDefault::transform(WorkingSetMember* member) const {
    if (member->hasObj()) {
        MutableDocument doc(Document(member->obj.value()));
        appendMetadata(member, &doc, _logicalProjection);

        log() << "ian: applying projection" << std::endl;
        Document out = doProjectionTransformation(doc.freeze());
        transitionMemberToOwnedObj(out.toBson(), member);
    } else {
        // only inclusion projections can be covered.
        parsed_aggregation_projection::ParsedInclusionProjection* inclusionProj =
            dynamic_cast<parsed_aggregation_projection::ParsedInclusionProjection*>(
                _projExec.get());
        invariant(inclusionProj);

        DocumentSource::GetModPathsReturn modPaths = inclusionProj->getModifiedPaths();

        MutableDocument md;

        // TODO: This is horribly inefficient. We should use the projected fields from the
        // projection spec.  In find projection, the user is required to write out: each field
        // name, so the list of projected fields is easy to get. We'll have to write something
        // similar for agg projection.
        //
        // We don't support covering for subfields (e.g. if your index is on a but projection is on
        // 'a.b' you can't get a covered projection). So it should be sufficient to just list all
        // of the "leaf node" fields from the projection.
        //
        // E.g.
        // db.c.createIndex({a: 1});
        // db.c.find({}, {_id: 0, "a.b": 1}).explain()
        // Still gives collscan
        //
        for (auto&& path : modPaths.paths) {
            auto elt = IndexKeyDatum::getFieldDotted(member->keyData, path);
            invariant(elt);
            md.setNestedField(path, Value(*elt));
        }
        appendMetadata(member, &md, _logicalProjection);

        Document doc(md.freeze());
        Document out = doProjectionTransformation(doc);
        transitionMemberToOwnedObj(out.toBson(), member);
    }

    return Status::OK();
}

ProjectionStageCovered::ProjectionStageCovered(OperationContext* opCtx,
                                               const BSONObj& projObj,
                                               WorkingSet* ws,
                                               std::unique_ptr<PlanStage> child,
                                               const BSONObj& coveredKeyObj)
    : ProjectionStage(opCtx, projObj, ws, std::move(child), "PROJECTION_COVERED"),
      _coveredKeyObj(coveredKeyObj) {
    invariant(projObjHasOwnedData());
    // Figure out what fields are in the projection.
    getSimpleInclusionFields(_projObj, &_includedFields);

    // If we're pulling data out of one index we can pre-compute the indices of the fields
    // in the key that we pull data from and avoid looking up the field name each time.

    // Sanity-check.
    invariant(_coveredKeyObj.isOwned());

    BSONObjIterator kpIt(_coveredKeyObj);
    while (kpIt.more()) {
        BSONElement elt = kpIt.next();
        auto fieldIt = _includedFields.find(elt.fieldNameStringData());
        if (_includedFields.end() == fieldIt) {
            // Push an unused value on the back to keep _includeKey and _keyFieldNames
            // in sync.
            _keyFieldNames.push_back(StringData());
            _includeKey.push_back(false);
        } else {
            // If we are including this key field store its field name.
            _keyFieldNames.push_back(fieldIt->first);
            _includeKey.push_back(true);
        }
    }
}

Status ProjectionStageCovered::transform(WorkingSetMember* member) const {
    BSONObjBuilder bob;

    // We're pulling data out of the key.
    invariant(1 == member->keyData.size());
    size_t keyIndex = 0;

    // Look at every key element...
    BSONObjIterator keyIterator(member->keyData[0].keyData);
    while (keyIterator.more()) {
        BSONElement elt = keyIterator.next();
        // If we're supposed to include it...
        if (_includeKey[keyIndex]) {
            // Do so.
            bob.appendAs(elt, _keyFieldNames[keyIndex]);
        }
        ++keyIndex;
    }

    transitionMemberToOwnedObj(bob.obj(), member);
    return Status::OK();
}

ProjectionStageSimple::ProjectionStageSimple(OperationContext* opCtx,
                                             const BSONObj& projObj,
                                             WorkingSet* ws,
                                             std::unique_ptr<PlanStage> child)
    : ProjectionStage(opCtx, projObj, ws, std::move(child), "PROJECTION_SIMPLE") {
    invariant(projObjHasOwnedData());
    // Figure out what fields are in the projection.
    getSimpleInclusionFields(_projObj, &_includedFields);
}

Status ProjectionStageSimple::transform(WorkingSetMember* member) const {
    BSONObjBuilder bob;
    // SIMPLE_DOC implies that we expect an object so it's kind of redundant.
    // If we got here because of SIMPLE_DOC the planner shouldn't have messed up.
    invariant(member->hasObj());

    // Apply the SIMPLE_DOC projection.
    // Look at every field in the source document and see if we're including it.
    BSONObjIterator inputIt(member->obj.value());
    while (inputIt.more()) {
        BSONElement elt = inputIt.next();
        auto fieldIt = _includedFields.find(elt.fieldNameStringData());
        if (_includedFields.end() != fieldIt) {
            // If so, add it to the builder.
            bob.append(elt);
        }
    }

    transitionMemberToOwnedObj(bob.obj(), member);
    return Status::OK();
}

}  // namespace mongo
