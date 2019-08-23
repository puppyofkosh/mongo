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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/db/exec/index_key.h"

#include "mongo/db/exec/working_set_common.h"
#include "mongo/util/log.h"

namespace mongo {
using namespace fmt::literals;

PlanStage::StageState IndexKeyStage::doWork(WorkingSetID* out) {
    WorkingSetID id = WorkingSet::INVALID_ID;
    StageState status = child()->work(&id);

    // Note that we don't do the normal if isEOF() return EOF thing here.  Our child might be a
    // tailable cursor and isEOF() would be true even if it had more data.
    if (PlanStage::ADVANCED == status) {
        WorkingSetMember* member = _ws.get(id);
        Status indexKeyStatus = _extractIndexKey(member);

        if (!indexKeyStatus.isOK()) {
            warning() << "Couldn't execute {}, status = {}"_format(kStageName,
                                                                   redact(indexKeyStatus));
            *out = WorkingSetCommon::allocateStatusMember(&_ws, indexKeyStatus);
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

std::unique_ptr<PlanStageStats> IndexKeyStage::getStats() {
    _commonStats.isEOF = isEOF();

    auto ret = std::make_unique<PlanStageStats>(_commonStats, stageType());
    ret->specific = std::make_unique<IndexKeyStats>(_specificStats);
    ret->children.emplace_back(child()->getStats());
    return ret;
}

Status IndexKeyStage::_extractIndexKey(WorkingSetMember* member) {
    if (!_sortKeyMetaFields.empty() && !member->metadata().hasSortKey()) {
        return Status(ErrorCodes::InternalError,
                      "sortKey meta-projection requested but no data available");
    }

    auto indexKey = member->metadata().hasIndexKey() ? member->metadata().getIndexKey() : BSONObj();
    auto sortKey = !_sortKeyMetaFields.empty() && member->metadata().hasSortKey()
        ? member->metadata().getSortKey()
        : BSONObj();
    BSONObjBuilder bob;

    if (!indexKey.isEmpty()) {
        bob.appendElements(indexKey);
    }

    for (const auto& fieldName : _sortKeyMetaFields) {
        bob.append(fieldName, sortKey);
    }

    member->keyData.clear();
    member->recordId = {};
    member->obj = Snapshotted<BSONObj>{{}, bob.obj()};
    member->transitionToOwnedObj();

    return Status::OK();
}
}  // namespace mongo
