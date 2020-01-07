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

#include "mongo/platform/basic.h"

#include "mongo/db/exec/ensure_sorted.h"

#include <memory>

#include "mongo/db/exec/scoped_timer.h"
#include "mongo/db/query/find_common.h"

namespace mongo {

using std::unique_ptr;

const char* EnsureSortedStage::kStageType = "ENSURE_SORTED";

EnsureSortedStage::EnsureSortedStage(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                     BSONObj pattern,
                                     WorkingSet* ws,
                                     std::unique_ptr<PlanStage> child)
    : PlanStage(kStageType, expCtx), _ws(ws), _sortKeyComparator(pattern) {
    _children.emplace_back(std::move(child));
}

bool EnsureSortedStage::isEOF() {
    return child()->isEOF();
}

PlanStage::StageState EnsureSortedStage::doWork(WorkingSetID* out) {
    StageState stageState = child()->work(out);

    if (PlanStage::ADVANCED == stageState) {
        // We extract the sort key from the WSM's metadata. This must have been generated by a
        // SortKeyGeneratorStage descendent in the execution tree.
        WorkingSetMember* member = _ws->get(*out);
        auto curSortKey = member->metadata().getSortKey();
        invariant(!curSortKey.missing());

        if (!_prevSortKey.missing() && !isInOrder(_prevSortKey, curSortKey)) {
            // 'member' is out of order. Drop it from the result set.
            _ws->free(*out);
            ++_specificStats.nDropped;
            return PlanStage::NEED_TIME;
        }

        _prevSortKey = curSortKey;
        return PlanStage::ADVANCED;
    }

    return stageState;
}

unique_ptr<PlanStageStats> EnsureSortedStage::getStats() {
    _commonStats.isEOF = isEOF();
    unique_ptr<PlanStageStats> ret =
        std::make_unique<PlanStageStats>(_commonStats, STAGE_ENSURE_SORTED);
    ret->specific = std::make_unique<EnsureSortedStats>(_specificStats);
    ret->children.emplace_back(child()->getStats());
    return ret;
}

const SpecificStats* EnsureSortedStage::getSpecificStats() const {
    return &_specificStats;
}

bool EnsureSortedStage::isInOrder(const Value& lhsKey, const Value& rhsKey) const {
    // No need to compare with a collator, since the sort keys were extracted by the
    // SortKeyGenerator, which has already mapped strings to their comparison keys.
    return _sortKeyComparator(lhsKey, rhsKey) <= 0;
}

}  // namespace mongo
