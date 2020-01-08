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

/**
 * This file tests db/exec/limit.cpp and db/exec/skip.cpp.
 */


#include "mongo/platform/basic.h"

#include <memory>

#include "mongo/client/dbclient_cursor.h"
#include "mongo/db/client.h"
#include "mongo/db/exec/limit.h"
#include "mongo/db/exec/plan_stage.h"
#include "mongo/db/exec/queued_data_stage.h"
#include "mongo/db/exec/skip.h"
#include "mongo/db/json.h"
#include "mongo/dbtests/dbtests.h"

using namespace mongo;

namespace {

using std::max;
using std::min;
using std::unique_ptr;

static const int N = 50;

/* Populate a QueuedDataStage and return it.  Caller owns it. */
std::unique_ptr<QueuedDataStage> getMS(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                       WorkingSet* ws) {
    auto ms = std::make_unique<QueuedDataStage>(expCtx, ws);

    // Put N ADVANCED results into the mock stage, and some other stalling results (YIELD/TIME).
    for (int i = 0; i < N; ++i) {
        ms->pushBack(PlanStage::NEED_TIME);

        WorkingSetID id = ws->allocate();
        WorkingSetMember* wsm = ws->get(id);
        wsm->doc = {SnapshotId(), Document{BSON("x" << i)}};
        wsm->transitionToOwnedObj();
        ms->pushBack(id);

        ms->pushBack(PlanStage::NEED_TIME);
    }

    return ms;
}

int countResults(PlanStage* stage) {
    int count = 0;
    while (!stage->isEOF()) {
        WorkingSetID id = WorkingSet::INVALID_ID;
        PlanStage::StageState status = stage->work(&id);
        if (PlanStage::ADVANCED != status) {
            continue;
        }
        ++count;
    }
    return count;
}

//
// Insert 50 objects.  Filter/skip 0, 1, 2, ..., 100 objects and expect the right # of results.
//
class QueryStageLimitSkipBasicTest {
public:
    void run() {
        const CollatorInterface* collator = nullptr;
        const boost::intrusive_ptr<ExpressionContext> expCtx(
            new ExpressionContext(_opCtx, collator));

        for (int i = 0; i < 2 * N; ++i) {
            WorkingSet ws;

            unique_ptr<PlanStage> skip =
                std::make_unique<SkipStage>(expCtx, i, &ws, getMS(expCtx, &ws));
            ASSERT_EQUALS(max(0, N - i), countResults(skip.get()));

            unique_ptr<PlanStage> limit =
                std::make_unique<LimitStage>(expCtx, i, &ws, getMS(expCtx, &ws));
            ASSERT_EQUALS(min(N, i), countResults(limit.get()));
        }
    }

protected:
    const ServiceContext::UniqueOperationContext _uniqOpCtx = cc().makeOperationContext();
    OperationContext* const _opCtx = _uniqOpCtx.get();
};

class All : public OldStyleSuiteSpecification {
public:
    All() : OldStyleSuiteSpecification("query_stage_limit_skip") {}

    void setupTests() {
        add<QueryStageLimitSkipBasicTest>();
    }
};

OldStyleSuiteInitializer<All> queryStageLimitSkipAll;

}  // namespace
