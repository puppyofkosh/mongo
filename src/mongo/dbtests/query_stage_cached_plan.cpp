/**
 *    Copyright (C) 2015 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/client.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/exec/cached_plan.h"
#include "mongo/db/exec/queued_data_stage.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/query/canonical_query.h"
#include "mongo/db/query/get_executor.h"
#include "mongo/db/query/plan_cache.h"
#include "mongo/db/query/plan_yield_policy.h"
#include "mongo/db/query/query_knobs.h"
#include "mongo/db/query/query_planner_params.h"
#include "mongo/dbtests/dbtests.h"
#include "mongo/stdx/memory.h"

namespace QueryStageCachedPlan {

static const NamespaceString nss("unittests.QueryStageCachedPlan");

class QueryStageCachedPlanBase {
public:
    QueryStageCachedPlanBase() {
        // If collection exists already, we need to drop it.
        dropCollection();

        // Add indices.
        addIndex(BSON("a" << 1));
        addIndex(BSON("b" << 1));

        OldClientWriteContext ctx(&_opCtx, nss.ns());
        Collection* collection = ctx.getCollection();
        ASSERT(collection);

        // Add data.
        for (int i = 0; i < 10; i++) {
            insertDocument(collection, BSON("_id" << i << "a" << i << "b" << 1));
        }
    }

    void addIndex(const BSONObj& obj) {
        ASSERT_OK(dbtests::createIndex(&_opCtx, nss.ns(), obj));
    }

    void dropCollection() {
        Lock::DBLock dbLock(&_opCtx, nss.db(), MODE_X);
        Database* database = DatabaseHolder::getDatabaseHolder().get(&_opCtx, nss.db());
        if (!database) {
            return;
        }

        WriteUnitOfWork wuow(&_opCtx);
        database->dropCollection(&_opCtx, nss.ns()).transitional_ignore();
        wuow.commit();
    }

    void insertDocument(Collection* collection, BSONObj obj) {
        WriteUnitOfWork wuow(&_opCtx);

        const bool enforceQuota = false;
        OpDebug* const nullOpDebug = nullptr;
        ASSERT_OK(
            collection->insertDocument(&_opCtx, InsertStatement(obj), nullOpDebug, enforceQuota));
        wuow.commit();
    }

    OperationContext* opCtx() {
        return &_opCtx;
    }

    static size_t getNumResultsForStage(const WorkingSet& ws,
                                        CachedPlanStage* cachedPlanStage,
                                        CanonicalQuery* cq) {
        size_t numResults = 0;
        PlanStage::StageState state = PlanStage::NEED_TIME;
        while (state != PlanStage::IS_EOF) {
            WorkingSetID id = WorkingSet::INVALID_ID;
            state = cachedPlanStage->work(&id);

            ASSERT_NE(state, PlanStage::FAILURE);
            ASSERT_NE(state, PlanStage::DEAD);

            if (state == PlanStage::ADVANCED) {
                WorkingSetMember* member = ws.get(id);
                ASSERT(cq->root()->matchesBSON(member->obj.value()));
                numResults++;
            }
        }

        return numResults;
    }

    void forceReplanning(Collection* collection, CanonicalQuery* cq) {
        // Get planner params.
        QueryPlannerParams plannerParams;
        fillOutPlannerParams(&_opCtx, collection, cq, &plannerParams);

        const size_t decisionWorks = 10;
        const size_t mockWorks =
            1U + static_cast<size_t>(internalQueryCacheEvictionRatio * decisionWorks);
        auto mockChild = stdx::make_unique<QueuedDataStage>(&_opCtx, &_ws);
        for (size_t i = 0; i < mockWorks; i++) {
            mockChild->pushBack(PlanStage::NEED_TIME);
        }

        CachedPlanStage cachedPlanStage(
            &_opCtx, collection, &_ws, cq, plannerParams, decisionWorks, mockChild.release());

        // This should succeed after triggering a replan.
        PlanYieldPolicy yieldPolicy(PlanExecutor::NO_YIELD,
                                    _opCtx.getServiceContext()->getFastClockSource());
        ASSERT_OK(cachedPlanStage.pickBestPlan(&yieldPolicy));
    }

protected:
    const ServiceContext::UniqueOperationContext _opCtxPtr = cc().makeOperationContext();
    OperationContext& _opCtx = *_opCtxPtr;
    WorkingSet _ws;
};

/**
 * Test that on failure, the cached plan stage replans the query but does not create a new cache
 * entry.
 */
class QueryStageCachedPlanFailure : public QueryStageCachedPlanBase {
public:
    void run() {
        AutoGetCollectionForReadCommand ctx(&_opCtx, nss);
        Collection* collection = ctx.getCollection();
        ASSERT(collection);

        // Query can be answered by either index on "a" or index on "b".
        auto qr = stdx::make_unique<QueryRequest>(nss);
        qr->setFilter(fromjson("{a: {$gte: 8}, b: 1}"));
        auto statusWithCQ = CanonicalQuery::canonicalize(opCtx(), std::move(qr));
        ASSERT_OK(statusWithCQ.getStatus());
        const std::unique_ptr<CanonicalQuery> cq = std::move(statusWithCQ.getValue());

        // We shouldn't have anything in the plan cache for this shape yet.
        PlanCache* cache = collection->infoCache()->getPlanCache();
        ASSERT(cache);
        CachedSolution* rawCachedSolution;
        ASSERT_NOT_OK(cache->get(*cq, &rawCachedSolution));

        // Get planner params.
        QueryPlannerParams plannerParams;
        fillOutPlannerParams(&_opCtx, collection, cq.get(), &plannerParams);

        // Queued data stage will return a failure during the cached plan trial period.
        auto mockChild = stdx::make_unique<QueuedDataStage>(&_opCtx, &_ws);
        mockChild->pushBack(PlanStage::FAILURE);

        // High enough so that we shouldn't trigger a replan based on works.
        const size_t decisionWorks = 50;
        CachedPlanStage cachedPlanStage(
            &_opCtx, collection, &_ws, cq.get(), plannerParams, decisionWorks, mockChild.release());

        // This should succeed after triggering a replan.
        PlanYieldPolicy yieldPolicy(PlanExecutor::NO_YIELD,
                                    _opCtx.getServiceContext()->getFastClockSource());
        ASSERT_OK(cachedPlanStage.pickBestPlan(&yieldPolicy));

        ASSERT_EQ(getNumResultsForStage(_ws, &cachedPlanStage, cq.get()), 2U);

        // Plan cache should still be empty, as we don't write to it when we replan a failed
        // query.
        ASSERT_NOT_OK(cache->get(*cq, &rawCachedSolution));
    }
};

/**
 * Test that hitting the cached plan stage trial period's threshold for work cycles causes the
 * query to be replanned. Also verify that the replanning results in a new plan cache entry.
 */
class QueryStageCachedPlanHitMaxWorks : public QueryStageCachedPlanBase {
public:
    void run() {
        AutoGetCollectionForReadCommand ctx(&_opCtx, nss);
        Collection* collection = ctx.getCollection();
        ASSERT(collection);

        // Query can be answered by either index on "a" or index on "b".
        auto qr = stdx::make_unique<QueryRequest>(nss);
        qr->setFilter(fromjson("{a: {$gte: 8}, b: 1}"));
        auto statusWithCQ = CanonicalQuery::canonicalize(opCtx(), std::move(qr));
        ASSERT_OK(statusWithCQ.getStatus());
        const std::unique_ptr<CanonicalQuery> cq = std::move(statusWithCQ.getValue());

        // We shouldn't have anything in the plan cache for this shape yet.
        PlanCache* cache = collection->infoCache()->getPlanCache();
        ASSERT(cache);
        CachedSolution* rawCachedSolution;
        ASSERT_NOT_OK(cache->get(*cq, &rawCachedSolution));

        // Get planner params.
        QueryPlannerParams plannerParams;
        fillOutPlannerParams(&_opCtx, collection, cq.get(), &plannerParams);

        // Set up queued data stage to take a long time before returning EOF. Should be long
        // enough to trigger a replan.
        const size_t decisionWorks = 10;
        const size_t mockWorks =
            1U + static_cast<size_t>(internalQueryCacheEvictionRatio * decisionWorks);
        auto mockChild = stdx::make_unique<QueuedDataStage>(&_opCtx, &_ws);
        for (size_t i = 0; i < mockWorks; i++) {
            mockChild->pushBack(PlanStage::NEED_TIME);
        }

        CachedPlanStage cachedPlanStage(
            &_opCtx, collection, &_ws, cq.get(), plannerParams, decisionWorks, mockChild.release());

        // This should succeed after triggering a replan.
        PlanYieldPolicy yieldPolicy(PlanExecutor::NO_YIELD,
                                    _opCtx.getServiceContext()->getFastClockSource());
        ASSERT_OK(cachedPlanStage.pickBestPlan(&yieldPolicy));

        ASSERT_EQ(getNumResultsForStage(_ws, &cachedPlanStage, cq.get()), 2U);

        // This time we expect to find something in the plan cache. Replans after hitting the
        // works threshold result in a cache entry.
        ASSERT_OK(cache->get(*cq, &rawCachedSolution));
        const std::unique_ptr<CachedSolution> cachedSolution(rawCachedSolution);
    }
};

/**
 * Test the way cache entries are added (either "active" or "inactive") to the plan cache.
 */
class QueryStageCachedPlanAddsActiveCacheEntries : public QueryStageCachedPlanBase {
public:
    void run() {
        AutoGetCollectionForReadCommand ctx(&_opCtx, nss);
        Collection* collection = ctx.getCollection();
        ASSERT(collection);

        // Query can be answered by either index on "a" or index on "b".
        auto qr = stdx::make_unique<QueryRequest>(nss);
        qr->setFilter(fromjson("{a: {$gte: 11}, b: {$gte: 11}}"));
        auto statusWithCQ = CanonicalQuery::canonicalize(opCtx(), std::move(qr));
        ASSERT_OK(statusWithCQ.getStatus());
        const std::unique_ptr<CanonicalQuery> cq = std::move(statusWithCQ.getValue());

        // We shouldn't have anything in the plan cache for this shape yet.
        PlanCache* cache = collection->infoCache()->getPlanCache();
        ASSERT(cache);
        CachedSolution* rawCachedSolution;
        ASSERT_NOT_OK(cache->get(*cq, &rawCachedSolution));

        // Step 1: Run the CachedPlanStage with a long-running child plan. Replanning should be
        // triggered and a tombstone will be added.
        forceReplanning(collection, cq.get());

        // Check for an inactive cache entry.
        ASSERT_EQ(cache->getEntryStatus(*cq.get()), PlanCache::CacheEntryStatus::kPresentInactive);

        // The worksThreshold should be 1 for the entry, since the query we ran should not have any
        // results.
        auto entry = assertGet(cache->getEntry(*cq.get()));
        size_t worksThreshold = 1U;
        const size_t kExpectedNumWorks = 10;
        ASSERT_EQ(entry->worksThreshold, worksThreshold);

        for (int i = 0; i < std::ceil(std::log(kExpectedNumWorks) / std::log(2)); ++i) {
            worksThreshold *= 2;
            // Step 2: Run another query of the same shape, which is less selective, and therefore
            // takes longer).
            auto qr2 = stdx::make_unique<QueryRequest>(nss);
            // Each of the documents {a:1} through {a:9} will match the filter. Therefore this
            // query will require 9 works for each document, and one for the EOF, adding up to 10.
            qr2->setFilter(fromjson("{a: {$gte: 1}, b: {$gte: 0}}"));
            auto cq2 = assertGet(CanonicalQuery::canonicalize(opCtx(), std::move(qr2)));
            forceReplanning(collection, cq2.get());

            ASSERT_EQ(cache->getEntryStatus(*cq2.get()),
                      PlanCache::CacheEntryStatus::kPresentInactive);
            // The worksThreshold on the cache entry should have doubled.
            entry = assertGet(cache->getEntry(*cq2.get()));
            ASSERT_EQ(entry->worksThreshold, worksThreshold);
        }

        // Step 3: Run another query which takes less time, and be sure an active entry is created.

        // This time we expect to find something in the plan cache. Replans after hitting the
        // works threshold result in a cache entry.
        ASSERT_OK(cache->get(*cq, &rawCachedSolution));
        const std::unique_ptr<CachedSolution> cachedSolution(rawCachedSolution);
    }
};

class All : public Suite {
public:
    All() : Suite("query_stage_cached_plan") {}

    void setupTests() {
        add<QueryStageCachedPlanFailure>();
        add<QueryStageCachedPlanHitMaxWorks>();
        add<QueryStageCachedPlanAddsActiveCacheEntries>();
    }
};

SuiteInstance<All> all;

}  // namespace QueryStageCachedPlan
