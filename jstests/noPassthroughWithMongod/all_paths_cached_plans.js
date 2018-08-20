/**
 * Test that cached plans which use allPaths indexes work.
 */
(function() {
    "use strict";

    load('jstests/libs/analyze_plan.js');  // For getPlanStage().

    assert.commandWorked(
        db.adminCommand({setParameter: 1, internalQueryAllowAllPathsIndexes: true}));

    const coll = db.all_paths_cached_plans;
    coll.drop();
    assert.commandWorked(coll.createIndex({"b.$**": 1}));
    assert.commandWorked(coll.createIndex({"a": 1}));

    // In order for the plan cache to be used, there must be more than one plan available. Insert
    // data into the collection such that the b.$** index will be far more selective than the index
    // on 'a' for the query {a: 1, b: 1}.
    for (let i = 0; i < 1000; i++) {
        assert.commandWorked(coll.insert({a: 1}));
    }
    assert.commandWorked(coll.insert({a: 1, b: 1}));

    function getCacheEntryForQuery(query) {
        const key = {query: query, sort: {}, projection: {}};
        const res = coll.runCommand('planCacheListPlans', key);
        assert.commandWorked(res);
        assert(res.hasOwnProperty('plans'));
        return res;
    }

    function getQueryHash(query) {
        const hash = coll.explain().find(query).finish().queryPlanner.queryHash;
        assert.eq(typeof(hash), "string");
        return hash;
    }

    const query = {a: 1, b: 1};

    // The plan cache should be empty.
    assert.eq(getCacheEntryForQuery(query).plans.length, 0);

    // Run the query twice, once to create the cache entry, and again to make the cache entry
    // active.
    for (let i = 0; i < 2; i++) {
        assert.eq(coll.find(query).itcount(), 1);
    }

    // The plan cache should no longer be empty. Check that the chosen plan uses the b.$** index.
    const cacheEntry = getCacheEntryForQuery(query);
    assert.eq(cacheEntry.isActive, true);
    // Should be at least two plans: one using the {a: 1} index and the other using the b.$** index.
    assert.gte(cacheEntry.plans.length, 2, tojson(cacheEntry.plans));
    const plan = cacheEntry.plans[0].reason.stats;
    const ixScanStage = getPlanStage(plan, "IXSCAN");
    assert.neq(ixScanStage, null, () => tojson(plan));
    assert.eq(ixScanStage.keyPattern, {"_path": 1, "b": 1}, () => tojson(plan));

    // Run the query again. This time it should use the cached plan. We should get the same result
    // as earlier.
    assert.eq(coll.find(query).itcount(), 1);

    // Now run a query where b is null. This should have a different shape key from the previous
    // query since $** indexes are sparse.
    const queryWithBNull = {a: 1, b: null};
    for (let i = 0; i < 2; i++) {
        assert.eq(coll.find({a: 1, b: null}).itcount(), 1000);
    }
    assert.neq(getQueryHash(queryWithBNull), getQueryHash(query));

    // There should only have been one solution for the above query, so it would not get cached.
    assert.eq(getCacheEntryForQuery({a: 1, b: null}).plans.length, 0);

    // Check that indexability discriminators work with collations.
    (function() {
        // Create allPaths index with a collation.
        assert.eq(coll.drop(), true);
        assert.commandWorked(
            db.createCollection(coll.getName(), {collation: {locale: "en_US", strength: 1}}));
        assert.commandWorked(coll.createIndex({"b.$**": 1}));

        // Run a query which uses a different collation, but does not use string bounds.
        const queryWithoutStringExplain =
            coll.explain().find({a: 5, b: 5}).collation({locale: "fr"}).finish();
        let ixScans = getPlanStages(queryWithoutStringExplain.queryPlanner.winningPlan, "IXSCAN");
        assert.eq(ixScans.length, 1);
        assert.eq(ixScans[0].keyPattern, {_path: 1, b: 1});

        // Run a query which uses a different collation and does have string bounds.
        const queryWithStringExplain =
            coll.explain().find({a: 5, b: "a string"}).collation({locale: "fr"}).finish();
        ixScans = getPlanStages(queryWithStringExplain.queryPlanner.winningPlan, "IXSCAN");
        assert.eq(ixScans.length, 0);

        // Check that the shapes are different since the query which matches on a string will not
        // be eligible to use the b.$** index (since the index has a different collation).
        assert.neq(queryWithoutStringExplain.queryPlanner.queryHash,
                   queryWithStringExplain.queryPlanner.queryHash);
    })();

    // TODO SERVER-35336: Update this test to use a partial $** index, and be sure indexability
    // discriminators also work for partial indices.
})();
