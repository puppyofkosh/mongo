/**
 * Test that cached plans which use allPaths indexes work.
 */
(function() {
    "use strict";

    load('jstests/libs/analyze_plan.js');  // For getPlanStage().

    assert.commandWorked(
        db.adminCommand({setParameter: 1, internalQueryAllowAllPathsIndexes: true}));

    const coll = db.c;
    coll.drop();
    assert.commandWorked(coll.createIndex({"b.$**": 1}));
    assert.commandWorked(coll.createIndex({"a": 1}));

    // In order for the plan cache to be used, there must be more than one plan available.  Insert
    // data into the collection such that the b.$** will be far more selective than the index on
    // 'a'.
    for (let i = 0; i < 1000; i++) {
        assert.commandWorked(coll.insert({a: 1}));
    }
    assert.commandWorked(coll.insert({a: 1, b: 1}));
    assert.commandWorked(coll.insert({a: 2, b: 1}));

    function getPlansForCacheEntry(query) {
        const key = {query: query, sort: {}, projection: {}};
        const res = coll.runCommand('planCacheListPlans', key);
        assert.commandWorked(res);
        assert(res.hasOwnProperty('plans'));
        return res.plans;
    }

    const q = {a: 1, b: 1};
    // The plan cache should be empty.
    assert.eq(getPlansForCacheEntry(q).length, 0);

    // Run the query twice, so the plan gets cached.
    for (let i = 0; i < 2; i++) {
        assert.eq(coll.find(q).itcount(), 1);
    }

    // The plan cache should no longer be empty. Check that the chosen plan uses the b.$** index.
    const plan = getPlansForCacheEntry(q)[0].reason.stats;
    const ixScanStage = getPlanStage(plan, "IXSCAN");
    assert.neq(ixScanStage, null, plan);
    assert.eq(ixScanStage.keyPattern.b, 1, plan);

    // Run the query again. This time it should use the cached plan. We should get the same result
    // as earlier.
    assert.eq(coll.find(q).itcount(), 1);

    // Now run a query where b is null. This should have a different shape key from the previous
    // query since the index is sparse.
    for (let i = 0; i < 2; i++) {
        assert.eq(coll.find({a: 1, b: null}).itcount(), 1000);
    }

    // There should only have been one solution for the above query, so it would not get cached.
    assert.eq(getPlansForCacheEntry({a: 1, b: null}).length, 0);

    // Check that indexability discriminators work with collations.
    (function() {
        // create allPaths index with a collation.
        assert.eq(coll.drop(), true);
        assert.commandWorked(
            db.createCollection(coll.getName(), {collation: {locale: "en_US", strength: 1}}));
        assert.commandWorked(coll.createIndex({"$**": 1}));

        // Get the shape-hash for a query which uses a different collation, but does not use string
        // bounds.
        const hash1 = coll.explain()
                          .find({a: 5, b: 5})
                          .collation({locale: "fr"})
                          .finish()
                          .queryPlanner.queryHash;
        // Get the shape-hash for a query which uses a different collation, and does have string
        // bounds. This query should have a different shape since it will not be eligible to use
        // the b.$** index (since the index has a different collation).
        const hash2 = coll.explain()
                          .find({a: 5, b: "a string"})
                          .collation({locale: "fr"})
                          .finish()
                          .queryPlanner.queryHash;

        assert.neq(hash1, hash2);
    })();

    // TODO SERVER-35336: Update this test to use a partial $** index .
})();
