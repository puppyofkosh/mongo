/**
 * Test that cached plans which use allPaths indexes work.
 * TODO: Flesh out!
 */
(function() {
    assert.commandWorked(
        db.adminCommand({setParameter: 1, internalQueryAllowAllPathsIndexes: true}));

    const coll = db.c;
    coll.drop();
    assert.commandWorked(coll.createIndex({"b.$**": 1}));
    assert.commandWorked(coll.createIndex({"a": 1}));
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

    let res = coll.find(q).toArray();
    printjson(res);
    assert.eq(res.length, 1, () => tojson(res));

    res = coll.find(q).toArray();
    assert.eq(res.length, 1, () => tojson(res));

    // The plan cache should be empty.
    assert.gt(getPlansForCacheEntry(q).length, 0);

    res = coll.find(q).toArray();
    assert.eq(res.length, 1, () => tojson(res));

    // Now run a query where a is null. This should have a different shape key from the previous
    // query.
    res = coll.find({a: 1, b: null}).toArray();
    assert.eq(res.length, 1000, () => tojson(res));
})();
