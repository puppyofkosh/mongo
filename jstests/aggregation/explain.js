// Tests the behavior of explain() when used with the aggregation
// pipeline.
(function() {
    "use strict";

    load('jstests/libs/analyze_plan.js');  // For getAggPlanStage().

    let coll = db.explain;

    // Explain() should not read or modify the plan cache.
    coll.drop();

    assert.commandWorked(coll.createIndex({x: 1}));
    assert.commandWorked(coll.createIndex({y: 1}));

    let result = coll.explain().aggregate([{$match: {x: 1, y: 1}}]);
    assert.eq(null, getAggPlanStage(result, "CACHED_PLAN"));

    // At this point, there should be no entries in the plan cache.
    result = coll.explain().aggregate([{$match: {x: 1, y: 1}}]);
    assert.eq(null, getAggPlanStage(result, "CACHED_PLAN"));

    // Now add entry in the cache without explain().
    result = coll.aggregate([{$match: {x: 1, y: 1}}]);

    // Now there's an entry in the cache, make sure explain() doesn't use it.
    result = coll.explain().aggregate([{$match: {x: 1, y: 1}}]);
    assert.eq(null, getAggPlanStage(result, "CACHED_PLAN"));

    // Explain() should respect limit.
    coll.drop();
    assert.commandWorked(coll.createIndex({a: 1}));

    for (let i = 0; i < 10; i++) {
        assert.writeOK(coll.insert({a: 1}));
    }

    const res1 = db.explain.explain("queryPlanner").aggregate([{$match: {a: 1}}, {$limit: 3}]);
    getResults(res1, "queryPlanner");

    const res2 = db.explain.explain("executionStats").aggregate([{$match: {a: 1}}, {$limit: 3}]);
    getResults(res2, "executionStats");

    const res3 = db.explain.explain("allPlansExecution").aggregate([{$match: {a: 1}}, {$limit: 3}]);
    getResults(res3, "allPlansExecution");

})();

function getResults(results, verbosity) {
    for (let elem in getAggPipelineCursorStage(results)) {
        let result = getAggPipelineCursorStage(results)[elem];
        assert.eq(result.limit, NumberLong(3), tojson(results));

        if (verbosity === "queryPlanner") {
            assert(!result.hasOwnProperty("executionStats"), tojson(results));
        } else {
            // if its "executionStats" or "allPlansExecution"
            assert.eq(result.executionStats.nReturned, 3, tojson(results));
            assert.lt(result.executionStats.totalKeysExamined, 10, tojson(results));
            assert.lt(result.executionStats.totalDocsExamined, 10, tojson(results));
        }
    }
}
