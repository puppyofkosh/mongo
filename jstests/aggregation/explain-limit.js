// Tests the behavior of explain() when used with the aggregation
// pipeline and limits.
(function() {
    "use strict";

    load('jstests/libs/analyze_plan.js');  // For getAggPipelineCursorStage().

    let coll = db.explain;

    const COLLSIZE = 10;
    const LIMIT = 3;

    function checkResults(results, verbosity) {
        var cursorSubdocs = getAggPipelineCursorStage(results);
        for (let elem in cursorSubdocs) {
            let result = cursorSubdocs[elem];
            assert.eq(result.limit, NumberLong(LIMIT), tojson(results));

            if (verbosity === "queryPlanner") {
                assert(!result.hasOwnProperty("executionStats"), tojson(results));
            } else {
                // if its "executionStats" or "allPlansExecution"
                assert.eq(result.executionStats.nReturned, LIMIT, tojson(results));
                assert.lt(result.executionStats.totalKeysExamined, COLLSIZE, tojson(results));
                assert.lt(result.executionStats.totalDocsExamined, COLLSIZE, tojson(results));
            }
        }
    }

    // Explain() should respect limit.
    coll.drop();
    assert.commandWorked(coll.createIndex({a: 1}));

    for (let i = 0; i < COLLSIZE; i++) {
        assert.writeOK(coll.insert({a: 1}));
    }

    const res1 = db.explain.explain("queryPlanner").aggregate([{$match: {a: 1}}, {$limit: LIMIT}]);
    checkResults(res1, "queryPlanner");

    const res2 =
        db.explain.explain("executionStats").aggregate([{$match: {a: 1}}, {$limit: LIMIT}]);
    checkResults(res2, "executionStats");

    const res3 =
        db.explain.explain("allPlansExecution").aggregate([{$match: {a: 1}}, {$limit: LIMIT}]);
    checkResults(res3, "allPlansExecution");
})();
