// Tests the behavior of explain() when used with the aggregation
// pipeline and limits.
// @tags: [do_not_wrap_aggregations_in_facets]
(function() {
    "use strict";

    load("jstests/libs/analyze_plan.js");  // For getAggPipelineCursorStage().

    let coll = db.explain_limit;

    const MULTIPLANNER_LIMIT = 101;
    const COLLSIZE = MULTIPLANNER_LIMIT + 5;
    const LIMIT = 10;

    function checkResults(results, verbosity, multiPlanner) {
        if (verbosity !== "queryPlanner") {
            assert(results.executionSuccess);
        }
        var cursorSubdocs = getAggPipelineCursorStage(results);
        for (let elem in cursorSubdocs) {
            let result = cursorSubdocs[elem];
            assert.eq(result.limit, NumberLong(LIMIT), tojson(results));

            if (verbosity === "queryPlanner") {
                assert(!result.hasOwnProperty("executionStats"), tojson(results));
            } else {
                // if it's "executionStats" or "allPlansExecution".
                if (multiPlanner) {
                    assert.lte(result.executionStats.nReturned,
                               MULTIPLANNER_LIMIT,
                               tojson(results));
                    assert.lte(result.executionStats.totalKeysExamined,
                               MULTIPLANNER_LIMIT,
                               tojson(results));
                    assert.lte(result.executionStats.totalDocsExamined,
                               MULTIPLANNER_LIMIT,
                               tojson(results));
                } else {
                    assert.eq(result.executionStats.nReturned, LIMIT, tojson(results));
                    assert.eq(result.executionStats.totalKeysExamined, LIMIT, tojson(results));
                    assert.eq(result.executionStats.totalDocsExamined, LIMIT, tojson(results));
                }
            }
        }
    }

    // explain() should respect limit.
    coll.drop();
    assert.commandWorked(coll.createIndex({a: 1}));

    for (let i = 0; i < COLLSIZE; i++) {
        assert.writeOK(coll.insert({a: 1}));
    }

    const pipeline = [{$match: {a: 1}}, {$limit: LIMIT}];

    const res1 = coll.explain("queryPlanner").aggregate(pipeline);
    checkResults(res1, "queryPlanner", false);

    const res2 = coll.explain("executionStats").aggregate(pipeline);
    checkResults(res2, "executionStats", false);

    const res3 = coll.explain("allPlansExecution").aggregate(pipeline);
    checkResults(res3, "allPlansExecution", false);

    // Create a second index so that we're forced to use the MultiPlanner.
    assert.commandWorked(coll.createIndex({a: 1, b: 1}));

    const res4 = coll.explain("queryPlanner").aggregate(pipeline);
    checkResults(res4, "queryPlanner", true);

    const res5 = coll.explain("executionStats").aggregate(pipeline);
    checkResults(res5, "executionStats", true);

    const res6 = coll.explain("allPlansExecution").aggregate(pipeline);
    checkResults(res6, "allPlansExecution", true);
})();
