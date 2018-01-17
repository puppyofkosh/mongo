// Tests the behavior of explain() when used with the aggregation pipeline and limits.
// @tags: [do_not_wrap_aggregations_in_facets]
(function() {
    "use strict";

    load("jstests/libs/analyze_plan.js");  // For getAggPipelineCursorStage().

    let coll = db.explain_limit;

    const kMultipleSolutionLimit = 101;
    const kCollSize = kMultipleSolutionLimit + 5;
    const kLimit = 10;

    // Return whether or explain() was successful and contained the appropriate
    // fields given the requested verbosity. Checks that the number of documents
    // examined is correct based on whether there was more than one plan available.
    function checkResults({results, verbosity, multipleSolutions}) {
        var cursorSubdocs = getAggPlanStages(results, "$cursor")
        for (let elem in cursorSubdocs) {
            let stageResult = cursorSubdocs[elem];
            assert(stageResult.hasOwnProperty("$cursor"));
            let result = stageResult.$cursor;

            assert.eq(result.limit, NumberLong(kLimit), tojson(results));

            if (verbosity === "queryPlanner") {
                assert(!result.hasOwnProperty("executionStats"), tojson(results));
            } else {
                // If it's "executionStats" or "allPlansExecution".
                if (multipleSolutions) {
                    // If there's more than one plan available, we may run several of them against
                    // each other to see which is fastest. During this, our limit may be ignored
                    // and so explain may return that it examined more documents than we asked it
                    // to.
                    assert.lte(result.executionStats.nReturned,
                               kMultipleSolutionLimit,
                               tojson(results));
                    assert.lte(result.executionStats.totalKeysExamined,
                               kMultipleSolutionLimit,
                               tojson(results));
                    assert.lte(result.executionStats.totalDocsExamined,
                               kMultipleSolutionLimit,
                               tojson(results));
                } else {
                    assert.eq(result.executionStats.nReturned, kLimit, tojson(results));
                    assert.eq(result.executionStats.totalKeysExamined, kLimit, tojson(results));
                    assert.eq(result.executionStats.totalDocsExamined, kLimit, tojson(results));
                }
            }
        }
    }

    // explain() should respect limit.
    coll.drop();
    assert.commandWorked(coll.createIndex({a: 1}));

    for (let i = 0; i < kCollSize; i++) {
        assert.writeOK(coll.insert({a: 1}));
    }

    const pipeline = [{$match: {a: 1}}, {$limit: kLimit}];

    var plannerLevel = coll.explain("queryPlanner").aggregate(pipeline);
    checkResults({results:plannerLevel, verbosity:"queryPlanner", multipleSolutions:false});

    var execLevel = coll.explain("executionStats").aggregate(pipeline);
    checkResults({results:execLevel, verbosity:"executionStats", multipleSolutions:false});

    var allPlansExecLevel = coll.explain("allPlansExecution").aggregate(pipeline);
    checkResults({results:allPlansExecLevel, verbosity:"allPlansExecution", multipleSolutions:false});

    // Create a second index so that more than one plan is available.
    assert.commandWorked(coll.createIndex({a: 1, b: 1}));

    plannerLevel= coll.explain("queryPlanner").aggregate(pipeline);
    checkResults({results:plannerLevel, verbosity:"queryPlanner", multipleSolutions:true});

    execLevel = coll.explain("executionStats").aggregate(pipeline);
    checkResults({results:execLevel, verbosity:"executionStats", multipleSolutions:true});

    allPlansExecLevel = coll.explain("allPlansExecution").aggregate(pipeline);
    checkResults({results:allPlansExecLevel, verbosity:"allPlansExecution", multipleSolutions:true});
})();
