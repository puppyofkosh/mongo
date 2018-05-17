// Tests for the mongos explain command.
(function() {
    'use strict';

    // Create a cluster with 3 shards.
    var st = new ShardingTest({shards: 2});

    var db = st.s.getDB("test");
    var explain;

    // Setup a collection that will be sharded. The shard key will be 'a'. There's also an index on
    // 'b'.
    var collSharded = db.getCollection("mongos_explain_cmd");
    collSharded.drop();
    collSharded.ensureIndex({a: 1});
    collSharded.ensureIndex({b: 1});

    // Enable sharding.
    assert.commandWorked(db.adminCommand({enableSharding: db.getName()}));
    st.ensurePrimaryShard(db.getName(), st.shard1.shardName);
    db.adminCommand({shardCollection: collSharded.getFullName(), key: {a: 1}});

    // Pre-split the collection to ensure that both shards have chunks. Explicitly
    // move chunks since the balancer is disabled.
    assert.commandWorked(db.adminCommand({split: collSharded.getFullName(), middle: {a: 1}}));
    assert.commandWorked(db.adminCommand(
        {moveChunk: collSharded.getFullName(), find: {a: 1}, to: st.shard0.shardName}));

    assert.commandWorked(db.adminCommand({split: collSharded.getFullName(), middle: {a: 2}}));
    assert.commandWorked(db.adminCommand(
        {moveChunk: collSharded.getFullName(), find: {a: 2}, to: st.shard1.shardName}));

    // Put data on each shard.
    for (var i = -5; i < 5; i++) {
        collSharded.insert({_id: i, a: i, b: 1});
    }

    st.printShardingStatus();

    // Test a scatter-gather count command.
    assert.eq(10, collSharded.count({b: 1}));

    // Explain the scatter-gather count.
    explain = db.runCommand(
        {explain: {count: collSharded.getName(), query: {b: 1}}, verbosity: "allPlansExecution"});

    // Validate some basic properties of the result.
    assert.commandWorked(explain, tojson(explain));
    assert("queryPlanner" in explain);
    assert("executionStats" in explain);
    assert.eq(2, explain.queryPlanner.winningPlan.shards.length);
    assert.eq(2, explain.executionStats.executionStages.shards.length);

    // An explain of a command that doesn't exist should fail gracefully.
    explain = db.runCommand({
        explain: {nonexistent: collSharded.getName(), query: {b: 1}},
        verbosity: "allPlansExecution"
    });
    assert.commandFailed(explain, tojson(explain));

    // -------

    // Setup a collection that is not sharded.
    var collUnsharded = db.getCollection("mongos_explain_cmd_unsharded");
    collUnsharded.drop();
    collUnsharded.ensureIndex({a: 1});
    collUnsharded.ensureIndex({b: 1});

    for (var i = 0; i < 3; i++) {
        collUnsharded.insert({_id: i, a: i, b: 1});
    }
    assert.eq(3, collUnsharded.count({b: 1}));

    explain = db.runCommand({
        explain: {
            group: {
                ns: collUnsharded.getName(),
                key: "a",
                cond: "b",
                $reduce: function(curr, result) {},
                initial: {}
            }
        },
        verbosity: "allPlansExecution"
    });

    // Basic validation: a group command can only be passed through to an unsharded collection,
    // so we should confirm that the mongos stage is always SINGLE_SHARD.
    assert.commandWorked(explain, tojson(explain));
    assert("queryPlanner" in explain);
    assert("executionStats" in explain);
    assert.eq("SINGLE_SHARD", explain.queryPlanner.winningPlan.stage);

    // The same group should fail over the sharded collection, because group is only supported
    // if it is passed through to an unsharded collection.
    explain = db.runCommand({
        explain: {
            group: {
                ns: collSharded.getName(),
                key: "a",
                cond: "b",
                $reduce: function(curr, result) {},
                initial: {}
            }
        },
        verbosity: "allPlansExecution"
    });
    assert.commandFailed(explain, tojson(explain));

    // -------

    // Explain a delete operation and verify that it hits all shards without the shard key
    explain = db.runCommand({
        explain: {delete: collSharded.getName(), deletes: [{q: {b: 1}, limit: 0}]},
        verbosity: "allPlansExecution"
    });
    assert.commandWorked(explain, tojson(explain));
    assert.eq(explain.queryPlanner.winningPlan.stage, "SHARD_WRITE");
    assert.eq(explain.queryPlanner.winningPlan.shards.length, 2);
    assert.eq(explain.queryPlanner.winningPlan.shards[0].winningPlan.stage, "DELETE");
    assert.eq(explain.queryPlanner.winningPlan.shards[1].winningPlan.stage, "DELETE");
    // Check that the deletes didn't actually happen.
    assert.eq(10, collSharded.count({b: 1}));

    // Explain a delete operation and verify that it hits only one shard with the shard key
    explain = db.runCommand({
        explain: {delete: collSharded.getName(), deletes: [{q: {a: 1}, limit: 0}]},
        verbosity: "allPlansExecution"
    });
    assert.commandWorked(explain, tojson(explain));
    assert.eq(explain.queryPlanner.winningPlan.shards.length, 1);
    // Check that the deletes didn't actually happen.
    assert.eq(10, collSharded.count({b: 1}));

    // Check that we fail gracefully if we try to do an explain of a write batch that has more
    // than one operation in it.
    explain = db.runCommand({
        explain: {
            delete: collSharded.getName(),
            deletes: [{q: {a: 1}, limit: 1}, {q: {a: 2}, limit: 1}]
        },
        verbosity: "allPlansExecution"
    });
    assert.commandFailed(explain, tojson(explain));

    // Explain a multi upsert operation and verify that it hits all shards
    explain = db.runCommand({
        explain:
            {update: collSharded.getName(), updates: [{q: {}, u: {$set: {b: 10}}, multi: true}]},
        verbosity: "allPlansExecution"
    });
    assert.commandWorked(explain, tojson(explain));
    assert.eq(explain.queryPlanner.winningPlan.shards.length, 2);
    assert.eq(explain.queryPlanner.winningPlan.stage, "SHARD_WRITE");
    assert.eq(explain.queryPlanner.winningPlan.shards.length, 2);
    assert.eq(explain.queryPlanner.winningPlan.shards[0].winningPlan.stage, "UPDATE");
    assert.eq(explain.queryPlanner.winningPlan.shards[1].winningPlan.stage, "UPDATE");
    // Check that the update didn't actually happen.
    assert.eq(0, collSharded.count({b: 10}));

    // Explain an upsert operation and verify that it hits only a single shard
    explain = db.runCommand({
        explain: {update: collSharded.getName(), updates: [{q: {a: 10}, u: {a: 10}, upsert: true}]},
        verbosity: "allPlansExecution"
    });
    assert.commandWorked(explain, tojson(explain));
    assert.eq(explain.queryPlanner.winningPlan.shards.length, 1);
    // Check that the upsert didn't actually happen.
    assert.eq(0, collSharded.count({a: 10}));

    // Explain an upsert operation which cannot be targeted, ensure an error is thrown
    explain = db.runCommand({
        explain: {update: collSharded.getName(), updates: [{q: {b: 10}, u: {b: 10}, upsert: true}]},
        verbosity: "allPlansExecution"
    });
    assert.commandFailed(explain, tojson(explain));

    //
    // Explain a find with a sort on b.
    //
    explain = collSharded.explain().find().sort({b: 1}).finish();
    // The plan should have a sort on the mongos.
    assert.eq(explain.queryPlanner.winningPlan.stage, "SHARD_MERGE_SORT", tojson(explain));

    // Each shard should have a project for the sort key.
    for (let shardExplain of explain.queryPlanner.winningPlan.shards) {
        assert.eq(shardExplain.winningPlan.stage, "PROJECTION", tojson(explain));
        assert.eq(shardExplain.winningPlan.transformBy.$sortKey.$meta, "sortKey", tojson(explain));
    }

    //
    // Explain a find with a skip.
    //
    explain = collSharded.explain("executionStats").find().skip(3).finish();
    assert.eq(explain.executionStats.nReturned, 10 - 3, tojson(explain));
    assert.eq(explain.executionStats.totalDocsExamined, 10, tojson(explain));
    for (let shardExplain of explain.queryPlanner.winningPlan.shards) {
        // The shards shouldn't have a SKIP stage.
        assert.eq(shardExplain.winningPlan.stage, "SHARDING_FILTER", tojson(explain));
        assert.eq(shardExplain.winningPlan.inputStage.stage, "COLLSCAN", tojson(explain));
    }

    //
    // Explain a find with a limit.
    //
    explain = collSharded.explain("executionStats").find().limit(4).finish();
    assert.eq(explain.executionStats.nReturned, 4, tojson(explain));
    for (let shardExplain of explain.queryPlanner.winningPlan.shards) {
        // Each shard should have a LIMIT stage at the top level.
        assert.eq(shardExplain.winningPlan.stage, "LIMIT", tojson(explain));
        assert.eq(shardExplain.winningPlan.limitAmount, 4, tojson(explain));
        assert.eq(shardExplain.winningPlan.inputStage.stage, "SHARDING_FILTER", tojson(explain));
        assert.eq(shardExplain.winningPlan.inputStage.inputStage.stage,
                  "COLLSCAN",
                  tojson(explain));
    }

    //
    // Explain a find with a skip and a limit.
    //
    explain = collSharded.explain("executionStats").find().skip(3).limit(4).finish();
    assert.eq(explain.executionStats.nReturned, 4, tojson(explain));
    for (let shardExplain of explain.queryPlanner.winningPlan.shards) {
        // Each shard should have a LIMIT stage at the top level. There should be no SKIP stage.
        assert.eq(shardExplain.winningPlan.stage, "LIMIT", tojson(explain));
        assert.eq(shardExplain.winningPlan.limitAmount, 3 + 4, tojson(explain));
        assert.eq(shardExplain.winningPlan.inputStage.stage, "SHARDING_FILTER", tojson(explain));
        assert.eq(shardExplain.winningPlan.inputStage.inputStage.stage,
                  "COLLSCAN",
                  tojson(explain));
    }

    st.stop();
})();
