// @tags: [requires_getmore]
//
// Uses getMore to pin an open cursor.
//
// Run a query on a sharded cluster where one of the shards hangs. Running killCursors on the
// mongos should interrupt the getMore and cause the cursor to be cleaned up in a timely manner.

(function() {
    "use strict";

    const kFailPointName = "keepCursorPinnedDuringGetMore";

    const st = new ShardingTest({shards: 2, config: 3});
    assert.neq(null, st, "sharded cluster failed to start up");
    const mongosDB = st.s.getDB("test");
    const shard0DB = st.shard0.getDB("test");

    let coll = mongosDB.jstest_kill_pinned_cursor;
    coll.drop();

    for (let i = 0; i < 10; i++) {
        assert.writeOK(coll.insert({_id: i}));
    }

    // Now split up the data so that [0,5) go to shard 0 and [5,10) go to shard 1.
    assert.commandWorked(st.s.adminCommand({enableSharding: "test"}));
    st.ensurePrimaryShard("test", st.shard0.shardName);
    assert.commandWorked(st.s.adminCommand({shardCollection: coll.getFullName(), key: {_id: 1}}));
    assert.commandWorked(st.s.adminCommand({split: coll.getFullName(), middle: {_id: 5}}));

    assert.commandWorked(st.s.adminCommand({
        moveChunk: coll.getFullName(),
        find: {_id: 6},
        to: st.shard1.shardName,
        _waitForDelete: true,
    }));

    // Set up the first mongod to hang on a getMore request.
    let cleanup = null;
    let cursorId;

    try {
        // ONLY set the failpoint on the first mongod. Setting the failpoint on the mongos will
        // only cause it to spin, and not actually send any requests out.
        assert.commandWorked(
            shard0DB.adminCommand({configureFailPoint: kFailPointName, mode: "alwaysOn"}));

        // Run a find with a sort, so that we must return the results from shard 0 before the
        // results from shard 1. This should cause the entire query to hang.
        let cmdRes = mongosDB.runCommand({find: coll.getName(), sort: {_id: 1}, batchSize: 2});
        assert.commandWorked(cmdRes);
        cursorId = cmdRes.cursor.id;
        assert.neq(cursorId, NumberLong(0));

        let runGetMoreAndExpectError = function() {
            let response = db.runCommand({getMore: cursorId, collection: collName});
            // We expect that the operation will get interrupted and fail.
            assert.commandFailedWithCode(response, ErrorCodes.CursorKilled);
        };
        let code = `let cursorId = ${cursorId.toString()};`;
        code += `let collName = "${coll.getName()}";`;
        code += `(${runGetMoreAndExpectError.toString()})();`;
        cleanup = startParallelShell(code, st.s.port);

        // Sleep until we know the cursor is pinned on the mongod.
        assert.soon(() => shard0DB.serverStatus().metrics.cursor.open.pinned > 0);

        cmdRes = mongosDB.runCommand({killCursors: coll.getName(), cursors: [cursorId]});
        assert.commandWorked(cmdRes);
        assert.eq(cmdRes.cursorsKilled, [cursorId]);
        assert.eq(cmdRes.cursorsAlive, []);
        assert.eq(cmdRes.cursorsNotFound, []);
        assert.eq(cmdRes.cursorsUnknown, []);
    } finally {
        assert.commandWorked(shard0DB.adminCommand({configureFailPoint: kFailPointName,
                                                    mode: "off"}));
        if (cleanup) {
            cleanup();
        }
    }

    // Eventually the cursor on the mongod should be cleaned up, now that we've disabled the
    // failpoint.
    assert.soon(() => shard0DB.serverStatus().metrics.cursor.open.pinned == 0);

    // Eventually the cursor should get reaped, at which point the next call to killCursors
    // should report that nothing was killed.
    let cmdRes = null;
    assert.soon(function() {
        cmdRes = mongosDB.runCommand({killCursors: coll.getName(), cursors: [cursorId]});
        assert.commandWorked(cmdRes);
        return cmdRes.cursorsKilled.length == 0;
    });

    assert.eq(cmdRes.cursorsAlive, []);
    assert.eq(cmdRes.cursorsNotFound, [cursorId]);
    assert.eq(cmdRes.cursorsUnknown, []);

    st.stop();
})();
