// @tags: [requires_getmore, does_not_support_stepdowns]
//
// Uses getMore to pin an open cursor.
//
// Does not support stepdowns because if a stepdown were to occur between running find() and
// calling killCursors on the cursor ID returned by find(), the killCursors might be sent to
// different node than the one which has the cursor. This would result in the node returning
// "CursorNotFound."
//
// Test killing a pinned cursor. Since cursors are generally pinned for short periods while result
// batches are generated, this requires some special machinery to keep a cursor permanently pinned.

(function() {
    "use strict";

    const kFailPointName = "keepCursorPinnedDuringGetMore";

    // TODO: More than one shard
    var st = new ShardingTest({shards: 1, config: 1});
    assert.neq(null, st, "sharded cluster failed to start up");
    const mongosDB = st.s.getDB("test");
    const mongodDB = st.shard0.getDB("test");

    let coll = mongosDB.jstest_kill_pinned_cursor;
    coll.drop();

    for (let i = 0; i < 10; i++) {
        assert.writeOK(coll.insert({_id: i}));
    }

    let cleanup = null;
    let cursorId;

    // kill the cursor associated with the command and assert that we get the
    // OperationInterrupted error.
    try {
        print("setting up failpoint on mongod");
        // ONLY set the failpoint on the mongod. Setting the failpoint on the mongos will
        // only cause it to spin, and not actually send any requests out.
        assert.commandWorked(
            mongodDB.adminCommand({configureFailPoint: kFailPointName, mode: "alwaysOn"}));

        print("running find() command");
        let cmdRes = mongosDB.runCommand({find: coll.getName(), batchSize: 2});
        assert.commandWorked(cmdRes);
        cursorId = cmdRes.cursor.id;
        assert.neq(cursorId, NumberLong(0));

        print("running getMore in other shell");
        let runGetMoreAndExpectError = function() {
            let response = db.runCommand({getMore: cursorId, collection: collName});
            // We expect that the operation will get interrupted and fail.
            assert.commandFailedWithCode(response, ErrorCodes.CursorKilled);
        };
        let code = "let cursorId = " + cursorId.toString() + ";";
        code += "let collName = '" + coll.getName() + "';";
        code += "(" + runGetMoreAndExpectError.toString() + ")();";
        cleanup = startParallelShell(code, st.s.port);

        // Sleep until we know the cursor is pinned on the mongod.
        print("waiting for cursor to be pinned");
        assert.soon(() => mongodDB.serverStatus().metrics.cursor.open.pinned > 0);

        print("Running killCursors");
        cmdRes = mongosDB.runCommand({killCursors: coll.getName(), cursors: [cursorId]});
        assert.commandWorked(cmdRes);
        assert.eq(cmdRes.cursorsKilled, [cursorId]);
        assert.eq(cmdRes.cursorsAlive, []);
        assert.eq(cmdRes.cursorsNotFound, []);
        assert.eq(cmdRes.cursorsUnknown, []);
    } finally {
        assert.commandWorked(mongodDB.adminCommand({configureFailPoint: kFailPointName,
                                                    mode: "off"}));
        if (cleanup) {
            cleanup();
        }
    }

    // Eventually the cursor on the mongod should be cleaned up, now that we've disabled the
    // failpoint.
    assert.soon(() => mongodDB.serverStatus().metrics.cursor.open.pinned == 0);

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
