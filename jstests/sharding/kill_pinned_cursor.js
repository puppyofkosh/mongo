/**
 * Run a query on a sharded cluster where one of the shards hangs. Running killCursors on the mongos
 * should always succeed.
 *
 * Uses getMore to pin an open cursor.
 * @tags: [requires_getmore]
 */

(function() {
    "use strict";

    const st = new ShardingTest({shards: 2});
    const kDBName = "test";
    const mongosDB = st.s.getDB(kDBName);
    const shard0DB = st.shard0.getDB(kDBName);

    let coll = mongosDB.jstest_kill_pinned_cursor;
    coll.drop();

    for (let i = 0; i < 10; i++) {
        assert.writeOK(coll.insert({_id: i}));
    }

    st.shardColl(coll, {_id: 1}, {_id: 5}, {_id: 6}, kDBName, false);

    function runPinnedCursorKillTest({failpointNodeDB, failPointName, runGetMoreFunc, waitForPin}) {
        // Set up the first mongod to hang on a getMore request.
        let cleanup = null;
        let cursorId;
        
        try {
            // ONLY set the failpoint on the first mongod. Setting the failpoint on the mongos will
            // only cause it to spin, and not actually send any requests out.
            assert.commandWorked(
                failpointNodeDB.adminCommand({configureFailPoint: failPointName, mode: "alwaysOn"}));

            // Run a find with a sort, so that we must return the results from shard 0 before the
            // results from shard 1. This should cause the mongos to hang, waiting on the network
            // for the response from the hung shard.
            let cmdRes = mongosDB.runCommand({find: coll.getName(), sort: {_id: 1}, batchSize: 2});
            assert.commandWorked(cmdRes);
            cursorId = cmdRes.cursor.id;
            assert.neq(cursorId, NumberLong(0));

            let code = `let cursorId = ${cursorId.toString()};`;
            code += `let collName = "${coll.getName()}";`;
            code += `(${runGetMoreFunc.toString()})();`;
            cleanup = startParallelShell(code, st.s.port);

            if (waitForPin) {
                assert.soon(() => shard0DB.serverStatus().metrics.cursor.open.pinned > 0);
            } else {
                sleep(1000 * 5);
            }

            cmdRes = mongosDB.runCommand({killCursors: coll.getName(), cursors: [cursorId]});

            // Now that we know the cursor is stuck waiting on a remote to respond, we test that we
            // can still successfully kill it without hanging or returning a "CursorInUse" error.
            assert.commandWorked(cmdRes);
            assert.eq(cmdRes.cursorsKilled, [cursorId]);
            assert.eq(cmdRes.cursorsAlive, []);
            assert.eq(cmdRes.cursorsNotFound, []);
            assert.eq(cmdRes.cursorsUnknown, []);
        } finally {
            assert.commandWorked(
                failpointNodeDB.adminCommand({configureFailPoint: failPointName, mode: "off"}));
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
    }

    // Test that killing the pinned cursor before it starts building the batch results in a
    // CursorKilled exception.
    let testParameters = {
        failpointNodeDB: shard0DB,
        failPointName: "keepCursorPinnedDuringGetMore",
        runGetMoreFunc: function() {
            const response = db.runCommand({getMore: cursorId, collection: collName});
            // We expect that the operation will get interrupted and fail.
            assert.commandFailedWithCode(response, ErrorCodes.CursorKilled);
        },
        waitForPin: true
    };
    runPinnedCursorKillTest(testParameters);

    // // Test that, if the pinned cursor is killed after it has finished building a batch, that batch
    // // is returned to the client but a subsequent getMore will fail with a 'CursorKilled' exception.
    testParameters = {
        failpointNodeDB: mongosDB,
        failPointName: "waitBeforeUnpinningCursorAfterGetMoreBatch",
        runGetMoreFunc: function() {
            const getMoreCmd = {getMore: cursorId, collection: collName, batchSize: 2};
            // We expect that the first getMore will succeed, while the second fails because the cursor
            // has been killed.
            assert.commandWorked(db.runCommand(getMoreCmd));
            assert.commandFailedWithCode(db.runCommand(getMoreCmd), ErrorCodes.CursorKilled);
        },
        waitForPin: false
    }

    st.stop();
})();
