// Confirms basic killOp execution via mongod and mongos.
// @tags: [requires_replication, requires_sharding]

(function() {
    "use strict";
    load("jstests/libs/fixture_helpers.js");  // For isMongos.

    const dbName = "killop";
    const collName = "test";

    // 'conn' is a connection to either a mongod when testing a replicaset or a mongos when testing
    // a sharded cluster. 'shardConn' is a connection to the mongod we enable failpoints on.
    // 'killLocalOp' indicates whether or not to kill the mongos op id if conn points to a mongos.
    function runTest(conn, shardConn, killLocalOp) {
        const db = conn.getDB(dbName);
        if (!FixtureHelpers.isMongos(db)) {
            // 'killLocalOp' should only be set to true when running against a mongos.
            assert.eq(killLocalOp, false);
        }

        assert.commandWorked(db.dropDatabase());
        assert.writeOK(db.getCollection(collName).insert({x: 1}));

        // Decide which node (mongos or mongod) we need to set a failpoint on.
        let failPointName = "waitAfterEstablishingCursorsBeforeMakingBatch";
        let connToSetFailPointOn = conn;
        if (!killLocalOp) {
            assert.commandWorked(
                shardConn.adminCommand({setParameter: 1, internalQueryExecYieldIterations: 1}));
            failPointName = "setYieldAllLocksHang";
            connToSetFailPointOn = shardConn;
        }

        assert.commandWorked(connToSetFailPointOn.adminCommand(
            {"configureFailPoint": failPointName, "mode": "alwaysOn"}));

        const queryToKill = "assert.commandWorked(db.getSiblingDB('" + dbName +
            "').runCommand({find: '" + collName + "', filter: {x: 1}}));";
        const awaitShell = startParallelShell(queryToKill, conn.port);

        function runCurOp() {
            const filter = {"ns": dbName + "." + collName, "command.filter": {x: 1}};
            return db.getSiblingDB("admin")
                .aggregate([{$currentOp: {localOps: killLocalOp}}, {$match: filter}])
                .toArray();
        }

        let opId;

        // Wait for the operation to start.
        assert.soon(
            function() {
                const result = runCurOp();

                // When running on a mongod, we wait until the operation has yielded, so that we're
                // sure it is hanging in the failpoint. Otherwise there's a chance that we may kill
                // the operation before it reaches the failpoint, which could cause an interrupt
                // check before the failpoint to trigger.
                if (result.length === 1 &&
                    (FixtureHelpers.isMongos(db) || result[0].numYields > 0)) {
                    opId = result[0].opid;
                    return true;
                }

                return false;
            },
            function() {
                return "Failed to find operation in currentOp() output: " +
                    tojson(db.currentOp({"ns": dbName + "." + collName}));
            });

        // Kill the operation.
        assert.commandWorked(db.killOp(opId));

        // Ensure that the operation gets marked kill pending while it's still hanging.
        let result = runCurOp();
        assert(result.length === 1, tojson(result));
        assert(result[0].hasOwnProperty("killPending"));
        assert.eq(true, result[0].killPending);

        // Release the failpoint. The operation should check for interrupt and then finish.
        assert.commandWorked(connToSetFailPointOn.adminCommand(
            {"configureFailPoint": failPointName, "mode": "off"}));

        const exitCode = awaitShell({checkExitSuccess: false});
        assert.neq(0, exitCode, "Expected shell to exit with failure due to operation kill");

        result = runCurOp();
        assert(result.length === 0, tojson(result));
    }

    const st = new ShardingTest({shards: 1, rs: {nodes: 1}, mongos: 1});
    const shardConn = st.rs0.getPrimary();

    // Test killOp against mongod.
    runTest(shardConn, shardConn, false);

    // Test killOp against mongos, killing the mongod opId.
    runTest(st.s, shardConn, false);

    // Test killOp against mongos, killing the mongos opId.
    runTest(st.s, shardConn, true);

    st.stop();
})();
