// Confirms basic killOp execution via mongod and mongos.
// @tags: [requires_replication, requires_sharding]

(function() {
    "use strict";

    const dbName = "killop";
    const collName = "test";

    // 'conn' is a connection to either a mongod when testing a replicaset or a mongos when testing
    // a sharded cluster. 'shardConn' is a connection to the mongod we enable failpoints on.
    // 'killLocalOp' indicates whether or not to kill the mongos op id if conn points to a mongos.
    function runTest(conn, shardConn, killLocalOp) {
        load("jstests/libs/fixture_helpers.js");  // For isMongos.
        const db = conn.getDB(dbName);
        if (!FixtureHelpers.isMongos(db)) {
            // 'killLocalOp' should only be set to true when running against a mongos.
            assert.eq(killLocalOp, false);
        }

        assert.commandWorked(db.dropDatabase());
        assert.writeOK(db.getCollection(collName).insert({x: 1}));

        assert.commandWorked(
            shardConn.adminCommand({setParameter: 1, internalQueryExecYieldIterations: 1}));
        assert.commandWorked(shardConn.adminCommand(
            {"configureFailPoint": "setYieldAllLocksHang", "mode": "alwaysOn"}));

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

        assert.soon(
            function() {
                const filter = {"ns": dbName + "." + collName, "command.filter": {x: 1}};
                const result = runCurOp();
                print("Current op results are " + tojson(result));

                if (result.length === 1) {
                    opId = result[0].opid;
                    return true;
                }

                return false;
            },
            function() {
                return "Failed to find operation in currentOp() output: " +
                    tojson(db.currentOp({"ns": dbName + "." + collName}));
            });

        print("opId to kill is " + tojson(opId));
        if (FixtureHelpers.isMongos(db)) {
            // mongos expects opId to be a string.
            opId = opId.toString();
        }

        assert.commandWorked(db.killOp(opId));

        let result = runCurOp();
        assert(result.length === 1, tojson(result));
        assert(result[0].hasOwnProperty("killPending"));
        assert.eq(true, result[0].killPending);

        assert.commandWorked(
            shardConn.adminCommand({"configureFailPoint": "setYieldAllLocksHang", "mode": "off"}));

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
