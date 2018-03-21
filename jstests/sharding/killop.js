// Confirms basic killOp execution via mongos.
// @tags: [requires_replication, requires_sharding]

(function() {
    "use strict";

    const st = new ShardingTest({shards: 2, mongos: 1});
    const conn = st.s;

    const dbName = "killop";
    const collName = "test";

    const db = conn.getDB(dbName);
    assert.commandWorked(db.dropDatabase());
    assert.writeOK(db.getCollection(collName).insert({x: 1}));

    // Decide which node (mongos or mongod) we need to set a failpoint on.
    let failPointName = "waitInFindAfterEstablishingCursorsBeforeMakingBatch";
    let connToSetFailPointOn = conn;

    assert.commandWorked(
        conn.adminCommand({"configureFailPoint": failPointName, "mode": "alwaysOn"}));

    const queryToKill = "assert.commandWorked(db.getSiblingDB('" + dbName +
        "').runCommand({find: '" + collName + "', filter: {x: 1}}));";
    const awaitShell = startParallelShell(queryToKill, conn.port);

    function runCurOp() {
        const filter = {"ns": dbName + "." + collName, "command.filter": {x: 1}};
        return db.getSiblingDB("admin")
            .aggregate([{$currentOp: {localOps: true}}, {$match: filter}])
            .toArray();
    }

    let opId;

    // Wait for the operation to start.
    assert.soon(
        function() {
            const result = runCurOp();

            if (result.length === 1 &&
                result[0].msg === "waitInFindAfterEstablishingCursorsBeforeMakingBatch") {
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
    assert.commandWorked(conn.adminCommand({"configureFailPoint": failPointName, "mode": "off"}));

    const exitCode = awaitShell({checkExitSuccess: false});
    assert.neq(0, exitCode, "Expected shell to exit with failure due to operation kill");

    result = runCurOp();
    assert(result.length === 0, tojson(result));

    st.stop();
})();
