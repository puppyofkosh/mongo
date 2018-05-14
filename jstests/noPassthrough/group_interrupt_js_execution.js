// Test what happens when javascript execution inside the group command is interrupted, either from
// killOp, or due to timeout.
(function() {
    const conn = MongoRunner.runMongod({});
    assert.neq(null, conn);

    const db = conn.getDB("test");
    const coll = db.group_with_stepdown;

    assert.commandWorked(coll.insert({name: "bob", foo: 1}));
    assert.commandWorked(coll.insert({name: "alice", foo: 1}));
    assert.commandWorked(coll.insert({name: "fred", foo: 3}));
    assert.commandWorked(coll.insert({name: "fred", foo: 4}));

    // Attempts to run the group command while the given failpoint is enabled. If 'shouldKillOp' is
    // true, it will run killOp on the operation while it is hanging. If 'shouldKillOp' is false,
    // it will let the operation hang until the javascript execution timeout is reached.
    function runTest(failPointName, shouldKillOp) {
        jsTestLog("Running with failPoint: " + failPointName + "shouldKillOp: " + shouldKillOp);
        let awaitShellFn = null;
        try {
            assert.commandWorked(
                db.adminCommand({configureFailPoint: failPointName, mode: "alwaysOn"}));

            // Run a group in the background. Wait until we hit the failpoint.
            function runHangingGroup() {
                const coll = db.group_with_stepdown;

                const err = assert.throws(() => coll.group({
                    key: {foo: 1},
                    initial: {count: 0},
                    reduce: function(obj, prev) {
                        prev.count++;
                    }
                }),
                                          [],
                                          "expected group() to fail");

                assert.eq(err.code, ErrorCodes.Interrupted);
            }
            awaitShellFn = startParallelShell(runHangingGroup, conn.port);

            // Wait until we know the failpoint has been reached.
            let opid = null;
            assert.soon(function() {
                const arr = db.getSiblingDB("admin")
                                .aggregate([{$currentOp: {}}, {$match: {"msg": failPointName}}])
                                .toArray();

                if (arr.length == 0) {
                    return false;
                }

                // Should never have more than one operation stuck on the failpoint.
                assert.eq(arr.length, 1);
                opid = arr[0].opid;
                return true;
            });
            assert.neq(opid, null);

            if (shouldKillOp) {
                // Kill the op running group.
                assert.commandWorked(db.killOp(opid));
            } else {
                // The javascript execution should time out on its own eventually, even if we don't
                // run killOp.
            }
        } finally {
            assert.commandWorked(db.adminCommand({configureFailPoint: failPointName, mode: "off"}));

            if (awaitShellFn) {
                awaitShellFn();
            }
        }
    }

    const kFailPoints = ["hangInGroupJsReduceInit", "hangInGroupJsCleanup"];
    for (let failPointName of kFailPoints) {
        for (let shouldKillOp of[true, false]) {
            runTest(failPointName, shouldKillOp);
        }
    }

    assert.eq(0, MongoRunner.stopMongod(conn), "expected mongod to shutdown cleanly");
})();
