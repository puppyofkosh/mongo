// TODO: move to noPassthrough
(function() {
    var replTest = new ReplSetTest({name: 'testSet', nodes: 2});
    var nodes = replTest.startSet();
    replTest.initiate();
    var master = replTest.getPrimary();

    const db = master.getDB("test");
    const coll = db.group_with_stepdown;
    
    coll.insert({name: 'bob', foo: 1});
    coll.insert({name: 'bob', foo: 2});
    coll.insert({name: 'alice', foo: 1});
    coll.insert({name: 'alice', foo: 3});
    coll.insert({name: 'fred', foo: 3});
    coll.insert({name: 'fred', foo: 4});


    function runTest(failPointName, shouldKillOp) {
        jsTestLog("Running with failPoint: " + failPointName + "shouldKillOp: " + shouldKillOp);
        let awaitShellFn = null;
        try {
            assert.commandWorked(
                db.adminCommand({configureFailPoint: failPointName,
                                 mode: "alwaysOn"}));
            print("ian: 1");

            // Run a group in the background. Wait until we hit the failpoint.
            function runHangingGroup() {
                const coll = db.group_with_stepdown;

                const err = assert.throws(() => coll.group({
                    key: {foo: 1},
                    initial: {count: 0, values: []},
                    reduce: function(obj, prev) {
                        prev.count++;
                        prev.values.push(obj.name);
                    }
                }), [], "expected group() to fail");

                print("ian: the error is " + tojson(err));
                assert.eq(err.code, ErrorCodes.Interrupted);
            }
            awaitShellFn = startParallelShell(runHangingGroup, master.port);
            print("ian: 2");

            // Wait until we know the failpoint has been reached.
            let opid = null;
            assert.soon(function() {
                const arr =
                      db.getSiblingDB("admin")
                      .aggregate(
                          [{$currentOp: {}}, {$match: {"msg": failPointName}}])
                      .toArray();

                print("array return is " + tojson(arr));
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
                print("ian: 3");

                // Kill the op running group.
                assert.commandWorked(db.killOp(opid));
            } else {
                // The javascript execution should time out on its own eventually, even if we don't
                // run killOp.
            }

            print("ian: 4");
        } finally {
            assert.commandWorked(db.adminCommand({configureFailPoint: failPointName,
                                                  mode: "off"}));

            print("ian: 5");

            if (awaitShellFn) {
                awaitShellFn();
            }
        }

        print("ian: 6");
    }

    const kFailPoints = ["hangInGroupJsReduceInit", "hangInGroupJsCleanup"];
    for (let failPointName of kFailPoints) {
        for (let shouldKillOp of [true, false]) {
            runTest(failPointName, shouldKillOp);
        }
    }

    replTest.stopSet();
})();
