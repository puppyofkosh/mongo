'use strict';

/**
 * count.js
 *
 * Base workload for count.
 * Runs count on a non-indexed field and verifies that the count
 * is correct.
 * Each thread picks a random 'modulus' in range [5, 10]
 * and a random 'countPerNum' in range [50, 100]
 * and then inserts 'modulus * countPerNum' documents. [250, 1000]
 * All threads insert into the same collection.
 */
load("jstests/libs/fixture_helpers.js");  // For isMongos.

var $config = (function() {

    function generateGroupCmdObj(collName) {
        return {
            group: {
                ns: collName,
                initial: {bucketCount: 0, bucketSum: 0},
                $keyf: function $keyf(doc) {
                    // place doc.rand into appropriate bucket
                    return {bucket: Math.floor(doc.rand * 10) + 1};
                },
                $reduce: function $reduce(curr, result) {
                    result.bucketCount++;
                    result.bucketSum += curr.rand;
                },
                finalize: function finalize(result) {
                    // calculate average float value per bucket
                    result.bucketAvg = result.bucketSum / (result.bucketCount || 1);
                }
            }
        };
    }

    var data = {
        numDocs: 1000,
        generateGroupCmdObj: generateGroupCmdObj,
    };

    var states = (function() {

        function init(db, collName) {
        }

        function group(db, collName) {
            const res = db.runCommand(this.generateGroupCmdObj(collName));

            // The only time this should fail is due to interrupt from the 'killOp'.
            if (res.ok) {
                assertAlways.commandWorked(res);
            } else {
                assertAlways.commandFailedWithCode(res, ErrorCodes.Interrupted) return;
            }

            // lte because the documents are generated randomly, and so not all buckets are
            // guaranteed to exist.
            assertWhenOwnColl.lte(res.count, this.numDocs);
            assertWhenOwnColl.lte(res.keys, 10);
        }

        function chooseRandomlyFrom(arr) {
            if (!Array.isArray(arr)) {
                throw new Error('Expected array for first argument, but got: ' + tojson(arr));
            }
            return arr[Random.randInt(arr.length)];
        }

        function killOp(db, collName) {
            // Find a group command to kill.
            const countOps = db.currentOp({"command.group.ns": collName}).inprog;
            if (countOps.length > 0) {
                print("ian: killing op");
                const op = chooseRandomlyFrom(countOps);
                const res = db.adminCommand({killOp: 1, op: op.opid});
                assertAlways.commandWorked(res);
            }
        }

        return {init: init, group: group, killOp: killOp};

    })();

    var transitions = {init: {group: 0.7, killOp: 0.3}, group: {init: 1}, killOp: {init: 1}};

    function setup(db, collName, cluster) {
        var bulk = db[collName].initializeUnorderedBulkOp();
        for (var i = 0; i < this.numDocs; ++i) {
            bulk.insert({rand: Random.rand()});
        }
        var res = bulk.execute();
        assertAlways.commandWorked(res);
        assertAlways.eq(this.numDocs, res.nInserted);
    }

    function teardown(db, collName, cluster) {
        assertWhenOwnColl(db[collName].drop());
    }

    return {
        data: data,
        threadCount: 40,
        iterations: 40,
        states: states,
        transitions: transitions,
        setup: setup,
        teardown: teardown
    };

})();
