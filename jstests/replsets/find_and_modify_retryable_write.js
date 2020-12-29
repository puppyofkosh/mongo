/**
 * Tests that a retryable findAndModify with a projection works correctly.
 * TODO: unban a bunch of other tests.
 */
(function() {
"use strict";

load("jstests/libs/retryable_writes_util.js");

if (!RetryableWritesUtil.storageEngineSupportsRetryableWrites(jsTest.options().storageEngine)) {
    jsTestLog("Retryable writes are not supported, skipping test");
    return;
}

const replTest = new ReplSetTest({nodes: 1});
replTest.startSet();
replTest.initiate();

// Run the command on the primary and wait for replication.
const primary = replTest.getPrimary();
const testDB = primary.getDB("test");
const collName = "famRetryableWrite";
const oplog = primary.getDB("local").oplog.rs;
    
// TODO: Do we need to test ID hack path specially?
assert.commandWorked(testDB[collName].insert({_id: 1, a:1, b:2, c:3}));

// Test FAM command

let famCmd = {
    findAndModify: collName,
    query: {_id: 1},
    update: {$inc: {a: 1}},

    "new": false, // TODO: test true
    fields: {
        _id: 1,
        a:1,
    },
    lsid: {id: UUID()},
    txnNumber: NumberLong(10),
};


    {
        // Run the command on the primary and wait for replication.
        let result = assert.commandWorked(testDB.runCommand(famCmd));

        printjson(oplog.find({ns: "test." + collName}).toArray());
    }
    

replTest.stopSet();
})();
