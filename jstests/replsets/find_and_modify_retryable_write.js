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
    const fullNs = "test." + collName;
const oplog = primary.getDB("local").oplog.rs;
    
    // TODO: Do we need to test ID hack path specially?
    // TODO: Test this with recordPreImage on.
    
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
    let result = assert.commandWorked(testDB.runCommand(famCmd));
    printjson(oplog.find({ns: fullNs}).toArray());
}
let updateEntries = oplog.find({ns: fullNs, op: "u"}).itcount();

{
    // Run the command again, ensure that another oplog entry isn't written.
    let result = assert.commandWorked(testDB.runCommand(famCmd));
    printjson(oplog.find({ns: fullNs}).toArray());

    assert.eq(updateEntries, oplog.find({ns: fullNs, op: "u"}).itcount());
}

// Check that the noop oplog entry only contains the fields _id and a.
let noopEntry = oplog.find({ns: fullNs, op: "n"}).sort({ts: -1}).toArray()[0];
assert.eq(noopEntry.o, {_id:1, a: 1});

// Check that the update entry stores the link in the 'queryResultOpTime' field.
let updateEntry = oplog.find({ns: fullNs, op: "u"}).sort({ts: -1}).toArray()[0];
assert.eq(updateEntry.queryResultOpTime.ts, noopEntry.ts);

replTest.stopSet();
})();
