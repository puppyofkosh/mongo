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

function findLastOplogEntry(oplog, opType) {
    return oplog.find({ns: fullNs, op: opType}).sort({$natural: -1}).toArray()[0];
}

    function countUpdateEntries() {
        return oplog.find({ns: fullNs, op: "u"}).itcount();
    }

// TODO: Test this with recordPreImage on.
    
    assert.commandWorked(testDB[collName].insert({_id: 1, a:1, otherField: "abc"}));

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

    // Make sure we can retry the command and that we are not storing the entire pre image in the
    // oplog.
    {
        let result = assert.commandWorked(testDB.runCommand(famCmd));
        assert.eq(result.value, {_id: 1, a: 1});
        let updateEntries = countUpdateEntries();
        // Run the command again, ensure that another oplog entry isn't written.
        let reapplyResult = assert.commandWorked(testDB.runCommand(famCmd));
        assert.eq(updateEntries, countUpdateEntries());
        assert.eq(reapplyResult.value, result.value);

        // Check that the noop oplog entry only contains the fields _id and a.
        let noopEntry = findLastOplogEntry(oplog, "n")
        assert.eq(noopEntry.o, {_id:1, a: 1});

        // Check that the update entry stores the link in the 'queryResultOpTime' field.
        let updateEntry = findLastOplogEntry(oplog, "u");
        assert.eq(updateEntry.queryResultOpTime.ts, noopEntry.ts);
    }

    // Make sure the command logs correctly when 'recordPreImages' is on.
    {
        // Now run another FAM command under a different txn, and this time, with the
        // server configured to record pre images.
        assert.commandWorked(testDB.runCommand({collMod: collName, recordPreImages: true}));

        famCmd.txnNumber = NumberLong(famCmd.txnNumber + 1);
        let result = assert.commandWorked(testDB.runCommand(famCmd));
        assert.eq(result.value, {_id: 1, a: 2});

        // Now check the oplog entry. There should be two links: one to the pre image and
        // another to the query's result.
        let updateEntry = findLastOplogEntry(oplog, "u");
        assert(updateEntry.queryResultOpTime);
        assert(updateEntry.preImageOpTime);

        const updateEntries = countUpdateEntries();

        let queryResultNoopEntry = oplog.find({ts: updateEntry.queryResultOpTime.ts}).toArray()[0];
        assert.eq(queryResultNoopEntry.o, result.value);
        assert.eq(queryResultNoopEntry.op, "n");

        let preImageEntry = oplog.find({ts: updateEntry.preImageOpTime.ts}).toArray()[0];
        assert.eq(preImageEntry.o, {_id:1, a:2, otherField: "abc"});
        assert.eq(preImageEntry.op, "n");

        // Retrying the command should not cause another entry to be written.
        assert.commandWorked(testDB.runCommand(famCmd));
        assert.eq(updateEntries, countUpdateEntries());
        

        // Disable the pre image recording.
        assert.commandWorked(testDB.runCommand({collMod: collName, recordPreImages: false}));
    }

replTest.stopSet();
})();
