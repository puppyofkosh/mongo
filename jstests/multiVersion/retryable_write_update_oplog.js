(function() {
const latest = "latest";
const lastStable = "4.4";

const famCmd = {
    findAndModify: "coll",
    query: {_id: 0},
    update: [{$set: {b: 4, a: {$add: ["$a", 1]}}}],
    txnNumber: NumberLong(0),
    lsid: {id: UUID()}
};

const rst = new ReplSetTest({nodes: 2, nodeOpts: {binVersion: latest, noCleanData: true}});

// Start 4.6 RS and do some inserts.
(function startLatestRSAndCreateDataFiles() {
    rst.startSet();
    rst.initiate();
    const primaryDB = rst.getPrimary().getDB("test");
    const coll = primaryDB["coll"];

    // Insert a document with a large field, so that the update system favors partial updates
    // over full document replacement.
    assert.commandWorked(coll.insert({_id: 0, a: 0, longField: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}));

    // Do an update in a retryable write and check it succeeds.
    assert.commandWorked(primaryDB.runCommand(famCmd));

    // Check that the oplog entry uses the new format.
    const oplogRes =
        rst.getPrimary().getDB("local")["oplog.rs"].find().sort({ts: -1}).limit(1).toArray();
    assert.eq(oplogRes.length, 1);
    assert.eq(oplogRes[0].o.$v, 2);

    // Downgrade FCV.
    assert.commandWorked(primaryDB.adminCommand({setFeatureCompatibilityVersion: "4.4"}));

    // Retry the write. Should succeed and not increment a again.
    assert.commandWorked(primaryDB.runCommand(famCmd));
    assert.eq(coll.findOne().a, 1);

    rst.stopSet(
        null,  // signal
        true   // for restart
    );
})();

(function startLastStableRSWithSameDataFiles() {
    rst.startSet({restart: true, binVersion: lastStable});

    const primaryDB = rst.getPrimary().getDB("test");
    const coll = primaryDB["coll"];

    assert.eq(coll.find().itcount(), 1);

    // Retry the write yet again. This should force the last stable binary to attempt parsing
    // the oplog entry created for the update.
    assert.commandWorked(primaryDB.runCommand(famCmd));
    assert.eq(coll.find().itcount(), 1);
    assert.eq(coll.findOne().a, 1);

    // Print the oplog, for debugging purposes:
    print("dbg: the oplog is");
    printjson(rst.getPrimary().getDB("local")["oplog.rs"].find().toArray());

    rst.stopSet();
})();
})();
