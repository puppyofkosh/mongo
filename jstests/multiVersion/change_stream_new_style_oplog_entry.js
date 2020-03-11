(function() {

const latest = "latest";
const lastStable = "4.4"

const rst = new ReplSetTest({nodes: 2, nodeOpts: {binVersion: latest, noCleanData: true}});

// Start 4.6 RS and do some inserts.
const resumeTokenFrom46 = (function() {
    rst.startSet();
    rst.initiate();
    const primaryDB = rst.getPrimary().getDB("test");
    const coll = primaryDB["coll"];

    // Start a change stream.
    const changeStream = coll.watch();

    // Do a bunch of inserts.
    for (let i = 0; i < 10; ++i) {
        assert.commandWorked(coll.insert({_id: i, a: 1}));
    }

    // Insert a document with a large field, so that the update system favors partial updates
    // over full document replacement.
    assert.commandWorked(
        coll.insert({_id: 150, a: 0, longField: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}));

    // Update the large document with a pipeline update.
    assert.commandWorked(coll.update({_id: 150}, [{$set: {foo: 1}}]));

    // Check that the oplog entry uses the new format.
    const oplogRes =
        rst.getPrimary().getDB("local")["oplog.rs"].find().sort({ts: -1}).limit(1).toArray();
    assert.eq(oplogRes.length, 1);
    assert.eq(oplogRes[0].o.$v, 2);

    // Downgrade FCV.
    assert.commandWorked(primaryDB.adminCommand({setFeatureCompatibilityVersion: "4.4"}));

    // Get the first change off the change stream, and its resume token.
    const change = changeStream.next();
    const resumeToken = change._id;

    rst.stopSet(
        null,  // signal
        true   // for restart
    );

    return resumeToken;
})();

(function startLastStableRSWithSameDataFiles() {
    rst.startSet({restart: true, binVersion: "4.4"});

    const primaryDB = rst.getPrimary().getDB("test");
    const coll = primaryDB["coll"];

    const changeStream = coll.watch([], {resumeAfter: resumeTokenFrom46});

    // Show the changes.
    while (1) {
        assert.soon(() => changeStream.hasNext());
        const next = changeStream.next();

        printjson(next);

        if (next.operationType == "update") {
            break;
        }
    }

    rst.stopSet();
})();
})();
