// Tests of $changeStream invalidate entries.

(function() {
    "use strict";

    load("jstests/libs/change_stream_util.js");
    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.

    let cst = new ChangeStreamTest(db);

    const collName = "change_stream_apply_ops";
    const otherCollName = "change_stream_apply_ops_2";
    const coll = assertDropAndRecreateCollection(db, collName);
    assertDropAndRecreateCollection(db, otherCollName);

    let aggcursor = cst.startWatchingChanges({pipeline: [{$changeStream: {}}], collection: coll});

    const sessionOptions = {causalConsistency: false};
    const session = db.getMongo().startSession(sessionOptions);
    const sessionDb = session.getDatabase(db.getName());

    session.startTransaction({readConcern: {level: "snapshot"}, writeConcern: {w: "majority"}});
    assert.commandWorked(sessionDb[collName].insert({_id: 1, a: 0}));
    assert.commandWorked(sessionDb[collName].insert({_id: 2, a: 0}));

    // One insert on a collection that we're not watching. This should be skipped in the change
    // stream.
    assert.commandWorked(sessionDb[otherCollName].insert({_id: 3, a: "SHOULD NOT READ THIS"}));
    session.commitTransaction();

    // Do applyOps on the collection that we care about. This is an "external" applyOps, though
    // (not run as part of a transaction) so its entries should be skipped in the change stream.
    assert.commandWorked(db.runCommand({
        applyOps: [
            {op: "i", ns: coll.getFullName(), o: {_id: 3, a: "SHOULD NOT READ THIS"}},
        ]
    }));

    // Drop the collection. This will trigger an "invalidate" event.
    assert.commandWorked(db.runCommand({drop: collName}));

    let change = cst.getOneChange(aggcursor);
    assert.eq(change.fullDocument._id, 1);
    assert.eq(change.operationType, "insert", tojson(change));
    const firstChangeTxnNumber = change.txnNumber;
    const firstChangeLsid = change.lsid;
    assert.eq(typeof firstChangeLsid, "object");

    change = cst.getOneChange(aggcursor);
    assert.eq(change.fullDocument._id, 2);
    assert.eq(change.operationType, "insert", tojson(change));
    assert.eq(firstChangeTxnNumber.valueOf(), change.txnNumber);
    assert.eq(0, bsonWoCompare(firstChangeLsid, change.lsid));

    cst.assertNextChangesEqual({
        cursor: aggcursor,
        expectedChanges: [{operationType: "invalidate"}],
        expectInvalidate: true
    });

    cst.cleanUp();
}());
