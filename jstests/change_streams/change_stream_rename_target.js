// Tests that watching a collection which another collection is renamed _to_ causes an invalidate.
(function() {
    "use strict";

    load("jstests/libs/change_stream_util.js");        // For ChangeStreamTest.
    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.

    const testDB = db.getSiblingDB(jsTestName());
    let cst = new ChangeStreamTest(testDB);

    // Write a document to the collection and test that the change stream returns it
    // and getMore command closes the cursor afterwards.
    const collName1 = "change_stream_rename_1";
    const collName2 = "change_stream_rename_2";
    let coll = assertDropAndRecreateCollection(testDB, collName1);
    assertDropCollection(testDB, collName2);

    // Watch the collection which doesn't exist yet.
    let aggCursor =
        cst.startWatchingChanges({pipeline: [{$changeStream: {}}], collection: collName2});

    // Insert something to the collection.
    assert.writeOK(coll.insert({_id: 1}));
    cst.assertNextChangesEqual({cursor: aggCursor, expectedChanges: []});

    // Now rename the collection TO the collection that's being watched. This should invalidate the
    // change stream.
    assert.commandWorked(coll.renameCollection(collName2));
    cst.assertNextChangesEqual({
        cursor: aggCursor,
        expectedChanges: [{operationType: "invalidate"}],
        expectInvalidate: true
    });

    cst.cleanUp();
}());
