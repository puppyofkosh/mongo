// Tests resuming on a change stream that was invalidated due to rename.

(function() {
    "use strict";

    load("jstests/libs/change_stream_util.js");
    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.

    for (let modeName of Object.keys(ChangeStreamTest.WatchMode)) {
        const watchMode = ChangeStreamTest.WatchMode[modeName];
        jsTestLog("Running test in mode " + watchMode);

        const dbToStartOn = ChangeStreamTest.getDBForChangeStream(watchMode, db);
        const cst = new ChangeStreamTest(dbToStartOn);

        const coll = assertDropAndRecreateCollection(db, "change_stream_invalidate_resumability");

        // Drop the collection we'll rename to _before_ starting the changeStream, so that we don't
        // get accidentally an invalidate when running on the whole DB or cluster.
        assertDropCollection(db, coll.getName() + "_renamed");

        let cursor = cst.getChangeStream({watchMode: watchMode, coll: coll});

        // Create an 'insert' oplog entry.
        assert.writeOK(coll.insert({_id: 1}));

        assert.commandWorked(coll.renameCollection(coll.getName() + "_renamed"));

        // Insert another document after the rename.
        assert.commandWorked(coll.insert({_id: 2}));

        // We should get 2 oplog entries of type insert and invalidate.
        let change = cst.getOneChange(cursor);
        assert.eq(change.operationType, "insert", tojson(change));
        assert.docEq(change.fullDocument, {_id: 1});

        change = cst.getOneChange(cursor, true);
        assert.eq(change.operationType, "invalidate", tojson(change));

        // Try resuming from the invalidate.
        let resumeCursor = cst.getChangeStream({watchMode: watchMode,
                                                coll: coll,
                                                resumeAfter: change._id});

        // Be sure we can see the change after the rename.
        change = cst.getOneChange(resumeCursor);
        assert.eq(change.operationType, "insert", tojson(change));
        assert.docEq(change.fullDocument, {_id: 2});

        cst.cleanUp();
    }
}());
