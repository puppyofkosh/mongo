// Tests resuming from $changeStream invalidate entries.

(function() {
    "use strict";

    load("jstests/libs/change_stream_util.js");
    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.

    const WatchMode = {
        kCollection: 1,
        kDb: 2,
        kCluster: 3,
    };

    function getChangeStreamStage(watchMode, resumeToken) {
        const changeStreamDoc = {};
        if (resumeToken) {
            changeStreamDoc.resumeAfter = resumeToken;
        }

        if (watchMode == WatchMode.kCluster) {
            changeStreamDoc.allChangesForCluster = true;
        }
        return changeStreamDoc;
    }

    function getChangeStream({cst, watchMode, coll, resumeToken}) {
        return cst.startWatchingChanges({
            pipeline: [{$changeStream: getChangeStreamStage(watchMode, resumeToken)}],
            collection: (watchMode == WatchMode.kCollection ? coll : 1),
            // Use a batch size of 0 to prevent any notifications from being returned in the first
            // batch. These would be ignored by ChangeStreamTest.getOneChange().
            aggregateOptions: {cursor: {batchSize: 0}},
        });
    }

    function runTest(watchMode) {
        const dbToStartOn = watchMode == WatchMode.kCluster ? db.getSiblingDB("admin") : db;
        const cst = new ChangeStreamTest(dbToStartOn);

        const coll = assertDropAndRecreateCollection(db, "change_stream_invalidate_resumability");

        let cursor = getChangeStream({cst: cst, watchMode: watchMode, coll: coll});

        // Create an 'insert' oplog entry.
        assert.writeOK(coll.insert({_id: 1}));

        // Drop the collection.
        assertDropCollection(db, coll.getName() + "_renamed");
        assert.commandWorked(coll.renameCollection(coll.getName() + "_renamed"));

        // We should get 2 oplog entries of type insert and invalidate.
        let change = cst.getOneChange(cursor);
        assert.eq(change.operationType, "insert", tojson(change));

        change = cst.getOneChange(cursor, true);
        assert.eq(change.operationType, "invalidate", tojson(change));

        // Try resuming from the invalidate.
        const command = {
            aggregate: (watchMode == WatchMode.kCollection ? coll.getName() : 1),
            pipeline:
            [{$changeStream: getChangeStreamStage(watchMode, change._id)},
             // Throw in another stage, so that a collation is needed.
             {$project: {x: 5}}],
            cursor: {}
        };
        const res = dbToStartOn.runCommand(command);
        assert.commandWorked(res);

        cst.cleanUp();
    }

    for (let modeName of Object.keys(WatchMode)) {
        jsTestLog("Running test in mode " + modeName);
        runTest(WatchMode[modeName]);
    }
}());
