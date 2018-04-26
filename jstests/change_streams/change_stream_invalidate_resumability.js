// Tests resuming from $changeStream invalidate entries.
(function() {
    "use strict";

    load('jstests/replsets/libs/two_phase_drops.js');  // For 'TwoPhaseDropCollectionTest'.
    load("jstests/libs/change_stream_util.js");
    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.

    for (let modeName of Object.keys(ChangeStreamTest.WatchMode)) {
        const watchMode = ChangeStreamTest.WatchMode[modeName];
        jsTestLog("Running test in mode " + modeName);

        const dbToAggOn = ChangeStreamTest.getDBForChangeStream(watchMode, db);
        const cst = new ChangeStreamTest(dbToAggOn);

        const coll = assertDropAndRecreateCollection(db, "change_stream_invalidate_resumability");

        let cursor = cst.getChangeStream({watchMode: watchMode, coll: coll});

        // Create an 'insert' oplog entry.
        assert.writeOK(coll.insert({_id: 1}));

        // Drop the collection.
        assert.commandWorked(db.runCommand({drop: coll.getName()}));
        assert.soon(function() {
            return !TwoPhaseDropCollectionTest.collectionIsPendingDropInDatabase(db,
                                                                                 coll.getName());
        });

        // We should get 2 oplog entries of type insert and invalidate.
        let change = cst.getOneChange(cursor);
        assert.eq(change.operationType, "insert", tojson(change));

        change = cst.getOneChange(cursor, true);
        assert.eq(change.operationType, "invalidate", tojson(change));

        const command = {
            aggregate: ChangeStreamTest.getAggregateArg(watchMode, coll),
            pipeline: [
                {$changeStream: cst.getChangeStreamStage(watchMode, change._id)},
                // Throw in another stage, so that a collation is needed.
                {$project: {x: 5}}
            ],
            cursor: {}
        };
        const res = dbToAggOn.runCommand(command);

        if (watchMode == ChangeStreamTest.WatchMode.kCollection) {
            // TODO: This behavior (or at least, error code) will likely be changed in Nick's patch.
            assert.commandFailedWithCode(res, 40615);

            // Run the command again, but specify a collation.
            if (false) {
                // TODO: SERVER-32088 enable once Nick's patch is done.
                command.collation = {locale: "simple"};
                const res2 = dbToStartOn.runCommand(command);
                assert.commandWorked(res2);
            }
        } else {
            assert.commandWorked(res);
        }

        cst.cleanUp();
    }
}());
