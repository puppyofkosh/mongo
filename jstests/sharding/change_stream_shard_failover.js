// Test resuming a change stream on a node other than the one it was started on. Accomplishes this
// by triggering a stepdown.
(function() {
    "use strict";
    // For supportsMajorityReadConcern().
    load("jstests/multiVersion/libs/causal_consistency_helpers.js");
    load("jstests/libs/change_stream_util.js");        // For ChangeStreamTest.
    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.
    load("jstests/libs/misc_util.js");                 // For assert[Drop|Create]Collection.

    const st = new ShardingTest({
        shards: 2,
        rs: {nodes: 3, setParameter: {periodicNoopIntervalSecs: 1, writePeriodicNoops: true}}
    });
    if (!supportsMajorityReadConcern()) {
        jsTestLog("Skipping test since storage engine doesn't support majority read concern.");
        return;
    }

    for (let key of Object.keys(ChangeStreamTest.WatchMode)) {
        const watchMode = ChangeStreamTest.WatchMode[key];
        jsTestLog("Running test for mode " + watchMode);

        const sDB = st.s.getDB("test");
        const coll = assertDropAndRecreateCollection(sDB, "change_stream_failover");

        const nDocs = 10;

        // Split so ids < nDocs / 2 are for one shard, ids >= nDocs / 2 + 1 for another.
        st.shardColl(coll,
                     {_id: 1},              // key
                     {_id: nDocs / 2},      // split
                     {_id: nDocs / 2 + 1},  // move
                     "test",                // dbName
                     false                  // waitForDelete
                     );

        // Be sure we'll only read from the primaries.
        st.s.setReadPref("primary");

        // Open a changeStream.
        const cst = new ChangeStreamTest(ChangeStreamTest.getDBForChangeStream(watchMode, sDB));
        let changeStream = cst.getChangeStream({watchMode: watchMode, coll: coll});

        // Be sure we can read from the change stream. Write some documents that will end up on
        // each shard. Use a bulk write to increase the chance that two of the writes get the same
        // cluster time on each shard.
        const kIdsToInsert = [];
        for (let i = 0; i < nDocs / 2; i++) {
            // Interleave elements which will end up on shard 0 with elements that will end up on
            // shard 1.
            kIdsToInsert.push(i);
            kIdsToInsert.push(i + nDocs / 2);
        }

        // Write some documents that will end up on each shard. Use {w: "majority"} so that we're
        // still guaranteed to be able to read after the failover.
        assert.writeOK(coll.insert(kIdsToInsert.map(objId => {return {_id: objId}}),
                                   {writeConcern: {w: "majority"}}));

        const firstChange = cst.getOneChange(changeStream);

        // Make one of the primaries step down.
        const oldPrimary = st.rs0.getPrimary();
        assert.commandWorked(st.rs0.getSecondary().adminCommand({replSetStepUp: 1}));

        st.rs0.awaitNodesAgreeOnPrimary();
        const newPrimary = st.rs0.getPrimary();
        // Be sure we got a different node that the previous primary.
        assert.neq(newPrimary.port, oldPrimary.port);

        // Read the remaining documents from the original stream.
        const docsFoundInOrder = [firstChange];
        for (let i = 0; i < nDocs - 1; i++) {
            const change = cst.getOneChange(changeStream);
            assert.docEq(change.ns, {db: sDB.getName(), coll: coll.getName()});
            assert.eq(change.operationType, "insert");

            docsFoundInOrder.push(change);
        }

        // Assert that we found the documents we inserted (in any order).
        assert(MiscUtil.setEq(new Set(kIdsToInsert),
                              new Set(docsFoundInOrder.map(doc => doc.fullDocument._id))));

        // Now resume using the resume token from the first change (before the failover).
        const resumeCursor =
            cst.getChangeStream({watchMode: watchMode, coll: coll, resumeAfter: firstChange._id});

        // Be sure we can read the remaining changes in the same order as we read them initially.
        cst.assertNextChangesEqual(
            {cursor: resumeCursor, expectedChanges: docsFoundInOrder.splice(1)});

        // Step up the old primary again. Necessary since some validation hooks have connections
        // open on the primary and assume that a stepdown has not happened.
        assert.commandWorked(oldPrimary.adminCommand({replSetStepUp: 1}));
        st.rs0.awaitNodesAgreeOnPrimary();

        // Do another write, which we expect to fail. This will force the underlying connection to
        // reselect which member of the replica set to talk to. Necessary to run so that the
        // validation hook doesn't fail with the NotMaster error.
        assert.commandFailedWithCode(coll.insert({_id: nDocs * 2}, {writeConcern: {w: "majority"}}),
                                     ErrorCodes.NotMaster);
    }

    st.stop();
}());
