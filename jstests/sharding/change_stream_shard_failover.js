// Test resuming a change stream on a node other than the one it was started on. Accomplishes this
// by triggering a stepdown.
(function() {
    "use strict";
    // For supportsMajorityReadConcern().
    load("jstests/multiVersion/libs/causal_consistency_helpers.js");
    load("jstests/libs/change_stream_util.js");        // For ChangeStreamTest.
    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.

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

        // Split so ids < 5 are for one shard, ids >= 5 for another.
        st.shardColl(coll, {_id: 1}, {_id: 5}, {_id: 6}, "test", false);

        // Be sure we'll only read from the primaries.
        st.s.setReadPref("primary");

        // Open a changeStream.
        const cst = new ChangeStreamTest(ChangeStreamTest.getDBForChangeStream(watchMode, sDB));

        let changeStream = cst.getChangeStream({watchMode: watchMode, coll: coll});

        // Be sure we can read from the change stream. Write some documents that will end up on
        // each shard. Use {w: "majority"} so that we're still guaranteed to be able to read after
        // the failover.
        assert.writeOK(coll.insert({_id: 0}, {writeConcern: {w: "majority"}}));
        assert.writeOK(coll.insert({_id: 7}, {writeConcern: {w: "majority"}}));
        assert.writeOK(coll.insert({_id: 2}, {writeConcern: {w: "majority"}}));

        const firstChange = cst.getOneChange(changeStream);
        assert.docEq(firstChange.fullDocument, {_id: 0});

        // Make one of the primaries step down.
        const oldPrimary = st.rs0.getPrimary();
        print("Nodes are " + tojson(st.rs0.nodes));
        assert.commandWorked(st.rs0.getSecondary().adminCommand({replSetStepUp: 1}));

        st.rs0.awaitNodesAgreeOnPrimary();
        const newPrimary = st.rs0.getPrimary();
        // Be sure we got a different node that the previous primary.
        assert.neq(newPrimary.port, oldPrimary.port);

        // Be sure we can still read from the change stream.
        const expectedChanges = [
            {
              documentKey: {_id: 7},
              fullDocument: {_id: 7},
              ns: {db: sDB.getName(), coll: coll.getName()},
              operationType: "insert",
            },
            {
              documentKey: {_id: 2},
              fullDocument: {_id: 2},
              ns: {db: sDB.getName(), coll: coll.getName()},
              operationType: "insert",
            },
        ];
        cst.assertNextChangesEqual({cursor: changeStream, expectedChanges: expectedChanges});

        // Now resume using the resume token from the first change (before the failover).
        const resumeCursor =
            cst.getChangeStream({watchMode: watchMode, coll: coll, resumeAfter: firstChange._id});

        // Be sure we can read the remaining changes.
        cst.assertNextChangesEqual({cursor: resumeCursor, expectedChanges: expectedChanges});

        // Step up the old primary again. Necessary since some validation hooks have connections
        // open on the primary and assume that a stepdown has not happened.
        assert.commandWorked(oldPrimary.adminCommand({replSetStepUp: 1}));
        st.rs0.awaitNodesAgreeOnPrimary();

        // Do another write, which we expect to fail this will force the underlying connection to
        // reselect which member of the replica set to talk to.
        assert.commandFailedWithCode(coll.insert({_id: 9}, {writeConcern: {w: "majority"}}),
                                     ErrorCodes.NotMaster);
    }

    st.stop();
}());
