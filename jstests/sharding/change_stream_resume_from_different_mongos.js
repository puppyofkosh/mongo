// Test resuming a change stream on a mongos other than the one the change stream was started on.
(function() {
    "use strict";
    // For supportsMajorityReadConcern().
    load("jstests/multiVersion/libs/causal_consistency_helpers.js");
    load("jstests/libs/change_stream_util.js");        // For ChangeStreamTest.
    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.

    const st = new ShardingTest({
        shards: 2,
        mongos: 2,
        rs: {nodes: 3, setParameter: {periodicNoopIntervalSecs: 1, writePeriodicNoops: true}}
    });
    if (!supportsMajorityReadConcern()) {
        jsTestLog("Skipping test since storage engine doesn't support majority read concern.");
        return;
    }

    for (let key of Object.keys(ChangeStreamTest.WatchMode)) {
        const watchMode = ChangeStreamTest.WatchMode[key];
        jsTestLog("Running test for mode " + watchMode);

        const s0DB = st.s0.getDB("test");
        const s1DB = st.s1.getDB("test");
        const coll = assertDropAndRecreateCollection(s0DB, "change_stream_failover");

        // Split so ids < 5 are for one shard, ids >= 5 for another.
        st.shardColl(coll,
                     {_id: 1},  // key
                     {_id: 5},  // split
                     {_id: 6},  // move
                     "test",    // dbName
                     false      // waitForDelete
                     );

        // Open a changeStream.
        const cst = new ChangeStreamTest(ChangeStreamTest.getDBForChangeStream(watchMode, s0DB));
        let changeStream = cst.getChangeStream({watchMode: watchMode, coll: coll});

        // Be sure we can read from the change stream. Write some documents that will end up on
        // each shard.
        assert.writeOK(coll.insert({_id: 0}));
        assert.writeOK(coll.insert({_id: 7}));
        assert.writeOK(coll.insert({_id: 2}));

        const firstChange = cst.getOneChange(changeStream);
        assert.docEq(firstChange.fullDocument, {_id: 0});

        const expectedChanges = [
            {
              documentKey: {_id: 7},
              fullDocument: {_id: 7},
              ns: {db: s0DB.getName(), coll: coll.getName()},
              operationType: "insert",
            },
            {
              documentKey: {_id: 2},
              fullDocument: {_id: 2},
              ns: {db: s0DB.getName(), coll: coll.getName()},
              operationType: "insert",
            },
        ];

        // Now resume using the resume token from the first change on a different mongos.
        const otherCst =
            new ChangeStreamTest(ChangeStreamTest.getDBForChangeStream(watchMode, s1DB));

        const resumeCursor = otherCst.getChangeStream(
            {watchMode: watchMode, coll: coll, resumeAfter: firstChange._id});

        // Be sure we can read the remaining changes.
        otherCst.assertNextChangesEqual({cursor: resumeCursor, expectedChanges: expectedChanges});
    }

    st.stop();
}());
