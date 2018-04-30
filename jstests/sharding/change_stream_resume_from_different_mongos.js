// Test resuming a change stream on a node other than the one it was started on. Accomplishes this
// by triggering a stepdown.
(function() {
    "use strict";
    // For supportsMajorityReadConcern().
    load("jstests/multiVersion/libs/causal_consistency_helpers.js");
    load("jstests/libs/change_stream_util.js");        // For ChangeStreamTest.
    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.

    const st = new ShardingTest({shards: 2, rs: {nodes: 3}});
    if (!supportsMajorityReadConcern()) {
        jsTestLog("Skipping test since storage engine doesn't support majority read concern.");
        return;
    }

    const watchMode = ChangeStreamTest.WatchMode.kCollection;
    jsTestLog("Running test for mode " + watchMode);

    const sDB = st.s.getDB("test");
    const coll = assertDropAndRecreateCollection(sDB, "change_stream_failover");

    // Split so ids < 5 are for one shard, ids >= 5 for another.
    st.shardColl(coll,
                 {_id: 1}, // key
                 {_id: 5}, // split
                 {_id: 6}, // move
                 "test",
                 true);

    // Write some dummy data.
    assert.writeOK(coll.insert({_id: 1, x: "somestring"}, {writeConcern: {w: "majority"}}));
    assert.writeOK(coll.insert({_id: 6, x: "somestring"}, {writeConcern: {w: "majority"}}));

    // Be sure we'll only read from the primaries.
    st.s.setReadPref("primary");

    // Open a changeStream.
    const cst =
          new ChangeStreamTest(ChangeStreamTest.getDBForChangeStream(watchMode, sDB));

    let changeStream = cst.getChangeStream({watchMode: watchMode, coll: coll});

    // Be sure we can read from the change stream. Write some documents that will end up on
    // each shard.
    assert.writeOK(coll.insert({_id: 0, x: "somestring"}, {writeConcern: {w: "majority"}}));
    assert.writeOK(coll.insert({_id: 7, x: "somestring"}, {writeConcern: {w: "majority"}}));
    assert.writeOK(coll.insert({_id: 100, x: "somestring"}, {writeConcern: {w: "majority"}}));

    const firstChange = cst.getOneChange(changeStream);
    print("ian: change is " + tojson(firstChange));
    assert.docEq(firstChange.fullDocument, {_id: 0, x: "somestring"});

    let change = cst.getOneChange(changeStream);
    print("ian: change is " + tojson(change));
    assert.docEq(change.fullDocument, {_id: 7, x: "somestring"});

    change = cst.getOneChange(changeStream);
    print("ian: change is " + tojson(change));
    assert.docEq(change.fullDocument, {_id: 100, x: "somestring"});

    st.stop();
}());
