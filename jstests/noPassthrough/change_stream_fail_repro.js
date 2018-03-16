(function() {
    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.
    load("jstests/libs/change_stream_util.js");        // For 'ChangeStreamTest'.

    const st = new ShardingTest({
        shards: 2,
        mongos: 1,
        rs: {
            nodes: 1,
        },
    });
    const s = st.s;
    let db = s.getDB("test");

    print("Disabling balancer...");
    db.adminCommand( { balancerStop: 1 } )
    let cst = new ChangeStreamTest(db);

    const caseInsensitive = {locale: "en_US", strength: 2};

    let caseInsensitiveCollectionName = "change_stream_case_insensitive";
    assertDropCollection(db, caseInsensitiveCollectionName);

    const caseInsensitiveCollection =
          assertCreateCollection(db, caseInsensitiveCollectionName, {collation: caseInsensitive});


    assert.commandWorked(db.adminCommand({enableSharding: "test"}));
    st.ensurePrimaryShard('test', st.shard0.shardName);

    // TODO: Insert some docs (to shard 0)
    for (let i = 0; i < 10; i += 2) {
        assert.writeOK(caseInsensitiveCollection.insert({_id: i, text: "aBc"}));
        assert.writeOK(caseInsensitiveCollection.insert({_id: i + 1, text: "abc"}));
    }

    assert.commandWorked(caseInsensitiveCollection.createIndex({_id: "hashed"},
                                                               {collation: {locale: "simple"}}));
    let res = db.adminCommand(
        {shardCollection: caseInsensitiveCollection.getFullName(),
         key: {_id: 'hashed'}, collation: {locale: "simple"}});
    assert.commandWorked(res);

    // open CS.
    const implicitCaseInsensitiveStream = cst.startWatchingChanges({
        pipeline: [
            {$changeStream: {}},
            {$match: {"fullDocument.text": "abc"}},
            // Be careful not to use _id in this projection, as startWatchingChanges() will exclude
            // it by default, assuming it is the resume token.
            {$project: {docId: "$documentKey._id"}}
        ],
        collection: caseInsensitiveCollection
    });
    
    // moveChunk to shard 1.
    assert.commandWorked(db.adminCommand({
        moveChunk: caseInsensitiveCollection.getFullName(),
        find: {_id: 1},
        to: st.rs1.getURL(),
        _waitForDelete: false
    }));

    // Read from CS.
    cst.assertNextChangesEqual(
        {cursor: implicitCaseInsensitiveStream, expectedChanges: [{docId: 0}, {docId: 1}]});

    st.stop();
})();
