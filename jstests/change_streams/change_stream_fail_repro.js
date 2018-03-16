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
    let db = st.s0.getDB("change_stream_collation");
    st.s0.setBalancerState(false);
    let cst = new ChangeStreamTest(db);

    const caseInsensitive = {locale: "en_US", strength: 2};

    let caseInsensitiveCollection = "change_stream_case_insensitive";
    let fullName = "test." + caseInsensitiveCollection;
    assertDropCollection(db, caseInsensitiveCollection);

    // Create the collection with a non-default collation - this should invalidate the stream we
    // opened before it existed.
    caseInsensitiveCollection =
        assertCreateCollection(db, caseInsensitiveCollection, {collation: caseInsensitive});

    assert.commandWorked(db.adminCommand({enableSharding: "test"}));

    let res = db.adminCommand(
        {shardCollection: fullName, key: {_id: 'hashed'}, collation: {locale: "simple"}});

    // TODO: Insert some docs to shard 0

    // moveChunk

    // read from CS

    st.stop();
})();
