/**
 * Tests that Collection.count(), when run with a predicate (not a "fast count"), filters out
 * orphan documents. This is intended to test the fix for SERVER-3645.
 *
 * The test works by inserting a bunch of documents, enabling the balancer to trigger a moveChunk,
 * and while the moveChunk is happening, running count().
 *
 * Resource intensive because it writes 10000 documents.
 * @tags: [resource_intensive]
 */
(function() {
    "use strict";

    const st = new ShardingTest({shards: 2});
    const shards = st.s.getCollection("config.shards").find().toArray();
    const kNs = "test.slowcount";
    const shard0Coll = st.shard0.getCollection(kNs);
    const admin = st.getDB("admin");
    const num = 10000;
    const middle = num / 2;

    assert.commandWorked(admin.runCommand({enableSharding: "test"}));
    assert.commandWorked(admin.runCommand({movePrimary: "test", to: shards[0]._id}));
    assert.commandWorked(admin.runCommand({shardCollection: kNs, key: {x: 1}}));

    assert.commandWorked(admin.runCommand({split: kNs, middle: {x: middle}}));
    assert.commandWorked(admin.runCommand(
        {moveChunk: kNs, find: {x: middle}, to: shards[1]._id, _waitForDelete: true}));

    function getNthDocument(n) {
        return {_id: n, one: 1, x: n};
    }

    // Insert some docs.
    for (let i = 0; i < num; i++) {
        assert.writeOK(st.getDB("test").slowcount.insert(getNthDocument(i)));
    }

    // Insert some orphan documents to shard 0. These are just documents outside the range
    // which shard 0 owns.
    for (let i = middle + 1; i < middle + 11; i++) {
        assert.writeOK(shard0Coll.insert(getNthDocument(i)));
    }

    // Run a count on the whole collection. The orphaned documents on shard 0 shouldn't be double
    // counted.
    assert.eq(st.getDB("test").slowcount.count({one: 1}), num);

    st.stop();
})();
