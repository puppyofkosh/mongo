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

    const s = new ShardingTest({shards: 2});
    const kNs = "test.slowcount";

    assert.commandWorked(s.getDB("admin").runCommand({enableSharding: "test"}));
    assert.commandWorked(s.getDB("admin").runCommand({shardCollection: kNs, key: {x: 1}}));

    const num = 10000;
    assert.commandWorked(s.s.adminCommand({split: kNs, middle: {x: num / 2}}));
    for (let i = 0; i < num; i++) {
        assert.writeOK(s.getDB("test").slowcount.insert({_id: i, one: 1, x: i}));
    }
    s.startBalancer();

    // The balancer should move one of the chunks off of the primary shard.
    assert.soon(function() {
        var d0Chunks = s.getDB("config").chunks.count({ns: kNs, shard: "shard0000"});
        var d1Chunks = s.getDB("config").chunks.count({ns: kNs, shard: "shard0001"});
        var totalChunks = s.getDB("config").chunks.count({ns: kNs});

        print("chunks: " + d0Chunks + " " + d1Chunks + " " + totalChunks);

        // Run a non-"fast count" (a count with a predicate). In this case the predicate is always
        // true, so we should get the total number of documents. The count should be accurate even
        // while the moveChunk is happening. This check is the crux of the test.
        assert.eq(s.getDB("test").slowcount.count({one: 1}), num);

        return d0Chunks > 0 && d1Chunks > 0 && (d0Chunks + d1Chunks == totalChunks);
    }, "Chunks failed to balance", 60000, 500);

    s.stop();
})();
