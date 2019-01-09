// Test that a pipeline with a $lookup stage on a sharded foreign collection may be run on a mongod.
(function() {
    const sharded = new ShardingTest({mongos: 1, shards: 2});

    assert.commandWorked(sharded.s.adminCommand({enableSharding: "test"}));
    sharded.ensurePrimaryShard('test', sharded.shard0.shardName);

    // TODO: Rewrite test in a way such that it can run with the mainColl sharded and unsharded.

    // Shard the main collection.
    sharded.shardColl("mainColl",
                      {_id: 1},  // shard key
                      {_id: 5},  // split
                      {_id: 5},  // move
                      "test",    // dbName
                      true       // waitForDelete
                      );

    // Shard the foreign collection.
    sharded.shardColl("foreignColl",
                      {_id: 1},  // shard key
                      {_id: 5},  // split
                      {_id: 5},  // move
                      "test",    // dbName
                      true       // waitForDelete
                      );

    const coll = sharded.s.getDB('test').mainColl;
    const foreignColl = sharded.s.getDB('test').foreignColl;
    const smallColl = sharded.s.getDB("test").smallColl;

    const nDocsMainColl = 10;
    const nDocsForeignColl = 2 * nDocsMainColl;

    for (let i = 0; i < nDocsMainColl; i++) {
        assert.commandWorked(coll.insert({_id: i, collName: "mainColl", foreignId: i}));

        assert.commandWorked(
            foreignColl.insert({_id: 2 * i, key: i, collName: "foreignColl", data: "hello-0"}));
        assert.commandWorked(
            foreignColl.insert({_id: 2 * i + 1, key: i, collName: "foreignColl", data: "hello-1"}));
    }
    assert.commandWorked(smallColl.insert({_id: 0, collName: "smallColl"}));

    print("ian: main coll shard distribution: " + tojson(coll.getShardDistribution()));
    print("ian: foreign coll shard distribution: " + tojson(foreignColl.getShardDistribution()));

    (function() {
        // Run a pipeline which must be merged on a shard. This should force the $lookup (on the
        // sharded collection) to be run on a mongod.
        pipeline = [
            {
              $lookup: {
                  localField: "foreignId",
                  foreignField: "key",
                  from: "foreignColl",
                  as: "foreignDoc"
              }
            },
            {$_internalSplitPipeline: {mergeType: "anyShard"}}
        ];

        print("ian: Running aggregation");
        const results = coll.aggregate(pipeline, {allowDiskUse: true}).toArray();
        print("ian: " + tojson(results));
        assert.eq(results.length, nDocsMainColl);
        for (let i = 0; i < results.length; i++) {
            assert.eq(results[i].foreignDoc.length, 2, results[i]);
        }
    })();

    (function() {
        // TODO
        return;

        // Pipeline with $lookup inside $lookup.
        pipeline = [
            {
              $lookup: {
                  from: "foreignColl",
                  as: "foreignDoc",
                  pipeline: [
                      {$lookup: {from: "smallColl", as: "doc", pipeline: []}},
                  ],
              }
            },
            {$_internalSplitPipeline: {mergeType: "anyShard"}}
        ];
        const results = coll.aggregate(pipeline).toArray();
        print("ian: res is " + tojson(results));

        const expl = coll.explain().aggregate(pipeline);
        print("ian: explain is " + tojson(expl));

        assert.eq(results.length, nDocsMainColl);
        for (let i = 0; i < results.length; i++) {
            assert.eq(results[i].foreignDoc.length, nDocsForeignColl);
            for (let j = 0; j < nDocsForeignColl; j++) {
                // Each document pulled from the foreign collection should have one document from
                // "smallColl":
                assert.eq(results[i].foreignDoc[j].collName, "foreignColl");
                assert.eq(results[i].foreignDoc[j].doc.length, 1);
                assert.eq(results[i].foreignDoc[j].doc[0].collName, "smallColl");
            }
        }
    })();

    sharded.stop();
})();
