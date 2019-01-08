(function() {
    const sharded = new ShardingTest({mongos: 1, shards: 2});

    assert.commandWorked(sharded.s.adminCommand({enableSharding: "test"}));

    assert.commandWorked(sharded.s.adminCommand({shardCollection: "test.mainColl", key: {_id: 1}}));

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
            foreignColl.insert({key: i, collName: "foreignColl", data: "hello-0"}));
        assert.commandWorked(
            foreignColl.insert({key: i, collName: "foreignColl", data: "hello-1"}));
    }
    assert.commandWorked(smallColl.insert({_id: 0, collName: "smallColl"}));

    (function() {
        // Run a pipeline which must be merged on a shard. This should force the $lookup (on the
        // sharded collection) to run on the primary shard.
        pipeline = [
            {
              $lookup: {
                  localField: "foreignId",
                  foreignField: "key",
                  from: "foreignColl",
                  as: "foreignDoc"
              }
            },
            {$unwind: {path: "$foreignDoc", includeArrayIndex: "index"}},
            {$sort: {"foreignDoc.data": 1, _id: 1}},
            {$_internalSplitPipeline: {mergeType: "primaryShard"}}
        ];

        const results = coll.aggregate(pipeline, {allowDiskUse: true}).toArray();
        assert.eq(results.length, nDocsForeignColl);
    })();

    (function() {
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
            {$_internalSplitPipeline: {mergeType: "primaryShard"}}
        ];
        const results = coll.aggregate(pipeline).toArray();
        print("ian: res is " + tojson(results));

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
