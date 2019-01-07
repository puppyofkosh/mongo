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

    coll = sharded.s.getDB('test').mainColl;
    foreignColl = sharded.s.getDB('test').foreignColl;

    for (let i = 0; i < 10; i++) {
        assert.commandWorked(coll.insert({_id: i, foreignId: i}));

        assert.commandWorked(foreignColl.insert({key: i, data: "hello-0"}));
        assert.commandWorked(foreignColl.insert({key: i, data: "hello-1"}));
    }

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
    print("ian: " + tojson(results));
    assert.eq(results.length, 20);

    const expl = coll.explain().aggregate(pipeline, {allowDiskUse: true});
    print("ian: explain" + tojson(expl));

    sharded.stop();
})();
