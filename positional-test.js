(function() {
    assert.commandWorked(db.adminCommand({setParameter: 1, traceExceptions: true}));

    const coll = db.c;
    coll.drop();
    assert.commandWorked(coll.insert({a: [{b: 1}, {b: 2}]}));
    let res = coll.find({"a.b": 1}, {"a.b.$": 1}).toArray();
    assert.eq(res[0].a, [{b: 1}]);

    coll.drop();
    assert.commandWorked(coll.insert({x: {y: [{a: 1, b: 1}, {a: 1, b: 2}]}}));
    res = coll.find({'x.y.a': 1}, {'x.y.$': 1}).toArray();
    assert.eq(res[0].x, {y: [{a: 1, b: 1}]});

    coll.drop();
    assert.commandWorked(coll.insert({a: [{b: [1, 2]}, {b: [2, 3]}]}));
    res = coll.find({}, {d: 1, "a.b": {$slice: 1}}).toArray();
    assert.eq(res[0].a, [{b: [1]}, {b: [2]}]);

    coll.drop();
    assert.commandWorked(coll.insert({a: [{b: {c: [1, 2]}}, {b: [{c: [2, 3]}, {c: [3, 4]}]}]}));
    res = coll.find({}, {d: 1, "a.b.c": {$slice: 1}}).toArray();
    assert.eq(res[0].a, [{b: {c: [1]}}, {b: [{c: [2]}, {c: [3]}]}]);

    // Same with exclusion projection
    res = coll.find({}, {d: 0, "a.b.c": {$slice: 1}}).toArray();
    assert.eq(res[0].a, [{b: {c: [1]}}, {b: [{c: [2]}, {c: [3]}]}]);

    let err = assert.throws(() => coll.find({}, {"a.b": 1, "a.b.c": 1}).itcount());
    assert.commandFailedWithCode(err, ErrorCodes.BadValue);
})();
