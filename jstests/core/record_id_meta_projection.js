(function() {
    const pipeline = [{$project: {rid: {$meta: "recordId"}}}];

    assert.commandWorked(db.c.insert({a: 1}));
    assert.commandWorked(db.c.insert({a: 1}));
    assert.commandWorked(db.c.insert({a: 1}));

    printjson(db.c.find({}, {a: {$meta: "recordId"}}).explain());
    printjson(db.c.find({}, {a: {$meta: "recordId"}}).toArray());
})();
