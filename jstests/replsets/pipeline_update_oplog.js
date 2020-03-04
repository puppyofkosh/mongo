(function() {
const rst = new ReplSetTest({name: "pipeline_update_oplog", nodes: 2});

rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const primaryColl = primary.getDB("test").coll;
// populate the collection
for (i = 0; i < 5; i++) {
    primaryColl.insert({_id: i, x: i, y: i});
}

rst.awaitReplication();

primaryColl.update({x: 1}, {$set: {a: "old style", y: 999}});

rst.awaitReplication();

print("running pipeline style update now");
// Project away x.
primaryColl.update({x: 3}, [{$set: {a: "new style", y: 3}}]);
primaryColl.update({x: 2}, [{$set: {a: "new style", y: 1}}, {$project: {x: 0}}]);

rst.awaitReplication();

assert.eq(primaryColl.find({x: 1}).toArray(), [{_id: 1, x: 1, y: 999, a: "old style"}]);
assert.eq(primaryColl.find({x: 3}).toArray(), [{_id: 3, x: 3, y: 3, a: "new style"}]);
assert.eq(primaryColl.find({_id: 2}).toArray(), [{_id: 2, y: 1, a: "new style"}]);

rst.stopSet();
})();
