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
primaryColl.insert({_id: 5, x: 5, subObj: {a: 1, b: 2}});
primaryColl.insert({_id: 6, x: 6, subObj: {a: 1, b: 2}});
primaryColl.insert({_id: 7, x: 7, subObj: {a: [{b: 1}]}});

rst.awaitReplication();

primaryColl.update({x: 1}, {$set: {a: "old style", y: 999}});

rst.awaitReplication();

// TODO: Rewrite this test to have a list of documents, replacements, and expected results. Be sure
// not to remove the documents since we want the dbhash thing to run.

print("running pipeline style update now");
// Project away x.
primaryColl.update({x: 2}, [{$set: {a: "new style", y: 1}}, {$project: {x: 0}}]);
primaryColl.update({x: 3}, [{$set: {a: "new style", y: 3}}]);
primaryColl.update({x: 4}, {$set: {a: "new style", y: 3}, $unset: {x: ""}});
primaryColl.update({x: 5}, [{$set: {"subObj.a": "foo", y: 1}}]);
// TODO: Try this case.
// primaryColl.update({x: 6}, [{$unset: ["subObj.a"]}]);

primaryColl.update({x: 7}, [{$set: {"subObj.a.b": "foo"}}]);

rst.awaitReplication();

// Check the latest oplog entry has roughly the format we expect.
function checkOplogEntry(node) {
    const oplog = node.getDB("local").getCollection("oplog.rs");

    printjson(oplog.find().toArray());
    const res = oplog.find().sort({"ts": -1}).limit(1).toArray();
    assert.eq(res.length, 1);

    assert.eq(res[0].o.$v, 2);
    assert.eq(typeof (res[0].o.$set) == "object" || typeof (res[0].o.$unset) == "object", true);
};

checkOplogEntry(primary);
checkOplogEntry(rst.getSecondary());

assert.eq(primaryColl.find({x: 1}).toArray(), [{_id: 1, x: 1, y: 999, a: "old style"}]);
assert.eq(primaryColl.find({x: 3}).toArray(), [{_id: 3, x: 3, y: 3, a: "new style"}]);
assert.eq(primaryColl.find({_id: 2}).toArray(), [{_id: 2, y: 1, a: "new style"}]);
assert.eq(primaryColl.find({_id: 5}).toArray(), [{_id: 5, x: 5, subObj: {a: "foo", b: 2}, y: 1}]);
// assert.eq(primaryColl.find({_id: 6}).toArray(), [{_id: 6, x: 6, subObj: {b: 2}}]);

rst.stopSet();
})();
