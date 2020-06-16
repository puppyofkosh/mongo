(function() {
const rst = new ReplSetTest({name: "pipeline_update_oplog", nodes: 2});

rst.startSet();
rst.initiate();

const primary = rst.getPrimary();
const primaryColl = primary.getDB("test").coll;
const secondary = rst.getSecondary();
const secondaryColl = secondary.getDB("test").coll;

// Used for padding documents, in order to make full replacements expensive.
function makeGiantStr() {
    let s = "";
    for (let i = 0; i < 1024; i++) {
        s += "_";
    }
    return s;
}

const kGiantStr = makeGiantStr();

let idGenGlob = 0;
function generateId() {
    return idGenGlob++;
}

function checkOplogEntry(node, expectUpdateOplogEntry) {
    const oplog = node.getDB("local").getCollection("oplog.rs");

    const res = oplog.find().sort({"ts": -1}).limit(1).toArray();
    assert.eq(res.length, 1);

    if (expectUpdateOplogEntry) {
        assert.eq(res[0].o.$v, 2, res[0]);
        assert.eq(typeof (res[0].o.diff), "object", res[0]);

        // Verify that none of the fields in the 'insert' section of the oplog entry are present
        // in the 'delete' section of the oplog entry.
        const insertSection = res[0].o.i;
        const deleteSection = res[0].o.d;
        if (insertSection && deleteSection) {
            for (const fieldPath of Object.keys(insertSection)) {
                assert(!(fieldPath in deleteSection));
            }
        }
    } else {
        assert.eq(res[0].o.hasOwnProperty("$v"), false, res[0]);
    }
}

// Last parameter is whether we expect the oplog entry to only record an update rather than
// replacement.
function testUpdateReplicates(document, pipeline, expectedPostImage, expectUpdateOplogEntry) {
    const idKey = document._id;
    assert.commandWorked(primaryColl.insert(document));
    assert.commandWorked(primaryColl.update({_id: idKey}, pipeline));

    rst.awaitReplication();
    const secondaryDoc = secondaryColl.findOne({_id: idKey});
    const oplog = primary.getDB("local").getCollection("oplog.rs");
    const res = oplog.find().sort({"ts": -1}).limit(1).toArray();
    assert.eq(res.length, 1);
    jsTestLog("oplog entry: " + tojson(res));
    assert.eq(expectedPostImage, secondaryDoc);

    // Don't remove it. At the end we want the DBHash checker will make sure there's no corruption.
    checkOplogEntry(primary, expectUpdateOplogEntry);
}

const oplog = primary.getDB("local").getCollection("oplog.rs");
let id;

// Removing fields.
id = generateId();
    testUpdateReplicates({_id: id, x: 3, y: 3, giantStr: kGiantStr},
                         [{$unset: ["x", "y"]}],
                         {_id: id, giantStr: kGiantStr},
                         true);
    
// Adding a field and updating an existing one.
id = generateId();
testUpdateReplicates(
    {_id: id, x: 3, y: 3}, [{$set: {a: "foo", y: 3}}], {_id: id, x: 3, y: 3, a: "foo"}, true);

// Updating a subfield.
id = generateId();
testUpdateReplicates({_id: id, x: 4, subObj: {a: 1, b: 2}},
                     [{$set: {"subObj.a": "foo", y: 1}}],
                     {_id: id, x: 4, subObj: {a: "foo", b: 2}, y: 1},
                     true);

id = generateId();
testUpdateReplicates({_id: id, x: 4, subObj: {a: NumberLong(1), b: 2}},
                     [{$set: {"subObj.a": 1, y: 1}}],
                     {_id: id, x: 4, subObj: {a: 1, b: 2}, y: 1},
                     true);

// Adding a subfield.
id = generateId();
testUpdateReplicates({_id: id, subObj: {a: 1, b: 2}},
                     [{$set: {"subObj.c": "foo"}}],
                     {_id: id, subObj: {a: 1, b: 2, c: "foo"}},
                     true);

// Add subfield and remove top level field.
id = generateId();
testUpdateReplicates({_id: id, subObj: {a: 1, b: 2}, toRemove: "foo", giantStr: kGiantStr},
                     [{$project: {subObj: 1, giantStr: 1}}, {$set: {"subObj.c": "foo"}}],
                     {_id: id, subObj: {a: 1, b: 2, c: "foo"}, giantStr: kGiantStr},
                     true);

// Inclusion projection dropping a field (cannot be handled by static analysis).
id = generateId();
testUpdateReplicates({_id: id, x: "foo", subObj: {a: 1, b: 2}},
                     [{$project: {subObj: 1}}],
                     {_id: id, subObj: {a: 1, b: 2}},
                     true);

// Inclusion projection dropping a subfield (subObj.b).
id = generateId();
testUpdateReplicates({_id: id, x: "foo", subObj: {a: 1, b: 2}, giantStr: kGiantStr},
                     [{$project: {subObj: {a: 1}, giantStr: 1}}],
                     {_id: id, subObj: {a: 1}, giantStr: kGiantStr},
                     true);

// Replace root with a similar document.
id = generateId();
testUpdateReplicates({_id: id, x: "foo", subObj: {a: 1, b: 2}, giantStr: kGiantStr},
                     [{$replaceRoot: {newRoot:
                                      {x: "bar", subObj: {a: 1, b: 2}, giantStr: kGiantStr}}}],
                     {_id: id, x: "bar", subObj: {a: 1, b: 2}, giantStr: kGiantStr},
                     true);

// Replace root with a very different document should fall back to replacement style update.
id = generateId();
testUpdateReplicates(
    {_id: id, x: "foo", subObj: {a: 1, b: 2}},
    [{$replaceRoot: {newRoot: {_id: id, newField: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}}}],
    {_id: id, newField: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
    false);

// Make sure nothing goes crazy with arrays. These documents have padding so that replacing the
// entire thing (rather than resetting just the array field) would be expensive.
id = generateId();
testUpdateReplicates({_id: id, x: kGiantStr, arrField: [{x: 1}, {x: 2}]},
                     [{$set: {"arrField.x": 5}}],
                     {_id: id, x: kGiantStr, arrField: [{x: 5}, {x: 5}]},
                     true);

// Reorder fields with replaceRoot. (This requires internalRemoveTombstones to be used)
id = generateId();
testUpdateReplicates({_id: id, padding: kGiantStr, x: "foo", y: "bar"},
                     [{$replaceRoot: {newRoot: {_id: id, padding: kGiantStr, y: "bar", x: "foo"}}}],
                     {_id: id, padding: kGiantStr, y: "bar", x: "foo"},
                     true);

id = generateId();
testUpdateReplicates({_id: id, p: kGiantStr, a: 1, b: 1, c: 1, d: 1},
                     [{$replaceRoot: {newRoot: {_id: id, p: kGiantStr, a: 1, c: 1, b: 1, d: 1}}}],
                     {_id: id, p: kGiantStr, a: 1, c: 1, b: 1, d: 1},
                     true);

// Verify that 'insert' oplog entries work as expected.

// Combine upserts of existing fields and insertions of new fields.
id = generateId();
testUpdateReplicates(
    {_id: id, padding: kGiantStr, a: 1, b: {c: 2, d: {e: 3, f: 6}}, z: 3},
    [{$unset: ["b.d.f"]}, {$set: {"b.a": 5, "b.b": 3, "b.c": 2, "b.d.d": 2, "b.d.e": 10, z: 7}}],
    {_id: id, padding: kGiantStr, a: 1, b: {c: 2, d: {e: 10, d: 2}, a: 5, b: 3}, z: 7},
    true);

// Arrays.

// Modify a random element of an array.
id = generateId();
testUpdateReplicates({_id: id, padding: kGiantStr, a: [1, 2, 3, 4, 5]},
                     [{$set: {a: [1, 2, 999, 4, 5]}}],
                     {_id: id, padding: kGiantStr, a: [1, 2, 999, 4, 5]},
                     true);
let oplogEntry = oplog.find().sort({ts: -1}).limit(1).toArray()[0];

// Modify an object inside an array.
id = generateId();
testUpdateReplicates({_id: id, padding: kGiantStr, a: [1, 2, 3, {b: 1}, 5]},
                     [{$set: {a: [1, 2, 3, {b: 2}, 5]}}],
                     {_id: id, padding: kGiantStr, a: [1, 2, 3, {b: 2}, 5]},
                     true);

// object inside an array inside an object inside an array
id = generateId();
testUpdateReplicates({_id: id, padding: kGiantStr, a: [1, 2, 3, {b: [{c: 1}]}, 5]},
                     [{$set: {a: [1, 2, 3, {b: [{c: 999}]}, 5]}}],
                     {_id: id, padding: kGiantStr, a: [1, 2, 3, {b: [{c: 999}]}, 5]},
                     true);

// Case where we append to an array.
id = generateId();
testUpdateReplicates({_id: id, padding: kGiantStr, a: [1, 2, 3]},
                     [{$set: {a: [1, 2, 3, 4, 5]}}],
                     {_id: id, padding: kGiantStr, a: [1, 2, 3, 4, 5]},
                     true);

// Case where we make an array shorter.
// TODO: Think about this!!
id = generateId();
testUpdateReplicates({_id: id, padding: kGiantStr, a: [1, 2, 3]},
                     [{$set: {a: [1, 2]}}],
                     {_id: id, padding: kGiantStr, a: [1, 2]},
                     true);

// Change element of array AND shorten it
id = generateId();
testUpdateReplicates({_id: id, padding: kGiantStr, a: [1, {b: 10}, 3]},
                     [{$set: {a: [1, {b: 9}]}}],
                     {_id: id, padding: kGiantStr, a: [1, {b: 9}]},
                     true);

// Remove element from the middle of an array. Should still use a delta, and only rewrite the last
// parts of the array.
id = generateId();
testUpdateReplicates({_id: id, padding: kGiantStr, a: [1, 2, 999, 3, 4]},
                     [{$set: {a: [1, 2, 3, 4]}}],
                     {_id: id, padding: kGiantStr, a: [1, 2, 3, 4]},
                     true);

// TODO: More tests!


// TODO: Remove this. (Useful for debugging though).
if (false) {
    printjson(oplog.find().sort({"ts": -1}).toArray());
}

rst.stopSet();
})();
