/**
 * Tests that a change stream can use a user-specified, or collection-default collation.
python buildscripts/resmoke.py --basePort=40000 --dbpathPrefix=~/two/data/ --suites=change_streams_sharded_collections_passthrough jstests/change_streams/change_stream_collation_fast_fail.js --repeat=1000 --continueOnFailure > log.txt

myresmoke --suites=change_streams_sharded_collections_passthrough jstests/change_streams/change_stream_collation_fast_fail.js --repeat=1000 > log.txt
 
 */
(function() {
    "use strict";

    load("jstests/libs/collection_drop_recreate.js");  // For assert[Drop|Create]Collection.
    load("jstests/libs/change_stream_util.js");        // For 'ChangeStreamTest'.

    let cst = new ChangeStreamTest(db);

    const caseInsensitive = {locale: "en_US", strength: 2};

    let caseInsensitiveCollection = "change_stream_case_insensitive";
    assertDropCollection(db, caseInsensitiveCollection);

    // Test that you can open a change stream before the collection exists, and it will use the
    // simple collation.
    const simpleCollationStream = cst.startWatchingChanges(
        {pipeline: [{$changeStream: {}}], collection: caseInsensitiveCollection});

    // Create the collection with a non-default collation - this should invalidate the stream we
    // opened before it existed.
    caseInsensitiveCollection =
        assertCreateCollection(db, caseInsensitiveCollection, {collation: caseInsensitive});

    cst.assertNextChangesEqual({
        cursor: simpleCollationStream,
        expectedChanges: [{operationType: "invalidate"}],
        expectInvalidate: true
    });

    const implicitCaseInsensitiveStream = cst.startWatchingChanges({
        pipeline: [
            {$changeStream: {}},
            {$match: {"fullDocument.text": "abc"}},
            // Be careful not to use _id in this projection, as startWatchingChanges() will exclude
            // it by default, assuming it is the resume token.
            {$project: {docId: "$documentKey._id"}}
        ],
        collection: caseInsensitiveCollection
    });
    const explicitCaseInsensitiveStream = cst.startWatchingChanges({
        pipeline: [
            {$changeStream: {}},
            {$match: {"fullDocument.text": "abc"}},
            {$project: {docId: "$documentKey._id"}}
        ],
        collection: caseInsensitiveCollection,
        aggregateOptions: {collation: caseInsensitive}
    });

    assert.writeOK(caseInsensitiveCollection.insert({_id: 0, text: "aBc"}));
    assert.writeOK(caseInsensitiveCollection.insert({_id: 1, text: "abc"}));

    print("ian: about to run buggy statement");
    cst.assertNextChangesEqual(
        {cursor: implicitCaseInsensitiveStream, expectedChanges: [{docId: 0}, {docId: 1}]});
    print("ian: ran buggy statement");
    cst.assertNextChangesEqual(
        {cursor: explicitCaseInsensitiveStream, expectedChanges: [{docId: 0}, {docId: 1}]});

    return;
})();
