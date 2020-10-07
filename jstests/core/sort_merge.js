/**
 * Tests $or queries which can be answered with a SORT_MERGE stage.
 */
(function() {
    const coll = db.sort_merge;

    assert.commandWorked(coll.createIndex({filterFieldA:1, sortFieldA: 1, sortFieldB: 1}));
    assert.commandWorked(coll.createIndex({filterFieldA:1, sortFieldA: -1, sortFieldB: -1}));
    
    // Insert some random data and dump the seed to logs for easy reproductions.
    const seed = new Date().getTime();
    Random.srand(seed);
    print("Seed for test is " + seed);

    const kRandomValCeil = 5;
    // Generates a random value between [0, kRandomValCeil).
    function randomInt() {
        return Math.floor(Math.random() * kRandomValCeil);
    }

    for (let i = 0; i < 100; ++i) {
        assert.commandWorked(coll.insert({filterFieldA: randomInt(),
                                          filterFieldB: randomInt(),
                                          sortFieldA: randomInt(),
                                          sortFieldB: randomInt()}));
    }

    function isSorted(array, lessThanFunction) {
        for (let i = 1; i < array.length; ++i) {
            if (lessThanFunction(array[i], array[i - 1])) {
                return false;
            }
        }
        return true;
    }

    // TODO: Test compound sorts
    // TODO: Test descending sorts
    // TODO: Check explain output

    // Case where the children of the $or all require a FETCH and the sort has just one component.
    (function testFetchedChildrenSimpleSort() {
        const kFilterPredicates = [
            // $or with two children.
            {$or: [{filterFieldA: 4, filterFieldB: 4},
                   {filterFieldA: 3, filterFieldB: 3}]},

            // $or with three children.
            {$or: [{filterFieldA: 4, filterFieldB: 4},
                   {filterFieldA: 3, filterFieldB: 3},
                   {filterFieldA: 1, filterFieldB: 1}]},

            // $or with four children.
            {$or: [{filterFieldA: 4, filterFieldB: 4},
                   {filterFieldA: 3, filterFieldB: 3},
                   {filterFieldA: 2, filterFieldB: 2},
                   {filterFieldA: 1, filterFieldB: 1}]},
        ];

        const kSorts = [
            {sortPattern: {sortFieldA: 1},
             cmpFunction: (docA, docB) => docA.sortFieldA < docB.sortFieldA},
            {sortPattern: {sortFieldA: -1},
             cmpFunction: (docA, docB) => docA.sortFieldA > docB.sortFieldA},
            {sortPattern: {sortFieldA: 1, sortFieldB: 1},
             cmpFunction: (docA, docB) => docA.sortFieldA < docB.sortFieldA ||
             (docA.sortFieldA == docB.sortFieldA && docA.sortFieldB < docB.sortFieldB)},
            {sortPattern: {sortFieldA: -1, sortFieldB: -1},
             cmpFunction: (docA, docB) => docA.sortFieldA > docB.sortFieldA ||
             (docA.sortFieldA == docB.sortFieldA && docA.sortFieldB > docB.sortFieldB)},
        ];

        for (let sortInfo of kSorts) {
            for (let filter of kFilterPredicates) {
                let res = coll.find(filter).sort(sortInfo.sortPattern).toArray();
                assert(isSorted(res, sortInfo.cmpFunction),
                       () => "Assertion failed for filter: " + filter + "\n" +
                       "sort pattern " + sortInfo.sortPattern);
            }
        }
    })();

    // TODO: Delete this TODO or file a ticket.
    // TODO SERVER-XYZ: Test with children that are not fetched.

})();
