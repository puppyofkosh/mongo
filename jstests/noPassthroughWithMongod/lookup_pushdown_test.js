(function() {
    "use strict";

    //assert.commandWorked(db.adminCommand({setParameter: 1, "traceExceptions": true}));

    const groupBy = db.groupBy;
    assert.commandWorked(groupBy.insert({a: 1, b: 1}));
    assert.commandWorked(groupBy.insert({a: 1, b: 1}));
    assert.commandWorked(groupBy.insert({a: 2, b: 1}));
    assert.commandWorked(groupBy.insert({a: 2, b: 1}));

    assert.commandWorked(groupBy.insert({a: {b: 3}, c: 0}));
    assert.commandWorked(groupBy.insert({a: {b: 3}, c: -1}));
    assert.commandWorked(groupBy.insert({a: {b: 4}, c: 2}));
    assert.commandWorked(groupBy.insert({a: {b: 4}, c: 3}));

    let pipeline = [{$match: {"a.b": {$type: "number"}}}, {$group: {_id: "$a.b", minVal: {$min: "$c"}}}];
    print("ian: running expl " + tojson(groupBy.explain().aggregate(pipeline)));
    print("ian: running query " + tojson(groupBy.aggregate(pipeline).toArray()));

    // $group with multi planning
    {
        const groupByMp = db.groupByMp;
        assert.commandWorked(groupBy.insert({ka: 1, kb: 1, a: 1, b: 1}));
        assert.commandWorked(groupBy.insert({ka: 1, kb: 1, a: 1, b: 2}));
        assert.commandWorked(groupBy.insert({ka: 1, kb: 1, a: 2, b: 3}));
        assert.commandWorked(groupBy.insert({ka: 1, kb: 1, a: 2, b: 4}));

        assert.commandWorked(groupByMp.createIndex({ka:1}));
        assert.commandWorked(groupByMp.createIndex({kb:1}));

        let pipeline = [{$match: {ka: 1, kb: 1}}, {$group: {_id: "$a", minVal: {$min: "$b"}}}];
        print("ian: running expl " + tojson(groupByMp.explain().aggregate(pipeline)));
        print("ian: running query " + tojson(groupBy.aggregate(pipeline).toArray()));
    }

    // $lookup with small foreign collection. (HJ)
    {
        print("Running a simple $lookup\n");
        const local = db.local;
        const foreign = db.foreign;

        assert.commandWorked(local.insert({_id: 0, joinFieldLocal: "a"}));
        assert.commandWorked(local.insert({_id: 1, joinFieldLocal: "b"}));

        for (let i = 0; i < 3; ++i) {
            assert.commandWorked(foreign.insert({joinFieldForeign: "a", x: 1}));
        }
        assert.commandWorked(foreign.insert({joinFieldForeign: "a", x: 1}));
        assert.commandWorked(foreign.insert({joinFieldForeign: "a", x: 2}));
        assert.commandWorked(foreign.insert({joinFieldForeign: "b", x: 3}));
        assert.commandWorked(foreign.insert({joinFieldForeign: "b", x: 4}));

        printjson(local.aggregate([{$lookup: {
            from: "foreign",
            localField: "joinFieldLocal",
            foreignField: "joinFieldForeign",
            as: "arr"
        }}]).toArray());

        // TODO: Try $unwind!
    }

    // $lookup with "big" foreign collection (NLJ)
    {
        print("Running a simple $lookup\n");
        const local = db.local;
        const foreign = db.foreign;
        local.drop();
        foreign.drop();

        assert.commandWorked(local.insert({_id: 0, joinFieldLocal: "a"}));
        assert.commandWorked(local.insert({_id: 1, joinFieldLocal: "b"}));

        for (let i = 0; i < 100; ++i) {
            assert.commandWorked(foreign.insert({joinFieldForeign: "a", x: 1}));
        }
        assert.commandWorked(foreign.insert({joinFieldForeign: "a", x: 1}));
        assert.commandWorked(foreign.insert({joinFieldForeign: "a", x: 2}));
        assert.commandWorked(foreign.insert({joinFieldForeign: "b", x: 3}));
        assert.commandWorked(foreign.insert({joinFieldForeign: "b", x: 4}));

        printjson(local.aggregate([{$lookup: {
            from: "foreign",
            localField: "joinFieldLocal",
            foreignField: "joinFieldForeign",
            as: "arr"
        }}]).toArray());
    }


    // Perf test.
    const results = [];
    if (false) {
        const local = db.local;
        const foreign = db.foreign;
        local.drop();
        foreign.drop();

        for (let i = 0; i < 10000; ++i) {
            assert.commandWorked(local.insert({_id: i, joinFieldLocal: i % 100}));
        }

        for (let i = 0; i < 1000; ++i) {
            assert.commandWorked(foreign.insert({joinFieldForeign: i % 100, x: 1}));
        }

        let iters = 10;
        let totalTime = 0;
        for (let i = 0; i < 10; ++i) {
            let start = Date.now();
            
            let res = local.aggregate([{$lookup: {
                from: "foreign",
                localField: "joinFieldLocal",
                foreignField: "joinFieldForeign",
                as: "arr"
            }}]).toArray();

            let end = Date.now();
            print("Elapsed time " + (end - start));

            totalTime += (end - start);
            results.push(res);
        }

        print ("Average time " + totalTime/iters);

        //printjson(results[0]);
    }
    
})();
