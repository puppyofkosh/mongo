(function() {
    "use strict";

    assert.commandWorked(db.adminCommand({setParameter: 1, "traceExceptions": true}));

    const groupBy = db.groupBy;
    assert.commandWorked(groupBy.insert({a: 1, b: 1}));
    assert.commandWorked(groupBy.insert({a: 1, b: 1}));
    assert.commandWorked(groupBy.insert({a: 2, b: 1}));
    assert.commandWorked(groupBy.insert({a: 2, b: 1}));

    print("ian: running expl " + tojson(groupBy.explain().aggregate([{$group: {_id: "$a"}}])));
    print("ian: running query " + tojson(groupBy.aggregate([{$group: {_id: "$a"}}]).toArray()));

    if (0) {
        const local = db.local;
        const foreign = db.foreign;

        assert.commandWorked(local.insert({_id: 0, joinFieldLocal: "a"}));
        assert.commandWorked(local.insert({_id: 1, joinFieldLocal: "b"}));

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
})();
