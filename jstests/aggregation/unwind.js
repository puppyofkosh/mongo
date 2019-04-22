// SERVER-8088: test $unwind with a scalar
(function () {
    "use strict";
    load('jstests/libs/analyze_plan.js');  // For getAggPlanStage().

    const t = db.agg_unwind;
    t.drop();

    assert.commandWorked(t.insert({_id: 1}));
    assert.commandWorked(t.insert({_id: 2, x: null}));
    assert.commandWorked(t.insert({_id: 3, x: []}));
    assert.commandWorked(t.insert({_id: 4, x: [1, 2]}));
    assert.commandWorked(t.insert({_id: 5, x: [3]}));
    assert.commandWorked(t.insert({_id: 6, x: 4}));

    let res = t.aggregate([{$unwind: "$x"}, {$sort: {_id: 1}}]).toArray();
    assert.eq(4, res.length);
    assert.eq([1, 2, 3, 4], res.map(function(z) {
        return z.x;
    }));

    // Test that exlpain() of a $unwind with the nested: true option indicates that multiple unwinds
    // happen.

    assert.commandWorked(
        t.insert({_id: 7, outer: [{firstInner: [{secondInner: 1}]}, {firstInner: []}]}));
    let expl =
        t.explain().aggregate([{$unwind: {path: "$outer.firstInner.secondInner", nested: true}}]);
    const unwinds = getAggPlanStages(expl, "$unwind");
    assert.eq(unwinds.length, 3);
    assert.eq(unwinds[0].$unwind.path, "$outer");
    assert.eq(unwinds[1].$unwind.path, "$outer.firstInner");
    assert.eq(unwinds[2].$unwind.path, "$outer.firstInner.secondInner");

})();
