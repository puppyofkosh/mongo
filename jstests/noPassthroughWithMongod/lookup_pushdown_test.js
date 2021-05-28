(function() {
    "use strict";
    
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
})();
