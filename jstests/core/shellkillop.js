baseName = "jstests_shellkillop";

// 'retry' should be set to true in contexts where an exception should cause the test to be retried
// rather than to fail.
retry = true;

function testShellAutokillop() {
    db[baseName].drop();
    db[baseName].insert({_id: 0});

    // mongo --autokillop suppressed the ctrl-c "do you want to kill current operation" message
    // it's just for testing purposes and thus not in the shell help
    var evalStr = "print('SKO subtask started'); let curs = db." + baseName +
        ".find({$where: 'for(var i=0;i<100000;i++) sleep(1000)'}); curs.next(); print('Subtask finished');";
    print("shellkillop.js evalStr:" + evalStr);
    spawn = startMongoProgramNoConnect(
        "mongo", "--autokillop", "--port", myPort(), "--eval", evalStr);

    sleep(1000);

    let currentOps0 = db.getSiblingDB("admin")
        .aggregate([{$currentOp: {localOps: true}}])
        .toArray();
    print("ian: Operations currently are : " + tojson(currentOps0));

    // printjson(spawn);
    // print("Spawn field is " + spawn[""]);
    // const toKill = {
    //     pid: spawn[""],
    //     signal: 2
    // };

    stopMongoProgramByPid(spawn);

    sleep(1000);

    print("Remaining operations are");
    const currentOps = db.getSiblingDB("admin")
          .aggregate([{$currentOp: {localOps: true}}])
          .toArray();
    printjson(currentOps);
    for (let op of currentOps) {
        if (op.ns == "test." + baseName)
            throw Error("shellkillop.js op is still running: " + tojson(op));
    }
}

testShellAutokillop();

print("shellkillop.js SUCCESS");
