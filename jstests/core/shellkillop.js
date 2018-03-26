baseName = "jstests_shellkillop";

// 'retry' should be set to true in contexts where an exception should cause the test to be retried
// rather than to fail.
retry = true;

function testShellAutokillop() {
    if (true) {  // toggle to disable test
        db[baseName].drop();

        // mongo --autokillop suppressed the ctrl-c "do you want to kill current operation" message
        // it's just for testing purposes and thus not in the shell help
        var evalStr = "print('SKO subtask started'); db." + baseName +
            ".find({$where: 'for(var i=0;i<100000;i++) sleep(1000)'})";
        print("shellkillop.js evalStr:" + evalStr);
        spawn = startMongoProgramNoConnect(
            "mongo", "--autokillop", "--port", myPort(), "--eval", evalStr);

        sleep(1000);

        // printjson(spawn);
        // print("Spawn field is " + spawn[""]);
        // const toKill = {
        //     pid: spawn[""],
        //     signal: 2
        // };

        stopMongoProgramByPid(spawn);

        sleep(100);

        var inprog = db.currentOp().inprog;
        for (i in inprog) {
            if (inprog[i].ns == "test." + baseName)
                throw Error("shellkillop.js op is still running: " + tojson(inprog[i]));
        }
    }
}

testShellAutokillop();

print("shellkillop.js SUCCESS");
