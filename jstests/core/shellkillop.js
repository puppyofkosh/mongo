(function() {

    const collName = "jstests_shellkillop";
    function getCurOps() {
        let currentOps =
            db.getSiblingDB("admin")
                .aggregate([{$currentOp: {localOps: true}}, {$match: {ns: "test." + collName}}])
                .toArray();
        return currentOps;
    }
    db[collName].drop();
    for (var i = 0; i < 5000; i++) {
        db[collName].insert({_id: i});
    }

    // Run a find that will take several hours if not killed.
    function runLongFind(collName) {
        const curs = db[collName].find({$where: 'sleep(1000 * 10)'});
        curs.itcount();
    }

    const evalStr = "(" + runLongFind.toString() + ")('" + collName + "');";
    // mongo --autokillop suppresses the ctrl-c "do you want to kill current operation" message.
    // It's just for testing purposes and thus not in the shell help.
    const shellPid =
        startMongoProgramNoConnect("mongo", "--autokillop", "--port", myPort(), "--eval", evalStr);

    assert.soon(function() {
        return getCurOps().length === 1;
    }, "Did not find any operations running under namespace.");

    // Send SIGINT, the signal triggered by Ctrl-C.
    const kSigInt = 2;
    stopMongoProgramByPid(shellPid, kSigInt);

    assert.soon(function() {
        return getCurOps().length === 0;
    }, "Found operations still running after the shell was killed ");
})();
