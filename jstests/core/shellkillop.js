(function() {

    const baseName = "jstests_shellkillop";
    function getCurOps() {
        return currentOps =
                   db.getSiblingDB("admin")
                       .aggregate(
                           [{$currentOp: {localOps: true}}, {$match: {ns: "test." + baseName}}])
                       .toArray();
    }
    db[baseName].drop();
    db[baseName].insert({_id: 0});

    // Run a find that will take forever.
    const evalFn =
        function(collName) {
        print('SKO subtask started');
        const curs = db[collName].find({$where: 'for(var i=0;i<100000;i++) sleep(1000)'});
        curs.next();
        print('Subtask finished');
    }

    const evalStr = "(" + evalFn.toString() + ")('" + baseName + "');";
    // mongo --autokillop suppresses the ctrl-c "do you want to kill current operation" message.
    // It's just for testing purposes and thus not in the shell help.
    spawn =
        startMongoProgramNoConnect("mongo", "--autokillop", "--port", myPort(), "--eval", evalStr);

    assert.soon(function() {
        return getCurOps().length === 1;
    }, "Did not find any operations running under namespace.");

    // Send SIGINT, the signal triggered by Ctrl-C.
    const SIGINT = 2;
    stopMongoProgramByPid(spawn, SIGINT);

    assert.soon(function() {
        return getCurOps().length === 0;
    }, "Found operations still running after the shell was killed ");
})();
