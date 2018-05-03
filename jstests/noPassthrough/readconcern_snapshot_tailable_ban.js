// Check that opening a tailable cursor within a transaction is not allowed.
(function() {
    const r = new ReplSetTest({nodes: 2});
    r.startSet();
    r.initiate();

    const dbName = 'test';
    const collName = 'tailable-cursor-ban';

    const sessionOptions = {causalConsistency: false};
    const session = r.getPrimary().startSession(sessionOptions);
    const sessionDb = session.getDatabase(dbName);
    const sessionColl = sessionDb.getCollection(collName);

    assert.commandWorked(sessionDb.runCommand(
        {create: collName, writeConcern: {w: "majority"}, capped: true, size: 2048}));

    session.startTransaction();
    sessionColl.insert({x: 1});

    const cmdRes =
          sessionDb.runCommand({find: collName, batchSize: 2, awaitData: true, tailable: true});
    assert.commandFailedWithCode(cmdRes, 50842);

    session.endSession();

    r.stopSet();
})();
