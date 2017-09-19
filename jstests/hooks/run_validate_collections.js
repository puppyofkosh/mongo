// Runner for validateCollections that runs full validation on all collections when loaded into
// the mongo shell.
'use strict';

(function() {
    assert.eq(typeof db, 'object', 'Invalid `db` object, is the shell connected to a mongod?');
    load('jstests/hooks/validate_collections.js');  // For validateCollections.

    function getReplSetMembers(conn) {
        // If conn does not point to a repl set, then this function returns [conn].
        var res = conn.adminCommand({"isMaster": 1});
        var connections = [];

        if (res.hasOwnProperty("hosts")) {
            for (var hostString of res.hosts) {
                connections.push(new Mongo(hostString));
            }
        } else {
            connections.push(conn);
        }

        return connections;
    }

    function getConfigConnStr(db) {
        var shardMap = db.adminCommand({"getShardMap": 1});
        if (!shardMap.hasOwnProperty("map")) {
            throw new Error('Expected getShardMap to return an object with a "map" field');
        }

        var map = shardMap.map;

        if (!map.hasOwnProperty('config')) {
            throw new Error('Expected getShardMap().map to have a "config" field');
        }

        return map.config;
    }

    function isMongos(db) {
        var res = db.adminCommand({isdbgrid: 1});

        if (!res.ok) {
            throw new Error('isdbgrid returned not ok');
        }

        return res.isdbgrid === 1;
    }

    function getServerList() {
        var serverList = [];

        if (isMongos(db)) {
            // We're connected to a sharded cluster.

            // 1) Add all the config servers to the server list.
            var configConnStr = getConfigConnStr(db);
            var configServerReplSetConn = new Mongo(configConnStr);
            serverList.push(...getReplSetMembers(configServerReplSetConn));

            // 2) Add shard members to the server list.
            var configDB = db.getSiblingDB("config");
            var res = configDB.shards.find();

            while (res.hasNext()) {
                var shard = res.next();
                var shardReplSetConn = new Mongo(shard.host);
                serverList.push(...getReplSetMembers(shardReplSetConn));
            }
        } else {
            // We're connected to a mongod.

            var cmdLineOpts = db.adminCommand('getCmdLineOpts');
            assert.commandWorked(cmdLineOpts);

            if (cmdLineOpts.parsed.hasOwnProperty('replication') &&
                cmdLineOpts.parsed.replication.hasOwnProperty('replSet')) {
                // We're connected to a replica set.

                var rst = new ReplSetTest(db.getMongo().host);
                // Call getPrimary to populate rst with information about the nodes.
                var primary = rst.getPrimary();
                assert(primary, 'calling getPrimary() failed');
                serverList.push(primary);
                serverList.push(...rst.getSecondaries());
            } else {
                // We're connected to a standalone.
                serverList.push(db.getMongo());
            }

        }

        return serverList;
    }


    var serverList = getServerList();
    for (var server of serverList) {
        print('Running validate() on ' + server.host);
        server.setSlaveOk();
        var dbNames = server.getDBNames();
        for (var dbName of dbNames) {
            if (!validateCollections(server.getDB(dbName), {full: true})) {
                throw new Error('Collection validation failed');
            }
        }
    }
})();
