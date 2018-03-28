// mongo/shell/shell_utils.cpp
/*
 *    Copyright 2010 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/shell/shell_utils.h"

#include "mongo/client/dbclientinterface.h"
#include "mongo/client/replica_set_monitor.h"
#include "mongo/platform/random.h"
#include "mongo/scripting/engine.h"
#include "mongo/shell/bench.h"
#include "mongo/shell/shell_options.h"
#include "mongo/shell/shell_utils_extended.h"
#include "mongo/shell/shell_utils_launcher.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/text.h"
#include "mongo/util/version.h"

namespace mongo {

using std::set;
using std::map;
using std::string;

namespace JSFiles {
extern const JSFile servers;
extern const JSFile shardingtest;
extern const JSFile servers_misc;
extern const JSFile replsettest;
extern const JSFile bridge;
}

namespace shell_utils {

std::string _dbConnect;
std::string _dbAuth;

const char* argv0 = 0;
void RecordMyLocation(const char* _argv0) {
    argv0 = _argv0;
}

// helpers

BSONObj makeUndefined() {
    BSONObjBuilder b;
    b.appendUndefined("");
    return b.obj();
}
const BSONObj undefinedReturn = makeUndefined();

BSONElement singleArg(const BSONObj& args) {
    uassert(12597, "need to specify 1 argument", args.nFields() == 1);
    return args.firstElement();
}

const char* getUserDir() {
#ifdef _WIN32
    return getenv("USERPROFILE");
#else
    return getenv("HOME");
#endif
}

// real methods

BSONObj JSGetMemInfo(const BSONObj& args, void* data) {
    ProcessInfo pi;
    uassert(10258, "processinfo not supported", pi.supported());

    BSONObjBuilder e;
    e.append("virtual", pi.getVirtualMemorySize());
    e.append("resident", pi.getResidentSize());

    BSONObjBuilder b;
    b.append("ret", e.obj());

    return b.obj();
}

#if !defined(_WIN32)
thread_local unsigned int _randomSeed = 0;
#endif

BSONObj JSSrand(const BSONObj& a, void* data) {
    unsigned int seed;
    // grab the least significant bits of either the supplied argument or
    // a random number from SecureRandom.
    if (a.nFields() == 1 && a.firstElement().isNumber())
        seed = static_cast<unsigned int>(a.firstElement().numberLong());
    else {
        std::unique_ptr<SecureRandom> rand(SecureRandom::create());
        seed = static_cast<unsigned int>(rand->nextInt64());
    }
#if !defined(_WIN32)
    _randomSeed = seed;
#else
    srand(seed);
#endif
    return BSON("" << static_cast<double>(seed));
}

BSONObj JSRand(const BSONObj& a, void* data) {
    uassert(12519, "rand accepts no arguments", a.nFields() == 0);
    unsigned r;
#if !defined(_WIN32)
    r = rand_r(&_randomSeed);
#else
    r = rand();
#endif
    return BSON("" << double(r) / (double(RAND_MAX) + 1));
}

BSONObj isWindows(const BSONObj& a, void* data) {
    uassert(13006, "isWindows accepts no arguments", a.nFields() == 0);
#ifdef _WIN32
    return BSON("" << true);
#else
    return BSON("" << false);
#endif
}

BSONObj getBuildInfo(const BSONObj& a, void* data) {
    uassert(16822, "getBuildInfo accepts no arguments", a.nFields() == 0);
    BSONObjBuilder b;
    VersionInfoInterface::instance().appendBuildInfo(&b);
    return BSON("" << b.done());
}

BSONObj computeSHA256Block(const BSONObj& a, void* data) {
    std::vector<ConstDataRange> blocks;

    auto ele = a[0];

    BSONObjBuilder bob;
    switch (ele.type()) {
        case BinData: {
            int len;
            const char* ptr = ele.binData(len);
            SHA256Block::computeHash({ConstDataRange(ptr, len)}).appendAsBinData(bob, ""_sd);

            break;
        }
        case String: {
            auto str = ele.valueStringData();
            SHA256Block::computeHash({ConstDataRange(str.rawData(), str.size())})
                .appendAsBinData(bob, ""_sd);
            break;
        }
        default:
            uasserted(ErrorCodes::BadValue, "Can only computeSHA256Block of strings and bindata");
    }

    return bob.obj();
}

BSONObj replMonitorStats(const BSONObj& a, void* data) {
    uassert(17134,
            "replMonitorStats requires a single string argument (the ReplSet name)",
            a.nFields() == 1 && a.firstElement().type() == String);

    ReplicaSetMonitorPtr rsm = ReplicaSetMonitor::get(a.firstElement().valuestrsafe());
    if (!rsm) {
        return BSON(""
                    << "no ReplSetMonitor exists by that name");
    }

    BSONObjBuilder result;
    rsm->appendInfo(result);
    return result.obj();
}

BSONObj useWriteCommandsDefault(const BSONObj& a, void* data) {
    return BSON("" << shellGlobalParams.useWriteCommandsDefault);
}

BSONObj writeMode(const BSONObj&, void*) {
    return BSON("" << shellGlobalParams.writeMode);
}

BSONObj readMode(const BSONObj&, void*) {
    return BSON("" << shellGlobalParams.readMode);
}

BSONObj shouldRetryWrites(const BSONObj&, void* data) {
    return BSON("" << shellGlobalParams.shouldRetryWrites);
}

BSONObj interpreterVersion(const BSONObj& a, void* data) {
    uassert(16453, "interpreterVersion accepts no arguments", a.nFields() == 0);
    return BSON("" << getGlobalScriptEngine()->getInterpreterVersionString());
}

BSONObj fileExistsJS(const BSONObj& a, void*) {
    uassert(40678,
            "fileExists expects one string argument",
            a.nFields() == 1 && a.firstElement().type() == String);
    return BSON("" << fileExists(a.firstElement().valuestrsafe()));
}

void installShellUtils(Scope& scope) {
    scope.injectNative("getMemInfo", JSGetMemInfo);
    scope.injectNative("_replMonitorStats", replMonitorStats);
    scope.injectNative("_srand", JSSrand);
    scope.injectNative("_rand", JSRand);
    scope.injectNative("_isWindows", isWindows);
    scope.injectNative("interpreterVersion", interpreterVersion);
    scope.injectNative("getBuildInfo", getBuildInfo);
    scope.injectNative("computeSHA256Block", computeSHA256Block);
    scope.injectNative("fileExists", fileExistsJS);

#ifndef MONGO_SAFE_SHELL
    // can't launch programs
    installShellUtilsLauncher(scope);
    installShellUtilsExtended(scope);
#endif
}

void initScope(Scope& scope) {
    // Need to define this method before JSFiles::utils is executed.
    scope.injectNative("_useWriteCommandsDefault", useWriteCommandsDefault);
    scope.injectNative("_writeMode", writeMode);
    scope.injectNative("_readMode", readMode);
    scope.injectNative("_shouldRetryWrites", shouldRetryWrites);
    scope.externalSetup();
    mongo::shell_utils::installShellUtils(scope);
    scope.execSetup(JSFiles::servers);
    scope.execSetup(JSFiles::shardingtest);
    scope.execSetup(JSFiles::servers_misc);
    scope.execSetup(JSFiles::replsettest);
    scope.execSetup(JSFiles::bridge);

    scope.injectNative("benchRun", BenchRunner::benchRunSync);
    scope.injectNative("benchRunSync", BenchRunner::benchRunSync);
    scope.injectNative("benchStart", BenchRunner::benchStart);
    scope.injectNative("benchFinish", BenchRunner::benchFinish);

    if (!_dbConnect.empty()) {
        uassert(12513, "connect failed", scope.exec(_dbConnect, "(connect)", false, true, false));
    }
    if (!_dbAuth.empty()) {
        uassert(12514, "login failed", scope.exec(_dbAuth, "(auth)", true, true, false));
    }
}

Prompter::Prompter(const string& prompt) : _prompt(prompt), _confirmed() {}

bool Prompter::confirm() {
    if (_confirmed) {
        return true;
    }

    // The printf and scanf functions provide thread safe i/o.

    printf("\n%s (y/n): ", _prompt.c_str());

    char yn = '\0';
    int nScanMatches = scanf("%c", &yn);
    bool matchedY = (nScanMatches == 1 && (yn == 'y' || yn == 'Y'));

    return _confirmed = matchedY;
}

ConnectionRegistry::ConnectionRegistry() = default;

void ConnectionRegistry::registerConnection(DBClientBase& client) {
    BSONObj info;
    if (client.runCommand("admin", BSON("whatsmyuri" << 1), info)) {
        string connstr = client.getServerAddress();
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        _connectionUris[connstr].insert(info["you"].str());
    }
}

void processOp(DBClientBase& conn, BSONObj op, const set<string>& myUris) {
    // For sharded clusters, `client_s` is used instead and `client` is not present.
    string client;
    if (auto elem = op["client"]) {
        if (elem.type() != String) {
            warning() << "Ignoring operation " << op["opid"].toString(false)
                      << "; expected 'client' field in $currentOp response to have type "
                "string, but found "
                      << typeName(elem.type());
            return;
        }
        client = elem.str();
    } else {
        // Malformed results from the server.
        warning() << "Expected $currentOp response to include 'client' field but it did not: "
                  << op;
        return;
    }

    // The $currentOp query should have filtered out any operations not started by us.
    invariant(myUris.count(client));

    // The operation originated from us, so we get rid of it. We run the $currentOp on a separate
    // connection, so we don't have to worry about killing our own $currentOp.
    BSONObjBuilder cmdBob;
    BSONObj info;
    log() << "Killing op " << op["opid"];
    cmdBob.appendAs(op["opid"], "op");
    auto cmdArgs = cmdBob.done();
    const BSONObj killOp = BSON("killOp" << 1 << "op" << op["opid"]);
    BSONObj killOpResponse;
    conn.runCommand("admin", killOp, killOpResponse);

    if (!killOpResponse["ok"].trueValue()) {
        // Cannot happen since killOp always returns success...for now.
        warning() << "Failed to kill op " << op["opid"] <<
            "Expected ok response but got " << killOpResponse;
    }
}

void ConnectionRegistry::killOperationsOnAllConnections(bool withPrompt) const {
    Prompter prompter("do you want to kill the current op(s) on the server?");
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    for (map<string, set<string>>::const_iterator i = _connectionUris.begin();
         i != _connectionUris.end();
         ++i) {
        auto status = ConnectionString::parse(i->first);
        if (!status.isOK()) {
            continue;
        }

        const set<string>& myUris = i->second;
        const ConnectionString cs(status.getValue());

        string errmsg;
        std::unique_ptr<DBClientBase> conn(cs.connect("MongoDB Shell", errmsg));
        if (!conn) {
            continue;
        }

        if (withPrompt && !prompter.confirm()) {
            // The user didn't want us to kill anything anyway.
            continue;
        }

        BSONArrayBuilder uriBuilder;
        for (auto&& a : myUris) {
            uriBuilder.append(a);
        }

        BSONObj currentOpRes;
        BSONObj cmd = BSON("aggregate" << 1 <<
                           "pipeline" << BSON_ARRAY(
                               // 'localOps' true so that when run on a sharded cluster, we get the
                               // mongos operations.
                               BSON("$currentOp" << BSON("localOps" << true)) <<
                               // Match any operations started by us.
                               BSON("$match" << BSON("client" << BSON("$in" << uriBuilder.arr()))))
                           // Must be provided for the 'aggregate' command.
                           << "cursor" << BSONObj());
        conn->runCommand("admin", cmd, currentOpRes);

        BSONObj cursorObj = currentOpRes["cursor"].Obj();
        BSONObj firstBatch = cursorObj["firstBatch"].Obj();
        BSONObjIterator it(firstBatch);
        while (it.more()) {
            BSONElement e = it.next();
            processOp(*conn, e.Obj(), myUris);
        }

        const long long cursorId = cursorObj["id"].Long();
        if (cursorId != 0) {
            auto cursor = conn->getMore("admin", cursorId, 0, 0);

            while (cursor->more()) {
                processOp(*conn, cursor->next(), myUris);
            }
        }
    }
}

ConnectionRegistry connectionRegistry;

bool _nokillop = false;
void onConnect(DBClientBase& c) {
    if (_nokillop) {
        return;
    }

    // Only override the default rpcProtocols if they were set on the command line.
    if (shellGlobalParams.rpcProtocols) {
        c.setClientRPCProtocols(*shellGlobalParams.rpcProtocols);
    }

    connectionRegistry.registerConnection(c);
}

bool fileExists(const std::string& file) {
    try {
#ifdef _WIN32
        boost::filesystem::path p(toWideString(file.c_str()));
#else
        boost::filesystem::path p(file);
#endif
        return boost::filesystem::exists(p);
    } catch (...) {
        return false;
    }
}


stdx::mutex& mongoProgramOutputMutex(*(new stdx::mutex()));
}
}
