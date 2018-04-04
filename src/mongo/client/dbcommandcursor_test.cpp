/**
 *    Copyright (C) 2013 10gen Inc.
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

/**
 * This file contains tests for DBClientReplicaSet. The tests mocks the servers
 * the DBClientReplicaSet talks to, so the tests only covers the client side logic.
 */

#include "mongo/platform/basic.h"

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/base/string_data.h"
#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/client/connpool.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/client/dbcommandcursor.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/query/killcursors_response.h"
#include "mongo/dbtests/mock/mock_conn_registry.h"
#include "mongo/dbtests/mock/mock_dbclient_connection.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"

namespace mongo {
namespace {

using std::make_pair;
using std::map;
using std::pair;
using std::string;
using std::unique_ptr;
using std::vector;

class DBCommandCursorTest : public unittest::Test {
protected:
    void setUp() {
        _server = std::make_unique<MockRemoteDBServer>("test");
        _conn = std::make_unique<MockDBClientConnection>(_server.get());
    }

    std::unique_ptr<MockRemoteDBServer> _server;
    std::unique_ptr<MockDBClientConnection> _conn;
};


// Constants to use throughout the test.
const NamespaceString kNs = NamespaceString("someNamespaceString");
const std::string kCursorGeneratingCommandName = "cursorGeneratingCommand";

BSONObj getCommand() {
    // DBCommandCursor will not inspect the initial command object, so there's no need to
    // construct a "real" command object.
    return BSON(kCursorGeneratingCommandName << 1);
}

BSONObj getNthResponseObject(int n) {
    return BSON("x" << n);
}

BSONObj getEmptyResponse() {
    return CursorResponse(kNs, 0LL, std::vector<BSONObj>())
        .toBSON(CursorResponse::ResponseType::InitialResponse);
}


TEST_F(DBCommandCursorTest, CommandWithNoResults) {
    DBCommandCursor cursor(_conn.get(), getCommand(), kNs.toString());
    _server->setCommandReply(kCursorGeneratingCommandName, getEmptyResponse());
    ASSERT_FALSE(cursor.more());
}

TEST_F(DBCommandCursorTest, CommandWithOneResult) {
    DBCommandCursor cursor(_conn.get(), getCommand(), kNs.toString());

    BSONObj result = BSON("x" << 1);

    BSONObj response =
        CursorResponse(kNs, 0LL, {result}).toBSON(CursorResponse::ResponseType::InitialResponse);
    _server->setCommandReply(kCursorGeneratingCommandName, response);

    ASSERT_TRUE(cursor.more());
    auto swNext = cursor.next();
    ASSERT_OK(swNext);
    ASSERT_EQ(swNext.getValue().woCompare(result), 0);

    ASSERT_FALSE(cursor.more());

    // Implicitly checks that killCursors is _not_ sent, since there's no response set for it
    // on the mock server.
}

TEST_F(DBCommandCursorTest, CommandWithManyResults) {
    DBCommandCursor cursor(_conn.get(), getCommand(), "admin");

    int docNum = 1;
    std::vector<BSONObj> firstBatch = {
        BSON("x" << docNum++), BSON("x" << docNum++), BSON("x" << docNum++)};
    BSONObj firstResponse = CursorResponse(kNs, 123LL, firstBatch)
                                .toBSON(CursorResponse::ResponseType::InitialResponse);
    _server->setCommandReply(kCursorGeneratingCommandName, firstResponse);

    for (auto expected : firstBatch) {
        ASSERT_TRUE(cursor.more());
        auto swNext = cursor.next();
        ASSERT_OK(swNext);
        ASSERT_EQ(swNext.getValue().woCompare(expected), 0);
    }

    // Do a few rounds of getMores.
    const int nIters = 3;
    for (int i = 0; i < nIters; i++) {
        std::vector<BSONObj> nextBatch = {BSON("x" << docNum++), BSON("x" << docNum++)};

        // On the last iteration the server will return cursor id of 0.
        CursorId returnedCursorId = i + 1 == nIters ? 0LL : 123LL;
        BSONObj getMoreResponse = CursorResponse(kNs, returnedCursorId, nextBatch)
                                      .toBSON(CursorResponse::ResponseType::SubsequentResponse);
        _server->setCommandReply("getMore", getMoreResponse);
        for (auto expected : nextBatch) {
            ASSERT_TRUE(cursor.more());
            auto swNext = cursor.next();
            ASSERT_OK(swNext);
            ASSERT_EQ(swNext.getValue().woCompare(expected), 0);
        }
    }

    // Implicitly checks that killCursors is _not_ sent, since there's no response set for it
    // on the mock server.
}

TEST_F(DBCommandCursorTest, ErrorOnInitialCommand) {
    Status status(ErrorCodes::OperationFailed, "some error");
    BSONObj err = BSON("ok" << 0 << "errmsg" << status.reason() << "code" << int(status.code()));

    DBCommandCursor cursor(_conn.get(), getCommand(), kNs.toString());
    _server->setCommandReply(kCursorGeneratingCommandName, err);

    ASSERT_TRUE(cursor.more());

    auto swNext = cursor.next();
    ASSERT_NOT_OK(swNext);
    ASSERT_EQ(swNext.getStatus(), status);
    // Status operator== does not compare the message string.
    ASSERT_EQ(swNext.getStatus().reason(), status.reason());
}

TEST_F(DBCommandCursorTest, ErrorOnGetMore) {
    // TODO: write this
    // DBCommandCursor cursor(_conn.get(), getCommand(), "admin");
}

TEST_F(DBCommandCursorTest, KillsCursorOnDestruction) {
    size_t cmdCount = 0;
    {
        DBCommandCursor cursor(_conn.get(), getCommand(), kNs.toString());

        BSONObj result = BSON("x" << 1);

        BSONObj response = CursorResponse(kNs, 123LL, {result})
                               .toBSON(CursorResponse::ResponseType::InitialResponse);
        _server->setCommandReply(kCursorGeneratingCommandName, response);

        ASSERT_TRUE(cursor.more());
        auto swNext = cursor.next();
        ASSERT_OK(swNext);
        ASSERT_EQ(swNext.getValue().woCompare(result), 0);

        // The cursor was still open! Make sure it gets killed.
        BSONObj resp = KillCursorsResponse({123LL}, {}, {}, {}).toBSON();
        cmdCount = _server->getCmdCount();
        _server->setCommandReply("killCursors", resp);
    }

    // Make sure that a command was run on the server when the DBCommandCursor was destroyed.
    ASSERT_EQ(cmdCount + 1, _server->getCmdCount());
}

}  // namespace
}  // namespace mongo
