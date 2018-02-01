/**
 *    Copyright 2015 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/s/query/cluster_client_cursor_impl.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/service_context_noop.h"
#include "mongo/s/query/router_stage_mock.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"

namespace mongo {

namespace {

class ClusterClientCursorImplTest : public unittest::Test {
protected:
    ServiceContextNoop _serviceContext;
    ServiceContext::UniqueOperationContext _opCtx;
    Client* _client;

    template<typename... Args>
    std::unique_ptr<ClusterClientCursorImpl> makeAttachedCursor(Args&&... args) {
        auto cursor = std::make_unique<ClusterClientCursorImpl>(std::forward<Args>(args)...);
        cursor->reattachToOperationContext(_opCtx.get());
        return cursor;
    }

private:
    void setUp() final {
        auto client = _serviceContext.makeClient("testClient");
        _opCtx = client->makeOperationContext();
        _client = client.get();
        Client::setCurrent(std::move(client));
    }

    void tearDown() final {
        if (_opCtx) {
            _opCtx.reset();
        }

        Client::releaseCurrent();
    }
};

TEST_F(ClusterClientCursorImplTest, NumReturnedSoFar) {
    auto mockStage = stdx::make_unique<RouterStageMock>(nullptr);
    for (int i = 1; i < 10; ++i) {
        mockStage->queueResult(BSON("a" << i));
    }

    auto cursor = makeAttachedCursor(std::move(mockStage),
                                     ClusterClientCursorParams(NamespaceString("unused"), {}),
                                     boost::none);

    ASSERT_EQ(cursor->getNumReturnedSoFar(), 0);

    for (int i = 1; i < 10; ++i) {
        auto result = cursor->next(RouterExecStage::ExecContext::kInitialFind);
        ASSERT(result.isOK());
        ASSERT_BSONOBJ_EQ(*result.getValue().getResult(), BSON("a" << i));
        ASSERT_EQ(cursor->getNumReturnedSoFar(), i);
    }
    // Now check that if nothing is fetched the getNumReturnedSoFar stays the same.
    auto result = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT_OK(result.getStatus());
    ASSERT_TRUE(result.getValue().isEOF());
    ASSERT_EQ(cursor->getNumReturnedSoFar(), 9LL);
}

TEST_F(ClusterClientCursorImplTest, QueueResult) {
    auto mockStage = stdx::make_unique<RouterStageMock>(nullptr);
    mockStage->queueResult(BSON("a" << 1));
    mockStage->queueResult(BSON("a" << 4));

    auto cursor = makeAttachedCursor(std::move(mockStage),
                                     ClusterClientCursorParams(NamespaceString("unused"), {}),
                                     boost::none);

    auto firstResult = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT_OK(firstResult.getStatus());
    ASSERT(firstResult.getValue().getResult());
    ASSERT_BSONOBJ_EQ(*firstResult.getValue().getResult(), BSON("a" << 1));

    cursor->queueResult(BSON("a" << 2));
    cursor->queueResult(BSON("a" << 3));

    auto secondResult = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT_OK(secondResult.getStatus());
    ASSERT(secondResult.getValue().getResult());
    ASSERT_BSONOBJ_EQ(*secondResult.getValue().getResult(), BSON("a" << 2));

    auto thirdResult = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT_OK(thirdResult.getStatus());
    ASSERT(thirdResult.getValue().getResult());
    ASSERT_BSONOBJ_EQ(*thirdResult.getValue().getResult(), BSON("a" << 3));

    auto fourthResult = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT_OK(fourthResult.getStatus());
    ASSERT(fourthResult.getValue().getResult());
    ASSERT_BSONOBJ_EQ(*fourthResult.getValue().getResult(), BSON("a" << 4));

    auto fifthResult = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT_OK(fifthResult.getStatus());
    ASSERT(fifthResult.getValue().isEOF());

    ASSERT_EQ(cursor->getNumReturnedSoFar(), 4LL);
}

TEST_F(ClusterClientCursorImplTest, RemotesExhausted) {
    auto mockStage = stdx::make_unique<RouterStageMock>(nullptr);
    mockStage->queueResult(BSON("a" << 1));
    mockStage->queueResult(BSON("a" << 2));
    mockStage->markRemotesExhausted();

    auto cursor = makeAttachedCursor(std::move(mockStage),
                                     ClusterClientCursorParams(NamespaceString("unused"), {}),
                                     boost::none);
    ASSERT_TRUE(cursor->remotesExhausted());

    auto firstResult = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT_OK(firstResult.getStatus());
    ASSERT(firstResult.getValue().getResult());
    ASSERT_BSONOBJ_EQ(*firstResult.getValue().getResult(), BSON("a" << 1));
    ASSERT_TRUE(cursor->remotesExhausted());

    auto secondResult = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT_OK(secondResult.getStatus());
    ASSERT(secondResult.getValue().getResult());
    ASSERT_BSONOBJ_EQ(*secondResult.getValue().getResult(), BSON("a" << 2));
    ASSERT_TRUE(cursor->remotesExhausted());

    auto thirdResult = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT_OK(thirdResult.getStatus());
    ASSERT_TRUE(thirdResult.getValue().isEOF());
    ASSERT_TRUE(cursor->remotesExhausted());

    ASSERT_EQ(cursor->getNumReturnedSoFar(), 2LL);
}

TEST_F(ClusterClientCursorImplTest, ForwardsAwaitDataTimeout) {
    auto mockStage = stdx::make_unique<RouterStageMock>(nullptr);
    auto mockStagePtr = mockStage.get();
    ASSERT_NOT_OK(mockStage->getAwaitDataTimeout().getStatus());

    auto cursor = makeAttachedCursor(std::move(mockStage),
                                     ClusterClientCursorParams(NamespaceString("unused"), {}),
                                     boost::none);
    ASSERT_OK(cursor->setAwaitDataTimeout(Milliseconds(789)));

    auto awaitDataTimeout = mockStagePtr->getAwaitDataTimeout();
    ASSERT_OK(awaitDataTimeout.getStatus());
    ASSERT_EQ(789, durationCount<Milliseconds>(awaitDataTimeout.getValue()));
}

TEST_F(ClusterClientCursorImplTest, ChecksForInterrupt) {
    auto mockStage = stdx::make_unique<RouterStageMock>(nullptr);
    for (int i = 1; i < 2; ++i) {
        mockStage->queueResult(BSON("a" << i));
    }

    auto cursor = makeAttachedCursor(std::move(mockStage),
                                     ClusterClientCursorParams(NamespaceString("unused"), {}),
                                     boost::none);

    // Pull one result out of the cursor.
    auto result = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT(result.isOK());
    ASSERT_BSONOBJ_EQ(*result.getValue().getResult(), BSON("a" << 1));

    // Now interrupt the opCtx which the cursor is running under.
    {
        stdx::lock_guard<Client> lk(*_client);
        _opCtx->markKilled(ErrorCodes::CursorKilled);
    }

    // Now check that a subsequent call to next() will fail.
    result = cursor->next(RouterExecStage::ExecContext::kInitialFind);
    ASSERT_NOT_OK(result.getStatus());
    ASSERT_EQ(result.getStatus(), ErrorCodes::CursorKilled);
}

TEST_F(ClusterClientCursorImplTest, LogicalSessionIdsOnCursors) {
    // Make a cursor with no lsid
    auto mockStage = stdx::make_unique<RouterStageMock>(nullptr);
    ClusterClientCursorParams params(NamespaceString("test"), {});
    ClusterClientCursorImpl cursor{std::move(mockStage), std::move(params), boost::none};
    ASSERT(!cursor.getLsid());

    // Make a cursor with an lsid
    auto mockStage2 = stdx::make_unique<RouterStageMock>(nullptr);
    ClusterClientCursorParams params2(NamespaceString("test"), {});
    auto lsid = makeLogicalSessionIdForTest();
    ClusterClientCursorImpl cursor2{std::move(mockStage2), std::move(params2), lsid};
    ASSERT(*(cursor2.getLsid()) == lsid);
}

}  // namespace

}  // namespace mongo
