/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include <boost/intrusive_ptr.hpp>
#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "mongo/bson/bsonmisc.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/json.h"
#include "mongo/db/pipeline/aggregation_context_fixture.h"
#include "mongo/db/pipeline/dependencies.h"
#include "mongo/db/pipeline/document_source_explode_at_path.h"
#include "mongo/db/pipeline/document_source_mock.h"
#include "mongo/db/pipeline/document_source_unwind.h"  // TODO ian: remove
#include "mongo/db/pipeline/document_value_test_util.h"
#include "mongo/db/pipeline/expression_context_for_test.h"
#include "mongo/db/pipeline/value_comparator.h"
#include "mongo/db/query/query_test_service_context.h"
#include "mongo/db/service_context.h"
#include "mongo/dbtests/dbtests.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {
using boost::intrusive_ptr;
using std::deque;
using std::string;
using std::unique_ptr;
using std::vector;

/**
 * New-style fixture for testing the $unwind stage. Provides access to an ExpressionContext which
 * can be used to construct DocumentSourceUnwind.
 */
// TODO ian: rename
class UnwindStageTest : public AggregationContextFixture {
public:
    intrusive_ptr<DocumentSource> createUnwind(BSONObj spec) {
        auto specElem = spec.firstElement();
        return DocumentSourceUnwind::createFromBson(specElem, getExpCtx());
    }

    bool resultsMatch(const vector<DocumentSource::GetNextResult>& input,
                      const vector<DocumentSource::GetNextResult>& expectedOut,
                      const std::string& path) {
        auto explodeAtPath = DocumentSourceExplodeAtPath::create(getExpCtx(), path);
        auto source = DocumentSourceMock::create(
            std::deque<DocumentSource::GetNextResult>(input.begin(), input.end()));

        explodeAtPath->setSource(source.get());

        for (auto&& expected : expectedOut) {
            const auto res = explodeAtPath->getNext();

            if (expected.getStatus() != res.getStatus()) {
                log() << "expected status " << static_cast<int>(expected.getStatus()) << " but got "
                      << static_cast<int>(res.getStatus());
                return false;
            }

            if (expected.isAdvanced()) {
                DocumentComparator comp;
                if (comp.evaluate(expected.getDocument() != res.getDocument())) {
                    log() << "expected " << expected.getDocument() << " got " << res.getDocument();
                    return false;
                }
            }
        }

        return explodeAtPath->getNext().isEOF();
    }
};

TEST_F(UnwindStageTest, EmptyColl) {
    ASSERT_TRUE(resultsMatch({}, {}, "array"));
}

TEST_F(UnwindStageTest, EmptyArray) {
    ASSERT_TRUE(resultsMatch({Document{{"array", vector<Value>{}}}}, {}, "array"));
}

TEST_F(UnwindStageTest, MissingValue) {
    ASSERT_TRUE(resultsMatch({Document{{"unrelated", vector<Value>{}}}}, {}, "array"));
}

TEST_F(UnwindStageTest, ScalarValue) {
    ASSERT_TRUE(resultsMatch({{Document{{"array", Value(3)}}}}, {}, "array"));
}

TEST_F(UnwindStageTest, SingletonArray) {
    ASSERT_TRUE(resultsMatch({{Document{{"array", vector<Value>{Value(1)}}}}},
                             {Document{{"array", Value(1)}}},
                             "array"));
}

// TEST_F(UnwindStageTest, TwoValueArray) {
//     auto unwind = DocumentSourceExplodeAtPath::create(getExpCtx(), "array");
//     auto source =
//         DocumentSourceMock::create({Document{{"array", vector<Value>{Value(3), Value(4)}}}});
//     unwind->setSource(source.get());
//     auto res = unwind->getNext();
//     ASSERT_TRUE(res.isAdvanced());
//     Document expected{{"array", Value(3)}};
//     ASSERT_EQ(res.getDocument(), expected);

//     expected = {{"array", Value(4)}};
//     res = unwind->getNext();
//     ASSERT_TRUE(res.isAdvanced());
//     ASSERT_EQ(res.getDocument(), expected);
// }

TEST_F(UnwindStageTest, AddsUnwoundPathToDependencies) {
    auto unwind =
        DocumentSourceUnwind::create(getExpCtx(), "x.y.z", false, boost::optional<string>("index"));
    DepsTracker dependencies;
    ASSERT_EQUALS(DepsTracker::State::SEE_NEXT, unwind->getDependencies(&dependencies));
    ASSERT_EQUALS(1U, dependencies.fields.size());
    ASSERT_EQUALS(1U, dependencies.fields.count("x.y.z"));
    ASSERT_EQUALS(false, dependencies.needWholeDocument);
    ASSERT_EQUALS(false, dependencies.getNeedsMetadata(DepsTracker::MetadataType::TEXT_SCORE));
}

TEST_F(UnwindStageTest, ShouldPropagatePauses) {
    const bool includeNullIfEmptyOrMissing = false;
    const boost::optional<std::string> includeArrayIndex = boost::none;
    auto unwind = DocumentSourceUnwind::create(
        getExpCtx(), "array", includeNullIfEmptyOrMissing, includeArrayIndex);
    auto source =
        DocumentSourceMock::create({Document{{"array", vector<Value>{Value(1), Value(2)}}},
                                    DocumentSource::GetNextResult::makePauseExecution(),
                                    Document{{"array", vector<Value>{Value(1), Value(2)}}}});

    unwind->setSource(source.get());

    ASSERT_TRUE(unwind->getNext().isAdvanced());
    ASSERT_TRUE(unwind->getNext().isAdvanced());

    ASSERT_TRUE(unwind->getNext().isPaused());

    ASSERT_TRUE(unwind->getNext().isAdvanced());
    ASSERT_TRUE(unwind->getNext().isAdvanced());

    ASSERT_TRUE(unwind->getNext().isEOF());
    ASSERT_TRUE(unwind->getNext().isEOF());
}

TEST_F(UnwindStageTest, UnwindOnlyModifiesUnwoundPathWhenNotIncludingIndex) {
    const bool includeNullIfEmptyOrMissing = false;
    const boost::optional<std::string> includeArrayIndex = boost::none;
    auto unwind = DocumentSourceUnwind::create(
        getExpCtx(), "array", includeNullIfEmptyOrMissing, includeArrayIndex);

    auto modifiedPaths = unwind->getModifiedPaths();
    ASSERT(modifiedPaths.type == DocumentSource::GetModPathsReturn::Type::kFiniteSet);
    ASSERT_EQUALS(1U, modifiedPaths.paths.size());
    ASSERT_EQUALS(1U, modifiedPaths.paths.count("array"));
}

TEST_F(UnwindStageTest, UnwindIncludesIndexPathWhenIncludingIndex) {
    const bool includeNullIfEmptyOrMissing = false;
    const boost::optional<std::string> includeArrayIndex = std::string("arrIndex");
    auto unwind = DocumentSourceUnwind::create(
        getExpCtx(), "array", includeNullIfEmptyOrMissing, includeArrayIndex);

    auto modifiedPaths = unwind->getModifiedPaths();
    ASSERT(modifiedPaths.type == DocumentSource::GetModPathsReturn::Type::kFiniteSet);
    ASSERT_EQUALS(2U, modifiedPaths.paths.size());
    ASSERT_EQUALS(1U, modifiedPaths.paths.count("array"));
    ASSERT_EQUALS(1U, modifiedPaths.paths.count("arrIndex"));
}

TEST_F(UnwindStageTest, UnwindRecursive) {
    const bool includeNullIfEmptyOrMissing = false;
    const bool recursive = true;
    auto unwind = DocumentSourceUnwind::create(
        getExpCtx(), "a.b.c", includeNullIfEmptyOrMissing, boost::none, recursive);

    // auto source = DocumentSourceMock::create(
    //     {Document(fromjson("{a: {b: {c: 1}}}"))});

    // TODO: Think about nested arrays e.g. [[1, 2]] (behavior should match what distinct() needs).
    auto source = DocumentSourceMock::create(
        {Document(fromjson("{a: [{b: [{c: 1}, {c: 2}, 45]}, {b: [{c: 3}, {c: [4, 5]}]}]}"))});
    // auto source =
    //     DocumentSourceMock::create({Document{{"a",
    //                                           vector<Value>{Value(Document{{"b", Value(1)}}),
    //                                                         Value(Document{{"b",
    //                                                         Value(2)}})}}}});

    unwind->setSource(source.get());
    ASSERT_TRUE(unwind->getNext().isAdvanced());
}

//
// Error cases.
//

TEST_F(UnwindStageTest, ShouldRejectNonObjectNonString) {
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind" << 1)), AssertionException, 15981);
}

TEST_F(UnwindStageTest, ShouldRejectSpecWithoutPath) {
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind" << BSONObj())), AssertionException, 28812);
}

TEST_F(UnwindStageTest, ShouldRejectNonStringPath) {
    ASSERT_THROWS_CODE(
        createUnwind(BSON("$unwind" << BSON("path" << 2))), AssertionException, 28808);
}

TEST_F(UnwindStageTest, ShouldRejectNonDollarPrefixedPath) {
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind"
                                         << "somePath")),
                       AssertionException,
                       28818);
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind" << BSON("path"
                                                           << "somePath"))),
                       AssertionException,
                       28818);
}

TEST_F(UnwindStageTest, ShouldRejectNonBoolPreserveNullAndEmptyArrays) {
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind" << BSON("path"
                                                           << "$x"
                                                           << "preserveNullAndEmptyArrays"
                                                           << 2))),
                       AssertionException,
                       28809);
}

TEST_F(UnwindStageTest, ShouldRejectNonStringIncludeArrayIndex) {
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind" << BSON("path"
                                                           << "$x"
                                                           << "includeArrayIndex"
                                                           << 2))),
                       AssertionException,
                       28810);
}

TEST_F(UnwindStageTest, ShouldRejectEmptyStringIncludeArrayIndex) {
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind" << BSON("path"
                                                           << "$x"
                                                           << "includeArrayIndex"
                                                           << ""))),
                       AssertionException,
                       28810);
}

TEST_F(UnwindStageTest, ShoudlRejectDollarPrefixedIncludeArrayIndex) {
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind" << BSON("path"
                                                           << "$x"
                                                           << "includeArrayIndex"
                                                           << "$"))),
                       AssertionException,
                       28822);
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind" << BSON("path"
                                                           << "$x"
                                                           << "includeArrayIndex"
                                                           << "$path"))),
                       AssertionException,
                       28822);
}

TEST_F(UnwindStageTest, ShouldRejectUnrecognizedOption) {
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind" << BSON("path"
                                                           << "$x"
                                                           << "preserveNullAndEmptyArrays"
                                                           << true
                                                           << "foo"
                                                           << 3))),
                       AssertionException,
                       28811);
    ASSERT_THROWS_CODE(createUnwind(BSON("$unwind" << BSON("path"
                                                           << "$x"
                                                           << "foo"
                                                           << 3))),
                       AssertionException,
                       28811);
}

}  // namespace
}  // namespace mongo
