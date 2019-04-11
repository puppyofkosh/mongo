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
class ExplodeAtPathStageTest : public AggregationContextFixture {
public:
    intrusive_ptr<DocumentSource> createDocumentSource(BSONObj spec) {
        auto specElem = spec.firstElement();
        return DocumentSourceExplodeAtPath::createFromBson(specElem, getExpCtx());
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

        const auto next = explodeAtPath->getNext();
        if (!next.isEOF()) {
            log() << "expected eof, but instead got status " << static_cast<int>(next.getStatus());
            if (next.isAdvanced()) {
                log() << "value was " << next.getDocument();
            }
            return false;
        }
        return true;
    }
};

TEST_F(ExplodeAtPathStageTest, EmptyColl) {
    ASSERT_TRUE(resultsMatch({}, {}, "array"));
}

TEST_F(ExplodeAtPathStageTest, EmptyArray) {
    ASSERT_TRUE(resultsMatch({Document{{"array", vector<Value>{}}}}, {}, "array"));
}

TEST_F(ExplodeAtPathStageTest, MissingValue) {
    ASSERT_TRUE(resultsMatch({Document{{"unrelated", vector<Value>{}}}}, {}, "array"));
}

TEST_F(ExplodeAtPathStageTest, ScalarValue) {
    ASSERT_TRUE(resultsMatch(
        {{Document{{"array", Value(3)}}}}, {{Document{{"array", Value(3)}}}}, "array"));
}

TEST_F(ExplodeAtPathStageTest, SingletonArray) {
    ASSERT_TRUE(resultsMatch({{Document{{"array", vector<Value>{Value(1)}}}}},
                             {Document{{"array", Value(1)}}},
                             "array"));
}

TEST_F(ExplodeAtPathStageTest, TwoValueArray) {
    ASSERT_TRUE(resultsMatch({{Document{{"array", vector<Value>{Value(3), Value(4)}}}}},
                             {Document{{"array", Value(3)}}, Document{{"array", Value(4)}}},
                             "array"));
}

TEST_F(ExplodeAtPathStageTest, ArrayWithSubObjects) {
    ASSERT_TRUE(
        resultsMatch({Document(fromjson("{array: [{a: 1}, {a: 2}]}"))},
                     {Document(fromjson("{array: {a:1}}")), Document(fromjson("{array: {a:2}}"))},
                     "array"));
}

TEST_F(ExplodeAtPathStageTest, DottedPathOnArrayWithSubObjects) {
    ASSERT_TRUE(
        resultsMatch({Document(fromjson("{array: [{a: 1}, {a: 2}]}"))},
                     {Document(fromjson("{array: {a:1}}")), Document(fromjson("{array: {a:2}}"))},
                     "array.a"));
}

TEST_F(ExplodeAtPathStageTest, DottedPathOnArrayWithSubArrays) {
    ASSERT_TRUE(resultsMatch({Document(fromjson("{array: [{a: [{b: 1}, {b: 2}]}, {a: 3}]}"))},
                             {Document(fromjson("{array: {a:{b:1}}}")),
                              Document(fromjson("{array: {a:{b:2}}}")),
                              Document(fromjson("{array: {a:3}}"))},
                             "array.a"));
}

TEST_F(ExplodeAtPathStageTest, LongerDottedPathOnArrayWithSubArrays) {
    ASSERT_TRUE(resultsMatch(
        {Document(fromjson("{array: [{a: [{b: 1}, {b: 2}]}, {a: 3}]}"))},
        {Document(fromjson("{array: {a: {b: 1}}}")), Document(fromjson("{array: {a: {b: 2}}}"))},
        "array.a.b"));
}

TEST_F(ExplodeAtPathStageTest, NestedArrays) {
    ASSERT_TRUE(
        resultsMatch({Document(fromjson("{array: [[1, 2], [2, 3]]}"))},
                     {Document(fromjson("{array: [1, 2]}")), Document(fromjson("{array: [2, 3]}"))},
                     "array"));
}

TEST_F(ExplodeAtPathStageTest, NestedArraysAreNotTraversedWhenTheyHaveObjects) {
    ASSERT_TRUE(
        resultsMatch({Document(fromjson("{array: [[{a: 1}, 2], [2, 3]]}"))}, {}, "array.a"));
}

TEST_F(ExplodeAtPathStageTest, MultipleDocumentInput) {
    // TODO: multiple documents
}

TEST_F(ExplodeAtPathStageTest, DeeplyNestedPath) {
    // TODO
}

TEST_F(ExplodeAtPathStageTest, AddsUnwoundPathToDependencies) {
    // TODO: ian
    // auto unwind =
    //     DocumentSourceUnwind::create(getExpCtx(), "x.y.z", false,
    //     boost::optional<string>("index"));
    // DepsTracker dependencies;
    // ASSERT_EQUALS(DepsTracker::State::SEE_NEXT, unwind->getDependencies(&dependencies));
    // ASSERT_EQUALS(1U, dependencies.fields.size());
    // ASSERT_EQUALS(1U, dependencies.fields.count("x.y.z"));
    // ASSERT_EQUALS(false, dependencies.needWholeDocument);
    // ASSERT_EQUALS(false, dependencies.getNeedsMetadata(DepsTracker::MetadataType::TEXT_SCORE));
}

TEST_F(ExplodeAtPathStageTest, ShouldPropagatePauses) {
    // TODO: ian
    // const bool includeNullIfEmptyOrMissing = false;
    // const boost::optional<std::string> includeArrayIndex = boost::none;
    // auto unwind = DocumentSourceUnwind::create(
    //     getExpCtx(), "array", includeNullIfEmptyOrMissing, includeArrayIndex);
    // auto source =
    //     DocumentSourceMock::create({Document{{"array", vector<Value>{Value(1), Value(2)}}},
    //                                 DocumentSource::GetNextResult::makePauseExecution(),
    //                                 Document{{"array", vector<Value>{Value(1), Value(2)}}}});

    // unwind->setSource(source.get());

    // ASSERT_TRUE(unwind->getNext().isAdvanced());
    // ASSERT_TRUE(unwind->getNext().isAdvanced());

    // ASSERT_TRUE(unwind->getNext().isPaused());

    // ASSERT_TRUE(unwind->getNext().isAdvanced());
    // ASSERT_TRUE(unwind->getNext().isAdvanced());

    // ASSERT_TRUE(unwind->getNext().isEOF());
    // ASSERT_TRUE(unwind->getNext().isEOF());
}

//
// Error cases.
//

TEST_F(ExplodeAtPathStageTest, ShouldRejectNonString) {
    ASSERT_THROWS_CODE(
        createDocumentSource(BSON("$explodeAtPath" << 1)), AssertionException, 31019);
}

TEST_F(ExplodeAtPathStageTest, ShouldRejectSpecWithoutPath) {
    ASSERT_THROWS_CODE(createDocumentSource(BSON("$explodeAtPath"
                                                 << "")),
                       AssertionException,
                       51173);
}

TEST_F(ExplodeAtPathStageTest, ShouldRejectNonDollarPrefixedPath) {
    ASSERT_THROWS_CODE(createDocumentSource(BSON("$explodePath"
                                                 << "somePath")),
                       AssertionException,
                       51174);
}

}  // namespace
}  // namespace mongo
