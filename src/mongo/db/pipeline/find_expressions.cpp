
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

#include "mongo/db/pipeline/expression.h"

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <cstdio>
#include <pcrecpp.h>
#include <utility>
#include <vector>

#include "mongo/db/commands/feature_compatibility_version_documentation.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/find_expressions.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/db/query/datetime/date_time_support.h"
#include "mongo/db/server_options.h"
#include "mongo/platform/bits.h"
#include "mongo/platform/decimal128.h"
#include "mongo/util/regex_util.h"
#include "mongo/util/str.h"
#include "mongo/util/string_map.h"
#include "mongo/util/summation.h"

namespace mongo {


/* -------------------------- ExpressionInternalFindElemMatch ------------------------------ */

REGISTER_EXPRESSION_WITH_MIN_VERSION(
    _internalFindElemMatch,
    ExpressionInternalFindElemMatch::parse,
    // TODO: Should be 4.4!
    ServerGlobalParams::FeatureCompatibility::Version::kFullyUpgradedTo42);

boost::intrusive_ptr<Expression> ExpressionInternalFindElemMatch::parse(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONElement expr,
    const VariablesParseState& vpsIn) {
    uassert(ErrorCodes::BadValue, "expr should be object", expr.type() == BSONType::Object);

    BSONObj obj = expr.embeddedObject();
    uassert(ErrorCodes::BadValue, "path should be string", obj["path"].type() == BSONType::String);
    uassert(
        ErrorCodes::BadValue, "match should be object", obj["match"].type() == BSONType::Object);

    BSONObjBuilder bob;
    bob.append(obj["path"].String(), expr["match"].embeddedObject());
    BSONObj match = bob.obj();

    std::unique_ptr<MatchExpression> matcher =
        uassertStatusOK(MatchExpressionParser::parse(match, expCtx));

    return ExpressionInternalFindElemMatch::create(
        expCtx, obj["path"].String(), match, std::move(matcher));
}

boost::intrusive_ptr<Expression> ExpressionInternalFindElemMatch::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const std::string& fp,
    BSONObj elemMatchObj,
    std::unique_ptr<MatchExpression> matchExpr) {

    // TODO: this create() is deprecated. Why?
    auto fieldPathExpr = ExpressionFieldPath::create(expCtx, fp);
    return new ExpressionInternalFindElemMatch(
        expCtx, std::move(fieldPathExpr), elemMatchObj, std::move(matchExpr));
}

Value ExpressionInternalFindElemMatch::evaluate(const Document& root) const {
    // Apply the elemMatch.
    Value val = _fieldPathToMatchOn->evaluate(root);

    MatchDetails arrayDetails;
    arrayDetails.requestElemMatchKey();

    if (!_matchExpr->matchesBSON(root.toBson(), &arrayDetails)) {
        return Value();
    }

    if (val.getType() != BSONType::Array) {
        return val;
    }

    boost::optional<size_t> optIndex = str::parseUnsignedBase10Integer(arrayDetails.elemMatchKey());
    invariant(optIndex);
    Value matchingElem = val[*optIndex];

    invariant(!matchingElem.missing());
    return Value(std::vector<Value>{matchingElem});
}

boost::intrusive_ptr<Expression> ExpressionInternalFindElemMatch::optimize() {
    return this;
}

Value ExpressionInternalFindElemMatch::serialize(bool explain) const {
    MONGO_UNREACHABLE;
}

/* -------------------------- ExpressionInternalFindPositional ------------------------------ */
REGISTER_EXPRESSION_WITH_MIN_VERSION(
    _internalFindPositional,
    ExpressionInternalFindPositional::parse,
    // TODO: Should be 4.4!
    ServerGlobalParams::FeatureCompatibility::Version::kFullyUpgradedTo42);
boost::intrusive_ptr<Expression> ExpressionInternalFindPositional::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const std::string& fp,
    std::unique_ptr<MatchExpression> matchExpr) {

    // TODO: this create() is deprecated. Why?
    auto fieldPathExpr = ExpressionFieldPath::create(expCtx, fp);
    return new ExpressionInternalFindPositional(
        expCtx, std::move(fieldPathExpr), std::move(matchExpr));
}

// TODO: maybe use ExpressionFixedArity
boost::intrusive_ptr<Expression> ExpressionInternalFindPositional::parse(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    BSONElement expr,
    const VariablesParseState& vpsIn) {
    uassert(ErrorCodes::BadValue, "expr should be object", expr.type() == BSONType::Object);

    BSONObj obj = expr.embeddedObject();
    uassert(
        ErrorCodes::BadValue, "match should be object", obj["match"].type() == BSONType::Object);
    auto match =
        uassertStatusOK(MatchExpressionParser::parse(obj["match"].embeddedObject(), expCtx));

    uassert(ErrorCodes::BadValue,
            "field name should be string",
            obj["field"].type() == BSONType::String);
    auto fieldName = obj["field"].String();

    return ExpressionInternalFindPositional::create(expCtx, fieldName, std::move(match));
}

Value ExpressionInternalFindPositional::evaluate(const Document& root) const {
    MatchDetails details;
    details.requestElemMatchKey();
    invariant(_matchExpr);

    invariant(_matchExpr->matchesBSON(root.toBson(), &details));

    // Match existing behavior in find().
    uassert(ErrorCodes::BadValue,
            "positional operator '.$' requires correspoding field in query specifier",
            details.hasElemMatchKey());

    Value val = _fieldPathToMatchOn->evaluate(root);
    if (val.getType() != BSONType::Array) {
        return val;
    }

    // Return an array with the first matching element.
    boost::optional<size_t> optIndex = str::parseUnsignedBase10Integer(details.elemMatchKey());
    invariant(optIndex);
    Value matchingElem = val[*optIndex];

    // Match existing behavior in find().
    uassert(ErrorCodes::BadValue, "positional operator element mismatch", !matchingElem.missing());

    std::cout << "matchingElem is " << matchingElem << std::endl;
    invariant(!matchingElem.missing());
    return Value(std::vector<Value>{matchingElem});
}

Value ExpressionInternalFindPositional::serialize(bool explain) const {
    MONGO_UNREACHABLE;
}

boost::intrusive_ptr<Expression> ExpressionInternalFindPositional::optimize() {
    return this;
}

}  // namespace mongo
