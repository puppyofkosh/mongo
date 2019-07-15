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

#include "mongo/db/query/find_projection_ast.h"

namespace mongo {
namespace {
bool isPositionalOperator(const char* fieldName) {
    return str::contains(fieldName, ".$") && !str::contains(fieldName, ".$ref") &&
        !str::contains(fieldName, ".$id") && !str::contains(fieldName, ".$db");
}

bool hasPositionalOperatorMatch(const MatchExpression* const query, StringData matchfield) {
    if (query->getCategory() == MatchExpression::MatchCategory::kLogical) {
        for (unsigned int i = 0; i < query->numChildren(); ++i) {
            if (hasPositionalOperatorMatch(query->getChild(i), matchfield)) {
                return true;
            }
        }
    } else {
        StringData queryPath = query->path();
        // We have to make a distinction between match expressions that are
        // initialized with an empty field/path name "" and match expressions
        // for which the path is not meaningful (eg. $where).
        if (!queryPath.rawData()) {
            return false;
        }
        StringData pathPrefix = str::before(queryPath, '.');
        return pathPrefix == matchfield;
    }
    return false;
}
}

namespace find_projection_ast {
FindProjectionAST FindProjectionAST::fromBson(const BSONObj& b,
                                              const MatchExpression* const query) {
    std::vector<std::unique_ptr<ProjectionASTNode>> node;

    // TODO:

    bool hasPositional = false;
    bool hasElemMatch = false;
    boost::optional<ProjectType> type;
    for (auto&& elem : b) {

        if (elem.type() == BSONType::Object) {
            BSONObj obj = elem.embeddedObject();
            BSONElement e2 = obj.firstElement();

            if (e2.fieldNameStringData() == "$slice") {
                if (e2.isNumber()) {
                    // This is A-OK.
                    node.push_back(std::make_unique<ProjectionASTNodeSlice>(
                        elem.fieldNameStringData(), e2.numberInt(), 0));
                } else if (e2.type() == Array) {
                    BSONObj arr = e2.embeddedObject();
                    if (2 != arr.nFields()) {
                        uasserted(ErrorCodes::BadValue, "$slice array wrong size");
                    }

                    BSONObjIterator it(arr);
                    int skip = it.next().numberInt();
                    // Skip over 'skip'.
                    it.next();
                    int limit = it.next().numberInt();
                    if (limit <= 0) {
                        uasserted(ErrorCodes::BadValue, "$slice limit must be positive");
                    }
                    node.push_back(std::make_unique<ProjectionASTNodeSlice>(
                        elem.fieldNameStringData(), skip, limit));
                } else {
                    uasserted(ErrorCodes::BadValue,
                              "$slice only supports numbers and [skip, limit] arrays");
                }
            } else if (e2.fieldNameStringData() == "$elemMatch") {
                // Validate $elemMatch arguments and dependencies.
                if (Object != e2.type()) {
                    uasserted(ErrorCodes::BadValue,
                              "elemMatch: Invalid argument, object required.");
                }

                if (hasPositional) {
                    uasserted(ErrorCodes::BadValue,
                              "Cannot specify positional operator and $elemMatch.");
                }

                if (str::contains(elem.fieldName(), '.')) {
                    uasserted(ErrorCodes::BadValue,
                              "Cannot use $elemMatch projection on a nested field.");
                }

                // Create a MatchExpression for the elemMatch.
                BSONObj elemMatchObj = elem.wrap();
                invariant(elemMatchObj.isOwned());

                // Not parsing the match expression itself because it would require an
                // expressionContext + operationContext.
                node.push_back(std::make_unique<ProjectionASTNodeElemMatch>(
                    elem.fieldNameStringData(), elemMatchObj));
                hasElemMatch = true;
            }

        } else if (elem.trueValue()) {
            if (!isPositionalOperator(elem.fieldName())) {
                node.push_back(
                    std::make_unique<ProjectionASTNodeInclusion>(elem.fieldNameStringData()));
            } else {
                // TODO continue
                node.push_back(std::make_unique<ProjectionASTNodePositional>(
                    elem.fieldNameStringData().toString()));

                if (hasPositional) {
                    uasserted(ErrorCodes::BadValue,
                              "Cannot specify more than one positional proj. per query.");
                }

                if (hasElemMatch) {
                    uasserted(ErrorCodes::BadValue,
                              "Cannot specify positional operator and $elemMatch.");
                }

                StringData after = str::after(elem.fieldNameStringData(), ".$");
                if (after.find(".$"_sd) != std::string::npos) {
                    str::stream ss;
                    ss << "Positional projection '" << elem.fieldName() << "' contains "
                       << "the positional operator more than once.";
                    uasserted(ErrorCodes::BadValue, ss);
                }

                StringData matchfield = str::before(elem.fieldNameStringData(), '.');
                if (query && !hasPositionalOperatorMatch(query, matchfield)) {
                    str::stream ss;
                    ss << "Positional projection '" << elem.fieldName() << "' does not "
                       << "match the query document.";
                    uasserted(ErrorCodes::BadValue, ss);
                }

                hasPositional = true;
            }

            uassert(ErrorCodes::BadValue,
                    "Should be inclusion",
                    !type || *type == ProjectType::kInclusion);
            type = ProjectType::kInclusion;
        } else {
            invariant(!elem.trueValue());
            node.push_back(std::make_unique<ProjectionASTNodeExclusion>(
                elem.fieldNameStringData().toString()));

            if (elem.fieldNameStringData() != "_id") {
                uassert(ErrorCodes::BadValue,
                        "Should be exclusion",
                        !type || *type == ProjectType::kExclusion);
                type = ProjectType::kExclusion;
            }
        }
    }

    return FindProjectionAST{std::move(node), b, type ? *type : ProjectType::kExclusion};
}
}
}
