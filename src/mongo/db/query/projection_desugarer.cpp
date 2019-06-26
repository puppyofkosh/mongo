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

#include "mongo/db/query/logical_projection.h"

#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/matcher/expression_algo.h"
#include "mongo/db/pipeline/transformer_interface.h"
#include "mongo/db/query/query_request.h"

namespace mongo {
namespace projection_desugarer {

namespace {
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

bool isPositionalOperator(StringData fieldName) {
    return str::contains(fieldName, ".$") && !str::contains(fieldName, ".$ref") &&
        !str::contains(fieldName, ".$id") && !str::contains(fieldName, ".$db");
}

void validatePositionalProjection(const std::string& lhs, const MatchExpression* query) {
    StringData after = str::after(lhs, ".$");
    if (after.find(".$"_sd) != std::string::npos) {
        uasserted(ErrorCodes::BadValue,
                  str::stream() << "Positional projection '" << lhs << "' contains "
                                << "the positional operator more than once.");
    }

    StringData matchfield = str::before(lhs, '.');
    if (query && !hasPositionalOperatorMatch(query, matchfield)) {
        uasserted(ErrorCodes::BadValue,
                  str::stream() << "Positional projection '" << lhs << "' does not "
                                << "match the query document.");
    }
}
boost::optional<BSONObj> convertToAggSlice(BSONElement elt) {
    if (elt.type() == BSONType::Object) {
        BSONObj obj = elt.embeddedObject();

        BSONElement firstElem = obj.firstElement();
        if (firstElem.fieldNameStringData() == "$slice") {
            if (firstElem.isNumber()) {
                return BSON("$slice" << BSON_ARRAY(("$" + elt.fieldNameStringData())
                                                   << obj.firstElement().numberInt()));
            } else if (firstElem.type() == BSONType::Array) {
                BSONObj arr = firstElem.embeddedObject();
                invariant(2 == arr.nFields());

                BSONObjIterator it(arr);
                int skip = it.next().numberInt();
                int limit = it.next().numberInt();

                return BSON("$slice"
                            << BSON_ARRAY(("$" + elt.fieldNameStringData()) << skip << limit));
            }
        }
    }

    return boost::none;
}

}  // namespace

// TODO: Eventually this should probably do two passes: the first checks whether there are even any
// positional projections
DesugaredProjection desugarProjection(const BSONObj& originalProjection, MatchExpression* me) {
    BSONObjBuilder bob;

    bool foundPositional = false;
    for (auto&& elem : originalProjection) {
        if (!isPositionalOperator(elem.fieldNameStringData())) {

            // If it's not positional is it $slice?
            auto convertedSlice = convertToAggSlice(elem);
            if (convertedSlice) {
                bob.append(elem.fieldNameStringData(), *convertedSlice);
                continue;
            }

            if (elem.type() == BSONType::Object) {
                BSONObj obj = elem.embeddedObject();

                // Is it an $elemMatch?
                if (obj.firstElementFieldNameStringData() == "$elemMatch") {
                    BSONObjBuilder convertedElemMatch(bob.subobjStart(elem.fieldNameStringData()));
                    BSONObjBuilder elemMatch(
                        convertedElemMatch.subobjStart("$_internalFindElemMatch"));
                    elemMatch.append("path", elem.fieldNameStringData());
                    uassert(ErrorCodes::BadValue,
                            "$elemMatch should be object",
                            obj.firstElement().type() == BSONType::Object);
                    elemMatch.append("match", obj);

                    continue;
                }
            }

            bob.append(elem);
            continue;
        }

        uassert(ErrorCodes::BadValue,
                "Cannot specify more than one positional proj. per query.",
                !foundPositional);
        foundPositional = true;

        validatePositionalProjection(elem.fieldName(), me);

        // In order to be consistent with existing behavior, we actually just find the place before
        // the '.' (even though you'd think it should be before the ".$").
        StringData beforePositional = str::before(elem.fieldNameStringData(), ".$");

        {
            BSONObjBuilder subObj(bob.subobjStart(beforePositional));
            BSONObjBuilder elemMatch(subObj.subobjStart("$_internalFindPositional"));

            elemMatch.append("field", beforePositional);

            BSONObjBuilder match(subObj.subobjStart("match"));
            me->serialize(&match);
        }
    }

    return {bob.obj()};
}
}  // namespace projection_desugarer
}  // namespace mongo
