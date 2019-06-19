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

#include "mongo/db/pipeline/parsed_aggregation_projection.h"

#include <boost/optional.hpp>
#include <string>

#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/matcher/expression_algo.h"
#include "mongo/db/pipeline/field_path.h"
#include "mongo/db/pipeline/parsed_exclusion_projection.h"
#include "mongo/db/pipeline/parsed_inclusion_projection.h"
#include "mongo/db/query/logical_projection.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/str.h"

namespace mongo {
namespace parsed_aggregation_projection {

using TransformerType = TransformerInterface::TransformerType;

using expression::isPathPrefixOf;

//
// ProjectionSpecValidator
//

void ProjectionSpecValidator::uassertValid(const BSONObj& spec, StringData stageName) {
    try {
        ProjectionSpecValidator(spec).validate();
    } catch (DBException& ex) {
        ex.addContext("Invalid " + stageName.toString());
        throw;
    }
}

void ProjectionSpecValidator::ensurePathDoesNotConflictOrThrow(const std::string& path) {
    auto result = _seenPaths.emplace(path);
    auto pos = result.first;

    // Check whether the path was a duplicate of an existing path.
    auto conflictingPath = boost::make_optional(!result.second, *pos);

    // Check whether the preceding path prefixes this path.
    if (!conflictingPath && pos != _seenPaths.begin()) {
        conflictingPath =
            boost::make_optional(isPathPrefixOf(*std::prev(pos), path), *std::prev(pos));
    }

    // Check whether this path prefixes the subsequent path.
    if (!conflictingPath && std::next(pos) != _seenPaths.end()) {
        conflictingPath =
            boost::make_optional(isPathPrefixOf(path, *std::next(pos)), *std::next(pos));
    }

    uassert(40176,
            str::stream() << "specification contains two conflicting paths. "
                             "Cannot specify both '"
                          << path
                          << "' and '"
                          << *conflictingPath
                          << "': "
                          << _rawObj.toString(),
            !conflictingPath);
}

void ProjectionSpecValidator::validate() {
    if (_rawObj.isEmpty()) {
        uasserted(40177, "specification must have at least one field");
    }
    for (auto&& elem : _rawObj) {
        std::cout << "ian: parsing elem " << elem << std::endl;
        parseElement(elem, FieldPath(elem.fieldName(), true));
    }
}

void ProjectionSpecValidator::parseElement(const BSONElement& elem, const FieldPath& pathToElem) {
    if (elem.type() == BSONType::Object) {
        parseNestedObject(elem.Obj(), pathToElem);
    } else {
        ensurePathDoesNotConflictOrThrow(pathToElem.fullPath());
    }
}

void ProjectionSpecValidator::parseNestedObject(const BSONObj& thisLevelSpec,
                                                const FieldPath& prefix) {
    if (thisLevelSpec.isEmpty()) {
        uasserted(
            40180,
            str::stream() << "an empty object is not a valid value. Found empty object at path "
                          << prefix.fullPath());
    }
    for (auto&& elem : thisLevelSpec) {
        auto fieldName = elem.fieldNameStringData();
        if (fieldName[0] == '$') {
            // This object is an expression specification like {$add: [...]}. It will be parsed
            // into an Expression later, but for now, just track that the prefix has been
            // specified and skip it.
            if (thisLevelSpec.nFields() != 1) {
                uasserted(40181,
                          str::stream() << "an expression specification must contain exactly "
                                           "one field, the name of the expression. Found "
                                        << thisLevelSpec.nFields()
                                        << " fields in "
                                        << thisLevelSpec.toString()
                                        << ", while parsing object "
                                        << _rawObj.toString());
            }
            ensurePathDoesNotConflictOrThrow(prefix.fullPath());
            continue;
        }
        if (fieldName.find('.') != std::string::npos) {
            uasserted(40183,
                      str::stream() << "cannot use dotted field name '" << fieldName
                                    << "' in a sub object: "
                                    << _rawObj.toString());
        }
        parseElement(elem, FieldPath::getFullyQualifiedPath(prefix.fullPath(), fieldName));
    }
}

namespace {

using ComputedFieldsPolicy = ProjectionPolicies::ComputedFieldsPolicy;

}  // namespace

std::unique_ptr<ParsedAggregationProjection> ParsedAggregationProjection::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    LogicalProjection* lp,
    ProjectionPolicies policies,
    const MatchExpression* matchExpression) {
    // We can't use make_unique() here, since the branches have different types.
    std::unique_ptr<ParsedAggregationProjection> parsedProject(
        lp->type() == LogicalProjection::ProjectType::kInclusion
            ? static_cast<ParsedAggregationProjection*>(
                  new ParsedInclusionProjection(expCtx, policies, matchExpression))
            : static_cast<ParsedAggregationProjection*>(
                  new ParsedExclusionProjection(expCtx, policies)));

    // Actually parse the specification.
    parsedProject->parse(lp->getProjObj());
    return parsedProject;
}

std::unique_ptr<ParsedAggregationProjection> ParsedAggregationProjection::create(
    const boost::intrusive_ptr<ExpressionContext>& expCtx,
    const BSONObj& spec,
    ProjectionPolicies policies,
    const MatchExpression* matchExpression) {
    // Check that the specification was valid. Status returned is unspecific because validate()
    // is used by the $addFields stage as well as $project.
    // If there was an error, uassert with a $project-specific message.
    ProjectionSpecValidator::uassertValid(spec, "$project");

    auto lp = LogicalProjection::parse({spec}, policies);

    return create(expCtx, lp.get(), policies, matchExpression);
}
}  // namespace parsed_aggregation_projection
}  // namespace mongo
