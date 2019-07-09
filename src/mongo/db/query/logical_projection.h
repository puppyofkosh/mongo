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

#pragma once

#include "mongo/db/jsobj.h"
#include "mongo/db/matcher/expression_parser.h"
#include "mongo/db/pipeline/projection_policies.h"

#include "mongo/db/query/projection_desugarer.h"
#include "mongo/util/str.h"

namespace mongo {

using ComputedFieldsPolicy = ProjectionPolicies::ComputedFieldsPolicy;

/**
 * This class is responsible for determining what type of $project stage it specifies.
 */
class LogicalProjection {
public:
    enum class ProjectType { kInclusion, kExclusion };

    LogicalProjection(const BSONObj& spec, ProjectionPolicies policies)
        : _rawObj(spec), _policies(policies) {}

    /**
     * Returns true if the projection requires match details from the query, and false
     * otherwise. This is only relevant for find() projection, because of the positional projection
     * operator.
     */
    bool requiresMatchDetails() const {
        return _requiresMatchDetails;
    }

    /**
     * Is the full document required to compute this projection?
     */
    bool requiresDocument() const {
        invariant(!(_hasExpression && (*_parsedType == ProjectType::kExclusion)));

        return (_hasExpression || (*_parsedType == ProjectType::kExclusion)) &&
            !_hasIndexKeyProjection;
    }

    const std::vector<std::string>& sortKeyMetaFields() const {
        return _sortKeyMetaFields;
    }

    bool needsSortKey() const {
        return !_sortKeyMetaFields.empty();
    }

    /**
     * If requiresDocument() == false, what fields are required to compute
     * the projection?
     */
    const std::vector<std::string>& getRequiredFields() const {
        return _requiredFields;
    }

    /**
     * Get the raw BSONObj proj spec obj
     */
    const BSONObj& getProjObj() const {
        return _rawObj;
    }

    bool wantTextScore() const {
        return _wantTextScore;
    }

    /**
     * Does the projection want geoNear metadata?  If so any geoNear stage should include them.
     */
    bool wantGeoNearDistance() const {
        return _wantGeoNearDistance;
    }

    bool wantGeoNearPoint() const {
        return _wantGeoNearPoint;
    }

    bool wantIndexKey() const {
        return _hasIndexKeyProjection;
    }

    bool wantSortKey() const {
        return _wantSortKey;
    }

    /**
     * Returns true if the element at 'path' is preserved entirely after this projection is applied,
     * and false otherwise. For example, the projection {a: 1} will preserve the element located at
     * 'a.b', and the projection {'a.b': 0} will not preserve the element located at 'a'.
     */
    bool isFieldRetainedExactly(StringData path) const;

    /**
     * Returns true if the project contains any paths with multiple path pieces (e.g. returns true
     * for {_id: 0, "a.b": 1} and returns false for {_id: 0, a: 1, b: 1}).
     */
    bool hasDottedFieldPath() const {
        return _hasDottedFieldPath;
    }

    /**
     * Parses 'spec' to determine whether it is an inclusion or exclusion projection. 'Computed'
     * fields (ones which are defined by an expression or a literal) are treated as inclusion
     * projections for in this context of the $project stage.
     */
    static std::unique_ptr<LogicalProjection> parse(const DesugaredProjection& spec,
                                                    ProjectionPolicies policies) {
        auto parser = std::make_unique<LogicalProjection>(spec.desugaredObj, policies);
        parser->parse();
        invariant(parser->_parsedType);

        return parser;
    }

    ProjectType type() {
        invariant(_parsedType);
        return *_parsedType;
    }

    ProjectionPolicies getPolicies() {
        return _policies;
    }

private:
    /**
     * Traverses '_rawObj' to determine the type of projection, populating '_parsedType' in the
     * process.
     */
    void parse();

    /**
     * Parses a single BSONElement. 'pathToElem' should include the field name of 'elem'.
     *
     * Delegates to parseSubObject() if 'elem' is an object. Otherwise updates '_parsedType' if
     * appropriate.
     *
     * Throws a AssertionException if this element represents a mix of projection types. If we are
     * parsing in ComputedFieldsPolicy::kBanComputedFields mode, an inclusion projection
     * which contains computed fields will also be rejected.
     */
    void parseElement(const BSONElement& elem, const FieldPath& pathToElem);

    /**
     * Traverses 'thisLevelSpec', parsing each element in turn.
     *
     * Throws a AssertionException if 'thisLevelSpec' represents an invalid mix of projections. If
     * we are parsing in ComputedFieldsPolicy::kBanComputedFields mode, an inclusion
     * projection which contains computed fields will also be rejected.
     */
    void parseNestedObject(const BSONObj& thisLevelSpec, const FieldPath& prefix);

    void parseMetaObject(StringData fieldName, StringData metadataRequested);


    // The original object. Used to generate more helpful error messages.
    BSONObj _rawObj;

    // This will be populated during parse().
    boost::optional<ProjectType> _parsedType;

    // Policies associated with the projection which determine its runtime behaviour.
    ProjectionPolicies _policies;

    // Whether there's a positional projection.
    bool _requiresMatchDetails = false;
    bool _hasExpression = false;

    bool _hasId = true;

    bool _wantTextScore = false;
    bool _hasIndexKeyProjection = false;
    bool _wantGeoNearDistance = false;
    bool _wantGeoNearPoint = false;
    bool _wantSortKey = false;

    bool _hasDottedFieldPath = false;

    // Track fields required so we can get a covered projection in certain cases.  While parsing,
    // we will begin to populate this, but if we discover that it's an exclusion projection or it's
    // not coverable, it will be re-set to empty.
    std::vector<std::string> _requiredFields;

    // Which fields were specifically excluded.
    std::vector<std::string> _excludedFields;

    // Keep track of which fields have expressions as values.
    std::vector<std::string> _expressionFields;

    // All of the fields which had sortKey metadata requested about them.
    std::vector<std::string> _sortKeyMetaFields;
};

}  // namespace mongo
