/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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
#include "mongo/db/pipeline/dependencies.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/query/projection_ast.h"
#include "mongo/util/str.h"

namespace mongo {

namespace projection_ast {

struct ProjectionDependencies {
    // Whether MatchDetails of the query's filter are required.
    bool requiresMatchDetails = false;
    bool requiresDocument = false;
    boost::optional<std::vector<std::string>> requiredFields;

    bool needsGeoDistance = false;
    bool needsGeoPoint = false;
    bool needsSortKey = false;
    bool needsTextScore = false;

    bool hasDottedPath = false;
};

//
// Used to represent a projection by itself and do dependency analysis.
//
enum class ProjectType { kInclusion, kExclusion };
class Projection {
public:

    // Static function for determining what the projection depends on.
    static ProjectionDependencies analyzeProjection(ProjectionPathASTNode* root, ProjectType type);
    
    Projection(ProjectionPathASTNode root, ProjectType type);

    ProjectionPathASTNode* root() {
        return &_root;
    }

    ProjectType type() const {
        return _type;
    }

    /**
     * Returns true if the projection requires match details from the query,
     * and false otherwise.
     */
    bool requiresMatchDetails() const {
        return _deps.requiresMatchDetails;
    }

    /**
     * Is the full document required to compute this projection?
     */
    bool requiresDocument() const {
        return _deps.requiresDocument;
    }

    /**
     * If requiresDocument() == false, what fields are required to compute
     * the projection?
     *
     * Returned StringDatas are owned by, and have the lifetime of, the ParsedProjection.
     */
    const std::vector<StringData>& getRequiredFields() const {
        MONGO_UNREACHABLE;
    }

    /**
     * Get the raw BSONObj proj spec obj
     */
    // const BSONObj& getProjObj() const {
    //     return _source;
    // }

    // TODO: ian: replace with a wantMetadata(MetaType) function.
    /**
     * Does the projection want geoNear metadata?  If so any geoNear stage should include them.
     */
    bool wantGeoNearDistance() const {
        return _deps.needsGeoDistance;
    }

    bool wantGeoNearPoint() const {
        return _deps.needsGeoPoint;
    }

    bool wantSortKey() const {
        return _deps.needsSortKey;
    }

    bool wantTextScore() const {
        return _deps.needsTextScore;
    }
    
    /**
     * Returns true if the element at 'path' is preserved entirely after this projection is applied,
     * and false otherwise. For example, the projection {a: 1} will preserve the element located at
     * 'a.b', and the projection {'a.b': 0} will not preserve the element located at 'a'.
     */
    bool isFieldRetainedExactly(StringData path) {
        MONGO_UNREACHABLE;
    }

    /**
     * Returns true if the project contains any paths with multiple path pieces (e.g. returns true
     * for {_id: 0, "a.b": 1} and returns false for {_id: 0, a: 1, b: 1}).
     */
    bool hasDottedFieldPath() const {
        return _deps.hasDottedPath;
    }

private:
    ProjectionPathASTNode _root;
    ProjectType _type;

    ProjectionDependencies _deps;
};

}  // namespace projection_ast
}  // namespace mongo
