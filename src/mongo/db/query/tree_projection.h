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
#include "mongo/db/query/logical_projection.h"
#include "mongo/db/query/tree_projection_node.h"
#include "mongo/util/str.h"

namespace mongo {

using ComputedFieldsPolicy = ProjectionPolicies::ComputedFieldsPolicy;

/**
 * This class is responsible for determining what type of $project stage it specifies.
 */
class TreeProjection {
public:
    enum class ProjectType { kInclusion, kExclusion };

    TreeProjection(ProjectionPolicies p)
        : policies(p), _root(new TreeProjectionNode(p)) {}

    /**
     * Parses 'spec' to determine whether it is an inclusion or exclusion projection. 'Computed'
     * fields (ones which are defined by an expression or a literal) are treated as inclusion
     * projections for in this context of the $project stage.
     */
    static std::unique_ptr<TreeProjection> parse(const LogicalProjection& spec,
                                                 ProjectionPolicies policies) {
        auto t = std::make_unique<TreeProjection>(policies);
        t->buildLogicalProjectionTree(spec.getProjObj());
        return t;
    }

    TreeProjectionNode* root() {
        return _root.get();
    }

    const ProjectionPolicies policies;

private:
    // For building the logical tree
    bool parseObjectAsExpression(StringData pathToObject,
                                 const BSONObj& objSpec,
                                 TreeProjectionNode* parent);
    void buildLogicalProjectionTree(const BSONObj& spec);
    void parseSubObject(const BSONObj& subObj, TreeProjectionNode* node);

    // Tree representation
    std::unique_ptr<TreeProjectionNode> _root;
};

}  // namespace mongo
