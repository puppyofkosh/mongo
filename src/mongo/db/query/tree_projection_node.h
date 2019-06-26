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

// TODO: This is awful
struct ProjectionValue {
    ProjectionValue(BSONObj b) : rawExpression(b) {}
    ProjectionValue(BSONElement e) : rawValue(e) {}
    ProjectionValue(bool v) : included(v) {}

    // Either it's an expression
    boost::optional<BSONObj> rawExpression;

    // or a true/false value
    boost::optional<bool> included;

    // or a raw value (computed field)
    boost::optional<BSONElement> rawValue;
};

class TreeProjectionNode {
public:
    TreeProjectionNode(ProjectionPolicies p) : _policies(p) {}

    /**
     * Recursively adds 'path' into the tree as a projected field, creating any child nodes if
     * necessary.
     */
    void addProjectionForPath(const FieldPath& path, ProjectionValue elt);

    /**
     * Creates the child if it doesn't already exist. 'field' is not allowed to be dotted. Returns
     * the child node if it already exists, or the newly-created child otherwise.
     */
    TreeProjectionNode* addOrGetChild(const std::string& field);

private:
    TreeProjectionNode* addChild(const std::string& field);
    TreeProjectionNode* getChild(const std::string& field) const;
    
    stdx::unordered_map<std::string, std::unique_ptr<TreeProjectionNode>> _children;

    // non-dotted field name -> element representing projection value.
    StringMap<ProjectionValue> _projections;
    const ProjectionPolicies _policies;

    // Our projection semantics are such that all field additions need to be processed in the order
    // specified. '_orderToProcessAdditionsAndChildren' tracks that order.
    //
    // For example, for the specification {a: <expression>, "b.c": <expression>, d: <expression>},
    // we need to add the top level fields in the order "a", then "b", then "d". This ordering
    // information needs to be tracked separately, since "a" and "d" will be tracked via
    // '_expressions', and "b.c" will be tracked as a child ProjectionNode in '_children'. For the
    // example above, '_orderToProcessAdditionsAndChildren' would be ["a", "b", "d"].
    std::vector<std::string> _orderToProcessAdditionsAndChildren;
};
}  // namespace mongo
