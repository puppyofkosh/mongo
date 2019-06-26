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

#include "mongo/db/query/tree_projection_node.h"

#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/matcher/expression_algo.h"
#include "mongo/db/pipeline/transformer_interface.h"
#include "mongo/db/query/query_request.h"

namespace mongo {
void TreeProjectionNode::addProjectionForPath(const FieldPath& path, ProjectionValue v) {
    if (path.getPathLength() == 1) {
        _projections.insert(std::make_pair(path.fullPath(), v));
        return;
    }
    // FieldPath can't be empty, so it is safe to obtain the first path component here.
    addOrGetChild(path.getFieldName(0).toString())->addProjectionForPath(path.tail(), v);
}

TreeProjectionNode* TreeProjectionNode::addOrGetChild(const std::string& field) {
    auto child = getChild(field);
    return child ? child : addChild(field);
}

TreeProjectionNode* TreeProjectionNode::addChild(const std::string& field) {
    invariant(!str::contains(field, "."));
    _orderToProcessAdditionsAndChildren.push_back(field);
    auto insertedPair = _children.emplace(std::make_pair(field, std::make_unique<TreeProjectionNode>(_policies)));
    return insertedPair.first->second.get();
}

TreeProjectionNode* TreeProjectionNode::getChild(const std::string& field) const {
    auto childIt = _children.find(field);
    return childIt == _children.end() ? nullptr : childIt->second.get();
}

}  // namespace mongo
