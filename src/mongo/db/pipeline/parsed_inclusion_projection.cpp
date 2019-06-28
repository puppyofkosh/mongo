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

#include "mongo/db/pipeline/parsed_inclusion_projection.h"

#include "mongo/db/pipeline/find_expressions.h"

#include <algorithm>

namespace mongo {

namespace parsed_aggregation_projection {

//
// InclusionNode
//

InclusionNode::InclusionNode(ProjectionPolicies policies, std::string pathToNode)
    : ProjectionNode(policies, std::move(pathToNode)) {}

InclusionNode* InclusionNode::addOrGetChild(const std::string& field) {
    return static_cast<InclusionNode*>(ProjectionNode::addOrGetChild(field));
}

void InclusionNode::reportDependencies(DepsTracker* deps) const {
    for (auto&& includedField : _projectedFields) {
        deps->fields.insert(FieldPath::getFullyQualifiedPath(_pathToNode, includedField));
    }

    if (!_pathToNode.empty() && !_expressions.empty()) {
        // The shape of any computed fields in the output will change depending on if the field is
        // an array or not, so in addition to any dependencies of the expression itself, we need to
        // add this field to our dependencies.
        deps->fields.insert(_pathToNode);
    }

    for (auto&& expressionPair : _expressions) {
        expressionPair.second->addDependencies(deps);
    }
    for (auto&& childPair : _children) {
        childPair.second->reportDependencies(deps);
    }
}

Document ParsedInclusionProjection::applyProjection(const Document& inputDoc) const {
    // All expressions will be evaluated in the context of the input document, before any
    // transformations have been applied.
    return _root->applyToDocument(inputDoc);
}

bool ParsedInclusionProjection::isSubsetOfProjection(const BSONObj& proj) const {
    std::set<std::string> preservedPaths;
    _root->reportProjectedPaths(&preservedPaths);
    for (auto&& includedField : preservedPaths) {
        if (!proj.hasField(includedField))
            return false;
    }

    // If the inclusion has any computed fields or renamed fields, then it's not a subset.
    std::set<std::string> computedPaths;
    StringMap<std::string> renamedPaths;
    _root->reportComputedPaths(&computedPaths, &renamedPaths);
    return computedPaths.empty() && renamedPaths.empty();
}

void ParsedInclusionProjection::convertNode(TreeProjectionNode* tp,
                                            InclusionNode* ic,
                                            bool isTopLevel) {
    // Tracks whether or not we should apply the default _id projection policy.
    bool idSpecified = false;

    for (auto&& projValue : tp->getProjections()) {
        idSpecified |= projValue.first == "_id"_sd;

        if (projValue.second.rawExpression) {
            ic->addExpressionForPath(projValue.first,
                                     Expression::parseExpression(_expCtx,
                                                                 *projValue.second.rawExpression,
                                                                 _expCtx->variablesParseState));

        } else if (projValue.second.included) {
            if (isTopLevel && projValue.first == "_id") {
                if (!*projValue.second.included) {
                    // Ignoring "_id" here will cause it to be excluded from result documents.
                    _idExcluded = true;
                    continue;
                }
            }

            ic->addProjectionForPath(projValue.first);
        } else if (projValue.second.rawValue) {
            _root->addExpressionForPath(projValue.first,
                                        Expression::parseOperand(_expCtx,
                                                                 *projValue.second.rawValue,
                                                                 _expCtx->variablesParseState));
        }
    }

    // Use the default policy if no _id was specified and we're parsing the top level of the
    // projection.
    if (isTopLevel && !idSpecified) {
        // _id wasn't specified, so apply the default _id projection policy here.
        if (_policies.idPolicy == ProjectionPolicies::DefaultIdPolicy::kExcludeId) {
            _idExcluded = true;
        } else {
            _root->addProjectionForPath(FieldPath("_id"));
        }
    }

    // Deal with nested projections
    for (auto&& child : tp->getChildren()) {
        InclusionNode* icChild = ic->addOrGetChild(child.first);
        convertNode(child.second.get(), icChild, false);
    }

    ic->setProcessingOrder(tp->getProcessingOrder());
}

void ParsedInclusionProjection::convertTree(TreeProjection* tp, InclusionNode* root) {
    convertNode(tp->root(), root, true);
}

ParsedInclusionProjection::ParsedInclusionProjection(
    const boost::intrusive_ptr<ExpressionContext>& expCtx, TreeProjection* tp)
    : ParsedAggregationProjection(expCtx, tp->policies), _root(new InclusionNode(tp->policies)) {
    convertTree(tp, _root.get());
}

}  // namespace parsed_aggregation_projection
}  // namespace mongo
