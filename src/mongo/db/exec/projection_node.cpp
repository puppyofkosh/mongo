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

#include "mongo/db/exec/projection_node.h"

namespace mongo::projection_executor {
using ArrayRecursionPolicy = ProjectionPolicies::ArrayRecursionPolicy;
using ComputedFieldsPolicy = ProjectionPolicies::ComputedFieldsPolicy;
using DefaultIdPolicy = ProjectionPolicies::DefaultIdPolicy;

ProjectionNodeBase::ProjectionNodeBase(ProjectionPolicies policies, std::string pathToNode)
    : _policies(policies), _pathToNode(std::move(pathToNode)) {}

void ProjectionNodeDocument::addProjectionForPath(const FieldPath& path) {
    makeOptimizationsStale();
    if (path.getPathLength() == 1) {
        _projectedFields.insert(path.fullPath());
        return;
    }
    // FieldPath can't be empty, so it is safe to obtain the first path component here.
    auto child = addOrGetChild(path.getFieldName(0).toString());
    invariant(dynamic_cast<ProjectionNodeDocument*>(child));
    static_cast<ProjectionNodeDocument*>(child)->addProjectionForPath(path.tail());
}

void ProjectionNodeDocument::addProjectionForArrayIndexPath(const ArrayIndexPathView& path) {
    makeOptimizationsStale();
    invariant(path.size > 0);
    invariant(stdx::holds_alternative<std::string>(*path.components));
    const std::string& field = stdx::get<std::string>(*path.components);

    if (path.size == 1) {
        _projectedFields.insert(field);
        return;
    }

    // which type of child to make depends on the next component.
    bool arrayChild = stdx::holds_alternative<size_t>(path.components[1]);
    if (arrayChild) {
        ProjectionNodeArray* child = addOrGetArrayChild(field);

        // TODO: This dummy string is gross. Fortunately, it's only used for dependency analysis,
        // which is not array-aware. So for now we get away with this. In a non-POC we should use
        // boost::optional or do something else.
        child->addProjectionForArrayIndexPath(path.tail(),
                                              [this]() { return makeChild("__ARRAY_INDEX__"); });
    } else {
        auto child = addOrGetChild(field);
        child->addProjectionForArrayIndexPath(path.tail());
    }
}

void ProjectionNodeDocument::addExpressionForPath(const FieldPath& path,
                                                  boost::intrusive_ptr<Expression> expr) {
    makeOptimizationsStale();
    // If the computed fields policy is 'kBanComputedFields', we should never reach here.
    invariant(_policies.computedFieldsPolicy == ComputedFieldsPolicy::kAllowComputedFields);

    // We're going to add an expression either to this node, or to some child of this node.
    // In any case, the entire subtree will contain at least one computed field.
    _subtreeContainsComputedFields = true;

    if (path.getPathLength() == 1) {
        auto fieldName = path.fullPath();
        _expressions[fieldName] = expr;
        _orderToProcessAdditionsAndChildren.push_back(fieldName);
        return;
    }
    // FieldPath can't be empty, so it is safe to obtain the first path component here.
    auto child = addOrGetChild(path.getFieldName(0).toString());
    invariant(dynamic_cast<ProjectionNodeDocument*>(child));
    static_cast<ProjectionNodeDocument*>(child)->addExpressionForPath(path.tail(), expr);
}

void ProjectionNodeDocument::addExpressionForArrayIndexPath(const ArrayIndexPathView& path,
                                                            boost::intrusive_ptr<Expression> expr) {
    invariant(path.size > 0);
    makeOptimizationsStale();
    // If the computed fields policy is 'kBanComputedFields', we should never reach here.
    invariant(_policies.computedFieldsPolicy == ComputedFieldsPolicy::kAllowComputedFields);
    _subtreeContainsComputedFields = true;

    invariant(stdx::holds_alternative<std::string>(path.components[0]));
    const auto field = stdx::get<std::string>(path.components[0]);

    if (path.size == 1) {
        _expressions[field] = expr;
        _orderToProcessAdditionsAndChildren.push_back(field);
        return;
    }

    bool arrayChild = stdx::holds_alternative<size_t>(path.components[1]);
    if (arrayChild) {
        ProjectionNodeArray* node = addOrGetArrayChild(field);
        node->addExpressionForArrayIndexPath(
            path.tail(), expr, [this]() { return makeChild("__ARRAY_INDEX__"); });
    } else {
        auto child = addOrGetChild(field);
        child->addExpressionForArrayIndexPath(path.tail(), expr);
    }
}

boost::intrusive_ptr<Expression> ProjectionNodeDocument::getExpressionForPath(
    const FieldPath& path) const {
    // The FieldPath always conatins at least one field.
    auto fieldName = path.getFieldName(0).toString();

    if (path.getPathLength() == 1) {
        if (_expressions.find(fieldName) != _expressions.end()) {
            return _expressions.at(fieldName);
        }
        return nullptr;
    }
    if (auto child = getChild(fieldName)) {
        invariant(dynamic_cast<ProjectionNodeDocument*>(child));
        return static_cast<ProjectionNodeDocument*>(child)->getExpressionForPath(path.tail());
    }
    return nullptr;
}

ProjectionNodeDocument* ProjectionNodeDocument::addOrGetChild(const std::string& field) {
    makeOptimizationsStale();
    auto child = getChild(field);
    if (child) {
        auto castedChild = dynamic_cast<ProjectionNodeDocument*>(child);
        invariant(castedChild);
        return castedChild;
    }
    return addChild(field);
}

ProjectionNodeArray* ProjectionNodeDocument::addOrGetArrayChild(const std::string& field) {
    makeOptimizationsStale();
    auto child = getChild(field);
    if (child) {
        auto castedChild = dynamic_cast<ProjectionNodeArray*>(child);
        invariant(castedChild);
        return castedChild;
    }
    return addArrayChild(field);
}

ProjectionNodeDocument* ProjectionNodeDocument::addChild(const std::string& field) {
    makeOptimizationsStale();
    invariant(!str::contains(field, "."));
    _orderToProcessAdditionsAndChildren.push_back(field);
    auto insertedPair = _children.emplace(std::make_pair(field, makeChild(field)));
    return static_cast<ProjectionNodeDocument*>(insertedPair.first->second.get());
}

ProjectionNodeArray* ProjectionNodeDocument::addArrayChild(const std::string& field) {
    makeOptimizationsStale();
    invariant(!str::contains(field, "."));
    _orderToProcessAdditionsAndChildren.push_back(field);
    auto newChild = std::make_unique<ProjectionNodeArray>(
        _policies, FieldPath::getFullyQualifiedPath(_pathToNode, field));
    auto insertedPair = _children.emplace(std::make_pair(field, std::move(newChild)));
    return static_cast<ProjectionNodeArray*>(insertedPair.first->second.get());
}

ProjectionNodeBase* ProjectionNodeDocument::getChild(const std::string& field) const {
    auto childIt = _children.find(field);
    return childIt == _children.end() ? nullptr : childIt->second.get();
}

Document ProjectionNodeDocument::applyToDocument(const Document& inputDoc) const {
    // Defer to the derived class to initialize the output document, then apply.
    MutableDocument outputDoc{initializeOutputDocument(inputDoc)};
    applyProjections(inputDoc, &outputDoc);

    if (_subtreeContainsComputedFields) {
        applyExpressions(inputDoc, &outputDoc);
    }

    // Make sure that we always pass through any metadata present in the input doc.
    if (inputDoc.metadata()) {
        outputDoc.copyMetaDataFrom(inputDoc);
    }
    return outputDoc.freeze();
}

void ProjectionNodeDocument::applyProjections(const Document& inputDoc,
                                              MutableDocument* outputDoc) const {
    // Iterate over the input document so that the projected document retains its field ordering.
    auto it = inputDoc.fieldIterator();
    size_t projectedFields = 0;

    while (it.more()) {
        auto fieldName = it.fieldName();
        absl::string_view fieldNameKey{fieldName.rawData(), fieldName.size()};

        if (_projectedFields.find(fieldNameKey) != _projectedFields.end()) {
            outputProjectedField(
                fieldName, applyLeafProjectionToValue(it.next().second), outputDoc);
            ++projectedFields;
        } else if (auto childIt = _children.find(fieldNameKey); childIt != _children.end()) {
            outputProjectedField(
                fieldName, childIt->second->applyProjectionsToValue(it.next().second), outputDoc);
            ++projectedFields;
        } else {
            it.advance();
        }

        // Check if we can avoid reading from the document any further.
        if (_maxFieldsToProject && _maxFieldsToProject <= projectedFields) {
            break;
        }
    }
}

Value ProjectionNodeDocument::applyProjectionsToValue(Value inputValue) const {
    if (inputValue.getType() == BSONType::Object) {
        MutableDocument outputSubDoc{initializeOutputDocument(inputValue.getDocument())};
        applyProjections(inputValue.getDocument(), &outputSubDoc);
        return outputSubDoc.freezeToValue();
    } else if (inputValue.getType() == BSONType::Array) {
        std::vector<Value> values = inputValue.getArray();
        for (auto& value : values) {
            // If this is a nested array and our policy is to not recurse, skip the array.
            // Otherwise, descend into the array and project each element individually.
            const bool shouldSkip = value.isArray() &&
                _policies.arrayRecursionPolicy == ArrayRecursionPolicy::kDoNotRecurseNestedArrays;
            value = (shouldSkip ? transformSkippedValueForOutput(value)
                                : applyProjectionsToValue(value));
        }
        return Value(std::move(values));
    } else {
        // This represents the case where we are projecting children of a field which does not have
        // any children; for instance, applying the projection {"a.b": true} to the document {a: 2}.
        return transformSkippedValueForOutput(inputValue);
    }
}

void ProjectionNodeDocument::outputProjectedField(StringData field,
                                                  Value val,
                                                  MutableDocument* doc) const {
    doc->setField(field, val);
}

void ProjectionNodeDocument::applyExpressions(const Document& root,
                                              MutableDocument* outputDoc) const {
    for (auto&& field : _orderToProcessAdditionsAndChildren) {
        auto childIt = _children.find(field);
        if (childIt != _children.end()) {
            outputDoc->setField(
                field, childIt->second->applyExpressionsToValue(root, outputDoc->peek()[field]));
        } else {
            auto expressionIt = _expressions.find(field);
            invariant(expressionIt != _expressions.end());
            outputDoc->setField(
                field,
                expressionIt->second->evaluate(
                    root, &expressionIt->second->getExpressionContext()->variables));
        }
    }
}

Value ProjectionNodeDocument::applyExpressionsToValue(const Document& root,
                                                      Value inputValue) const {
    if (inputValue.getType() == BSONType::Object) {
        MutableDocument outputDoc(inputValue.getDocument());
        applyExpressions(root, &outputDoc);
        return outputDoc.freezeToValue();
    } else if (inputValue.getType() == BSONType::Array) {
        std::vector<Value> values = inputValue.getArray();
        for (auto& value : values) {
            value = applyExpressionsToValue(root, value);
        }
        return Value(std::move(values));
    } else {
        if (_subtreeContainsComputedFields) {
            // Our semantics in this case are to replace whatever existing value we find with a new
            // document of all the computed values. This case represents applying a projection like
            // {"a.b": {$literal: 1}} to the document {a: 1}. This should yield {a: {b: 1}}.
            MutableDocument outputDoc;
            applyExpressions(root, &outputDoc);
            return outputDoc.freezeToValue();
        }
        // We didn't have any expressions, so just skip this value.
        return transformSkippedValueForOutput(inputValue);
    }
}

void ProjectionNodeDocument::reportProjectedPaths(std::set<std::string>* projectedPaths) const {
    for (auto&& projectedField : _projectedFields) {
        projectedPaths->insert(FieldPath::getFullyQualifiedPath(_pathToNode, projectedField));
    }

    for (auto&& childPair : _children) {
        childPair.second->reportProjectedPaths(projectedPaths);
    }
}

void ProjectionNodeDocument::reportComputedPaths(std::set<std::string>* computedPaths,
                                                 StringMap<std::string>* renamedPaths) const {
    for (auto&& computedPair : _expressions) {
        // The expression's path is the concatenation of the path to this node, plus the field name
        // associated with the expression.
        auto exprPath = FieldPath::getFullyQualifiedPath(_pathToNode, computedPair.first);
        auto exprComputedPaths = computedPair.second->getComputedPaths(exprPath);
        computedPaths->insert(exprComputedPaths.paths.begin(), exprComputedPaths.paths.end());

        for (auto&& rename : exprComputedPaths.renames) {
            (*renamedPaths)[rename.first] = rename.second;
        }
    }
    for (auto&& childPair : _children) {
        childPair.second->reportComputedPaths(computedPaths, renamedPaths);
    }
}

void ProjectionNodeDocument::optimize() {
    for (auto&& expressionIt : _expressions) {
        _expressions[expressionIt.first] = expressionIt.second->optimize();
    }
    for (auto&& childPair : _children) {
        childPair.second->optimize();
    }

    _maxFieldsToProject = maxFieldsToProject();
}

Document ProjectionNodeDocument::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    MutableDocument outputDoc;
    serialize(explain, &outputDoc);
    return outputDoc.freeze();
}

void ProjectionNodeDocument::serialize(boost::optional<ExplainOptions::Verbosity> explain,
                                       MutableDocument* output) const {
    // Determine the boolean value for projected fields in the explain output.
    const bool projVal = !applyLeafProjectionToValue(Value(true)).missing();

    // Always put "_id" first if it was projected (implicitly or explicitly).
    if (_projectedFields.find("_id") != _projectedFields.end()) {
        output->addField("_id", Value(projVal));
    }

    for (auto&& projectedField : _projectedFields) {
        if (projectedField != "_id") {
            output->addField(projectedField, Value(projVal));
        }
    }

    for (auto&& field : _orderToProcessAdditionsAndChildren) {
        auto childIt = _children.find(field);
        if (childIt != _children.end()) {
            MutableDocument subDoc;

            // Serialization is only supported for ProjectionNodeDocument, and not for
            // ProjectionNodeArray.
            auto castedChild = dynamic_cast<ProjectionNodeDocument*>(childIt->second.get());
            invariant(castedChild);

            castedChild->serialize(explain, &subDoc);
            output->addField(field, subDoc.freezeToValue());
        } else {
            invariant(_policies.computedFieldsPolicy == ComputedFieldsPolicy::kAllowComputedFields);
            auto expressionIt = _expressions.find(field);
            invariant(expressionIt != _expressions.end());
            output->addField(field, expressionIt->second->serialize(static_cast<bool>(explain)));
        }
    }
}
}  // namespace mongo::projection_executor
