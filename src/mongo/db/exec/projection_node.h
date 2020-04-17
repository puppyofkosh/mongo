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

#include "mongo/db/exec/projection_executor.h"

#include "mongo/db/array_index_path.h"
#include "mongo/db/query/projection_policies.h"

namespace mongo::projection_executor {
/**
 * The inheritance tree:
 *
 *                          ProjectionNodeBase
 *       ProjectionNodeDocument            ProjectionNodeArray
 *  InclusionNode   ExclusionNode
 */

/**
 * A node used to define the parsed structure of a projection. Each node represents one 'level' of
 * the parsed specification. The root node represents all top level projections or additions, with
 * any child node representing dotted or nested projections or additions.
 *
 * ProjectionNodeBase is an abstract base class for applying a projection to a non-atomic type
 * (right now just documents and arrays).
 */
class ProjectionNodeBase {
public:
    ProjectionNodeBase(ProjectionPolicies policies, std::string pathToNode = "");
    virtual ~ProjectionNodeBase() = default;

    /**
     * Reports dependencies on any fields that are required by this projection.
     */
    virtual void reportDependencies(DepsTracker* deps) const = 0;

    /**
     * Recursively report all paths that are referenced by this projection.
     */
    virtual void reportProjectedPaths(std::set<std::string>* preservedPaths) const = 0;

    /**
     * Recursively reports all computed paths in this projection, adding them into 'computedPaths'.
     *
     * Computed paths that are identified as the result of a simple rename are instead filled out in
     * 'renamedPaths'. Each entry in 'renamedPaths' maps from the path's new name to its old name
     * prior to application of this projection.
     */
    virtual void reportComputedPaths(std::set<std::string>* computedPaths,
                                     StringMap<std::string>* renamedPaths) const = 0;

    const std::string& getPath() const {
        return _pathToNode;
    }

    virtual void optimize() = 0;

    virtual Value applyExpressionsToValue(const Document& root, Value inputVal) const = 0;
    virtual Value applyProjectionsToValue(Value inputVal) const = 0;

protected:
    ProjectionPolicies _policies;
    std::string _pathToNode;
};

class ProjectionNodeArray;

/**
 * Abstract base class for a projection that gets applied to a document (as opposed to an array).
 *
 * Derived classes need only implement a small set of methods which define what the behavior of
 * "projecting" (inclusion or exclusion) a field is.
 */
class ProjectionNodeDocument : public ProjectionNodeBase {
public:
    ProjectionNodeDocument(ProjectionPolicies policies, std::string pathToNode = "")
        : ProjectionNodeBase(policies, std::move(pathToNode)) {}

    /**
     * Applies all projections and expressions, if applicable, and returns the resulting document.
     */
    virtual Document applyToDocument(const Document& inputDoc) const;

    /**
     * Recursively evaluates all expressions in the projection, writing the results to 'outputDoc'.
     */
    void applyExpressions(const Document& root, MutableDocument* outputDoc) const;

    /**
     * Recursively adds 'path' into the tree as a projected field, creating any child nodes if
     * necessary.
     *
     * 'path' is allowed to be dotted, and is assumed not to conflict with another path already in
     * the tree. For example, it is an error to add the path "a.b" from a tree which has already
     * added a computed field "a".
     */
    void addProjectionForPath(const FieldPath& path);

    /**
     * Similar to above, but the path may include array indexes. ProjectionNodeArrays will be added
     * to the tree as necessary.
     */
    void addProjectionForArrayIndexPath(const ArrayIndexPathView& arrPath);

    /**
     * Get the expression for the given path. Returns null if no expression for the given path is
     * found.
     */
    boost::intrusive_ptr<Expression> getExpressionForPath(const FieldPath& path) const;

    /**
     * Recursively adds 'path' into the tree as a computed field, creating any child nodes if
     * necessary.
     *
     * 'path' is allowed to be dotted, and is assumed not to conflict with another path already in
     * the tree. For example, it is an error to add the path "a.b" as a computed field to a tree
     * which has already projected the field "a".
     */
    void addExpressionForPath(const FieldPath& path, boost::intrusive_ptr<Expression> expr);

    /**
     * Similar to above, but the path may include array indexes.
     */
    void addExpressionForArrayIndexPath(const ArrayIndexPathView& arrPath,
                                        boost::intrusive_ptr<Expression> expr);

    /**
     * Creates the child if it doesn't already exist. 'field' is not allowed to be dotted. Returns
     * the child node if it already exists, or the newly-created child otherwise.
     */
    ProjectionNodeDocument* addOrGetChild(const std::string& field);
    ProjectionNodeArray* addOrGetArrayChild(const std::string& field);

    /**
     * Return an optional number, x, which indicates that it is safe to stop reading the document
     * being projected once x fields have been projected.
     */
    virtual boost::optional<size_t> maxFieldsToProject() const {
        return boost::none;
    }

    void reportComputedPaths(std::set<std::string>* computedPaths,
                             StringMap<std::string>* renamedPaths) const override;
    void reportProjectedPaths(std::set<std::string>* preservedPaths) const override;

    void optimize() override;


    Document serialize(boost::optional<ExplainOptions::Verbosity> explain) const;

    void serialize(boost::optional<ExplainOptions::Verbosity> explain,
                   MutableDocument* output) const;

    // Helpers for the 'applyProjections' and 'applyExpressions' methods. Applies the transformation
    // recursively to each element of any arrays, and ensures primitives are handled appropriately.
    Value applyExpressionsToValue(const Document& root, Value inputVal) const override;
    Value applyProjectionsToValue(Value inputVal) const override;

protected:
    // Returns a unique_ptr to a new instance of the implementing class for the given 'fieldName'.
    virtual std::unique_ptr<ProjectionNodeDocument> makeChild(
        const std::string& fieldName) const = 0;

    // Returns the initial document to which the current level of the projection should be applied.
    // For an inclusion projection this will be an empty document, to which we will add the fields
    // we wish to retain. For an exclusion this will be the complete document, from which we will
    // eliminate the fields we wish to omit.
    virtual MutableDocument initializeOutputDocument(const Document& inputDoc) const = 0;

    // Given an input leaf value, returns the value that should be added to the output document.
    // Depending on the projection type this will be either the value itself, or "missing".
    virtual Value applyLeafProjectionToValue(const Value& value) const = 0;

    // Given an input leaf that we intend to skip over, returns the value that should be added to
    // the output document. Depending on the projection this will be either the value, or "missing".
    virtual Value transformSkippedValueForOutput(const Value&) const = 0;

    // Writes the given value to the output doc, replacing the existing value of 'field' if present.
    virtual void outputProjectedField(StringData field, Value val, MutableDocument* outDoc) const;

    stdx::unordered_map<std::string, std::unique_ptr<ProjectionNodeBase>> _children;
    stdx::unordered_map<std::string, boost::intrusive_ptr<Expression>> _expressions;
    stdx::unordered_set<std::string> _projectedFields;

    // Whether this node or any child of this node contains a computed field.
    bool _subtreeContainsComputedFields{false};

private:
    // Iterates 'inputDoc' for each projected field, adding to or removing from 'outputDoc'. Also
    // copies over enough information to preserve the structure of the incoming document for the
    // fields this projection cares about.
    //
    // For example, given a ProjectionNodeDocument tree representing this projection:
    //    {a: {b: 1, c: <exp>}, "d.e": <exp>}
    // Calling applyProjections() with an 'inputDoc' of
    //    {a: [{b: 1, d: 1}, {b: 2, d: 2}], d: [{e: 1, f: 1}, {e: 1, f: 1}]}
    // and an empty 'outputDoc' will leave 'outputDoc' representing the document
    //    {a: [{b: 1}, {b: 2}], d: [{}, {}]}
    void applyProjections(const Document& inputDoc, MutableDocument* outputDoc) const;

    // Adds a new ProjectionNodeDocument as a child. 'field' cannot be dotted.
    ProjectionNodeDocument* addChild(const std::string& field);
    ProjectionNodeArray* addArrayChild(const std::string& field);


    // Returns nullptr if no such child exists.
    ProjectionNodeBase* getChild(const std::string& field) const;

    /**
     * Indicates that metadata computed by previous calls to optimize() is now stale and must be
     * recomputed. This must be called any time the tree is updated (an expression added or child
     * node added).
     */
    void makeOptimizationsStale() {
        _maxFieldsToProject = boost::none;
    }

    // Our projection semantics are such that all field additions need to be processed in the order
    // specified. '_orderToProcessAdditionsAndChildren' tracks that order.
    //
    // For example, for the specification {a: <expression>, "b.c": <expression>, d: <expression>},
    // we need to add the top level fields in the order "a", then "b", then "d". This ordering
    // information needs to be tracked separately, since "a" and "d" will be tracked via
    // '_expressions', and "b.c" will be tracked as a child ProjectionNodeDocument in '_children'.
    // For the example above, '_orderToProcessAdditionsAndChildren' would be ["a", "b", "d"].
    std::vector<std::string> _orderToProcessAdditionsAndChildren;

    // Maximum number of fields that need to be projected. This allows for an "early" return
    // optimization which means we don't have to iterate over an entire document. The value is
    // stored here to avoid re-computation for each document.
    boost::optional<size_t> _maxFieldsToProject;
};

/**
 * Class which represents a projection tree applied to an array. May
 * --Set individual elements of the array to the result of an expression.
 * --Apply other sub-projections to elements of an array which are documents.
 *
 * If applying projections and a non-array is encountered, the value will not be changed.
 *
 * If applying expressions and a non-array is encountered, it will be turned into an array. If the
 * array is too short (e.g. the node has an expression for element 5, but the array is length 2),
 * it will be padded with 'null' values.
 */
class ProjectionNodeArray : public ProjectionNodeBase {
public:
    using MakeNodeFn = std::function<std::unique_ptr<ProjectionNodeDocument>()>;

    ProjectionNodeArray(ProjectionPolicies policies, std::string pathToNode = "")
        : ProjectionNodeBase(policies, std::move(pathToNode)) {}

    /**
     * Reports dependencies on any fields that are required by this projection.
     */
    void reportDependencies(DepsTracker* deps) const override {
        deps->fields.insert(_pathToNode);

        for (auto&& expr : _expressions) {
            expr.second->addDependencies(deps);
        }

        for (auto&& child : _children) {
            child.second->reportDependencies(deps);
        }
    }

    /**
     * Recursively report all paths that are referenced by this projection.
     */
    void reportProjectedPaths(std::set<std::string>* preservedPaths) const override {
        // We do nothing here.

        // The deps tracker is not capable of tracking paths that go beneath arrays (e.g. it cannot
        // track the path a.0.b, where 'a' is an array). As a "coarse" solution to this, we report
        // that the field this ProjectionNodeArray represents is entirely "computed".
    }

    /**
     * Recursively reports all computed paths in this projection, adding them into 'computedPaths'.
     */
    void reportComputedPaths(std::set<std::string>* computedPaths,
                             StringMap<std::string>* renamedPaths) const override {
        // Report this entire path as computed.
        computedPaths->insert(_pathToNode);
    }

    void optimize() override {
        for (auto&& expr : _expressions) {
            _expressions[expr.first] = expr.second->optimize();
        }

        for (auto&& child : _children) {
            child.second->optimize();
        }
    }

    /**
     * Apply all expressions in this tree to given Value and return a new Value to take its place.
     *
     * Returns an array even if the input Value is not an array.
     */
    Value applyExpressionsToValue(const Document& root, Value inputVal) const override {
        std::vector<Value> vec;
        if (inputVal.getType() == BSONType::Array) {
            vec = inputVal.getArray();
        }

        // Pad the vector with nulls.
        if (vec.size() < _maxInd + 1) {
            vec.reserve(_maxInd + 1);
            for (size_t i = vec.size(); i <= _maxInd; ++i) {
                vec.push_back(Value(NullLabeler{}));
            }
        }

        for (auto ind : _orderToProcessAdditionsAndChildren) {
            if (auto it = _expressions.find(ind); it != _expressions.end()) {
                vec[ind] =
                    it->second->evaluate(root, &it->second->getExpressionContext()->variables);
            } else {
                auto childIt = _children.find(ind);
                invariant(childIt != _children.end());
                vec[ind] = childIt->second->applyExpressionsToValue(root, std::move(vec[ind]));
            }
        }

        return Value(vec);
    }

    /**
     * Apply child node projections to given Value, if it is an array.
     */
    Value applyProjectionsToValue(Value inputVal) const override {
        if (inputVal.getType() != BSONType::Array) {
            // NOTE: This represents the case where you have a projection like a.$[0].b and a
            // document {a: "foo"}. What should we do here? Erroring would be nice, but
            // unfortunately, is not acceptable for oplog application. Instead we just have this
            // leave the value alone.
            return transformSkippedValueForOutput(inputVal);
        }

        auto vec = inputVal.getArray();

        for (auto ind : _orderToProcessAdditionsAndChildren) {
            if (ind >= vec.size()) {
                continue;
            }

            if (auto childIt = _children.find(ind); childIt != _children.end()) {
                vec[ind] = childIt->second->applyProjectionsToValue(std::move(vec[ind]));
            }
        }
        return Value(vec);
    }

    //
    // The below methods are used for constructing trees of projection nodes. While these methods
    // are declared public so that sibling classes may call them, it's not recommended that outside
    // callers use them.
    //

    void addProjectionForArrayIndexPath(const ArrayIndexPathView& path,
                                        const MakeNodeFn& makeChild) {
        // We don't allow "projections" on array elements, so the path cannot end with an array
        // element.
        invariant(path.size > 1);
        invariant(stdx::holds_alternative<size_t>(path.components[0]));

        size_t ind = stdx::get<size_t>(path.components[0]);
        auto childIter = _children.find(ind);
        if (childIter == _children.end()) {
            _children[ind] = makeChild();
            _maxInd = std::max(_maxInd, ind);
            _orderToProcessAdditionsAndChildren.push_back(ind);
            childIter = _children.find(ind);
        }

        childIter->second->addProjectionForArrayIndexPath(path.tail());
    }

    void addExpressionForArrayIndexPath(const ArrayIndexPathView& path,
                                        boost::intrusive_ptr<Expression> expr,
                                        const MakeNodeFn& makeChild) {
        invariant(stdx::holds_alternative<size_t>(path.components[0]));
        size_t ind = stdx::get<size_t>(path.components[0]);

        if (path.size == 1) {
            _expressions[ind] = expr;
            _maxInd = std::max(ind, _maxInd);
            _orderToProcessAdditionsAndChildren.push_back(ind);
            return;
        }

        auto childIter = _children.find(ind);
        if (childIter == _children.end()) {
            auto pair = _children.insert({ind, makeChild()});
            _maxInd = std::max(ind, _maxInd);
            _orderToProcessAdditionsAndChildren.push_back(ind);
            invariant(pair.second);
            childIter = pair.first;
        }

        childIter->second->addExpressionForArrayIndexPath(path.tail(), expr);
    }

    ProjectionNodeDocument* addChild(size_t ind, std::unique_ptr<ProjectionNodeDocument> node) {
        invariant(!_expressions.count(ind));

        auto ret = _children.insert({ind, std::move(node)});
        // New element should have been inserted.
        invariant(ret.second);
        return ret.first->second.get();
    }

private:
    Value transformSkippedValueForOutput(Value in) const {
        // If we like, this can be forked off and we can have different behavior in this case.
        return in;
    }

    //
    // The keys for these maps must not overlap.
    //

    // Map from array index -> Expression.
    std::map<size_t, boost::intrusive_ptr<Expression>> _expressions;

    // Map from array index -> child projection.
    // Note that each child is a ProjectionNodeDocument NOT a ProjectionNodeBase. We do not allow
    // traversal of arrays directly nested within arrays.
    std::map<size_t, std::unique_ptr<ProjectionNodeDocument>> _children;

    // We do the transformations in the order they were requested, rather than in array-index order.
    // This is to match the behavior of Document projections.
    // NOTE: Do we really have to do this? Probably not.
    std::vector<size_t> _orderToProcessAdditionsAndChildren;
    size_t _maxInd = 0;
};

}  // namespace mongo::projection_executor
