/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

#include "mongo/db/pipeline/expression.h"

#include "mongo/db/array_index_path.h"
#include "mongo/util/visit_helper.h"

namespace mongo {
/**
 * An internal expression used to remove 'tombstone' values, that is, missing values which are
 * used as placeholders to retain the position of removed fields.
 *
 * Note that this expression does not traverse arrays. For instance, given the document:
 *
 * {a: [{b: 1, c: <TOMBSTONE>}]}
 *
 * this expression will not remove the tombstone for 'c'.
 */
class ExpressionInternalRemoveFieldTombstones final : public Expression {
public:
    explicit ExpressionInternalRemoveFieldTombstones(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        boost::intrusive_ptr<Expression> child)
        : Expression{expCtx, {std::move(child)}} {}

    Value evaluate(const Document& root, Variables* variables) const final {
        auto targetVal = _children[0]->evaluate(root, variables);
        uassert(4750600,
                str::stream() << "$_internalRemoveFieldTombstones requires a document "
                                 "input, found: "
                              << typeName(targetVal.getType()),
                targetVal.getType() == BSONType::Object);
        return _removeTombstones(targetVal.getDocument());
    }

    Value serialize(bool explain) const final {
        // NOTE: This is only implemented because DocumentSourceSingleDocumentTransformation (used
        // for $replaceRoot) requires that serialize() be implemented.
        return Value();
    }

    boost::intrusive_ptr<Expression> optimize() final {
        invariant(_children.size() == 1);
        _children[0] = _children[0]->optimize();
        return this;
    }

    void acceptVisitor(ExpressionVisitor* visitor) final {
        return visitor->visit(this);
    }

protected:
    void _doAddDependencies(DepsTracker* deps) const final {
        invariant(_children.size() == 1);
        _children[0]->addDependencies(deps);
    }

private:
    Value _removeTombstones(const Document& document) const {
        MutableDocument output;
        FieldIterator iter = document.fieldIterator();
        while (iter.more()) {
            Document::FieldPair pair = iter.next();
            auto val = pair.second;
            if (val.getType() == BSONType::Object)
                val = _removeTombstones(val.getDocument());
            output.addField(pair.first, val);
        }
        return Value(output.freeze());
    }
};

/**
 * Traverses an array index path and gets the value. Like ExpressionFieldPath but goes through
 * single array elements.
 */
class ExpressionInternalArrayIndexPath final : public Expression {
public:
    explicit ExpressionInternalArrayIndexPath(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                              const boost::intrusive_ptr<Expression>& child,
                                              ArrayIndexPath path)
        : Expression{expCtx, {std::move(child)}}, _path(path) {}

    Value evaluate(const Document& root, Variables* variables) const final {
        auto targetVal = _children[0]->evaluate(root, variables);
        uassert(ErrorCodes::BadValue,
                str::stream() << "$_internalArrayIndexPath requires a document "
                                 "input, found: "
                              << typeName(targetVal.getType()),
                targetVal.getType() == BSONType::Object);

        return traversePath(targetVal, _path);
    }

    Value serialize(bool explain) const final {
        // NOTE: This is only implemented because DocumentSourceSingleDocumentTransformation (used
        // for $replaceRoot) requires that serialize() be implemented.
        return Value();
    }

    boost::intrusive_ptr<Expression> optimize() final {
        invariant(_children.size() == 1);
        _children[0] = _children[0]->optimize();
        return this;
    }

    void acceptVisitor(ExpressionVisitor* visitor) final {
        return visitor->visit(this);
    }

protected:
    void _doAddDependencies(DepsTracker* deps) const final {
        invariant(_children.size() == 1);
        _children[0]->addDependencies(deps);
    }

private:
    static Value traversePath(Value val, ArrayIndexPathView pathView) {
        Value next =
            stdx::visit(visit_helper::Overloaded{[&val](size_t ind) -> Value {
                                                     if (val.getType() != BSONType::Array) {
                                                         return Value();
                                                     }

                                                     const auto& vec = val.getArray();
                                                     if (ind >= vec.size()) {
                                                         return Value();
                                                     }
                                                     return vec[ind];
                                                 },
                                                 [&val](const std::string& field) -> Value {
                                                     if (val.getType() != BSONType::Object) {
                                                         return Value();
                                                     }

                                                     return val.getDocument()[field];
                                                 }},
                        pathView.components[0]);

        if (next.missing() || pathView.size == 1) {
            return next;
        }
        return traversePath(next, pathView.tail());
    }

    ArrayIndexPath _path;
};

/**
 * Takes one child, which may return any Value. If the child produces an array, resizes the array
 * to be size N (padding with nulls if growing the array). If the child produces a non-array,
 * returns an array of exact size N with all nulls.
 */
class ExpressionInternalResizeArray final : public Expression {
public:
    explicit ExpressionInternalResizeArray(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                           size_t newSize,
                                           boost::intrusive_ptr<Expression> child)
        : Expression{expCtx, {std::move(child)}}, _newSize(newSize) {}

    Value evaluate(const Document& root, Variables* variables) const final {
        auto targetVal = _children[0]->evaluate(root, variables);

        std::vector<Value> vec;
        if (targetVal.getType() == BSONType::Array) {
            vec = targetVal.getArray();
        }

        // TODO: Not the most efficient...that's a problem for another day though.
        while (vec.size() < _newSize) {
            vec.push_back(Value(NullLabeler{}));
        }
        vec.resize(_newSize);

        return Value(vec);
    }

    Value serialize(bool explain) const final {
        // NOTE: This is only implemented because DocumentSourceSingleDocumentTransformation (used
        // for $replaceRoot) requires that serialize() be implemented.
        return Value();
    }

    boost::intrusive_ptr<Expression> optimize() final {
        invariant(_children.size() == 1);
        _children[0] = _children[0]->optimize();
        return this;
    }

    void acceptVisitor(ExpressionVisitor* visitor) final {
        return visitor->visit(this);
    }

protected:
    void _doAddDependencies(DepsTracker* deps) const final {
        invariant(_children.size() == 1);
        _children[0]->addDependencies(deps);
    }

private:
    size_t _newSize;
};

}  // namespace mongo
