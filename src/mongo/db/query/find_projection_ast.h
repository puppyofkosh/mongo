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
#include "mongo/db/pipeline/dependencies.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/util/str.h"

namespace mongo {

enum class NodeType {
    INTERNAL,

    INCLUSION,             // r-hand side is truthy value
    INCLUSION_POSITIONAL,  // r-hand side is truthy value, and positional projection is used
    EXCLUSION,

    // There are few enough of these that they each get their own node.
    EXPRESSION_SLICE,
    EXPRESSION_ELEMMATCH,

    // Includes all other expressions.
    EXPRESSION_OTHER,
};
enum class ProjectType { kInclusion, kExclusion };

template <class T>
using Children = std::vector<std::pair<std::string, std::unique_ptr<T>>>;

// Any node which can appear in a find() projection.
class ProjectionASTNode {
public:
    virtual NodeType type() const = 0;
    virtual std::string toString() const = 0;

    // Returns whether this node is meaningful in both the find and agg projection languages.
    virtual bool commonToAggAndFind() const {
        return false;
    }

    virtual void toBson(BSONObjBuilder* bob, const std::string& fieldName) const {
        MONGO_UNREACHABLE;
    };

    virtual std::unique_ptr<ProjectionASTNode> clone() const {
        std::cout << "this type is " << typeid(*this).name() << std::endl;
        MONGO_UNREACHABLE;
    }
};

// For nodes which are common to both agg and find.
// For nodes only in find() (such as positional projection and find $slice just use
// ProjectionASTNode)
class ProjectionASTNodeCommon : public ProjectionASTNode {
public:
    bool commonToAggAndFind() const override {
        return true;
    }
};

// An internal node which has children.
template <class ChildType>
class ProjectionASTNodeInternal : public ProjectionASTNodeCommon {
public:
    ProjectionASTNodeInternal(Children<ChildType> c) : children(std::move(c)) {}
    ProjectionASTNodeInternal(const ProjectionASTNodeInternal& other) {
        for (auto&& c : other.children) {
            std::unique_ptr<ChildType> castedChild(
                static_cast<ChildType*>(c.second->clone().release()));
            children.push_back(std::make_pair(c.first, std::move(castedChild)));
        }
    }
    ProjectionASTNodeInternal(ProjectionASTNodeInternal&& other)
        : children(std::move(other.children)) {}

    NodeType type() const override {
        return NodeType::INTERNAL;
    }

    std::string toString() const override {
        auto stream = str::stream();
        stream << "{";
        for (auto&& child : children) {

            if (!child.second) {
                std::cout << "bad node at " << child.first << std::endl;
            }

            stream << child.first << ": " << child.second->toString() << ", ";
        }
        stream << "}";
        return stream;
    }

    ProjectionASTNode* getChild(StringData field) const {
        for (auto&& c : children) {
            if (c.first == field)
                return c.second.get();
        }
        return nullptr;
    }

    // We could implement this by doing an AND on the children or something, but just don't call
    // it.
    bool commonToAggAndFind() const override {
        MONGO_UNREACHABLE
    }

    // TODO: This is a crutch used for getting us through an intermediate state. Get rid of it
    // eventually.
    void toBson(BSONObjBuilder* bob, const std::string& fieldName) const override {
        BSONObjBuilder sub(bob->subobjStart(fieldName));
        for (auto&& child : children) {
            child.second->toBson(&sub, child.first);
        }
    }

    std::unique_ptr<ProjectionASTNode> clone() const override {
        Children<ChildType> newChildren;
        for (auto&& c : children) {
            std::unique_ptr<ChildType> castedChild(
                static_cast<ChildType*>(c.second->clone().release()));

            newChildren.push_back(std::make_pair(c.first, std::move(castedChild)));
        }

        return std::make_unique<ProjectionASTNodeInternal>(std::move(newChildren));
    }

    // Public for convenience
    Children<ChildType> children;
};

using ProjectionASTNodeInternalBase = ProjectionASTNodeInternal<ProjectionASTNode>;
using ProjectionASTNodeInternalCommon = ProjectionASTNodeInternal<ProjectionASTNodeCommon>;

class ProjectionASTNodeInclusion : public ProjectionASTNodeCommon {
public:
    NodeType type() const override {
        return NodeType::INCLUSION;
    }

    std::string toString() const override {
        return "1";
    }

    void toBson(BSONObjBuilder* bob, const std::string& fieldName) const override {
        bob->append(fieldName, 1.0);
    }

    std::unique_ptr<ProjectionASTNode> clone() const override {
        return std::unique_ptr<ProjectionASTNode>(new ProjectionASTNodeInclusion());
    }
};

class ProjectionASTNodePositional : public ProjectionASTNode {
public:
    NodeType type() const override {
        return NodeType::INCLUSION_POSITIONAL;
    }

    std::string toString() const override {
        return "{$_positional: 1}";
    }
};

class ProjectionASTNodeExclusion : public ProjectionASTNodeCommon {
public:
    NodeType type() const override {
        return NodeType::EXCLUSION;
    }

    std::string toString() const override {
        return "0";
    }


    void toBson(BSONObjBuilder* bob, const std::string& fieldName) const override {
        bob->append(fieldName, 0.0);
    }

    std::unique_ptr<ProjectionASTNode> clone() const override {
        return std::unique_ptr<ProjectionASTNode>(new ProjectionASTNodeExclusion());
    }
};

class ProjectionASTNodeSlice : public ProjectionASTNode {
public:
    ProjectionASTNodeSlice(int skip, int limit) : skip(skip), limit(limit) {}

    NodeType type() const override {
        return NodeType::EXPRESSION_SLICE;
    }

    std::string toString() const override {
        return str::stream() << "{$slice: [" << skip << ", " << limit << "]}";
    }

    int skip;
    int limit;
};

class ProjectionASTNodeElemMatch : public ProjectionASTNodeCommon {
public:
    ProjectionASTNodeElemMatch(BSONObj matchExpr) : _matchExpr(matchExpr.getOwned()) {}

    NodeType type() const override {
        return NodeType::EXPRESSION_ELEMMATCH;
    }

    std::string toString() const override {
        return str::stream() << "{$elemMatch: " << _matchExpr << "}";
    }

    void toBson(BSONObjBuilder* bob, const std::string& fieldName) const override {
        MONGO_UNREACHABLE;  // Not supporting for this patch
    }

    std::unique_ptr<ProjectionASTNode> clone() const override {
        MONGO_UNREACHABLE;
    }

private:
    BSONObj _matchExpr;
};

class ProjectionASTNodeOtherExpression : public ProjectionASTNodeCommon {
public:
    ProjectionASTNodeOtherExpression(BSONObj obj, const boost::intrusive_ptr<Expression>& e)
        : _obj(obj.getOwned()), _expression(e) {}

    NodeType type() const override {
        return NodeType::EXPRESSION_OTHER;
    }

    std::string toString() const override {
        return str::stream() << _obj;
    }

    void toBson(BSONObjBuilder* bob, const std::string& fieldName) const override {
        bob->append(fieldName, _obj);
    }

    std::unique_ptr<ProjectionASTNode> clone() const override {
        return std::unique_ptr<ProjectionASTNode>(
            new ProjectionASTNodeOtherExpression(_obj, _expression));
    }

    Expression* expression() const {
        return _expression.get();
    }

private:
    BSONObj _obj;
    boost::intrusive_ptr<Expression> _expression;
};

// Syntax "tree" (list) for find projection.
struct FindProjectionAST {
    ProjectionASTNodeInternal<ProjectionASTNode> root;
    const ProjectType type;

    // It's bad that we need this.
    boost::intrusive_ptr<ExpressionContext> expCtx;

    static FindProjectionAST fromBson(const BSONObj& b, const MatchExpression* const query);

    std::string toString() {
        return root.toString();
    }
};


void walkProjectionAST(const std::function<void(const ProjectionASTNodeCommon*)>& fn,
                       const ProjectionASTNodeCommon* root);

struct SliceInfo {
    FieldPath path;  // path to slice
    int skip;
    int limit;
};

struct PositionalInfo {
    FieldPath path;
};

struct ProjectionASTCommon {
    ProjectionASTCommon(ProjectionASTNodeInternal<ProjectionASTNodeCommon> root,
                        ProjectType type,
                        std::vector<SliceInfo> sliceInfo,
                        boost::optional<PositionalInfo> positionalInfo)
        : _root(std::move(root)),
          _type(type),
          _sliceInfo(std::move(sliceInfo)),
          _positionalInfo(std::move(positionalInfo)) {}

    ProjectionASTCommon(ProjectionASTCommon&& o) = default;
    ProjectionASTCommon& operator=(ProjectionASTCommon&& o) = default;

    ProjectionASTCommon(const ProjectionASTCommon& o) = default;

    std::string toString() const {
        auto stream = str::stream();
        stream << _root.toString() << " [positional info: "
               << (_positionalInfo ? _positionalInfo->path.fullPath() : "<none>") << "]";

        for (auto&& s : _sliceInfo) {
            stream << "[slice: " << s.path.fullPath() << ": [" << s.skip << ", " << s.limit << "]]";
        }

        return stream;
    }

    BSONObj toBson() const {
        BSONObjBuilder bob;
        for (auto&& child : _root.children) {
            child.second->toBson(&bob, child.first);
        }

        return bob.obj();
    }

    /////////////////////////////////
    // Logical projection interface.
    /////////////////////////////////

    /**
     * Returns true if the projection requires match details from the query, and false
     * otherwise. This is only relevant for find() projection, because of the positional projection
     * operator.
     */
    bool requiresMatchDetails() const {
        return static_cast<bool>(_positionalInfo);
    }

    /**
     * Is the full document required to compute this projection?
     */
    bool requiresDocument() const {
        // TODO: There is a special case here for index key projection that I'm ignoring.
        return _type == ProjectType::kExclusion || hasExpression();
    }

    std::vector<std::string> sortKeyMetaFields() const {
        // TODO: This requires $meta to be able to handle sortKey
        return {};
    }

    bool needsSortKey() const {
        return !sortKeyMetaFields().empty();
    }

    /**
     * If requiresDocument() == false, what fields are required to compute
     * the projection?
     */
    std::vector<std::string> getRequiredFields() const;

    bool wantTextScore() const {
        bool res = false;
        auto fn = [&res](const ProjectionASTNodeCommon* node) {
            if (node->type() == NodeType::EXPRESSION_OTHER) {
                auto* exprNode = static_cast<const ProjectionASTNodeOtherExpression*>(node);
                Expression* expr = exprNode->expression();
                ExpressionMeta* meta = dynamic_cast<ExpressionMeta*>(expr);

                if (meta && meta->metaType() == ExpressionMeta::MetaType::TEXT_SCORE) {
                    res = true;
                }
            }
        };

        walkProjectionAST(fn, &_root);
        return res;
    }

    bool wantGeoNearDistance() const {
        // TODO: similar to wantTextScore()
        return false;
    }

    bool wantGeoNearPoint() const {
        // TODO: similar to wantTextScore()
        return false;
    }

    bool wantIndexKey() const {
        // TODO: similar to wantTextScore()
        return false;
    }

    bool wantSortKey() const {
        // TODO: similar to wantTextScore()
        return false;
    }

    /**
     * Returns true if the element at 'path' is preserved entirely after this projection is applied,
     * and false otherwise. For example, the projection {a: 1} will preserve the element located at
     * 'a.b', and the projection {'a.b': 0} will not preserve the element located at 'a'.
     */
    bool isFieldRetainedExactly(StringData path) const {
        MONGO_UNREACHABLE;
    }

    bool hasDottedFieldPath() const {
        MONGO_UNREACHABLE;
    }

    const boost::optional<PositionalInfo> getPositionalProjection() const {
        return _positionalInfo;
    }

    const boost::optional<SliceInfo> getSliceArgs() const {
        if (_sliceInfo.empty()) {
            return boost::none;
        }
        return _sliceInfo.front();
    }

    ProjectType type() const {
        return _type;
    }

private:
    bool hasExpression() const {
        bool res = false;
        auto fn = [&res](const ProjectionASTNodeCommon* node) {
            if (node->type() == NodeType::EXPRESSION_OTHER) {
                res = true;
            }
        };

        walkProjectionAST(fn, &_root);
        return res;
    }

    ProjectionASTNodeInternal<ProjectionASTNodeCommon> _root;
    ProjectType _type;

    // Information for post-processing the find expressions.
    std::vector<SliceInfo> _sliceInfo;
    boost::optional<PositionalInfo> _positionalInfo;
};


ProjectionASTCommon desugarFindProjection(FindProjectionAST ast);
}
