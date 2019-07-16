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
#include "mongo/util/str.h"

namespace mongo {

namespace find_projection_ast {

enum class NodeType {
    INTERNAL,

    INCLUSION,             // r-hand side is truthy value
    INCLUSION_POSITIONAL,  // r-hand side is truthy value, and positional projection is used
    EXCLUSION,

    // There are few enough of these that they each get their own node.
    EXPRESSION_SLICE,
    EXPRESSION_ELEMMATCH,

    // TODO: Remove
    EXPRESSION_META,

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

private:
    BSONObj _matchExpr;
};

class ProjectionASTNodeOtherExpression : public ProjectionASTNodeCommon {
public:
    ProjectionASTNodeOtherExpression(BSONObj obj) : _obj(obj.getOwned()) {}

    NodeType type() const override {
        return NodeType::EXPRESSION_OTHER;
    }

    std::string toString() const override {
        return str::stream() << _obj;
    }

private:
    BSONObj _obj;
};

// Syntax "tree" (list) for find projection.
struct FindProjectionAST {
    ProjectionASTNodeInternal<ProjectionASTNode> root;
    const ProjectType type;

    static FindProjectionAST fromBson(const BSONObj& b, const MatchExpression* const query);

    std::string toString() {
        return root.toString();
    }
};

struct SliceInfo {
    FieldPath path;  // path to slice
    int skip;
    int limit;
};

struct PositionalInfo {
    FieldPath path;
};

struct ProjectionASTCommon {
    ProjectionASTNodeInternal<ProjectionASTNodeCommon> root;
    const ProjectType type;

    // Information for post-processing the find expressions.
    const std::vector<SliceInfo> sliceInfo;
    const boost::optional<PositionalInfo> positionalInfo;

    std::string toString() {
        auto stream = str::stream();
        stream << root.toString() << " [positional info: "
               << (positionalInfo ? positionalInfo->path.fullPath() : "<none>") << "]";

        for (auto&& s : sliceInfo) {
            stream << "[slice: " << s.path.fullPath() << ": [" << s.skip << ", " << s.limit << "]]";
        }

        return stream;
    }
};

ProjectionASTCommon desugar(FindProjectionAST ast);
}
}
