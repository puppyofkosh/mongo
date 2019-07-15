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


class ProjectionASTNode {
public:
    using Children = std::vector<std::pair<std::string, std::unique_ptr<ProjectionASTNode>>>;

    virtual FieldPath getPath() const = 0;
    virtual NodeType getType() const = 0;
    virtual std::string toString() const = 0;
};

class ProjectionASTNodeBitter : public ProjectionASTNode {};

// An internal node which has children.
class ProjectionASTNodeInternal : public ProjectionASTNodeBitter {
public:
    ProjectionASTNodeInternal(Children c) : children(std::move(c)) {}

    FieldPath getPath() const override {
        MONGO_UNREACHABLE;
    }

    NodeType getType() const override {
        return NodeType::INTERNAL;
    }

    std::string toString() const override {
        auto stream = str::stream();
        stream << "{";
        for (auto&& child : children) {
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

    // Public for convenience
    Children children;
};

class ProjectionASTNodeInclusion : public ProjectionASTNodeBitter {
public:
    ProjectionASTNodeInclusion(FieldPath fp) : _fp(fp) {}

    FieldPath getPath() const override {
        return _fp;
    }

    NodeType getType() const override {
        return NodeType::INCLUSION;
    }

    std::string toString() const override {
        return _fp.fullPath() + ": 1";
    }

private:
    FieldPath _fp;
};

class ProjectionASTNodePositional : public ProjectionASTNode {
public:
    ProjectionASTNodePositional(FieldPath path) : _fp(path) {}

    FieldPath getPath() const override {
        return _fp;
    }

    NodeType getType() const override {
        return NodeType::INCLUSION_POSITIONAL;
    }

    std::string toString() const override {
        return _fp.fullPath() + ".$: 1";
    }

private:
    FieldPath _fp;
};

class ProjectionASTNodeExclusion : public ProjectionASTNodeBitter {
public:
    ProjectionASTNodeExclusion(FieldPath fp) : _fp(fp) {}

    FieldPath getPath() const override {
        return _fp;
    }

    NodeType getType() const override {
        return NodeType::EXCLUSION;
    }

    std::string toString() const override {
        return _fp.fullPath() + ": 0";
    }

private:
    FieldPath _fp;
};

class ProjectionASTNodeSlice : public ProjectionASTNode {
public:
    ProjectionASTNodeSlice(FieldPath fp, int skip, int limit)
        : _fp(fp), _skip(skip), _limit(limit) {}

    FieldPath getPath() const override {
        return _fp;
    }

    NodeType getType() const override {
        return NodeType::EXPRESSION_SLICE;
    }

    std::string toString() const override {
        return str::stream() << _fp.fullPath() << ": $slice [" << _skip << ", " << _limit << "]";
    }

private:
    FieldPath _fp;
    int _skip;
    int _limit;
};

class ProjectionASTNodeElemMatch : public ProjectionASTNodeBitter {
public:
    ProjectionASTNodeElemMatch(FieldPath fp, BSONObj matchExpr) : _fp(fp), _matchExpr(matchExpr) {}

    FieldPath getPath() const override {
        return _fp;
    }

    NodeType getType() const override {
        return NodeType::EXPRESSION_ELEMMATCH;
    }

    std::string toString() const override {
        return str::stream() << _fp.fullPath() << ": " << _matchExpr;
    }

private:
    FieldPath _fp;
    BSONObj _matchExpr;
};

class ProjectionASTNodeOtherExpression : public ProjectionASTNodeBitter {
public:
    ProjectionASTNodeOtherExpression(FieldPath fp, BSONObj obj) : _fp(fp), _obj(obj.getOwned()) {}

    FieldPath getPath() const override {
        return _fp;
    }

    NodeType getType() const override {
        return NodeType::EXPRESSION_OTHER;
    }

    std::string toString() const override {
        return str::stream() << _fp.fullPath() << ": " << _obj;
    }

private:
    FieldPath _fp;
    BSONObj _obj;
};

// Syntax "tree" (list) for find projection.
struct FindProjectionAST {
    ProjectionASTNodeInternal root;

    // To keep the BSONElements alive
    const BSONObj originalObject;

    const ProjectType type;

    static FindProjectionAST fromBson(const BSONObj& b, const MatchExpression* const query);

    std::string toString() {
        return root.toString();
    }
};

struct FindProjectionASTBitter {
    const std::vector<std::unique_ptr<ProjectionASTNodeBitter>> nodes;
    const BSONObj originalObject;
    const ProjectType type;

    // TODO: Some extra stuff to carry along with the projection.
    // positional info
    // $slice info
};

FindProjectionASTBitter desugar(FindProjectionAST ast);

// TODO: FindProjectionSweet/BitterAST
}
}
