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

#include "mongo/base/status.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/db/update/document_diff_serialization.h"
#include "mongo/db/update/log_builder_base.h"
#include "mongo/util/string_map.h"

namespace mongo {
class FieldRef;
namespace v2_log_builder {
/**
 * These are structs for a "diff tree" that is constructed while the update is applied. There are
 * two types of internal nodes: Document nodes and Array nodes. All other node types are always
 * leaves.
 *
 * When the update is complete, the diff tree is converted into a $v: 2 oplog entry.
 */
enum class NodeType { kDocument, kArray, kDelete, kUpdate, kInsert };

struct Node {
    virtual NodeType type() const = 0;
};

struct DeleteNode : public Node {
    NodeType type() const override {
        return NodeType::kDelete;
    }
};

/**
 * Common base class for Insert and Update, both of which contain a new value for their
 * associated field.
 */
struct LiteralValueNode : public Node {
    LiteralValueNode(BSONElement el) : elt(el) {}
    BSONElement elt;
};

struct InsertNode : public LiteralValueNode {
    InsertNode(BSONElement el) : LiteralValueNode{el} {}

    NodeType type() const override {
        return NodeType::kInsert;
    }
};

struct UpdateNode : public LiteralValueNode {
    UpdateNode(BSONElement el) : LiteralValueNode{el} {}

    NodeType type() const override {
        return NodeType::kUpdate;
    }
};

struct DocumentNode : public Node {
    DocumentNode() = default;
    DocumentNode(bool isCreated) : created(isCreated) {}

    NodeType type() const override {
        return NodeType::kDocument;
    }

    // Indicates whether the document this node represents was created as part of the update.
    // E.g. applying the update {$set: {"a.b.c": "foo"}} on document {} will create documents
    // at paths "a" and "a.b".
    bool created = false;

    std::map<std::string, std::unique_ptr<Node>> children;
};
struct ArrayNode : public Node {
    NodeType type() const override {
        return NodeType::kArray;
    }
    std::map<size_t, std::unique_ptr<Node>> children;
};

/**
 * A log builder which can produce $v: 2 oplog entries.
 *
 * This log builder accumulates updates, creates and deletes, and stores them in a tree. When the
 * update is done and serialize() is called, the tree is converted into a $v:2 oplog entry.
 *
 * TODO: Tests.
 */
class V2LogBuilder : public LogBuilderBase {
public:
    /**
     * The constructor for a V2 log builder requires a pointer to the set of array paths maintained
     * by the update system. Since it is ambiguous whether a FieldRef component is
     * an array or a field name, this set is used to disambiguate.
     *
     * For example, if a modification is made to the path 'a.0', we do not know whether "0" is an
     * array index, or if it is a field name until runtime. If it is an array index, the update
     * system will store 'a' in its set of array paths which are modified. When the update system
     * calls log*Field() on the path 'a.0', the V2LogBuilder will determine which parts of the path
     * refer to arrays by doing lookups in the provided set.
     */
    V2LogBuilder(const StringSet* modifiedArrayPaths) : _arrayPaths(modifiedArrayPaths) {}

    /**
     * Overload methods from the LogBuilder interface.
     */
    Status logUpdatedField(const FieldRef& path, mutablebson::Element elt) override;
    Status logUpdatedField(const FieldRef& path, BSONElement) override;
    Status logCreatedField(const FieldRef& path,
                           int idxOfFirstNewComponent,
                           mutablebson::Element elt) override;
    Status logDeletedField(const FieldRef& path) override;

    /**
     * Converts the in-memory tree to a $v:2 delta oplog entry.
     */
    BSONObj serialize() const override;

private:
    // Helpers for maintaining/updating the tree.
    std::unique_ptr<Node> createNewInternalNode(StringData fullPath, bool newPath);
    Node* createInternalNode(DocumentNode* parent,
                             const FieldRef& fullPath,
                             size_t indexOfChildPathComponent,
                             bool newPath);
    Node* createInternalNode(ArrayNode* parent,
                             const FieldRef& fullPath,
                             size_t indexOfChildPathComponent,
                             size_t childPathComponentValue,
                             bool newPath);

    // Helpers for adding nodes at a certain path. Returns false if the path was invalid/did
    // not exist.
    bool addNodeAtPathHelper(const FieldRef& path,
                             size_t pathIdx,
                             Node* root,
                             std::unique_ptr<Node> nodeToAdd,
                             boost::optional<size_t> idxOfFirstNewComponent);

    bool addNodeAtPath(const FieldRef& path,
                       Node* root,
                       std::unique_ptr<Node> nodeToAdd,
                       boost::optional<size_t> idxOfFirstNewComponent);

    // As a convention, everything in the tree is stored as a BSONElement. This is a helper for
    // converting mutablebson elements into BSONElements.
    BSONElement convertToBSONElement(mutablebson::Element);

    // Pointer to set of array paths that is maintained by the update tree.
    const StringSet* _arrayPaths;

    // Root of the tree.
    DocumentNode _root;

    // We may need to convert a mutable bson element to a BSONElement. Often this can be done
    // without making copies, but sometimes that is not possible. In the event that a copy is
    // necessary, the memory for the copies is owned here.
    std::vector<BSONObj> _storage;
};
}  // namespace v2_log_builder
}  // namespace mongo
