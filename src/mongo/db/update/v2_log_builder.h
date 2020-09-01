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
#include "mongo/db/update/log_builder_interface.h"
#include "mongo/db/update/runtime_update_path.h"
#include "mongo/util/string_map.h"

namespace mongo {
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
    virtual ~Node(){};
};


struct InsertNode : public Node {
    InsertNode(mutablebson::Element el) : elt(el) {}
    InsertNode(BSONElement el) : elt(el) {}

    NodeType type() const override {
        return NodeType::kInsert;
    }
    stdx::variant<mutablebson::Element, BSONElement> elt;
};

struct UpdateNode : public Node {
    UpdateNode(mutablebson::Element el) : elt(el) {}

    NodeType type() const override {
        return NodeType::kUpdate;
    }
    mutablebson::Element elt;
};

struct DeleteNode : public Node {
    NodeType type() const override {
        return NodeType::kDelete;
    }
};

// Struct representing non-leaf node.
struct InternalNode : public Node {
    virtual Node* addChild(StringData fieldName, std::unique_ptr<Node> node) = 0;
    virtual Node* getChild(StringData fieldName) const = 0;
};

struct DocumentNode : public InternalNode {
    DocumentNode(bool isCreated) : created(isCreated) {}

    Node* addChild(StringData fieldName, std::unique_ptr<Node> node);

    Node* getChild(StringData fieldName) const {
        auto it = children.find(fieldName);
        return (it != children.end()) ? it->second.get() : nullptr;
    }

    NodeType type() const override {
        return NodeType::kDocument;
    }

    bool isCreateNode(Node* node) {
        return (node->type() == NodeType::kInsert) ||
            (node->type() == NodeType::kDocument && static_cast<DocumentNode*>(node)->created);
    }

    std::vector<std::pair<std::string, std::unique_ptr<UpdateNode>>> updates;
    std::vector<std::pair<std::string, std::unique_ptr<DeleteNode>>> deletes;

    // We use std::list here, because the fieldName in 'children' references to the 'inserts' and
    // 'subDiffs'. std::vector can reallocate the objects as the size grows hence making the
    // references invalid.
    std::list<std::string> inserts;
    std::list<std::string> subDiffs;
    StringDataMap<std::unique_ptr<Node>> children;

    // Indicates whether the document this node represents was created as part of the update.
    // E.g. applying the update {$set: {"a.b.c": "foo"}} on document {} will create sub-documents
    // at paths "a" and "a.b".
    bool created = false;
};

struct ArrayNode : public InternalNode {
    Node* addChild(StringData fieldName, std::unique_ptr<Node> node);

    virtual Node* getChild(StringData fieldName) const {
        auto idx = str::parseUnsignedBase10Integer(fieldName);
        invariant(idx);
        auto it = children.find(*idx);
        return (it != children.end()) ? it->second.get() : nullptr;
    }

    NodeType type() const override {
        return NodeType::kArray;
    }

    // The map also represents the order of the childern, sorted by the array index.
    std::map<size_t, std::unique_ptr<Node>> children;
};

/**
 * A log builder which can produce $v:2 oplog entries.
 *
 * This log builder accumulates updates, creates and deletes, and stores them in a tree. When the
 * update is done and serialize() is called, the tree is converted into a $v:2 oplog entry. Note
 * that we don't need a pre-image for building the oplog.
 */
class V2LogBuilder : public LogBuilderInterface {
public:
    V2LogBuilder() : _root(false){};

    UpdateOplogEntryVersion oplogEntryVersion() const override {
        return UpdateOplogEntryVersion::kDeltaV2;
    }
    
    /**
     * Overload methods from the LogBuilder interface.
     */
    Status logUpdatedField(const RuntimeUpdatePath& path, mutablebson::Element elt) override;
    Status logCreatedField(const RuntimeUpdatePath& path,
                           int idxOfFirstNewComponent,
                           mutablebson::Element elt) override;
    Status logCreatedField(const RuntimeUpdatePath& path,
                           int idxOfFirstNewComponent,
                           BSONElement elt) override;
    Status logDeletedField(const RuntimeUpdatePath& path) override;

    /**
     * Converts the in-memory tree to a $v:2 delta oplog entry.
     */
    BSONObj serialize() const override;

private:
    // Helpers for maintaining/updating the tree.
    Node* createInternalNode(InternalNode* parent,
                             const RuntimeUpdatePath& fullPath,
                             size_t indexOfChildPathComponent,
                             bool newPath);

    // Helpers for adding nodes at a certain path. Returns false if the path was invalid/did
    // not exist.
    bool addNodeAtPathHelper(const RuntimeUpdatePath& path,
                             size_t pathIdx,
                             Node* root,
                             std::unique_ptr<Node> nodeToAdd,
                             boost::optional<size_t> idxOfFirstNewComponent);

    bool addNodeAtPath(const RuntimeUpdatePath& path,
                       Node* root,
                       std::unique_ptr<Node> nodeToAdd,
                       boost::optional<size_t> idxOfFirstNewComponent);

    // Root of the tree.
    DocumentNode _root;
};
}  // namespace v2_log_builder
}  // namespace mongo
