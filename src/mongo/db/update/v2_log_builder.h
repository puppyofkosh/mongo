#pragma once

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

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/base/status.h"
#include "mongo/db/update/log_builder_base.h"
#include "mongo/util/string_map.h"
#include "mongo/stdx/variant.h"

namespace mongo {
class FieldRef;
namespace v2_log_builder {
    enum class NodeType {
        kDocument,
        kArray,
        kDelete,
        kUpdate,
        kInsert
    };
    
    struct Node {
        virtual NodeType type() const = 0;
    };

    struct DeleteNode : public Node {
        NodeType type() const override {
            return NodeType::kDelete;
        }
    };

    struct InsertNode : public Node {
        InsertNode(BSONElement el) :elt(el) {}
        
        NodeType type() const override {
            return NodeType::kInsert;
        }

        BSONElement elt;
    };

    struct UpdateNode : public Node {
        UpdateNode(BSONElement el) : elt(el) {}
        
        NodeType type() const override {
            return NodeType::kUpdate;
        }
        BSONElement elt;
    };
    
    struct DocumentNode : public Node {
        NodeType type() const override {
            return NodeType::kDocument;
        }
        
        std::map<std::string, std::unique_ptr<Node>> children;
    };
    struct ArrayNode : public Node {
        NodeType type() const override {
            return NodeType::kArray;
        }
        std::map<size_t, std::unique_ptr<Node>> children;
    };

/**
 * TODO
 */
class V2LogBuilder : public LogBuilderBase {
public:
    V2LogBuilder(StringSet* modifiedArrayPaths)
        :_arrayPaths(modifiedArrayPaths)
    {}
    
    Status logUpdatedField(StringData path, mutablebson::Element elt) override;
    Status logUpdatedField(StringData path, BSONElement) override;
    Status logCreatedField(StringData path,
                           int idxOfFirstNewComponent,
                           mutablebson::Element elt) override;
    Status logDeletedField(StringData path) override;

    BSONObj serialize() const override;

private:
    std::unique_ptr<Node> createNewInternalNode(StringData fullPath);
    Node* createInternalNode(DocumentNode* parent,
                             const FieldRef& fullPath,
                             size_t indexOfChildPathComponent);
    Node* createInternalNode(ArrayNode* parent,
                             const FieldRef& fullPath,
                             size_t indexOfChildPathComponent,
                             size_t childPathComponentValue);
    
    bool addNodeAtPath(const FieldRef& path,
                       size_t pathIdx,
                       Node* root,
                       std::unique_ptr<Node> nodeToAdd);

    StringSet* _arrayPaths;
    DocumentNode _root;
};
}
}
