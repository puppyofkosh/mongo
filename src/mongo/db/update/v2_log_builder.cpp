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

#include "mongo/db/update/v2_log_builder.h"

#include "mongo/db/field_ref.h"
#include "mongo/util/str.h"
#include "mongo/db/update/document_diff_serialization.h"
#include "mongo/db/update/update_oplog_entry_serialization.h"

namespace mongo::v2_log_builder {
    Status V2LogBuilder::logUpdatedField(StringData path, mutablebson::Element elt) {
        invariant(elt.hasValue());
        std::cout << "update to " << path << " " << elt.getValue() << std::endl;
        auto newNode = std::make_unique<UpdateNode>(elt.getValue());
        // TODO: Can they just pass a field ref in?
        addNodeAtPath(FieldRef(path),
                      0,
                      &_root,
                      std::move(newNode));
        
        std::cout << "log updated field " << elt.toString() << " at " << path << std::endl;
        for (auto && s : *_arrayPaths) {
            std::cout << "array path " << s << std::endl;
        }
        
        return Status::OK();
    }

    Status V2LogBuilder::logUpdatedField(StringData path, BSONElement elt) {
        std::cout << "update to " << path << " " << elt << std::endl;
        auto newNode = std::make_unique<UpdateNode>(elt);
        // TODO: Can they just pass a field ref in?
        addNodeAtPath(FieldRef(path),
                      0,
                      &_root,
                      std::move(newNode));

        std::cout << "log updated field " << elt << " at " << path << std::endl;
        for (auto && s : *_arrayPaths) {
            std::cout << "array path " << s << std::endl;
        }        
        return Status::OK();
    }

    Status V2LogBuilder::logCreatedField(StringData path,
                                         int idxOfFirstNewComponent,
                                         mutablebson::Element elt) {
        //
        // TODO: Need to use idxOfFirstNewComponent to determine where the creation is.
        //
        
        invariant(elt.hasValue());
        std::cout << "create " << elt.getValue() << " " << std::endl;
        auto newNode = std::make_unique<InsertNode>(elt.getValue());
        // TODO: Can they just pass a field ref in?
        addNodeAtPath(FieldRef(path),
                      0,
                      &_root,
                      std::move(newNode));
        
        std::cout << "log created field " << elt.toString() << " at " << path <<
            " idx " << idxOfFirstNewComponent << std::endl;
        for (auto && s : *_arrayPaths) {
            std::cout << "array path " << s << std::endl;
        }

        return Status::OK();
    }

    Status V2LogBuilder::logDeletedField(StringData path) {
        addNodeAtPath(FieldRef(path),
                      0,
                      &_root,
                      std::make_unique<DeleteNode>());
        
        std::cout << "log deleted field " << path << std::endl;
        for (auto && s : *_arrayPaths) {
            std::cout << "array path " << s << std::endl;
        }

        return Status::OK();
    }

    std::unique_ptr<Node> V2LogBuilder::createNewInternalNode(StringData fullPath) {
        std::cout << "Creating internal node at " << fullPath << std::endl;
        std::unique_ptr<Node> newNode;
        if (_arrayPaths->count(fullPath)) {
            return std::make_unique<ArrayNode>();
        } else {
            return std::make_unique<DocumentNode>();
        }        
    }

    Node* V2LogBuilder::createInternalNode(DocumentNode* parent,
                                           const FieldRef& fullPath,
                                           size_t indexOfChildPathComponent) {
        const auto pathStr = fullPath.dottedSubstring(0, indexOfChildPathComponent + 1);
        std::unique_ptr<Node> newNode = createNewInternalNode(pathStr);
        auto ret = newNode.get();
        auto [it, inserted] = parent->children.emplace(fullPath.getPart(indexOfChildPathComponent),
                                                       std::move(newNode));
        invariant(inserted);
        
        return ret;
    }

    // TODO: comments
    Node* V2LogBuilder::createInternalNode(ArrayNode* parent,
                                           const FieldRef& fullPath,
                                           size_t indexOfChildPathComponent,
                                           size_t childPathComponentValue) {
        const auto pathStr = fullPath.dottedSubstring(0, indexOfChildPathComponent + 1);
        std::unique_ptr<Node> newNode = createNewInternalNode(pathStr);
        auto ret = newNode.get();
        auto [it, inserted] = parent->children.emplace(childPathComponentValue, std::move(newNode));
        invariant(inserted);
        
        return ret;    
    }
    
    bool V2LogBuilder::addNodeAtPath(const FieldRef& path,
                                     size_t pathIdx,
                                     Node* root,
                                     std::unique_ptr<Node> nodeToAdd) {
        if (root->type() == NodeType::kDocument) {
            DocumentNode* docNode = static_cast<DocumentNode*>(root);
            const auto part = path.getPart(pathIdx);
            if (pathIdx == static_cast<size_t>(path.numParts() - 1)) {
                docNode->children.emplace(part, std::move(nodeToAdd));
                return true;
            }

            if (auto it = docNode->children.find(part.toString()); it != docNode->children.end()) {
                return addNodeAtPath(path, pathIdx + 1, it->second.get(), std::move(nodeToAdd));
            } else {
                auto newNode = createInternalNode(docNode, path, pathIdx);
                return addNodeAtPath(path, pathIdx + 1, newNode, std::move(nodeToAdd));
            }
            MONGO_UNREACHABLE;
        } else if (root->type() == NodeType::kArray) {
            ArrayNode* arrNode = static_cast<ArrayNode*>(root);
            const auto part = path.getPart(pathIdx);
            invariant(FieldRef::isNumericPathComponentStrict(part));
            auto optInd = str::parseUnsignedBase10Integer(part);
            invariant(optInd);

            if (pathIdx == static_cast<size_t>(path.numParts() - 1)) {
                arrNode->children.emplace(*optInd, std::move(nodeToAdd));
                return true;
            }

            if (auto it = arrNode->children.find(*optInd); it != arrNode->children.end()) {
                return addNodeAtPath(path, pathIdx + 1, it->second.get(),
                                     std::move(nodeToAdd));
            } else {
                auto newNode = createInternalNode(arrNode, path, pathIdx, *optInd);
                return addNodeAtPath(path, pathIdx + 1, newNode,
                                     std::move(nodeToAdd));
            }
        }

        return false;
    }

    namespace {
        void writeArrayDiff(const ArrayNode& node,
                            doc_diff::ArrayDiffBuilder* builder) {
        }
        
        void writeDocumentDiff(const DocumentNode& node,
                               doc_diff::DocumentDiffBuilder* builder) {
            for (auto&& [name, child] : node.children) {
                switch(child->type()) {
                case NodeType::kDocument: {
                    auto childBuilder = builder->startSubObjDiff(name);
                    writeDocumentDiff(static_cast<const DocumentNode&>(*child),
                                      childBuilder.builder());
                    break;
                }
                case NodeType::kArray: {
                    auto childBuilder = builder->startSubArrDiff(name);
                    writeArrayDiff(static_cast<const ArrayNode&>(*child),
                                   childBuilder.builder());
                    break;
                }
                case NodeType::kDelete: {
                    builder->addDelete(name);
                    break;
                }
                case NodeType::kUpdate: {
                    builder->addUpdate(name, static_cast<const UpdateNode*>(child.get())->elt);
                    break;
                }
                case NodeType::kInsert: {
                    builder->addInsert(name, static_cast<const InsertNode*>(child.get())->elt);
                    break;
                }
                default:
                    MONGO_UNREACHABLE;
                }
            }
        }
    }

    BSONObj V2LogBuilder::serialize() const {
        doc_diff::DocumentDiffBuilder topBuilder;
        writeDocumentDiff(_root, &topBuilder);

        auto res = topBuilder.serialize();
        if (!res.isEmpty()) {
            std::cout << "Final oplog entry is " << res << std::endl;
            return update_oplog_entry::makeDeltaOplogEntry(res);
        }

        return BSONObj();
    }
}
