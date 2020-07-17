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

namespace mongo::v2_log_builder {
    Status V2LogBuilder::logUpdatedField(StringData path, mutablebson::Element elt) {
        std::cout << "log updated field " << elt.toString() << " at " << path << std::endl;
        for (auto && s : *_arrayPaths) {
            std::cout << "array path " << s << std::endl;
        }
        
        return Status::OK();
    }

    Status V2LogBuilder::logUpdatedField(StringData path, BSONElement elt) {
        std::cout << "log updated field " << elt << " at " << path << std::endl;
        for (auto && s : *_arrayPaths) {
            std::cout << "array path " << s << std::endl;
        }        
        return Status::OK();
    }

    Status V2LogBuilder::logCreatedField(StringData path,
                                         int idxOfFirstNewComponent,
                                         mutablebson::Element elt) {
        std::cout << "log created field " << elt.toString() << " at " << path <<
            " idx " << idxOfFirstNewComponent << std::endl;
        for (auto && s : *_arrayPaths) {
            std::cout << "array path " << s << std::endl;
        }

        return Status::OK();
    }

    Status V2LogBuilder::logDeletedField(StringData path) {
        std::cout << "log deleted field " << path << std::endl;
        for (auto && s : *_arrayPaths) {
            std::cout << "array path " << s << std::endl;
        }

        return Status::OK();
    }

    Node* V2LogBuilder::createInternalNode(DocumentNode* parent,
                                           StringData pathToParent,
                                           StringData newNode) {
        return nullptr;
    }
    Node* V2LogBuilder::createInternalNode(ArrayNode* parent, StringData pathToParent, size_t idx) {
        return nullptr;
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
                auto newNode = createInternalNode(docNode, path.dottedSubstring(0, pathIdx), part);
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
                auto newNode = createInternalNode(arrNode, path.dottedSubstring(0, pathIdx), *optInd);
                return addNodeAtPath(path, pathIdx + 1, newNode,
                                     std::move(nodeToAdd));
            }
        }

        return false;
    }
}
