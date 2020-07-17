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

    Node* V2LogBuilder::findOrCreateNode(const FieldRef& path, size_t pathIdx, Node* root) {
        if (root->type() == NodeType::kDocument) {
        } else if (root->type() == NodeType::kArray) {
        } else {
            return nullptr;
        }

        MONGO_UNREACHABLE;
    }
}
