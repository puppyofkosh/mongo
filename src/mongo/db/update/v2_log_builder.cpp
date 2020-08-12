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

#include "mongo/db/update/v2_log_builder.h"

#include "mongo/db/field_ref.h"
#include "mongo/db/update/update_oplog_entry_serialization.h"
#include "mongo/db/update/update_executor.h"
#include "mongo/util/str.h"

namespace mongo::v2_log_builder {
Status V2LogBuilder::logUpdatedField(const PathTaken& path, mutablebson::Element elt) {
    invariant(path.good());
    invariant(elt.ok());
    auto newNode = std::make_unique<UpdateNode>(elt);
    invariant(addNodeAtPath(path,
                            &_root,
                            std::move(newNode),
                            boost::none  // Index of first created component is none since this was
                                         // an update, not a create.
                            ));

    return Status::OK();
}

Status V2LogBuilder::logUpdatedField(const PathTaken& path, BSONElement elt) {
    invariant(path.good());
    auto newNode = std::make_unique<UpdateNode>(elt);
    invariant(addNodeAtPath(path,
                            &_root,
                            std::move(newNode),
                            boost::none  // Index of first created component is none since this was
                                         // an update, not a create.
                            ));

    return Status::OK();
}

Status V2LogBuilder::logCreatedField(const PathTaken& path,
                                     int idxOfFirstNewComponent,
                                     mutablebson::Element elt) {
    invariant(path.good());
    invariant(elt.ok());
    auto newNode = std::make_unique<InsertNode>(elt);
    invariant(addNodeAtPath(path, &_root, std::move(newNode), idxOfFirstNewComponent));

    return Status::OK();
}

Status V2LogBuilder::logDeletedField(const PathTaken& path) {
    invariant(path.good());
    invariant(addNodeAtPath(path, &_root, std::make_unique<DeleteNode>(), boost::none));

    return Status::OK();
}

    namespace {
BSONElement convertToBSONElement(const stdx::variant<BSONElement, mutablebson::Element>& eltVar,
                                 std::vector<BSONObj>* tempStorage) {
    if (stdx::holds_alternative<BSONElement>(eltVar)) {
        return stdx::get<BSONElement>(eltVar);
    }
    auto elt = stdx::get<mutablebson::Element>(eltVar);
    
    if (elt.hasValue()) {
        BSONElement bsonElt = elt.getValue();
        return bsonElt;
    } else if (elt.getType() == BSONType::Object) {
        BSONObjBuilder topLevelBob;

        {
            BSONObjBuilder bob(topLevelBob.subobjStart("dummy"));
            elt.writeTo(&bob);
        }
        tempStorage->push_back(topLevelBob.obj());
        return tempStorage->back().firstElement();
    } else if (elt.getType() == BSONType::Array) {
        BSONObjBuilder topLevelBob;
        {
            BSONArrayBuilder bob(topLevelBob.subarrayStart("dummy"));
            elt.writeArrayTo(&bob);
        }
        tempStorage->push_back(topLevelBob.obj());
        return tempStorage->back().firstElement();
    }
    MONGO_UNREACHABLE;
}
    }

std::unique_ptr<Node> V2LogBuilder::createNewInternalNode(const PathTaken& fullPath,
                                                          size_t indexOfChildPathComponent,
                                                          bool newPath) {
    invariant(indexOfChildPathComponent < fullPath.fr().numParts());
    invariant(fullPath.good());
    const auto pathStr = fullPath.fr().dottedSubstring(0, indexOfChildPathComponent + 1);

    std::unique_ptr<Node> newNode;
    if (fullPath.types()[indexOfChildPathComponent+1] == FieldComponentType::kArrayIndex) {
        invariant(_arrayPaths->count(pathStr));
        return std::make_unique<ArrayNode>();
    } else {
        invariant(!_arrayPaths->count(pathStr));
        invariant(fullPath.types()[indexOfChildPathComponent+1] == FieldComponentType::kFieldName);
        return std::make_unique<DocumentNode>(newPath);
    }
}

Node* V2LogBuilder::createInternalNode(DocumentNode* parent,
                                       const PathTaken& fullPath,
                                       size_t indexOfChildPathComponent,
                                       bool newPath) {
    std::unique_ptr<Node> newNode = createNewInternalNode(fullPath,
                                                          indexOfChildPathComponent,
                                                          newPath);
    auto ret = newNode.get();
    auto [it, inserted] =
        parent->children.emplace(fullPath.fr().getPart(indexOfChildPathComponent), std::move(newNode));
    invariant(inserted);

    return ret;
}

Node* V2LogBuilder::createInternalNode(ArrayNode* parent,
                                       const PathTaken& fullPath,
                                       size_t indexOfChildPathComponent,
                                       size_t childPathComponentValue,
                                       bool newPath) {
    std::unique_ptr<Node> newNode = createNewInternalNode(fullPath, indexOfChildPathComponent, newPath);
    auto ret = newNode.get();
    auto [it, inserted] = parent->children.emplace(childPathComponentValue, std::move(newNode));
    invariant(inserted);

    return ret;
}

bool V2LogBuilder::addNodeAtPath(const PathTaken& path,
                                 Node* root,
                                 std::unique_ptr<Node> nodeToAdd,
                                 boost::optional<size_t> idxOfFirstNewComponent) {
    return addNodeAtPathHelper(path, 0, root, std::move(nodeToAdd), idxOfFirstNewComponent);
}

bool V2LogBuilder::addNodeAtPathHelper(const PathTaken& path,
                                       size_t pathIdx,
                                       Node* root,
                                       std::unique_ptr<Node> nodeToAdd,
                                       boost::optional<size_t> idxOfFirstNewComponent) {
    // If our path is a.b.c.d and the first new component is "b" then we are dealing with a
    // newly created path for components b, c and d.
    const bool isNewPath = idxOfFirstNewComponent && (pathIdx >= *idxOfFirstNewComponent);

    if (root->type() == NodeType::kDocument) {
        DocumentNode* docNode = static_cast<DocumentNode*>(root);
        const auto part = path.fr().getPart(pathIdx);
        if (pathIdx == static_cast<size_t>(path.fr().numParts() - 1)) {
            docNode->children.emplace(part, std::move(nodeToAdd));
            return true;
        }

        if (auto it = docNode->children.find(part.toString()); it != docNode->children.end()) {
            return addNodeAtPathHelper(
                path, pathIdx + 1, it->second.get(), std::move(nodeToAdd), idxOfFirstNewComponent);
        } else {
            auto newNode = createInternalNode(docNode, path, pathIdx, isNewPath);
            return addNodeAtPathHelper(
                path, pathIdx + 1, newNode, std::move(nodeToAdd), idxOfFirstNewComponent);
        }
        MONGO_UNREACHABLE;
    } else if (root->type() == NodeType::kArray) {
        ArrayNode* arrNode = static_cast<ArrayNode*>(root);
        const auto part = path.fr().getPart(pathIdx);
        invariant(FieldRef::isNumericPathComponentStrict(part));
        auto optInd = str::parseUnsignedBase10Integer(part);
        invariant(optInd);

        if (pathIdx == static_cast<size_t>(path.fr().numParts() - 1)) {
            arrNode->children.emplace(*optInd, std::move(nodeToAdd));
            return true;
        }

        if (auto it = arrNode->children.find(*optInd); it != arrNode->children.end()) {
            return addNodeAtPathHelper(
                path, pathIdx + 1, it->second.get(), std::move(nodeToAdd), idxOfFirstNewComponent);
        } else {
            auto newNode = createInternalNode(arrNode, path, pathIdx, *optInd, isNewPath);
            return addNodeAtPathHelper(
                path, pathIdx + 1, newNode, std::move(nodeToAdd), idxOfFirstNewComponent);
        }
    }

    return false;
}

namespace {
void serializeNewlyCreatedDocument(const DocumentNode& node, BSONObjBuilder* out,
                                       std::vector<BSONObj>* tempStorage) {
    for (auto&& [name, child] : node.children) {
        switch (child->type()) {
            case NodeType::kDocument: {
                BSONObjBuilder childBuilder(out->subobjStart(name));
                serializeNewlyCreatedDocument(static_cast<const DocumentNode&>(*child),
                                              &childBuilder, tempStorage);
                break;
            }
            case NodeType::kInsert: {
                out->appendAs(convertToBSONElement(static_cast<const InsertNode&>(*child).elt, tempStorage), name);
                break;
            }

            case NodeType::kArray:
            case NodeType::kDelete:
            case NodeType::kUpdate:
            default:
                // A newly created document cannot contain arrays, deletes, or updates.
                MONGO_UNREACHABLE;
        }
    }
}

// Mutually recursive with writeArrayDiff().
void writeDocumentDiff(const DocumentNode& node,
                       doc_diff::DocumentDiffBuilder* builder,
                       std::vector<BSONObj>* tempStorage);

void writeArrayDiff(const ArrayNode& node,
                    doc_diff::ArrayDiffBuilder* builder,
                    std::vector<BSONObj>* tempStorage) {
    for (auto&& [idx, child] : node.children) {
        switch (child->type()) {
            case NodeType::kDocument: {
                const auto& docNode = static_cast<const DocumentNode&>(*child);
                if (docNode.created) {
                    // See comment in writeDocumentDiff() for explanation of this case.  This code
                    // is needed for situations with an update like {$set: {"a.0.b": "foo"}} on the
                    // document {a: []}. The document at position 0 in the 'a' array must be
                    // created.

                    BSONObjBuilder topLevelBob;
                    {
                        BSONObjBuilder bob(topLevelBob.subobjStart("dummy"));
                        serializeNewlyCreatedDocument(docNode, &bob, tempStorage);
                    }
                    tempStorage->push_back(topLevelBob.obj());
                    builder->addUpdate(idx, tempStorage->back().firstElement());
                } else {
                    // Recursive case.
                    auto childBuilder = builder->startSubObjDiff(idx);
                    writeDocumentDiff(docNode, childBuilder.builder(), tempStorage);
                }
                break;
            }
            case NodeType::kArray: {
                auto childBuilder = builder->startSubArrDiff(idx);
                writeArrayDiff(
                    static_cast<const ArrayNode&>(*child), childBuilder.builder(), tempStorage);
                break;
            }
            case NodeType::kInsert:
            case NodeType::kUpdate: {
                // In $v:2 entries, array updates and inserts are treated the same.
                const auto& valueNode = static_cast<const LiteralValueNode&>(*child);
                builder->addUpdate(idx, convertToBSONElement(valueNode.elt, tempStorage));
                break;
            }
            case NodeType::kDelete:
                // Deletes are logged by setting a field to null.
                tempStorage->push_back(BSON("" << NullLabeler{}));
                builder->addUpdate(idx, tempStorage->back().firstElement());
                break;
            default:
                MONGO_UNREACHABLE;
        }
    }
}

void writeDocumentDiff(const DocumentNode& node,
                       doc_diff::DocumentDiffBuilder* builder,
                       std::vector<BSONObj>* tempStorage) {
    for (auto&& [name, child] : node.children) {
        switch (child->type()) {
            case NodeType::kDocument: {
                const auto& docNode = static_cast<const DocumentNode&>(*child);
                if (docNode.created) {
                    // We should never get here if our parent was also a newly created node.
                    invariant(!node.created);

                    // This represents a new document. While the modifier-style update system
                    // was capable of writing paths which would implicitly create new
                    // documents, there is no equivalent in $v: 2 updates.
                    //
                    // For example {$set: {"a.b.c": 1}} would create document 'a' and 'a.b' if
                    // necessary.
                    //
                    // Since $v:2 entries don't have this capability, we instead build the new
                    // object and log it as an insert. For example, applying the above $set on
                    // document {a: {}} will be logged as an insert of the value {b: {c: 1}} on
                    // document 'a'.

                    // addInsert() only takes BSONElements so we have one extra layer of BSONObj
                    // here.
                    BSONObjBuilder topLevelBob;
                    {
                        BSONObjBuilder bob(topLevelBob.subobjStart("dummy"));
                        serializeNewlyCreatedDocument(docNode, &bob, tempStorage);
                    }
                    tempStorage->push_back(topLevelBob.obj());
                    builder->addInsert(name, tempStorage->back().firstElement());
                } else {
                    // Recursive case.
                    auto childBuilder = builder->startSubObjDiff(name);
                    writeDocumentDiff(docNode, childBuilder.builder(), tempStorage);
                }
                break;
            }
            case NodeType::kArray: {
                auto childBuilder = builder->startSubArrDiff(name);
                writeArrayDiff(
                    static_cast<const ArrayNode&>(*child), childBuilder.builder(), tempStorage);
                break;
            }
            case NodeType::kDelete: {
                builder->addDelete(name);
                break;
            }
            case NodeType::kUpdate: {
                builder->addUpdate(name,
                                   convertToBSONElement(
                                       static_cast<const UpdateNode*>(child.get())->elt,
                                       tempStorage));
                break;
            }
            case NodeType::kInsert: {
                builder->addInsert(name, convertToBSONElement(
                                       static_cast<const InsertNode*>(child.get())->elt,
                                       tempStorage));
                break;
            }
            default:
                MONGO_UNREACHABLE;
        }
    }
}
}  // namespace


BSONObj V2LogBuilder::serialize() const {
    // BSONObjs which are temporarily owned by this object while in the process of
    // serialize()ing the output. This is necessary because the DocumentDiffBuilder will not
    // take ownership of the memory we give to it until serialize() is called.
    std::vector<BSONObj> tempStorage;

    doc_diff::DocumentDiffBuilder topBuilder;
    writeDocumentDiff(_root, &topBuilder, &tempStorage);

    auto res = topBuilder.serialize();
    if (!res.isEmpty()) {
        return update_oplog_entry::makeDeltaOplogEntry(res);
    }

    return BSONObj();
}
}  // namespace mongo::v2_log_builder
