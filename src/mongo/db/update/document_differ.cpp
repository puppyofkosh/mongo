/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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

#include "mongo/platform/basic.h"

#include "mongo/db/update/document_differ.h"

namespace mongo {
namespace doc_diff {
DocumentDiff DocumentDiff::diffArrays(const BSONObj& pre,
                                      const BSONObj& post,
                                      const ArrayIndexPath& prefix) {
    BSONObjIterator preIt(pre);
    BSONObjIterator postIt(post);

    DocumentDiff ret;

    size_t index = 0;
    for (; preIt.more() && postIt.more(); ++index) {
        auto preElt = preIt.next();
        auto postElt = postIt.next();

        // Check they have the same index.
        uassert(ErrorCodes::BadValue, // TODO: error code
                  "Invalid BSON Array",
                  preElt.fieldNameStringData() == postElt.fieldNameStringData());
        // Check that there aren't missing indexes.
        uassert(ErrorCodes::BadValue,
                "Invalid BSON Array",
                std::to_string(index) == preElt.fieldNameStringData());

        if (preElt.binaryEqual(postElt)) {
            // They're identical so move on.
            continue;
        } else if (preElt.type() == BSONType::Object && postElt.type() == BSONType::Object) {
            // They're not identical but they're both objects, so we can diff them.
            ArrayIndexPath newPrefix(prefix);
            newPrefix.append(index);
            auto diff = DocumentDiff::computeDiffHelper(preElt.embeddedObject(),
                                                        postElt.embeddedObject(),
                                                        newPrefix);

            // TODO: Check if diff is bigger than postElt. For now I'm leaving this out to make
            // testing easier for me.
            ret.merge(std::move(diff));
        } else {
            // Record as an overwrite.
            ArrayIndexPath newFr(prefix);
            newFr.append(index);
            ret._toUpsert.push_back({std::move(newFr), postElt});            
        }
    }

    while (preIt.more()) {
        invariant(!postIt.more());

        // TODO: Check that 'index' matches the field name of the array.

        // TODO: think about this!
    }

    while (postIt.more()) {
        invariant(!preIt.more());
        auto newElem = postIt.next();

        uassert(ErrorCodes::BadValue,
                "Invalid BSON Array",
                std::to_string(index) == newElem.fieldNameStringData());
        ++index;
                
        // Record as an insert.
        ArrayIndexPath insertFr(prefix);
        insertFr.append(index);
        ret._toInsert.push_back({insertFr, newElem});
    }

    return ret;
}
    
DocumentDiff DocumentDiff::computeDiffHelper(const BSONObj& pre,
                                             const BSONObj& post,
                                             const ArrayIndexPath& prefix) {
    BSONObjIterator preIt(pre);
    BSONObjIterator postIt(post);

    DocumentDiff ret;

    while (preIt.more() && postIt.more()) {
        auto preElt = *preIt;
        auto postElt = *postIt;

        if (preElt.fieldNameStringData() == postElt.fieldNameStringData()) {
            if (preElt.binaryEqual(postElt)) {
                // They're identical. Move on.
            } else if (preElt.type() == BSONType::Object && postElt.type() == BSONType::Object) {
                // Both are objects, but not identical. We can compute the diff and merge it.
                ArrayIndexPath newPrefix(prefix);
                newPrefix.append(preElt.fieldName());
                DocumentDiff subDiff =
                    computeDiffHelper(preElt.embeddedObject(), postElt.embeddedObject(), newPrefix);

                // TODO: compute rough size of the diff. If it is bigger than postElt, we should
                // record this as a simple "set" of the subfield rather than a diff of it.
                // TODO: Write a test for this.
                ret.merge(std::move(subDiff));
            } else if (preElt.type() == BSONType::Array && postElt.type() == BSONType::Array) {
                ArrayIndexPath newPrefix(prefix);
                newPrefix.append(preElt.fieldName());
                auto arrDiff = diffArrays(preElt.embeddedObject(),
                                          postElt.embeddedObject(),
                                          newPrefix);
                // TODO: For real implementation consider writing a DiffBuilder class and maybe
                // make this merging stuff more efficient.
                ret.merge(std::move(arrDiff));
            } else {
                // Record this as an overwrite.
                ArrayIndexPath newFr(prefix);
                newFr.append({postElt.fieldName()});
                ret._toUpsert.push_back({std::move(newFr), postElt});
            }

            preIt.next();
            postIt.next();
        } else {
            // Record this as a deletion.
            ArrayIndexPath remove(prefix);
            remove.append(preElt.fieldName());
            ret._toDelete.push_back({std::move(remove)});
            preIt.next();
        }
    }

    // Record remaining fields in preElt as removals.
    while (preIt.more()) {
        ArrayIndexPath remove(prefix);
        remove.append((*preIt).fieldName());
        ret._toDelete.push_back({std::move(remove)});
        preIt.next();
    }

    while (postIt.more()) {
        // Record these as creates.
        ArrayIndexPath insertFr(prefix);
        insertFr.append((*postIt).fieldName());
        ret._toInsert.push_back({insertFr, (*postIt)});
        // If a field exists in the insert region, ensure that it doesn't also exist in the remove
        // region.
        // TODO: In real version make sure this isn't n^2.
        for (auto itr = ret._toDelete.cbegin(); itr != ret._toDelete.cend(); itr++) {
            if (insertFr == (*itr)) {
                ret._toDelete.erase(itr);
                break;
            }
        }
        postIt.next();
    }

    return ret;
}

DocumentDiff DocumentDiff::computeDiff(const BSONObj& pre, const BSONObj& post) {
    return computeDiffHelper(pre, post, ArrayIndexPath{});
}

void DocumentDiff::merge(DocumentDiff&& other) {
    _toUpsert.insert(_toUpsert.end(), other._toUpsert.begin(), other._toUpsert.end());
    _toDelete.insert(_toDelete.end(), other._toDelete.begin(), other._toDelete.end());
    _toInsert.insert(_toInsert.end(), other._toInsert.begin(), other._toInsert.end());
}

std::string DocumentDiff::toStringDebug() const {
    std::string ret;
    for (auto&& r : _toDelete) {
        ret += "remove: " + r.debugString() + "\n";
    }

    for (auto&& s : _toUpsert) {
        ret += str::stream() << "insert: "
                             << s.first.debugString() << " " << s.second.toString(false) << "\n";
    }

    for (auto&& s : _toInsert) {
        ret += str::stream() << "create: " << s.first.debugString()
                             << " " << s.second.toString(false) << "\n";
    }

    return ret;
}
}  // namespace doc_diff
};  // namespace mongo
