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
DocumentDiff DocumentDiff::computeDiffHelper(const BSONObj& pre,
                                             const BSONObj& post,
                                             const FieldRef& prefix) {
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
                FieldRef newPrefix(prefix);
                newPrefix.appendPart(preElt.fieldNameStringData());
                DocumentDiff subDiff =
                    computeDiffHelper(preElt.embeddedObject(), postElt.embeddedObject(), newPrefix);

                // TODO: compute rough size of the diff. If it is bigger than postElt, we should
                // record this as a simple "set" of the subfield rather than a diff of it.
                // TODO: Write a test for this.
                ret.merge(std::move(subDiff));
            } else {
                // Record this as an overwrite.
                FieldRef newFr(prefix);
                newFr.appendPart(postElt.fieldNameStringData());
                ret._toSet.push_back({std::move(newFr), postElt});
            }

            preIt.next();
            postIt.next();
        } else {
            // Record this as a deletion.
            FieldRef remove(prefix);
            remove.appendPart(preElt.fieldName());
            ret._toRemove.push_back({std::move(remove)});
            preIt.next();
        }
    }

    // Record remaining fields in preElt as removals.
    while (preIt.more()) {
        FieldRef remove(prefix);
        remove.appendPart((*preIt).fieldNameStringData());
        ret._toRemove.push_back({std::move(remove)});
        preIt.next();
    }

    while (postIt.more()) {
        // Record these as insertions.
        FieldRef insertFr(prefix);
        insertFr.appendPart((*postIt).fieldNameStringData());
        ret._toSet.push_back({std::move(insertFr), (*postIt)});
        postIt.next();
    }

    return ret;
}

DocumentDiff DocumentDiff::computeDiff(const BSONObj& pre, const BSONObj& post) {
    return computeDiffHelper(pre, post, FieldRef{});
}

void DocumentDiff::merge(DocumentDiff&& other) {
    _toSet.insert(_toSet.end(), other._toSet.begin(), other._toSet.end());
    _toRemove.insert(_toRemove.end(), other._toRemove.begin(), other._toRemove.end());
}

std::string DocumentDiff::toStringDebug() const {
    std::string ret;
    for (auto&& r : _toRemove) {
        ret += "remove " + r.dottedField() + "\n";
    }

    for (auto&& s : _toSet) {
        ret += str::stream() << "insert : " << s.second << "\n";
    }
    return ret;
}
}  // namespace doc_diff
};  // namespace mongo
