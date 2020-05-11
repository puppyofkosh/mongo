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
#include "mongo/util/string_map.h"

namespace mongo {
namespace doc_diff {
void computeDiffHelper(const BSONObj& pre,
                       const BSONObj& post,
                       OplogDiffBuilder* builder);
    
void diffArrays(const BSONObj& pre,
                const BSONObj& post,
                OplogDiffBuilder* builder) {
    BSONObjIterator preIt(pre);
    BSONObjIterator postIt(post);

    size_t index = 0;
    for (; preIt.more() && postIt.more(); ++index) {
        auto preElt = preIt.next();
        auto postElt = postIt.next();

        // Check they have the same index.
        uassert(ErrorCodes::BadValue,  // TODO: error code
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
            builder->appendIndex(index);

            builder->b().appendChar(Marker::kDiffMarker);

            {
                OplogDiffBuilder sub = builder->subStart();
                computeDiffHelper(preElt.embeddedObject(), postElt.embeddedObject(), &sub);
            }

            // TODO: Check if diff is bigger than postElt. For now I'm leaving this out to make
            // testing easier for me.
        } else {
            builder->appendIndex(index);
            // SUBTLE: We record these as "upsert" operations instead of "inserts" because we do not
            // want to move the field to the end, we simply want to change its value.

            builder->b().appendChar(Marker::kUpdateMarker);
            builder->appendElt(postElt);
        }
    }

    if (preIt.more()) {
        // We need to delete all these elements. So we resize (truncate) the array.
        builder->b().appendChar(Marker::kResizeMarker);
        builder->b().appendNum(static_cast<unsigned>(index));
        
        invariant(!postIt.more());
    }

    while (postIt.more()) {
        invariant(!preIt.more());
        auto newElem = postIt.next();

        uassert(ErrorCodes::BadValue,
                "Invalid BSON Array",
                std::to_string(index) == newElem.fieldNameStringData());
        builder->appendIndex(index);
        builder->b().appendChar(Marker::kInsertMarker);
        builder->appendElt(newElem);

        ++index;
    }

    builder->b().appendChar(0);
}

void computeDiffHelper(const BSONObj& pre,
                       const BSONObj& post,
                       OplogDiffBuilder* builder) {
    BSONObjIterator preIt(pre);
    BSONObjIterator postIt(post);

    // We cannot write the list of fields to remove right away.
    StringDataSet fieldsToRemove;

    while (preIt.more() && postIt.more()) {
        auto preElt = *preIt;
        auto postElt = *postIt;

        if (preElt.fieldNameStringData() == postElt.fieldNameStringData()) {
            if (preElt.binaryEqual(postElt)) {
                // They're identical. Move on.
            } else if (preElt.type() == BSONType::Object && postElt.type() == BSONType::Object) {
                // Technically this is safe since we know the StringData points to a
                // null-terminated string. Though we read off the end of the string data, we don't
                // read off the end of the backing string.
                builder->appendFieldName(preElt.fieldNameStringData());
                
                // Then record the diff for the subobj.
                builder->b().appendChar(Marker::kDiffMarker);

                {
                    OplogDiffBuilder sub(builder->subStart());
                    computeDiffHelper(preElt.embeddedObject(), postElt.embeddedObject(), &sub);
                }

                // TODO: compute rough size of the diff. If it is bigger than postElt, we should
                // record this as a simple "set" of the subfield rather than a diff of it. This
                // will require adding functionality to the diff builder.
                // TODO: Write a test for  this.
            } else if (preElt.type() == BSONType::Array && postElt.type() == BSONType::Array) {
                builder->appendFieldName(preElt.fieldNameStringData());

                builder->b().appendChar(Marker::kDiffMarker);
                {
                    OplogDiffBuilder sub(builder->subStart());
                    diffArrays(preElt.embeddedObject(), postElt.embeddedObject(), &sub);
                }
            } else {
                builder->appendFieldName(preElt.fieldNameStringData());

                // Record as an "update": the field changes value but retains its position in the
                // document.
                builder->b().appendChar(Marker::kUpdateMarker);
                // This is inefficient
                builder->appendElt(postElt);
            }

            preIt.next();
            postIt.next();
        } else {
            fieldsToRemove.insert(preElt.fieldNameStringData());
            preIt.next();
        }
    }

    // Record remaining fields in preElt as removals.
    while (preIt.more()) {
        fieldsToRemove.insert((*preIt).fieldNameStringData());
        preIt.next();
    }

    while (postIt.more()) {
        // Record these as inserts, indicating they should be put at the end of the object.
        StringData fieldName = (*postIt).fieldName();
        builder->appendFieldName(fieldName);
        builder->b().appendChar(Marker::kInsertMarker);
        builder->appendElt(*postIt);
        
        // If we're inserting a field at the end, make sure we don't also record it as a field to
        // delete.
        fieldsToRemove.erase((*postIt).fieldNameStringData());

        postIt.next();
    }

    for (auto && f : fieldsToRemove) {
        builder->appendFieldName(f);
        builder->b().appendChar(Marker::kExcludeMarker);
    }
    
    builder->b().appendChar(0);
}

OplogDiff computeDiff(const BSONObj& pre, const BSONObj& post) {
    BufBuilder storage;
    OplogDiffBuilder builder(storage);
    computeDiffHelper(pre, post, &builder);
    return builder.finish();
}
}  // namespace doc_diff
};  // namespace mongo
