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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <iomanip>

#include "mongo/db/update/update_executor.h"
#include "mongo/db/update/oplog_diff.h"
#include "mongo/util/string_map.h"

namespace mongo {

/**
 * An UpdateExecutor representing a replacement-style update.
 */
class DeltaApplyExecutor : public UpdateExecutor {

public:
    /**
     * Initializes the node with the document to replace with. Any zero-valued timestamps (except
     * for the _id) are updated to the current time.
     */
    explicit DeltaApplyExecutor(const char* delta, size_t len)
        :_delta(delta), _len(len)
    {
    }

    ApplyResult applyUpdate(ApplyParams applyParams) const final {
        auto preImage = applyParams.element.getDocument().getObject();
        const auto postImage = applyDiff(preImage);
        const bool postImageHasId = postImage.hasField("_id");

        return ObjectReplaceExecutor::applyReplacementUpdate(
            applyParams, postImage, postImageHasId);
    }

    Value serialize() const final {
        MONGO_UNREACHABLE;
    }

private:
    struct Tables {
        // Any field marked as 'deleted' or 'inserted' (which will get re-added at the end)
        // goes in here.
        StringDataSet fieldsToSkip;
        // fields to record with a different value when encountered. If not encountered,
        // they should be added to the end in specified order.
        StringDataMap<BSONElement> fieldsToSet;
        // fields which have sub-diffs.
        StringDataMap<const char*> fieldsWithSubDiffs;

        // Order to add new fields to the end. This is the combination of 'updated' fields and
        // 'insert' fields.  Updated fields which were already present in the document will need to
        // be skipped.
        std::vector<std::pair<StringData, BSONElement>> fieldsToInsert;
    };

    Tables buildTables(doc_diff::OplogDiffReader* reader) const {
        using namespace doc_diff;

        Tables out;
        auto str = reader->nextString();
        while (*str != 0) {
            std::cout << "ian: name entry " << str << std::endl;
            const auto marker = reader->nextByte();

            if (marker == Marker::kSubDiffMarker) {
                // TODO: Check return value of these
                const auto subDiffSize = reader->nextUnsigned(false);
                out.fieldsWithSubDiffs.insert({str, reader->rest()});
                reader->skip(subDiffSize);
            } else if (marker == Marker::kUpdateMarker) {
                BSONElement elt(reader->nextBsonElt());
                out.fieldsToSet.insert({str, elt});
                out.fieldsToInsert.push_back({str, elt});
            } else if (marker == Marker::kInsertMarker) {
                out.fieldsToSkip.insert(str);
                out.fieldsToInsert.push_back({str, reader->nextBsonElt()});
            } else if (marker == Marker::kExcludeMarker) {
                out.fieldsToSkip.insert(str);
            } else {
                MONGO_UNREACHABLE;
            }
            
            str = reader->nextString();
        }
        return out;
    }

    void applyDiffToArray(const BSONObj& preImage,
                          BSONArrayBuilder* builder,
                          doc_diff::OplogDiffReader* reader) const {
        using namespace doc_diff;

        // Skip size bytes.
        reader->skip(4);

        BSONObjIterator preImageIt(preImage);
        size_t preImageIndex = 0;
        size_t postImageIndex = 0;

        // Gets set when there's a resize entry encountered.
        boost::optional<uint32_t> resizeVal;
        
        auto marker = reader->nextByte();
        while (marker) {
            if (marker == Marker::kIndexMarker) {
                const uint32_t diffIndex = reader->nextUnsigned();
                std::cout << "ian: diff for index " << diffIndex << std::endl;
                while (preImageIndex < diffIndex && preImageIt.more()) {
                    std::cout << "ian: keeping ind " << preImageIndex << std::endl;
                    builder->append(preImageIt.next());
                    ++preImageIndex;
                    ++postImageIndex;
                }

                while (postImageIndex < diffIndex) {
                    std::cout << "ian: nulling " << postImageIndex << std::endl;
                    builder->appendNull();
                    postImageIndex++;
                }
                const auto changeType = reader->nextByte();
                if (changeType == Marker::kSubDiffMarker) {
                    const char* subDiffBytes = reader->rest();
                    const auto subDiffSize = reader->nextUnsigned();
                    const unsigned char subDiffTypeMarker = reader->nextByte();
                    
                    if (preImageIndex == postImageIndex && preImageIt.more()) {
                        // If we're in this case then the pre-image had a value at this path.

                        const auto arrayElemType = (*preImageIt).type();
                        if  (arrayElemType == BSONType::Object &&
                             subDiffTypeMarker == Marker::kObjDiffMarker) {
                            BSONObjBuilder sub(builder->subobjStart());
                            OplogDiffReader subReader(subDiffBytes);
                            applyDiffToObject((*preImageIt).embeddedObject(), &sub, &subReader);
                        } else if (arrayElemType == BSONType::Array &&
                                   subDiffTypeMarker == Marker::kArrayDiffMarker) {
                            BSONArrayBuilder sub(builder->subarrayStart());
                            OplogDiffReader subReader(subDiffBytes);
                            applyDiffToArray((*preImageIt).embeddedObject(), &sub, &subReader);
                        } else  {
                            // The type does not match what we expected. Leave the field alone.
                            builder->append(*preImageIt);
                        }
                        
                        preImageIndex++;
                        preImageIt++;
                    } else {
                        // If we're in this case then the pre-image's array was shorter than we
                        // expected. This means some future oplog entry will either re-write the
                        // value of this array index (or some parent) so we append a null and move
                        // on.
                        builder->appendNull();
                    }
                    // Skip remaining bytes (except for size bytes and type byte which we consumed
                    // already).
                    postImageIndex++;
                    reader->skip(subDiffSize - 5);
                } else if (changeType == Marker::kInsertMarker ||
                           changeType == Marker::kUpdateMarker) {
                    builder->append(reader->nextBsonElt());

                    postImageIndex++;
                    if (preImageIt.more()) {
                        preImageIt++;
                        preImageIndex++;
                    }
                } else {
                    MONGO_UNREACHABLE;
                }
            } else if (marker == Marker::kResizeMarker) {
                resizeVal = reader->nextUnsigned();
                // resize must come last.
                invariant(reader->nextByte(false) == 0);
            }

            marker = reader->nextByte();
        }

        // Everything else in the array gets kept, up until the resize value.
        while (preImageIt.more() && (!resizeVal || postImageIndex < *resizeVal)) {
            builder->append(preImageIt.next());
            postImageIndex++;
        }

        // If the resize value indicates that the array should be longer, we pad the array with
        // nulls.
        while (resizeVal && (postImageIndex < *resizeVal)) {
            builder->appendNull();
            postImageIndex++;
        }
        invariant(!resizeVal || *resizeVal == postImageIndex);
    }
    
    void applyDiffToObject(const BSONObj& preImage, BSONObjBuilder* builder,
                           doc_diff::OplogDiffReader* reader) const {
        using namespace doc_diff;

        // Skip over size bytes.
        reader->skip(4);
        invariant(reader->nextByte() == Marker::kObjDiffMarker);
        
        // Build some hash tables. A real implementation may want to combine these into one. All
        // memory for the strings and so on is owned inside the diff and doesn't need to be copied.
        Tables tables = buildTables(reader);
        StringDataSet fieldsInOutput;

        for (auto && elt : preImage) {
            auto fn = elt.fieldNameStringData();
            if (tables.fieldsToSkip.count(fn)) {
                std::cout << "ian: field " << fn << " is skipped\n";
                // Do nothing. We're skipping this field.
            } else if (auto it = tables.fieldsToSet.find(fn); it != tables.fieldsToSet.end()) {
                std::cout << "ian: field " << fn << " is set\n";
                builder->appendAs(it->second, fn);
                fieldsInOutput.insert(fn);
            } else if (auto it = tables.fieldsWithSubDiffs.find(fn);
                       it != tables.fieldsWithSubDiffs.end()) {
                const char* subDiffBytes = it->second;
                const unsigned char subDiffType = static_cast<unsigned char>(subDiffBytes[4]);
                invariant(subDiffType == Marker::kObjDiffMarker || subDiffType == Marker::kArrayDiffMarker);
                
                if (elt.type() == BSONType::Object && subDiffType == Marker::kObjDiffMarker) {
                    std::cout << "ian: subdiff for field " << fn << " is obj\n";
                    BSONObjBuilder sub(builder->subobjStart(fn));
                    OplogDiffReader subReader(subDiffBytes);
                    applyDiffToObject(elt.embeddedObject(), &sub, &subReader);
                } else if (elt.type() == BSONType::Array && subDiffType == Marker::kArrayDiffMarker) {
                    std::cout << "ian: subdiff for field " << fn << " is arr\n";
                    BSONArrayBuilder sub(builder->subarrayStart(fn));
                    OplogDiffReader subReader(subDiffBytes);
                    applyDiffToArray(elt.embeddedObject(), &sub, &subReader);
                } else {
                    std::cout << "ian: subdiff for field " << fn << " doesnt match\n";
                    // Otherwise there's a type mismatch. The diff was expecting one type but the
                    // pre image contains a value of a different type. This means we are in a state
                    // where we're "re-applying" an oplog entry.

                    // There must be some future operation which changed the type of this field
                    // from object/array to something else. We leave this field alone, and expect
                    // that a future operation will overwrite the value correctly.
                    builder->append(elt);
                }

                fieldsInOutput.insert(fn);
            } else {
                std::cout << "ian: field " << fn << " is kept\n";
                // The field isn't mentioned in the diff, so we keep it.
                builder->append(elt);
                fieldsInOutput.insert(fn);
            }
        }

        for (auto &[fieldName, val] : tables.fieldsToInsert) {
            if (!fieldsInOutput.count(fieldName)) {
                builder->appendAs(val, fieldName);
            }
        }
    }
    
    BSONObj applyDiff(const BSONObj& preImage) const {
        doc_diff::OplogDiffReader reader(_delta);
        BSONObjBuilder builder;
        applyDiffToObject(preImage, &builder, &reader);

        auto ret = builder.obj();
        std::cout << "ian: sec post image is " << ret << std::endl;
        std::cout << "ian: sec post image n fields is " << ret.nFields() << std::endl;
        std::cout << "ian: sec post image size is " << ret.objsize() << std::endl;

        std::cout << "sec raw data" << std::endl;
        for (int i = 0; i < ret.objsize(); ++i) {
            const char next = static_cast<char>(ret.objdata()[i]);
            std::cout << std::setfill('0') << std::setw(2) << std::hex << static_cast<int>(next) << std::dec << " ";
        }
        std::cout << "end raw\n";
        
        return ret;
    }
    
    const char* _delta;
    size_t _len;
};

}  // namespace mongo
