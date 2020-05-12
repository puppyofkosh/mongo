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
        const bool postImageHasId = !postImage.hasField("_id");

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

        // Order to add new fields to the end. This is the combination of 'updated' fields and 'insert' fields.
        // Updated fields which were already present in the document will need to be skipped.
        std::vector<std::pair<StringData, BSONElement>> fieldsToInsert;
    };

    void valueHelper(doc_diff::OplogDiffReader* reader, Tables* out, StringData fieldName) {
        using namespace doc_diff;

        const auto marker = reader->nextByte();

        if (marker == Marker::kSubDiffMarker) {
            // TODO
        } else if (marker == Marker::kUpdateMarker) {
            // auto elt = reader->nextBsonElt();
        } else if (marker == Marker::kInsertMarker) {
            // auto elt = reader->nextBsonElt();
        } else if (marker == Marker::kExcludeMarker) {
        } else {
            std::cout << "encountered value " << static_cast<int>(marker) << " for field " << fieldName << std::endl;
            MONGO_UNREACHABLE;
        }
    }

    Tables buildTables(doc_diff::OplogDiffReader* reader) const {
        using namespace doc_diff;

        Tables out;

        // TODO: Skip type byte as well.
        // Skip size byte.
        // reader->skip(4);
        // auto marker = reader->nextByte();
        // while (marker) {
        //     if (marker == Marker::kNameMarker) {
        //         // auto str = reader->nextString();
        //         // std::cout << "ian: name entry " << str << std::endl;
            
        //         // valueHelper(reader, builder, str);
        //     } else if (marker == Marker::kIndexMarker) {
        //         // const auto ind = reader->nextUnsigned();
        //         // std::cout << "ian: ind entry " << ind << std::endl;
            
        //         // std::string fieldName = std::to_string(ind);
        //         // valueHelper(reader, builder, fieldName);
        //     } else if (marker == Marker::kResizeMarker) {
        //         // const auto newSz = reader->nextUnsigned();
        //         // std::cout << "ian: resize entry " << newSz << std::endl;
            
        //         // builder->appendNumber("<resize>"_sd, static_cast<size_t>(newSz));
        //     } else {
        //         MONGO_UNREACHABLE;
        //     }
        //     marker = reader->nextByte();
        // }
        return out;
    }

    void applyDiffToArray(const BSONObj& preImage,
                          BSONArrayBuilder* builder,
                          doc_diff::OplogDiffReader* reader) const {

    }
    
    void applyDiffToObject(const BSONObj& preImage, BSONObjBuilder* builder,
                           doc_diff::OplogDiffReader* reader) const {
        // Build some hash tables. A real implementation may want to combine these into one. All
        // memory for the strings and so on is owned inside the diff and doesn't need to be copied.
        Tables tables = buildTables(reader);

        for (auto && elt : preImage) {
            auto fn = elt.fieldNameStringData();
            if (tables.fieldsToSkip.count(fn)) {
                continue;
            } else if (auto it = tables.fieldsToSet.find(fn); it != tables.fieldsToSet.end()) {
                builder->appendAs(it->second, fn);
            } else if (auto it = tables.fieldsWithSubDiffs.find(fn);
                       it != tables.fieldsWithSubDiffs.end()) {
                if (elt.type() == BSONType::Object || elt.type() == BSONType::Array) {
                    const char* subDiffBytes = it->second;
                    doc_diff::OplogDiffReader subReader(subDiffBytes);
                } else {
                    // Leave the value alone.
                    continue;
                }
            }
        }
    }
    
    
    BSONObj applyDiff(const BSONObj& preImage) const {
        doc_diff::OplogDiffReader reader(_delta);
        BSONObjBuilder builder;
        applyDiffToObject(preImage, &builder, &reader);
        //return builder.obj();
        return preImage;
    }
    
    const char* _delta;
    size_t _len;
};

}  // namespace mongo
