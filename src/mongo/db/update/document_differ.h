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

#pragma once

#include <string>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/array_index_path.h"
#include "mongo/stdx/variant.h"

namespace mongo {

namespace doc_diff {
// The BSONObjs from which this is constructed must outlive the Diff itself.
class DocumentDiff {
public:
    static DocumentDiff computeDiff(const BSONObj& pre, const BSONObj& post);

    void merge(DocumentDiff&& other);

    const std::vector<ArrayIndexPath>& toDelete() const {
        return _toDelete;
    }

    const std::vector<std::pair<ArrayIndexPath, BSONElement>>& toUpsert() const {
        return _toUpsert;
    }

    const std::vector<std::pair<ArrayIndexPath, BSONElement>>& toInsert() const {
        return _toInsert;
    }

    const std::vector<std::pair<ArrayIndexPath, size_t>>& toResize() const {
        return _toResize;
    }

    std::string toStringDebug() const;

    size_t computeApproxSize() const {
        size_t sum = 0;
        for (auto&& f : _toDelete) {
            sum += f.approximateSizeInBytes();
        }

        for (auto&& [fr, elt] : _toUpsert) {
            sum += fr.approximateSizeInBytes();
            sum += elt.size();
        }

        for (auto&& [fr, elt] : _toInsert) {
            sum += fr.approximateSizeInBytes();
            sum += elt.size();
        }
        return sum;
    }

private:
    static DocumentDiff computeDiffHelper(const BSONObj& pre,
                                          const BSONObj& post,
                                          const ArrayIndexPath& prefix);

    static DocumentDiff diffArrays(const BSONObj& pre,
                                   const BSONObj& post,
                                   const ArrayIndexPath& prefix);

    std::vector<ArrayIndexPath> _toDelete;
    std::vector<std::pair<ArrayIndexPath, BSONElement>> _toUpsert;
    std::vector<std::pair<ArrayIndexPath, BSONElement>> _toInsert;

    // Path to arrays that need to be resized along with new size.
    // TODO: Should I call this "truncations"??
    std::vector<std::pair<ArrayIndexPath, size_t>> _toResize;
};

};  // namespace doc_diff

};  // namespace mongo
