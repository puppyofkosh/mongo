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

#include "mongo/base/status.h"

#include "mongo/db/update/document_diff_serialization.h"
#include "mongo/db/update/update_oplog_entry_version.h"
#include "mongo/stdx/variant.h"
#include "mongo/util/visit_helper.h"

namespace mongo {
/**
 * A simpler version of LogBuilder which can only be used for full replacements and $v: 2 delta
 * style oplog entries. Unlike with LogBuilder, there's no interface for gradually constructing an
 * update using the 3.6 modifier style language.
 */
class V2UpdateLogEntry {
public:
    /**
     * A call to this indicates that we will log a delta style entry. With the diff provided.
     */
    void setDelta(const BSONObj& diff) {
        invariant(stdx::holds_alternative<NoValue>(_update));
        invariant(!_version);
        _update = Delta{diff};
        _version = UpdateOplogEntryVersion::kDeltaV2;
    }

    /**
     * A call to this indicates that we will log a replacement style update.
     */
    void setReplacement(const BSONObj& replacementBson) {
        invariant(stdx::holds_alternative<NoValue>(_update));
        invariant(!_version);
        _update = Replacement{replacementBson};
        // We do not set '_version' here, to indicate this is a replacement.
    }

    /**
     * Serializes to bson.
     */
    BSONObj toBson() const;
private:
    // These structs are so we can store a variant<Delta,Replacement> without trying to store
    // two identical types in a variant (which is possible, but leads to madness).
    struct Delta {
        BSONObj bson;
    };

    struct Replacement {
        BSONObj bson;
    };
    using NoValue = stdx::monostate;

    // Indicates which type of oplog entry is recorded. No value indicates replacement.
    // TODO: REMOVE
    boost::optional<UpdateOplogEntryVersion> _version;
    stdx::variant<NoValue, Delta, Replacement> _update;
};
}  // namespace mongo
