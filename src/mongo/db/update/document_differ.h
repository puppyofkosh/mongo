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
#include "mongo/db/field_ref.h"
#include "mongo/stdx/variant.h"

namespace mongo {

namespace doc_diff {

class ArrayIndexPath {
public:
    using Component = stdx::variant<size_t, std::string>;

    void append(Component c) {
        _components.push_back(std::move(c));
    }

    const std::vector<Component>& components() const {
        return _components;
    }

    std::string debugString() const {
        return toString();
    }

    std::string toString() const {
        // TODO: This syntax is fake.
        std::string s;
        for (size_t i = 0; i < _components.size(); ++i) {
            const auto& c = _components[i];
            if (stdx::holds_alternative<size_t>(c)) {
                s += "$[" + std::to_string(stdx::get<size_t>(c)) + "]";
            } else {
                s += stdx::get<std::string>(c);
            }

            if (i + 1 != _components.size()) {
                s += ".";
            }
        }
        return s;
    }

    size_t approximateSizeInBytes() const {
        // TODO: Someday we need to do this well, but not today.
        return debugString().size();
    }

    bool operator==(const ArrayIndexPath& o) const {
        if (_components.size() != o._components.size()) {
            return false;
        }
        for (size_t i = 0; i < _components.size(); ++i) {
            if (_components[i] != o._components[i]) {
                return false;
            }
        }
        return true;
    }
private:

    std::vector<Component> _components;
};
    
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
};

};  // namespace doc_diff

};  // namespace mongo
