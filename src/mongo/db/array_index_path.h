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

#include "mongo/stdx/variant.h"

namespace mongo {
/**
 * Represents a path which may include array indexes. For example on a document
 * {a: [{b: "foo"}, ...]}
 *
 * "foo" is at the path ["a", 0, "b"].
 *
 * Maybe some day this will be a "real" class but for now we're going to dump it in this namespace
 * and worry about where to put it later.
 */

class ArrayIndexPath {
public:
    using Component = stdx::variant<size_t, std::string>;

    // Calling this 'unsafe' because it's not very thorough with checking for bad input.
    static ArrayIndexPath parseUnsafe(std::string input) {
        ArrayIndexPath out;

        size_t dotPos;
        size_t startPos = 0;
        while (std::string::npos != (dotPos = input.find('.', startPos))) {
            StringData part(&input[startPos], dotPos - startPos);
            out.append(parseComponent(part));

            startPos = dotPos + 1;
        }

        Component c = parseComponent(StringData(&input[startPos], input.size() - startPos));
        out.append(std::move(c));
        return out;
    }

    ArrayIndexPath() = default;
    ArrayIndexPath(std::vector<Component> c) : _components(c) {}

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
    static Component parseComponent(StringData sd) {
        if (sd.startsWith("$["_sd)) {
            uassert(ErrorCodes::BadValue, "bad string index", sd.endsWith("]"_sd));

            return str::toUnsigned(sd.substr(2, sd.size() - 3).toString());
        }
        return sd.toString();
    }

    std::vector<Component> _components;
};

/**
 * Unowned "view" class for ArrayIndexPath. Useful for recursion.
 */
struct ArrayIndexPathView {
    using Component = ArrayIndexPath::Component;

    const Component* const components;
    const size_t size;

    ArrayIndexPathView(const ArrayIndexPath& p)
        : components(p.components().data()), size(p.components().size()) {}

    ArrayIndexPathView tail() const {
        invariant(size > 1);
        return {components + 1, size - 1};
    }

private:
    ArrayIndexPathView(const Component* c, size_t sz) : components(c), size(sz) {}
};

}  // namespace mongo
