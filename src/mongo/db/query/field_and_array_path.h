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
#include "mongo/db/field_ref.h"
#include "mongo/db/pipeline/field_path.h"
#include "mongo/stdx/variant.h"
#include "mongo/util/visit_helper.h"

namespace mongo {

/**
 * A "FieldAndArrayPath" is a type of path where field names and array indices are described
 * unambiguously. It does not represent any specific MQL concept, though it could be extended to do
 * so in the future. It's also not associated with any one particular serialization format. A
 * rudimentary "debug" format is provided via operator<<, but it is not intended for any "real"
 * use.
 *
 * A FieldAndArrayPath consists of a list of components, where each component is either an unsigned
 * int (array index) or a string (field name).
 */

/**
 * View class which has most of the logic needed when processing paths. Does not support modifying
 * the underlying FieldAndArrayPath. May represent an empty path. This class is useful for recursive
 * walks of an FieldAndArrayPath.
 *
 * It is expected that most operations which read from a FieldAndArrayPath will do so through a
 * FieldAndArrayPathView. Modifying the original FieldAndArrayPath while there is a view in use may
 * be dangerous (depending on how the FieldAndArrayPath is modified).
 */
class FieldAndArrayPathView {
public:
    using Component = stdx::variant<std::size_t, std::string>;

    /**
     * Returns first element (useful for recursion).
     */
    const Component& first() const {
        invariant(_size > 0);
        return _components[0];
    }

    /**
     * Returns all but first element (useful for recursion).
     */
    FieldAndArrayPathView rest() const {
        invariant(_size > 0);
        return FieldAndArrayPathView(_components + 1, _size - 1);
    }

    const Component& last() const {
        invariant(_size > 0);
        return _components[_size - 1];
    }

    bool empty() const {
        return _size == 0;
    }

    const Component& operator[](unsigned int i) const {
        invariant(i < _size);
        return _components[i];
    }

    size_t size() const {
        return _size;
    }

    const Component* data() const {
        return _components;
    }

    //
    // The following methods allow range-based for loops.
    //
    const Component* begin() const {
        return _components;
    }

    const Component* end() const {
        return _components + _size;
    }

    std::string serialize() const;

    FieldRef toAmbiguousFieldRef() {
        return FieldRef(serialize());
    }

private:
    FieldAndArrayPathView(const Component* components, size_t size)
        : _components(components), _size(size) {}

    const Component* _components;
    size_t _size;

    friend class FieldAndArrayPath;
};

inline bool operator==(const FieldAndArrayPathView& a, const FieldAndArrayPathView& b) {
    if (a.size() != b.size()) {
        return false;
    }

    for (size_t i = 0; i < a.size(); ++i) {
        if (a[i] != b[i]) {
            return false;
        }
    }
    return true;
}

inline bool operator!=(const FieldAndArrayPathView& a, const FieldAndArrayPathView& b) {
    return !(a == b);
}

//
// We intentionally do not implement operator< as paths have no inherent ordering.
//

/**
 * Useful for debug printing and unit tests. Not to be used for serialization.
 */
inline std::ostream& operator<<(std::ostream& stream,
                                const FieldAndArrayPathView::Component& component) {
    stdx::visit(
        visit_helper::Overloaded{[&stream](size_t index) { stream << index; },
                                 [&stream](const std::string& fieldName) { stream << fieldName; }},
        component);
    return stream;
}

/**
 * Useful for debug printing and unit tests. Not to be used for serialization.
 */
inline std::ostream& operator<<(std::ostream& stream, const FieldAndArrayPathView& view) {
    stream << "FieldAndArrayPath: ";
    for (size_t i = 0; i < view.size(); ++i) {
        stdx::visit(
            visit_helper::Overloaded{
                [&stream](size_t index) { stream << "$[" << index << "]"; },
                [&stream](const std::string& fieldName) { stream << fieldName; }},
            view[i]);

        if (i + 1 != view.size()) {
            stream << ".";
        }
    }
    return stream;
}

inline std::string FieldAndArrayPathView::serialize() const {
    StringBuilder sb;
    for (size_t i = 0; i < size(); ++i) {
        stdx::visit(
            visit_helper::Overloaded{[&sb](size_t index) { sb << std::to_string(index); },
                                     [&sb](const std::string& fieldName) { sb << fieldName; }},
            (*this)[i]);

        if (i + 1 != size()) {
            sb << ".";
        }
    }
    return sb.str();
}

/**
 * Class responsible for storing FieldAndArrayPath components.
 */
class FieldAndArrayPath {
public:
    using Component = FieldAndArrayPathView::Component;
    static std::string serializeComponent(const Component& comp) {
        return stdx::visit(
            visit_helper::Overloaded{[](size_t index) { return std::to_string(index); },
                                     [](const std::string& fieldName) { return fieldName; }},
            comp);
    }

    /**
     * Create a FieldAndArrayPath from a FieldPath. All numeric field names in the FieldPath are
     * treated strictly as field names, and never as array indices. For example the path "a.0" will
     * become the FieldAndArrayPath ["a", "0"] ("0" being a field name).
     */
    static FieldAndArrayPath fromFieldPath(const FieldPath& fp) {
        std::vector<Component> components;
        components.reserve(fp.getPathLength());
        for (size_t i = 0; i < fp.getPathLength(); ++i) {
            components.push_back(Component{fp.getFieldName(i).toString()});
        }
        return FieldAndArrayPath(components);
    }

    static FieldAndArrayPath fromAmbiguousFieldRef(const FieldRef& fr) {
        std::vector<Component> components;
        components.reserve(fr.numParts());
        for (size_t i = 0; i < fr.numParts(); ++i) {
            components.push_back(Component{fr.getPart(i).toString()});
        }
        return FieldAndArrayPath(components);
    }

    FieldAndArrayPath(std::vector<Component> components) : _components(std::move(components)) {}
    FieldAndArrayPath(const FieldAndArrayPathView& view)
        : _components(view.data(), view.data() + view.size()) {}

    FieldAndArrayPathView view() const& {
        return FieldAndArrayPathView(_components.data(), _components.size());
    }

    // Prohibit making a view of a temporary since it will often lead to bad things.
    FieldAndArrayPath view() const&& = delete;

    // Allow creating a view on a temporary if the caller explicitly asks for it.
    FieldAndArrayPathView viewFromTemp() const&& {
        return view();
    }

    // Operator for implicitly converting to view.
    operator FieldAndArrayPathView() const& {
        return view();
    }
    operator FieldAndArrayPathView() const&& = delete;

    size_t size() const {
        return _components.size();
    }

    bool empty() const {
        return _components.empty();
    }

    Component& operator[](unsigned int ind) {
        return _components[ind];
    }

    const Component& operator[](unsigned int ind) const {
        return _components[ind];
    }

    // Below methods allow for range-based for loop. They simply return iterators from the
    // underlying std::vector.
    auto begin() const {
        return _components.begin();
    }

    auto end() const {
        return _components.end();
    }

    auto begin() {
        return _components.begin();
    }

    auto end() {
        return _components.end();
    }

    // The below methods change the size of the FieldAndArrayPath. They must not be called while any
    // views of the FieldAndArrayPath exist.

    /**
     * Adds a component to the end of the path.
     */
    void append(Component component) {
        _components.push_back(std::move(component));
    }

    /**
     * Removes the last component of the path.
     */
    void removeLast() {
        invariant(!_components.empty());
        _components.pop_back();
    }

    void clear() {
        _components.clear();
    }

private:
    std::vector<Component> _components;
};

inline FieldAndArrayPath operator+(const FieldAndArrayPathView& a, const FieldAndArrayPathView& b) {
    std::vector<FieldAndArrayPath::Component> components;
    components.reserve(a.size() + b.size());
    components.insert(components.end(), a.data(), a.data() + a.size());
    components.insert(components.end(), b.data(), b.data() + b.size());
    return FieldAndArrayPath(std::move(components));
}

// Overloads for regular FieldAndArrayPath, so that + can be composed.
inline FieldAndArrayPath operator+(const FieldAndArrayPath& a, const FieldAndArrayPathView& b) {
    return a.view() + b;
}

inline FieldAndArrayPath operator+(const FieldAndArrayPathView& a, const FieldAndArrayPath& b) {
    return a + b.view();
}

inline FieldAndArrayPath operator+(const FieldAndArrayPath& a, const FieldAndArrayPath& b) {
    return a.view() + b.view();
}

inline FieldAndArrayPath operator+(const FieldAndArrayPathView& a,
                                   const FieldAndArrayPath::Component& b) {
    FieldAndArrayPath res(a);
    res.append(b);
    return res;
}
}  // namespace mongo
