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

#include "mongo/db/update/log_builder.h"
#include "mongo/util/str.h"

namespace mongo {

using mutablebson::Element;

namespace {
const char kSet[] = "$set";
const char kUnset[] = "$unset";
}  // namespace

constexpr StringData LogBuilder::kUpdateSemanticsFieldName;

inline Status LogBuilder::addToSection(Element newElt, Element* section, const char* sectionName) {
    // If we don't already have this section, try to create it now.
    if (!section->ok()) {
        // If we already have object replacement data, we can't also have section entries.
        if (hasObjectReplacement())
            return Status(ErrorCodes::IllegalOperation,
                          "LogBuilder: Invalid attempt to add a $set/$unset entry"
                          "to a log with an existing object replacement");

        if (_pipelineAccumulator.ok())
            return Status(ErrorCodes::IllegalOperation,
                          "LogBuilder: Invalid attempt to add a $set/$unset entry"
                          "to a log with an existing pipeline");

        mutablebson::Document& doc = _logRoot.getDocument();

        // We should not already have an element with the section name under the root.
        dassert(_logRoot[sectionName] == doc.end());

        // Construct a new object element to represent this section in the log.
        const Element newElement = doc.makeElementObject(sectionName);
        if (!newElement.ok())
            return Status(ErrorCodes::InternalError,
                          "LogBuilder: failed to construct Object Element for $set/$unset");

        // Enqueue the new section under the root, and record it as our out parameter.
        Status result = _logRoot.pushBack(newElement);
        if (!result.isOK())
            return result;
        *section = newElement;

        // Invalidate attempts to add an object replacement, now that we have a named
        // section under the root.
        _objectReplacementAccumulator = doc.end();
        _pipelineAccumulator = doc.end();
    }

    // Whatever transpired, we should now have an ok accumulator for the section, and not
    // have a replacement accumulator.
    dassert(section->ok());
    dassert(!_objectReplacementAccumulator.ok());

    // Enqueue the provided element to the section and propagate the result.
    return section->pushBack(newElt);
}

Status LogBuilder::addToSets(Element elt) {
    return addToSection(elt, &_setAccumulator, kSet);
}

Status LogBuilder::addToSetsWithNewFieldName(StringData name, const mutablebson::Element val) {
    mutablebson::Element elemToSet = _logRoot.getDocument().makeElementWithNewFieldName(name, val);
    if (!elemToSet.ok())
        return Status(ErrorCodes::InternalError,
                      str::stream()
                          << "Could not create new '" << name << "' element from existing element '"
                          << val.getFieldName() << "' of type " << typeName(val.getType()));

    return addToSets(elemToSet);
}

Status LogBuilder::addToSetsWithNewFieldName(StringData name, const BSONElement& val) {
    mutablebson::Element elemToSet = _logRoot.getDocument().makeElementWithNewFieldName(name, val);
    if (!elemToSet.ok())
        return Status(ErrorCodes::InternalError,
                      str::stream()
                          << "Could not create new '" << name << "' element from existing element '"
                          << val.fieldName() << "' of type " << typeName(val.type()));

    return addToSets(elemToSet);
}

Status LogBuilder::addToSets(StringData name, const SafeNum& val) {
    mutablebson::Element elemToSet = _logRoot.getDocument().makeElementSafeNum(name, val);
    if (!elemToSet.ok())
        return Status(ErrorCodes::InternalError,
                      str::stream() << "Could not create new '" << name << "' SafeNum from "
                                    << val.debugString());

    return addToSets(elemToSet);
}

Status LogBuilder::addToUnsets(StringData path) {
    mutablebson::Element logElement = _logRoot.getDocument().makeElementBool(path, true);
    if (!logElement.ok())
        return Status(ErrorCodes::InternalError,
                      str::stream() << "Cannot create $unset oplog entry for path" << path);

    return addToSection(logElement, &_unsetAccumulator, kUnset);
}

Status LogBuilder::setUpdateSemantics(UpdateSemantics updateSemantics) {
    if (hasObjectReplacement()) {
        return Status(ErrorCodes::IllegalOperation,
                      "LogBuilder: Invalid attempt to add a $v entry to a log with an existing "
                      "object replacement");
    }

    if (_updateSemantics.ok()) {
        return Status(ErrorCodes::IllegalOperation, "LogBuilder: Invalid attempt to set $v twice.");
    }

    mutablebson::Document& doc = _logRoot.getDocument();
    _updateSemantics =
        doc.makeElementInt(kUpdateSemanticsFieldName, static_cast<int>(updateSemantics));

    dassert(_logRoot[kUpdateSemanticsFieldName] == doc.end());

    return _logRoot.pushFront(_updateSemantics);
}

Status LogBuilder::getReplacementObject(Element* outElt) {
    // If the replacement accumulator is not ok, we must have started a $set or $unset
    // already, so an object replacement is not permitted.
    if (!_objectReplacementAccumulator.ok()) {
        dassert(_setAccumulator.ok() || _unsetAccumulator.ok() || _pipelineAccumulator.ok());
        return Status(ErrorCodes::IllegalOperation,
                      "LogBuilder: Invalid attempt to obtain the object replacement slot "
                      "for a log containing $set or $unset entries");
    }

    if (hasObjectReplacement())
        return Status(ErrorCodes::IllegalOperation,
                      "LogBuilder: Invalid attempt to acquire the replacement object "
                      "in a log with existing object replacement data");

    if (_updateSemantics.ok()) {
        return Status(ErrorCodes::IllegalOperation,
                      "LogBuilder: Invalid attempt to acquire the replacement object in a log with "
                      "an update semantics value");
    }

    // OK to enqueue object replacement items.
    *outElt = _objectReplacementAccumulator;
    return Status::OK();
}

Status LogBuilder::getPipelineUpdate(Element* outElt) {
    // If the replacement accumulator is not ok, we must have started a $set or $unset
    // already, so an object replacement is not permitted.
    if (_setAccumulator.ok() || _unsetAccumulator.ok() || hasObjectReplacement()) {
        return Status(ErrorCodes::IllegalOperation,
                      "LogBuilder: Invalid attempt to obtain the pipeline slot "
                      "for a log containing object replacement, $set or $unset entries");
    }

    if (_pipelineAccumulator.ok()) {
        return Status(ErrorCodes::IllegalOperation,
                      "LogBuilder: Invalid attempt to acquire the pipeline slot "
                      "in a log with existing pipeline data");
    }

    if (_updateSemantics.ok()) {
        return Status(ErrorCodes::IllegalOperation,
                      "LogBuilder: Invalid attempt to acquire the pipeline slot in a log with "
                      "an update semantics value");
    }

    mutablebson::Document& doc = _logRoot.getDocument();

    invariant(!_logRoot[kPipelineFieldName].ok());
    const Element newElement = doc.makeElementArray(kPipelineFieldName);
    invariant(newElement.ok());
    invariant(_logRoot.pushBack(newElement));
    _pipelineAccumulator = newElement;
    *outElt = _pipelineAccumulator;

    // Ensure that attempts to do a replacement style update fail.
    _objectReplacementAccumulator = doc.end();

    return Status::OK();
}

inline bool LogBuilder::hasObjectReplacement() const {
    if (!_objectReplacementAccumulator.ok())
        return false;

    dassert(!_setAccumulator.ok());
    dassert(!_unsetAccumulator.ok());

    return _objectReplacementAccumulator.hasChildren();
}

}  // namespace mongo
