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

#include "mongo/platform/basic.h"

#include "mongo/db/query/logical_projection.h"

#include "mongo/bson/simple_bsonobj_comparator.h"
#include "mongo/db/matcher/expression_algo.h"
#include "mongo/db/pipeline/transformer_interface.h"
#include "mongo/db/query/query_request.h"

namespace mongo {

namespace {
std::string makeBannedComputedFieldsErrorMessage(BSONObj projSpec) {
    return str::stream() << "Bad projection specification, cannot use computed fields when parsing "
                            "a spec in kBanComputedFields mode: "
                         << projSpec.toString();
}

bool isPrefixOf(StringData first, StringData second) {
    if (first.size() >= second.size()) {
        return false;
    }

    return second.startsWith(first) && second[first.size()] == '.';
}
}

void LogicalProjection::parse() {
    size_t nFields = 0;
    for (auto&& elem : _rawObj) {
        parseElement(elem, FieldPath(elem.fieldName(), true));
        nFields++;
    }

    // Check for the case where we only exclude '_id'.
    if (nFields == 1) {
        BSONElement elem = _rawObj.firstElement();
        if (elem.fieldNameStringData() == "_id" && (elem.isBoolean() || elem.isNumber()) &&
            !elem.trueValue()) {
            _parsedType = ProjectType::kExclusion;
        }
    }

    // Default to inclusion if nothing (except maybe '_id') is explicitly included or excluded.
    if (!_parsedType) {
        _parsedType = ProjectType::kInclusion;
    }

    // If we're keeping _id insert it to the front of the list of required fields.
    // TODO: Maybe don't insert to the front of a vector when doing this for real.
    if (_hasId) {
        _requiredFields.insert(_requiredFields.begin(), "_id");
    }
}

void LogicalProjection::parseElement(const BSONElement& elem, const FieldPath& pathToElem) {
    if (pathToElem.getPathLength() > 1) {
        _hasDottedFieldPath = true;
    }

    if (elem.type() == BSONType::Object) {
        return parseNestedObject(elem.Obj(), pathToElem);
    }

    // If this element is not a boolean or numeric value, then it is a literal value. These are
    // illegal if we are in kBanComputedFields parse mode.
    uassert(ErrorCodes::FailedToParse,
            makeBannedComputedFieldsErrorMessage(_rawObj),
            elem.isBoolean() || elem.isNumber() ||
                _policies.computedFieldsPolicy != ComputedFieldsPolicy::kBanComputedFields);

    if (pathToElem.fullPath() == "_id") {
        // If the _id field is a computed value, then this must be an inclusion projection. If
        // it is numeric or boolean, then this does not determine the projection type, due to
        // the fact that inclusions may explicitly exclude _id and exclusions may include _id.
        if (!elem.isBoolean() && !elem.isNumber()) {
            uassert(ErrorCodes::FailedToParse,
                    str::stream() << "Bad projection specification, '_id' may not be a "
                                     "computed field in an exclusion projection: "
                                  << _rawObj.toString(),
                    !_parsedType || _parsedType == ProjectType::kInclusion);
            _parsedType = ProjectType::kInclusion;
        }

        _hasId = elem.trueValue();
    } else if ((elem.isBoolean() || elem.isNumber()) && !elem.trueValue()) {
        // If this is an excluded field other than '_id', ensure that the projection type has
        // not already been set to kInclusionProjection.
        uassert(40178,
                str::stream() << "Bad projection specification, cannot exclude fields "
                                 "other than '_id' in an inclusion projection: "
                              << _rawObj.toString(),
                !_parsedType || (*_parsedType == ProjectType::kExclusion));
        _parsedType = ProjectType::kExclusion;

        _excludedFields.push_back(pathToElem.fullPath());
    } else {
        // A boolean true, a truthy numeric value, or any expression can only be used with an
        // inclusion projection. Note that literal values like "string" or null are also treated
        // as expressions.
        uassert(40179,
                str::stream() << "Bad projection specification, cannot include fields or "
                                 "add computed fields during an exclusion projection: "
                              << _rawObj.toString(),
                !_parsedType || (*_parsedType == ProjectType::kInclusion));
        _parsedType = ProjectType::kInclusion;

        // This was a "leaf" of an inclusion projection, so add it to the list of required fields
        // (unless it's _id, which will be taken care of separately).
        _requiredFields.push_back(pathToElem.fullPath());
    }
}

void LogicalProjection::parseMetaObject(StringData fieldName, StringData requestedMeta) {
    if (requestedMeta == QueryRequest::metaTextScore) {
        _wantTextScore = true;
    } else if (requestedMeta == QueryRequest::metaIndexKey) {
        _hasIndexKeyProjection = true;
    } else if (requestedMeta == QueryRequest::metaGeoNearDistance) {
        _wantGeoNearDistance = true;
    } else if (requestedMeta == QueryRequest::metaGeoNearPoint) {
        _wantGeoNearPoint = true;
    } else if (requestedMeta == QueryRequest::metaSortKey) {
        _wantSortKey = true;
        _sortKeyMetaFields.push_back(fieldName.toString());
    } else {
        // We don't recognize it, so the query layer doesn't care.
    }
}

void LogicalProjection::parseNestedObject(const BSONObj& thisLevelSpec, const FieldPath& prefix) {

    for (auto&& elem : thisLevelSpec) {
        auto fieldName = elem.fieldNameStringData();
        if (fieldName[0] == '$') {
            // This object is an expression specification like {$add: [...]}. It will be parsed
            // into an Expression later, but for now, just track that the prefix has been
            // specified, validate that computed projections are legal, and skip it.
            uassert(ErrorCodes::FailedToParse,
                    makeBannedComputedFieldsErrorMessage(_rawObj),
                    _policies.computedFieldsPolicy != ComputedFieldsPolicy::kBanComputedFields);
            uassert(40182,
                    str::stream() << "Bad projection specification, cannot include fields or "
                                     "add computed fields during an exclusion projection: "
                                  << _rawObj.toString(),
                    !_parsedType || _parsedType == ProjectType::kInclusion);
            _parsedType = ProjectType::kInclusion;

            if (fieldName == "$_internalFindPositional") {
                _requiresMatchDetails = true;
            } else if (fieldName == "$meta") {
                uassert(ErrorCodes::BadValue,  // TODO: badValue
                        "field for $meta should be string",
                        BSONType::String == elem.type());

                uassert(ErrorCodes::BadValue,  // TODO: badValue
                        "field for $meta cannot be nested",
                        prefix.getPathLength() == 1);
                parseMetaObject(prefix.fullPath(), elem.valuestr());
            }

            // TODO: The $meta sortKey may be covered.
            _hasExpression = true;
            _expressionFields.push_back(prefix.fullPath());
            continue;
        }

        _hasDottedFieldPath = true;
        parseElement(elem, FieldPath::getFullyQualifiedPath(prefix.fullPath(), fieldName));
    }
}

bool LogicalProjection::isFieldRetainedExactly(StringData path) const {
    // If a path, or a parent or child of the path, is contained in _metaFields or in _arrayFields,
    // our output likely does not preserve that field.
    for (auto&& expressionField : _expressionFields) {
        if (path == expressionField || isPrefixOf(path, expressionField) ||
            isPrefixOf(expressionField, path)) {
            return false;
        }
    }

    if (path == "_id" || isPrefixOf("_id", path)) {
        return _hasId;
    }

    invariant(_parsedType);
    if (*_parsedType == ProjectType::kExclusion) {
        // If we are an exclusion projection, and the path, or a parent or child of the path, is
        // contained in _excludedFields, our output likely does not preserve that field.
        for (auto&& excluded : _excludedFields) {
            if (path == excluded || isPrefixOf(excluded, path) || isPrefixOf(path, excluded)) {
                return false;
            }
        }
    } else {
        // If we are an inclusion projection, we may include parents of this path, but we cannot
        // include children.
        bool fieldIsIncluded = false;
        for (auto&& included : _requiredFields) {
            if (path == included || isPrefixOf(included, path)) {
                fieldIsIncluded = true;
            } else if (isPrefixOf(path, included)) {
                fieldIsIncluded = false;
            }
        }

        if (!fieldIsIncluded) {
            return false;
        }
    }

    return true;
}

}  // namespace mongo
