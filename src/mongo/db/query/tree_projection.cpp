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
#include "mongo/db/query/tree_projection.h"

namespace mongo {

bool TreeProjection::parseObjectAsExpression(StringData fieldName,
                                             const BSONObj& objSpec,
                                             TreeProjectionNode* parent) {
    if (objSpec.firstElementFieldName()[0] == '$') {
        // This is an expression like {$add: [...]}. We have already verified that it has only one
        // field.
        invariant(objSpec.nFields() == 1);

        // Treat it as a generic agg expression.
        parent->addProjectionForPath(FieldPath(fieldName), ProjectionValue(objSpec));
        return true;
    }
    return false;
}

void TreeProjection::parseSubObject(const BSONObj& subObj, TreeProjectionNode* node) {
    for (auto elem : subObj) {
        // It shouldn't be an expression.
        invariant(elem.fieldName()[0] != '$');

        // Dotted paths in a sub-object have already been disallowed in
        // ParsedAggregationProjection's parsing.
        invariant(elem.fieldNameStringData().find('.') == std::string::npos);

        switch (elem.type()) {
            case BSONType::Bool:
            case BSONType::NumberInt:
            case BSONType::NumberLong:
            case BSONType::NumberDouble:
            case BSONType::NumberDecimal: {
                node->addProjectionForPath(FieldPath(elem.fieldName(), false),
                                           ProjectionValue(elem.trueValue()));
                break;
            }
            case BSONType::Object: {
                // This is either an expression, or a nested specification.
                auto fieldName = elem.fieldNameStringData().toString();
                if (parseObjectAsExpression(fieldName, elem.Obj(), node)) {
                    break;
                }
                auto* child = node->addOrGetChild(fieldName);
                parseSubObject(elem.Obj(), child);
                break;
            }
            default: {
                // This is a literal value.
                node->addProjectionForPath(FieldPath(elem.fieldName()), ProjectionValue(elem));
            }
        }
    }
}

void TreeProjection::buildLogicalProjectionTree(const BSONObj& spec) {
    for (auto elem : spec) {
        auto fieldName = elem.fieldNameStringData();

        switch (elem.type()) {
            case BSONType::Bool:
            case BSONType::NumberInt:
            case BSONType::NumberLong:
            case BSONType::NumberDouble:
            case BSONType::NumberDecimal: {
                // This is an inclusion specification.
                _root->addProjectionForPath(FieldPath(elem.fieldName()),
                                            ProjectionValue(elem.trueValue()));
                break;
            }
            case BSONType::Object: {
                // This is either an expression, or a nested specification.
                if (parseObjectAsExpression(fieldName, elem.Obj(), _root.get())) {
                    // It was an expression.
                    break;
                }

                // The field name might be a dotted path. If so, we need to keep adding children
                // to our tree until we create a child that represents that path.
                auto remainingPath = FieldPath(elem.fieldName());
                auto* child = _root.get();
                while (remainingPath.getPathLength() > 1) {
                    child = child->addOrGetChild(remainingPath.getFieldName(0).toString());
                    remainingPath = remainingPath.tail();
                }
                // It is illegal to construct an empty FieldPath, so the above loop ends one
                // iteration too soon. Add the last path here.
                child = child->addOrGetChild(remainingPath.fullPath());

                parseSubObject(elem.Obj(), child);
                break;
            }
            default: {
                // This is a literal value.
                _root->addProjectionForPath(FieldPath(elem.fieldName(), true),
                                            ProjectionValue(elem));
            }
        }
    }
}
}  // namespace mongo
