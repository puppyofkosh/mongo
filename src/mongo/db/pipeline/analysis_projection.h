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

#include "mongo/platform/basic.h"

#include <boost/intrusive_ptr.hpp>
#include <memory>

#include "mongo/bson/bsonelement.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/field_path.h"
#include "mongo/db/pipeline/projection_policies.h"
#include "mongo/db/query/logical_projection.h"

namespace mongo {

class BSONObj;
class Document;
class ExpressionContext;

namespace parsed_aggregation_projection {
class ParsedAggregationProjection;

/*
 * Parsing + analysis
 */
class AnalysisProjection {
public:
    static std::unique_ptr<AnalysisProjection> create(
        const boost::intrusive_ptr<ExpressionContext>& expCtx, LogicalProjection* lp);

    AnalysisProjection(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                       ProjectionPolicies policies)
        : _expCtx(expCtx), _policies(policies){};

    /**
     * Parse the user-specified BSON object 'spec'. By the time this is called, 'spec' has
     * already
     * been verified to not have any conflicting path specifications, and not to mix and match
     * inclusions and exclusions. 'variablesParseState' is used by any contained expressions to
     * track which variables are defined so that they can later be referenced at execution time.
     */
    virtual void parse(const BSONObj& spec) = 0;

    virtual std::unique_ptr<ParsedAggregationProjection> convertToExecutionTree() = 0;

    // Methods used for planning. Taken from LogicalTree.
    /**
     * Returns true if the projection requires match details from the query, and false
     * otherwise. This is only relevant for find() projection, because of the positional projection
     * operator.
     */
    virtual bool requiresMatchDetails() const = 0;

    /**
     * Is the full document required to compute this projection?
     */
    virtual bool requiresDocument() const = 0;

    virtual const std::vector<std::string>& sortKeyMetaFields() const = 0;

    bool needsSortKey() const {
        return !sortKeyMetaFields().empty();
    }

    /**
     * If requiresDocument() == false, what fields are required to compute
     * the projection?
     */
    virtual const std::vector<std::string>& getRequiredFields() const = 0;

    virtual bool wantTextScore() const = 0;

    /**
     * Does the projection want geoNear metadata?  If so any geoNear stage should include them.
     */
    virtual bool wantGeoNearDistance() const = 0;
    virtual bool wantGeoNearPoint() const = 0;
    virtual bool wantIndexKey() const = 0;
    virtual bool wantSortKey() const = 0;
    virtual bool isFieldRetainedExactly(StringData path) const = 0;
    virtual bool hasDottedFieldPath() const = 0;

protected:
    boost::intrusive_ptr<ExpressionContext> _expCtx;

    ProjectionPolicies _policies;
};

}  // namespace parsed_aggregation_projection

using AnalysisProjection = parsed_aggregation_projection::AnalysisProjection;
}  // namespace mongo
