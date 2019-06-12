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

#include <algorithm>
#include <boost/intrusive_ptr.hpp>
#include <map>
#include <pcre.h>
#include <string>
#include <utility>
#include <vector>

#include "mongo/base/init.h"
#include "mongo/db/pipeline/dependencies.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/expression_context.h"
#include "mongo/db/pipeline/expression_visitor.h"
#include "mongo/db/pipeline/field_path.h"
#include "mongo/db/pipeline/value.h"
#include "mongo/db/pipeline/variables.h"
#include "mongo/db/query/datetime/date_time_support.h"
#include "mongo/db/server_options.h"
#include "mongo/stdx/functional.h"
#include "mongo/util/intrusive_counter.h"
#include "mongo/util/str.h"

namespace mongo {

// TODO: Maybe put this in a header file, maybe ExpressionInternal?
class ExpressionInternalFindElemMatch final : public Expression {
public:
    /**
     * Creates an ExpressionInternalFindElemMatch.
     */
    static boost::intrusive_ptr<Expression> create(const boost::intrusive_ptr<ExpressionContext>&,
                                                   const std::string& fieldPath,
                                                   BSONObj originalProj,
                                                   BSONObj elemMatchObj,
                                                   std::unique_ptr<MatchExpression> matchExpr);

    Value evaluate(const Document& root) const final;
    boost::intrusive_ptr<Expression> optimize() final;
    Value serialize(bool explain) const final;

    void acceptVisitor(ExpressionVisitor* visitor) final {
        // TODO:
        MONGO_UNREACHABLE;
        // return visitor->visit(this);
    }

protected:
    void _doAddDependencies(DepsTracker* deps) const final {
        _fieldPathToMatchOn->addDependencies(deps);
    }

private:
    ExpressionInternalFindElemMatch(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                    boost::intrusive_ptr<Expression> fieldPathExpr,
                                    BSONObj originalProjection,
                                    BSONObj elemMatchObj,
                                    std::unique_ptr<MatchExpression> matchExpr)
        : Expression(expCtx, {std::move(fieldPathExpr)}),
          _fieldPathToMatchOn(_children[0]),
          _originalProjection(originalProjection),
          _elemMatchObj(elemMatchObj),
          _matchExpr(std::move(matchExpr)) {}
    boost::intrusive_ptr<Expression>& _fieldPathToMatchOn;

    // keep a reference to the original projection around since _matchExpr references
    // values inside of it.
    BSONObj _originalProjection;

    BSONObj _elemMatchObj;
    std::unique_ptr<MatchExpression> _matchExpr;
};

class ExpressionInternalFindPositional final : public Expression {
public:
    /**
     * Creates an ExpressionInternalFindElemMatch.
     */
    static boost::intrusive_ptr<Expression> create(const boost::intrusive_ptr<ExpressionContext>&,
                                                   const std::string& fieldPath,
                                                   const MatchExpression* matchExpr);

    Value evaluate(const Document& root) const final;
    boost::intrusive_ptr<Expression> optimize() final;
    Value serialize(bool explain) const final;

    void acceptVisitor(ExpressionVisitor* visitor) final {
        // TODO:
        MONGO_UNREACHABLE;
        // return visitor->visit(this);
    }

protected:
    void _doAddDependencies(DepsTracker* deps) const final {
        _fieldPathToMatchOn->addDependencies(deps);
    }

private:
    ExpressionInternalFindPositional(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                     boost::intrusive_ptr<Expression> fieldPathExpr,
                                     const MatchExpression* matchExpr)
        : Expression(expCtx, {std::move(fieldPathExpr)}),
          _fieldPathToMatchOn(_children[0]),
          _matchExpr(std::move(matchExpr)) {}
    boost::intrusive_ptr<Expression>& _fieldPathToMatchOn;

    const MatchExpression* _matchExpr;
};

}  // namespace mongo
