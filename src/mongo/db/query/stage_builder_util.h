/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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

#include "mongo/db/query/classic_stage_builder.h"
#include "mongo/db/query/plan_yield_policy.h"
#include "mongo/db/query/sbe_stage_builder.h"

namespace mongo::stage_builder {
/**
 * Turns 'solution' into an executable tree of PlanStage(s). Returns a pointer to the root of
 * the plan stage tree.
 *
 * 'cq' must be the CanonicalQuery from which 'solution' is derived. Illegal to call if 'ws'
 * is nullptr, or if 'solution.root' is nullptr.
 *
 * The 'PlanStageType' type parameter defines a specific type of PlanStage the executable tree
 * will consist of.
 */
std::unique_ptr<PlanStage> buildClassicExecutableTree(OperationContext* opCtx,
                                                      const CollectionPtr& collection,
                                                      const CanonicalQuery& cq,
                                                      const QuerySolution& solution,
                                                      WorkingSet* ws);

std::pair<std::unique_ptr<sbe::PlanStage>, stage_builder::PlanStageData>
buildSlotBasedExecutableTree(OperationContext* opCtx,
                             const CollectionPtr& collection,
                             const CanonicalQuery& cq,
                             const QuerySolution& solution,
                             PlanYieldPolicy* yieldPolicy,
                             std::map<NamespaceString, CollectionInfo> collections);

}  // namespace mongo::stage_builder
