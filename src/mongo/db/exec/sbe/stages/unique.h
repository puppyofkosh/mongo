/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

#include <queue>

#include "mongo/db/exec/sbe/stages/stages.h"

namespace mongo::sbe {
/**
 * This stage deduplicates by a given key. Unlike a HashAgg, this stage is not blocking
 * and rows are returned in the same order as they appear in the input stream.
 */
class UniqueStage final : public PlanStage {
public:
    UniqueStage(std::unique_ptr<PlanStage> child,
                value::SlotVector keys,
                PlanNodeId planNodeId);

    ~UniqueStage() = default;

    std::unique_ptr<PlanStage> clone() const final;

    void prepare(CompileCtx& ctx) final;
    value::SlotAccessor* getAccessor(CompileCtx& ctx, value::SlotId slot) final;
    void open(bool reOpen) final;
    PlanState getNext() final;
    void close() final;

    std::unique_ptr<PlanStageStats> getStats() const final;
    const SpecificStats* getSpecificStats() const final;
    std::vector<DebugPrinter::Block> debugPrint() const final;

private:
    using TableType = stdx::unordered_set<value::MaterializedRow, value::MaterializedRowHasher>;

    const value::SlotVector _keySlots;

    std::vector<value::SlotAccessor*> _inKeyAccessors;
    std::vector<value::SlotAccessor*> _inValueAccessors;

    std::vector<value::ViewOfValueAccessor> _outAccessors;
};
}  // namespace mongo::sbe
