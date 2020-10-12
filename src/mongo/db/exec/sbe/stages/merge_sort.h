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
// TODO: Rename to SortMerge
class MergeSortStage final : public PlanStage {
public:
    MergeSortStage(std::vector<std::unique_ptr<PlanStage>> inputStages,
                   // Each element of 'inputKeys' must be the same size as 'dirs'.
                   std::vector<value::SlotVector> inputKeys,
                   std::vector<value::SortDirection> dirs,
                   // Each element of 'inputVals' must be the same size as 'outputVals'.
                   std::vector<value::SlotVector> inputVals,
                   value::SlotVector outputVals,
                   PlanNodeId planNodeId);

    ~MergeSortStage() = default;

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
    struct Branch {
        PlanStage* root = nullptr;

        std::vector<value::SlotAccessor*> inputKeyAccessors;
        std::vector<value::SlotAccessor*> inputValAccessors;
    };

    struct BranchComparator {
        BranchComparator(const std::vector<value::SortDirection>& dirs)
            :_dirs(&dirs) {}

        bool operator()(const Branch*, const Branch*);

        // Guaranteed to never be nullptr. Stored as pointer instead of reference to allow for
        // assignment operators.
        const std::vector<value::SortDirection>* _dirs;
    };

    //
    // The following fields are initialized at construction.
    //
    
    const std::vector<value::SlotVector> _inputKeys;
    const std::vector<value::SortDirection> _dirs;

    const std::vector<value::SlotVector> _inputVals;
    const value::SlotVector _outputVals;

    //
    // The following fields are initialized at prepare().
    //

    // Same size as size of each element of _inputVals.
    std::vector<value::ViewOfValueAccessor> _outAccessors;

    std::vector<Branch> _branches;
    
    //
    // The following fields are reinitialized at each call to open().
    //
    std::priority_queue<Branch*, std::vector<Branch*>, BranchComparator> _heap;

    //
    // The following fields may change across calls to getNext().
    //

    // Indicates the last branch which we popped from. At the beginning of a getNext()
    // call, this branch will _not_ be in the heap and must be reinserted. This is done
    // to avoid copying values.
    Branch* _lastBranchPopped = nullptr;
};
}  // namespace mongo::sbe
