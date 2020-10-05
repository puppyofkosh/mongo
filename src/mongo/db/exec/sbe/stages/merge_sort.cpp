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

#include "mongo/platform/basic.h"

#include "mongo/db/exec/sbe/stages/merge_sort.h"

#include "mongo/db/exec/sbe/expressions/expression.h"

namespace mongo {
namespace sbe {
    MergeSortStage::MergeSortStage(std::vector<std::unique_ptr<PlanStage>> inputStages,
                   // Each element of 'inputKeys' must be the same size as 'dirs'.
                   std::vector<value::SlotVector> inputKeys,
                   std::vector<value::SortDirection> dirs,
                   // Each element of 'inputVals' must be the same size as 'outputVals'.
                   std::vector<value::SlotVector> inputVals,
                   value::SlotVector outputVals,
                   PlanNodeId planNodeId)
    : PlanStage("sort_merge"_sd, planNodeId),
      _inputKeys(std::move(inputKeys)),
      _dirs(std::move(dirs)),
      _inputVals(std::move(inputVals)),
      _outputVals(std::move(outputVals)) {
    _children = std::move(inputStages);

    invariant(_inputKeys.size() == _children.size());
    invariant(_inputVals.size() == _children.size());

    invariant(std::all_of(
                  _inputVals.begin(),
                  _inputVals.end(),
                  [size = _outputVals.size()](const auto& slots) {
                      return slots.size() == size;
                  }));

    invariant(std::all_of(
                  _inputKeys.begin(),
                  _inputKeys.end(),
                  [size = _dirs.size()](const auto& slots) {
                      return slots.size() == size;
                  }));
}

std::unique_ptr<PlanStage> MergeSortStage::clone() const {
    std::vector<std::unique_ptr<PlanStage>> inputStages;
    inputStages.reserve(_children.size());
    for (auto& child : _children) {
        inputStages.emplace_back(child->clone());
    }
    return std::make_unique<MergeSortStage>(
        std::move(inputStages), _inputKeys, _dirs, _inputVals, _outputVals,
        _commonStats.nodeId);
}

void MergeSortStage::prepare(CompileCtx& ctx) {
    _inputAccessors.resize(_children.size());
    
    for (size_t childNum = 0; childNum < _children.size(); childNum++) {
        auto& child = _children[childNum];
        invariant(child);
        child->prepare(ctx);

        for (auto slot : _inputVals[childNum]) {
            auto accessor = child->getAccessor(ctx, slot);
            
            _inputAccessors[childNum].push_back(accessor);
        }
    }

    for (size_t i = 0; i <  _outputVals.size(); ++i) {
        _outAccessors.emplace_back(value::ViewOfValueAccessor{});
    }
}

value::SlotAccessor* MergeSortStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    for (size_t idx = 0; idx < _outputVals.size(); idx++) {
        if (_outputVals[idx] == slot) {
            return &_outAccessors[idx];
        }
    }

    return ctx.getAccessor(slot);
}

void MergeSortStage::open(bool reOpen) {
    MONGO_UNREACHABLE;
}

PlanState MergeSortStage::getNext() {
    MONGO_UNREACHABLE;
}

void MergeSortStage::close() {
    MONGO_UNREACHABLE;
}

std::unique_ptr<PlanStageStats> MergeSortStage::getStats() const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);
    for (auto&& child : _children) {
        ret->children.emplace_back(child->getStats());
    }
    return ret;
}

const SpecificStats* MergeSortStage::getSpecificStats() const {
    return nullptr;
}

std::vector<DebugPrinter::Block> MergeSortStage::debugPrint() const {
    MONGO_UNREACHABLE;
}
}  // namespace sbe
}  // namespace mongo
