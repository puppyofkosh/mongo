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

#include "mongo/db/exec/sbe/stages/zip.h"

#include "mongo/db/exec/sbe/expressions/expression.h"

namespace mongo {
namespace sbe {
ZipStage::ZipStage(std::vector<std::unique_ptr<PlanStage>> inputStages,
                                   std::vector<value::SlotVector> outputVals,
                                   PlanNodeId planNodeId)
    : PlanStage("zip"_sd, planNodeId),
      _outSlots(outputVals) {
    _children = std::move(inputStages);
    invariant(_outSlots.size() == _children.size());
}

std::unique_ptr<PlanStage> ZipStage::clone() const {
    std::vector<std::unique_ptr<PlanStage>> inputStages;
    inputStages.reserve(_children.size());
    for (auto& child : _children) {
        inputStages.emplace_back(child->clone());
    }
    return std::make_unique<ZipStage>(std::move(inputStages), _outSlots, _commonStats.nodeId);
}

void ZipStage::prepare(CompileCtx& ctx) {
    std::vector<std::vector<value::SlotAccessor*>> inputKeyAccessors;
    std::vector<PlanStage*> streams;

    for (size_t childNum = 0; childNum < _children.size(); childNum++) {
        auto& child = _children[childNum];
        child->prepare(ctx);
    }
}

value::SlotAccessor* ZipStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    for (size_t idx = 0; idx < _outSlots.size(); idx++) {
        for (size_t y = 0; y < _outSlots[idx].size(); ++y) {
            if (_outSlots[idx][y] == slot) {
                return _children[idx]->getAccessor(ctx, slot);
            }
        }
    }

    return ctx.getAccessor(slot);
}

void ZipStage::open(bool reOpen) {
    ++_commonStats.opens;

    for (size_t i = 0; i < _children.size(); ++i) {
        auto& child = _children[i];
        child->open(reOpen);
    }
}

PlanState ZipStage::getNext() {
    PlanState ret = PlanState::ADVANCED;
    for (auto && child : _children) {
        if (child->getNext() == PlanState::IS_EOF) {
            ret = PlanState::IS_EOF;
        }
    }
    return ret;
}

void ZipStage::close() {
    trackClose();
    for (auto& child : _children) {
        child->close();
    }
}

std::unique_ptr<PlanStageStats> ZipStage::getStats(bool includeDebugInfo) const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);
    return ret;
}

const SpecificStats* ZipStage::getSpecificStats() const {
    return nullptr;
}

std::vector<DebugPrinter::Block> ZipStage::debugPrint() const {
    auto ret = PlanStage::debugPrint();
    ret.emplace_back(DebugPrinter::Block("[`"));
    ret.emplace_back(DebugPrinter::Block::cmdIncIndent);
    for (size_t childNum = 0; childNum < _children.size(); childNum++) {
        ret.emplace_back(DebugPrinter::Block("[`"));
        for (size_t idx = 0; idx < _outSlots[childNum].size(); idx++) {
            if (idx) {
                ret.emplace_back(DebugPrinter::Block("`,"));
            }
            DebugPrinter::addIdentifier(ret, _outSlots[childNum][idx]);
        }
        ret.emplace_back(DebugPrinter::Block("`]"));

        DebugPrinter::addBlocks(ret, _children[childNum]->debugPrint());

        if (childNum + 1 < _children.size()) {
            ret.emplace_back(DebugPrinter::Block(","));
            DebugPrinter::addNewLine(ret);
        }
    }
    ret.emplace_back(DebugPrinter::Block::cmdDecIndent);
    ret.emplace_back(DebugPrinter::Block("`]"));

    return ret;
}
}  // namespace sbe
}  // namespace mongo
