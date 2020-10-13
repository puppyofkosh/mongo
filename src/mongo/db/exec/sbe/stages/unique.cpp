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

#include "mongo/db/exec/sbe/stages/unique.h"

namespace mongo {
namespace sbe {
UniqueStage::UniqueStage(
    std::unique_ptr<PlanStage> child,
    value::SlotVector keys,
    PlanNodeId planNodeId)
    :PlanStage("unique"_sd, planNodeId),
     _keySlots(keys) {
    _children.emplace_back(std::move(child));
}

std::unique_ptr<PlanStage> UniqueStage::clone() const {
    return std::make_unique<UniqueStage>(
        _children.front()->clone(), _keySlots,
        _commonStats.nodeId);
}

void UniqueStage::prepare(CompileCtx& ctx) {
    MONGO_UNREACHABLE;
}

value::SlotAccessor* UniqueStage::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    MONGO_UNREACHABLE;
}

void UniqueStage::open(bool reOpen) {
    ++_commonStats.opens;
    MONGO_UNREACHABLE;
}

PlanState UniqueStage::getNext() {
    MONGO_UNREACHABLE;
}

void UniqueStage::close() {
    MONGO_UNREACHABLE;
}

std::unique_ptr<PlanStageStats> UniqueStage::getStats() const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);
    for (auto&& child : _children) {
        ret->children.emplace_back(child->getStats());
    }
    return ret;
}

const SpecificStats* UniqueStage::getSpecificStats() const {
    // TODO: ian
    return nullptr;
}

std::vector<DebugPrinter::Block> UniqueStage::debugPrint() const {
    MONGO_UNREACHABLE;
}
}  // namespace sbe
}  // namespace mongo
