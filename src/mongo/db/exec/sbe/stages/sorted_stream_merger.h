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
template<class SortedStream>
class SortedStreamMerger final {
public:
    struct Branch {
        SortedStream* stream;

        std::vector<value::SlotAccessor*> inputKeyAccessors;
        std::vector<value::SlotAccessor*> inputValAccessors;
    };
    
    SortedStreamMerger(std::vector<Branch> inputBranches,
                       std::vector<value::SortDirection> dirs,
                       std::vector<value::ViewOfValueAccessor*> outAccessors)
        :_dirs(std::move(dirs)),
        _outAccessors(std::move(outAccessors)),
        _branches(std::move(inputBranches)),
        _heap(_dirs) {

        invariant(std::all_of(
                      _branches.begin(),
                      _branches.end(),
                      [dirSize = _dirs.size(), outValSize = _outAccessors.size()](const auto& branch) {
                          return branch.inputKeyAccessors.size() == dirSize &&
                              branch.inputValAccessors.size() == outValSize;
                      }));
    }

    void clear() {
        _heap = decltype(_heap)(_dirs);
    }

    void init() {
        clear();
        for (auto && branch : _branches) {
            if (branch.stream->getNext() == PlanState::ADVANCED) {
                _heap.push(&branch);
            }
        }
        _lastBranchPopped = nullptr;
    }

    PlanState getNext() {
        std::cout << "ian: getNext()\n";

        if (_lastBranchPopped && _lastBranchPopped->stream->getNext() == PlanState::ADVANCED) {
            // This branch was removed in the last call to getNext() on the stage.
            _heap.push(_lastBranchPopped);
            _lastBranchPopped = nullptr;
        } else if (_heap.empty()) {
            return PlanState::IS_EOF;
        }

        std::cout << "ian: popping from heap\n";
        auto top = _heap.top();
        _heap.pop();
        _lastBranchPopped = top;

        for (size_t i = 0; i < _outAccessors.size(); ++i) {
            auto [tag, val] = top->inputValAccessors[i]->getViewOfValue();
            _outAccessors[i]->reset(tag, val);
        }

        return PlanState::ADVANCED;
    }

    std::vector<Branch>& branches() {
        return _branches;
    }
private:
    struct BranchComparator {
        BranchComparator(const std::vector<value::SortDirection>& dirs)
            :_dirs(&dirs) {}

        bool operator()(const Branch*, const Branch*);

        // Guaranteed to never be nullptr. Stored as pointer instead of reference to allow for
        // assignment operators.
        const std::vector<value::SortDirection>* _dirs;
    };

    const std::vector<value::SortDirection> _dirs;
    
    // Same size as size of each element of _inputVals.
    std::vector<value::ViewOfValueAccessor*> _outAccessors;

    // Branches are owned here.
    std::vector<Branch> _branches;

    // Heap for maintaining order.
    std::priority_queue<Branch*, std::vector<Branch*>, BranchComparator> _heap;

    // Indicates the last branch which we popped from. At the beginning of a getNext() call, this
    // branch will _not_ be in the heap and must be reinserted. This is done to avoid calling
    // getNext() on the branch whose value is being returned, which would require an extra copy of
    // the output value.
    Branch* _lastBranchPopped = nullptr;
};

template<class SortedStream>
bool SortedStreamMerger<SortedStream>::BranchComparator::operator()(const Branch* left, const Branch* right) {
    // Because this comparator is used with std::priority_queue, which is a max heap,
    // return _true_ when left > right.
    
    // TODO: Is this duplicated?

    for (size_t i = 0; i <  left->inputKeyAccessors.size(); ++i) {
        const int coeff = ((*_dirs)[i] == value::SortDirection::Ascending ? 1 : -1);

        auto [lhsTag, lhsVal] = left->inputKeyAccessors[i]->getViewOfValue();
        auto [rhsTag, rhsVal] = right->inputKeyAccessors[i]->getViewOfValue();

        std::cout << "comparing " << std::make_pair(lhsTag, lhsVal) << " " <<
            std::make_pair(rhsTag, rhsVal) << std::endl;

        auto [_, val] = value::compareValue(lhsTag, lhsVal, rhsTag, rhsVal);
        const auto result = value::bitcastTo<int32_t>(val) * coeff;
        if (result < 0) {
            return false;
        }
        if (result > 0) {
            return true;
        }
        continue;
    }

    return false;
}
}  // namespace mongo::sbe
