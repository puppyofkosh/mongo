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

#include "mongo/platform/basic.h"

#include "mongo/db/exec/sbe/stages/makeobj.h"

#include "mongo/db/exec/sbe/values/bson.h"
#include "mongo/util/str.h"

namespace mongo::sbe {
template <MakeObjOutputType O>
MakeObjStageBase<O>::MakeObjStageBase(std::unique_ptr<PlanStage> input,
                                      value::SlotId objSlot,
                                      boost::optional<value::SlotId> rootSlot,
                                      std::vector<std::string> restrictFields,
                                      std::vector<std::string> projectFields,
                                      value::SlotVector projectVars,
                                      bool forceNewObject,
                                      bool returnOldObject,
                                      PlanNodeId planNodeId)
    : PlanStage("mkobj"_sd, planNodeId),
      _objSlot(objSlot),
      _rootSlot(rootSlot),
      _restrictFields(std::move(restrictFields)),
      _projectFields(std::move(projectFields)),
      _projectVars(std::move(projectVars)),
      _forceNewObject(forceNewObject),
      _returnOldObject(returnOldObject) {
    _children.emplace_back(std::move(input));
    invariant(_projectVars.size() == _projectFields.size());
}

template <MakeObjOutputType O>
std::unique_ptr<PlanStage> MakeObjStageBase<O>::clone() const {
    return std::make_unique<MakeObjStageBase<O>>(_children[0]->clone(),
                                                 _objSlot,
                                                 _rootSlot,
                                                 _restrictFields,
                                                 _projectFields,
                                                 _projectVars,
                                                 _forceNewObject,
                                                 _returnOldObject,
                                                 _commonStats.nodeId);
}

template <MakeObjOutputType O>
void MakeObjStageBase<O>::prepare(CompileCtx& ctx) {
    _children[0]->prepare(ctx);

    if (_rootSlot) {
        _root = _children[0]->getAccessor(ctx, *_rootSlot);
    }
    for (auto& p : _restrictFields) {
        if (p.empty()) {
            _restrictAllFields = true;
        } else {
            auto [it, inserted] = _restrictFieldsSet.emplace(p);
            uassert(4822818, str::stream() << "duplicate field: " << p, inserted);
        }
    }
    for (size_t idx = 0; idx < _projectFields.size(); ++idx) {
        auto& p = _projectFields[idx];

        auto [it, inserted] = _projectFieldsMap.emplace(p, idx);
        uassert(4822819, str::stream() << "duplicate field: " << p, inserted);
        _projects.emplace_back(p, _children[0]->getAccessor(ctx, _projectVars[idx]));
    }
    _compiled = true;
}

template <MakeObjOutputType O>
value::SlotAccessor* MakeObjStageBase<O>::getAccessor(CompileCtx& ctx, value::SlotId slot) {
    if (_compiled && slot == _objSlot) {
        return &_obj;
    } else {
        return _children[0]->getAccessor(ctx, slot);
    }
}

template <MakeObjOutputType O>
void MakeObjStageBase<O>::projectField(value::Object* obj, size_t idx) {
    const auto& p = _projects[idx];

    auto [tag, val] = p.second->getViewOfValue();
    if (tag != value::TypeTags::Nothing) {
        auto [tagCopy, valCopy] = value::copyValue(tag, val);
        obj->push_back(p.first, tagCopy, valCopy);
    }
}

template <MakeObjOutputType O>
void MakeObjStageBase<O>::projectField(BSONObjBuilder* bob, size_t idx) {
    const auto& p = _projects[idx];

    auto [tag, val] = p.second->getViewOfValue();
    bson::appendValueToBsonObj(*bob, p.first, tag, val);
}

template <MakeObjOutputType O>
void MakeObjStageBase<O>::open(bool reOpen) {
    _commonStats.opens++;
    _children[0]->open(reOpen);
}

template <MakeObjOutputType O>
void MakeObjStageBase<O>::produceSbeObject() {
    auto [tag, val] = value::makeNewObject();
    auto obj = value::getObjectView(val);
    absl::flat_hash_set<size_t> alreadyProjected;

    _obj.reset(tag, val);

    if (_root) {
        auto [tag, val] = _root->getViewOfValue();

        if (tag == value::TypeTags::bsonObject) {
            auto be = value::bitcastTo<const char*>(val);
            auto size = ConstDataView(be).read<LittleEndian<uint32_t>>();
            auto end = be + size;
            // Simple heuristic to determine number of fields.
            obj->reserve(size / 16);
            // Skip document length.
            be += 4;
            while (*be != 0) {
                auto sv = bson::fieldNameView(be);

                if (auto it = _projectFieldsMap.find(sv);
                    !isFieldRestricted(sv) && it == _projectFieldsMap.end()) {
                    auto [tag, val] = bson::convertFrom(true, be, end, sv.size());
                    auto [copyTag, copyVal] = value::copyValue(tag, val);
                    obj->push_back(sv, copyTag, copyVal);
                } else if (it != _projectFieldsMap.end()) {
                    projectField(obj, it->second);
                    alreadyProjected.insert(it->second);
                }

                be = bson::advance(be, sv.size());
            }
        } else if (tag == value::TypeTags::Object) {
            auto objRoot = value::getObjectView(val);
            obj->reserve(objRoot->size());
            for (size_t idx = 0; idx < objRoot->size(); ++idx) {
                std::string_view sv(objRoot->field(idx));

                if (auto it = _projectFieldsMap.find(sv);
                    !isFieldRestricted(sv) && it == _projectFieldsMap.end()) {
                    auto [tag, val] = objRoot->getAt(idx);
                    auto [copyTag, copyVal] = value::copyValue(tag, val);
                    obj->push_back(sv, copyTag, copyVal);
                } else if (it != _projectFieldsMap.end()) {
                    projectField(obj, it->second);
                    alreadyProjected.insert(it->second);
                }
            }
        } else {
            for (size_t idx = 0; idx < _projects.size(); ++idx) {
                if (alreadyProjected.count(idx) == 0) {
                    projectField(obj, idx);
                }
            }
            // If the result is non empty object return it.
            if (obj->size() || _forceNewObject) {
                return;
            }
            // Now we have to make a decision - return Nothing or the original _root.
            if (!_returnOldObject) {
                _obj.reset(false, value::TypeTags::Nothing, 0);
            } else {
                // _root is not an object return it unmodified.
                _obj.reset(false, tag, val);
            }
            return;
        }
    }
    for (size_t idx = 0; idx < _projects.size(); ++idx) {
        if (alreadyProjected.count(idx) == 0) {
            projectField(obj, idx);
        }
    }
}

template <MakeObjOutputType O>
void MakeObjStageBase<O>::produceBsonObject() {
    BSONObjBuilder bob;
    absl::flat_hash_set<size_t> alreadyProjected;

    auto finish = [this, &bob]() {
        auto obj = bob.obj();
        auto* data = obj.releaseSharedBuffer().constCast().detach();
        _obj.reset(value::TypeTags::bsonObject, value::bitcastFrom<char*>(data));
    };

    if (_root) {
        auto [tag, val] = _root->getViewOfValue();

        if (tag == value::TypeTags::bsonObject) {
            auto be = value::bitcastTo<const char*>(val);
            // Skip document length.
            be += 4;
            while (*be != 0) {
                auto sv = bson::fieldNameView(be);

                if (auto it = _projectFieldsMap.find(sv);
                    !isFieldRestricted(sv) && it == _projectFieldsMap.end()) {
                    bob.append(BSONElement(be, sv.size() + 1, -1, BSONElement::CachedSizeTag{}));
                } else if (it != _projectFieldsMap.end()) {
                    projectField(&bob, it->second);
                    alreadyProjected.insert(it->second);
                }

                be = bson::advance(be, sv.size());
            }
        } else if (tag == value::TypeTags::Object) {
            auto objRoot = value::getObjectView(val);
            for (size_t idx = 0; idx < objRoot->size(); ++idx) {
                std::string_view sv(objRoot->field(idx));

                if (auto it = _projectFieldsMap.find(sv);
                    !isFieldRestricted(sv) && it == _projectFieldsMap.end()) {
                    auto [tag, val] = objRoot->getAt(idx);
                    bson::appendValueToBsonObj(bob, sv, tag, val);
                } else if (it != _projectFieldsMap.end()) {
                    projectField(&bob, it->second);
                    alreadyProjected.insert(it->second);
                }
            }
        } else {
            for (size_t idx = 0; idx < _projects.size(); ++idx) {
                if (alreadyProjected.count(idx) == 0) {
                    projectField(&bob, idx);
                }
            }
            // If the result is non empty object return it.
            if (!bob.asTempObj().isEmpty() || _forceNewObject) {
                finish();
                return;
            }
            // Now we have to make a decision - return Nothing or the original _root.
            if (!_returnOldObject) {
                _obj.reset(false, value::TypeTags::Nothing, 0);
            } else {
                // _root is not an object return it unmodified.
                _obj.reset(false, tag, val);
            }

            return;
        }
    }
    for (size_t idx = 0; idx < _projects.size(); ++idx) {
        if (alreadyProjected.count(idx) == 0) {
            projectField(&bob, idx);
        }
    }
    finish();
}

template <MakeObjOutputType O>
PlanState MakeObjStageBase<O>::getNext() {
    auto state = _children[0]->getNext();

    if (state == PlanState::ADVANCED) {
        if constexpr (O == MakeObjOutputType::Object) {
            produceSbeObject();
        } else {
            produceBsonObject();
        }
    }
    return trackPlanState(state);
}

template <MakeObjOutputType O>
void MakeObjStageBase<O>::close() {
    _commonStats.closes++;
    _children[0]->close();
}

template <MakeObjOutputType O>
std::unique_ptr<PlanStageStats> MakeObjStageBase<O>::getStats() const {
    auto ret = std::make_unique<PlanStageStats>(_commonStats);
    ret->children.emplace_back(_children[0]->getStats());
    return ret;
}

template <MakeObjOutputType O>
const SpecificStats* MakeObjStageBase<O>::getSpecificStats() const {
    return nullptr;
}

template <MakeObjOutputType O>
std::vector<DebugPrinter::Block> MakeObjStageBase<O>::debugPrint() const {
    // TODO: Include obj type in the output
    auto ret = PlanStage::debugPrint();

    DebugPrinter::addIdentifier(ret, _objSlot);

    if (_rootSlot) {
        DebugPrinter::addIdentifier(ret, *_rootSlot);

        ret.emplace_back(DebugPrinter::Block("[`"));
        for (size_t idx = 0; idx < _restrictFields.size(); ++idx) {
            if (idx) {
                ret.emplace_back(DebugPrinter::Block("`,"));
            }

            DebugPrinter::addIdentifier(ret, _restrictFields[idx]);
        }
        ret.emplace_back(DebugPrinter::Block("`]"));
    }

    ret.emplace_back(DebugPrinter::Block("[`"));
    for (size_t idx = 0; idx < _projectFields.size(); ++idx) {
        if (idx) {
            ret.emplace_back(DebugPrinter::Block("`,"));
        }

        DebugPrinter::addIdentifier(ret, _projectFields[idx]);
        ret.emplace_back("=");
        DebugPrinter::addIdentifier(ret, _projectVars[idx]);
    }
    ret.emplace_back(DebugPrinter::Block("`]"));

    ret.emplace_back(_forceNewObject ? "true" : "false");
    ret.emplace_back(_returnOldObject ? "true" : "false");

    DebugPrinter::addNewLine(ret);
    DebugPrinter::addBlocks(ret, _children[0]->debugPrint());

    return ret;
}

// Explicit template instantiations.
template class MakeObjStageBase<MakeObjOutputType::Object>;
template class MakeObjStageBase<MakeObjOutputType::Bson>;
}  // namespace mongo::sbe
