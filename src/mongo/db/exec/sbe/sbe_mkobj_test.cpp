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

/**
 * This file contains tests for sbe::LimitSkipStage.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/exec/sbe/sbe_plan_stage_test.h"
#include "mongo/db/exec/sbe/stages/makeobj.h"

namespace mongo::sbe {

class MkObjTest : public PlanStageTestFixture {
public:
    enum class InputType {
        Bson,
        Object
    };

    enum class InclusionExclusion {
        Inclusion,
        Exclusion
    };
    
    std::pair<value::SlotId, std::unique_ptr<PlanStage>> buildMockScan(InputType type) {
        auto [inputTag, inputVal] = value::makeNewArray();
        value::ValueGuard inputGuard{inputTag, inputVal};
        {
            auto inputView = value::getArrayView(inputVal);

            if (type == InputType::Object) {
                auto obj = value::makeNewObject();
                auto objView = value::getObjectView(obj.second);
                objView->push_back("a", value::TypeTags::NumberInt32, value::bitcastFrom<int32_t>(1));
                objView->push_back("b", value::TypeTags::NumberInt32, value::bitcastFrom<int32_t>(2));
                objView->push_back("c", value::TypeTags::NumberInt32, value::bitcastFrom<int32_t>(3));
                inputView->push_back(obj.first, obj.second);
            } else {
                auto bsonObj = BSON("a" << 1 << "b" << 2 << "c" << 3);
                auto bsonData = value::bitcastFrom<const char*>(bsonObj.objdata());
                auto val = value::copyValue(value::TypeTags::bsonObject, bsonData);
                inputView->push_back(val.first, val.second);
            }
        }

        inputGuard.reset();
        return generateMockScan(inputTag, inputVal);
    }

    template<class MakeObjStageType>
    std::pair<value::SlotId, std::unique_ptr<PlanStage>> buildInclusionTree(const std::vector<std::string>& fieldsToInclude,
                                                                            value::SlotId scanSlot,
                                                                            std::unique_ptr<PlanStage> scanStage) {
        sbe::value::SlotMap<std::unique_ptr<sbe::EExpression>> projections;
        sbe::value::SlotVector fieldSlots;
        for (const auto& field : fieldsToInclude) {
            fieldSlots.push_back(generateSlotId());
            projections.emplace(
                fieldSlots.back(),
                sbe::makeE<sbe::EFunction>("getField",
                                           sbe::makeEs(sbe::makeE<sbe::EVariable>(scanSlot),
                                                       sbe::makeE<sbe::EConstant>(std::string_view{
                                                               field.c_str(), field.size()}))));
        }

        const auto objOutSlotId = generateSlotId();

        auto mkObj = makeS<MakeObjStageType>(
            sbe::makeS<sbe::ProjectStage>(std::move(scanStage),
                                          std::move(projections),
                                          kEmptyPlanNodeId),
            objOutSlotId,
            boost::none,
            std::vector<std::string>{}, // restrict fields (none).
            fieldsToInclude,
            std::move(fieldSlots),
            false,
            false,
            kEmptyPlanNodeId);

        return {objOutSlotId, std::move(mkObj)};
    }

    template<class MakeObjStageType>
    void runTestWithOptions(InclusionExclusion inclusionMode,
                            InputType inputType,
                            // Definition of "project" depends on 'inclusionMode'
                            const std::vector<std::string>& fieldsToProject,
                            const std::vector<std::string>& expectedFieldsRemaining) {
        auto [scanSlot, scanStage] = buildMockScan(InputType::Bson);

        std::unique_ptr<PlanStage> mkObj;
        value::SlotId objOutSlotId;
        
        if (inclusionMode == InclusionExclusion::Exclusion) {
            objOutSlotId = generateSlotId();
            mkObj = makeS<MakeObjStageType>(std::move(scanStage),
                                                 objOutSlotId,
                                                 scanSlot,
                                            fieldsToProject,
                                                 std::vector<std::string>{},
                                                 value::SlotVector{},
                                                 false,
                                                 false,
                                                 kEmptyPlanNodeId);
        } else {
            std::tie(objOutSlotId, mkObj) = buildInclusionTree<MakeObjStageType>(fieldsToProject,
                                                                                 scanSlot,
                                                                                 std::move(scanStage));       
        }
        auto resultAccessor = prepareTree(mkObj.get(), objOutSlotId);

        ASSERT_TRUE(mkObj->getNext() == PlanState::ADVANCED);
        auto [tag, val] = resultAccessor->getViewOfValue();

        if constexpr (std::is_same_v<MakeObjStageType, MakeBSONObjStage>) {
           ASSERT_TRUE(tag == value::TypeTags::bsonObject);
           auto* data = value::bitcastTo<const char*>(val);
           BSONObj obj(data);
           for (auto && field : expectedFieldsRemaining) {
               ASSERT_TRUE(obj.hasField(field));
           }
           ASSERT_EQ(obj.nFields(), expectedFieldsRemaining.size());
        } else {
            ASSERT_TRUE(tag == value::TypeTags::Object);

            auto obj = value::getObjectView(val);
            ASSERT_EQ(obj->size(), expectedFieldsRemaining.size());
            for (auto && field : expectedFieldsRemaining) {
                ASSERT_NE(obj->getField(field).first, value::TypeTags::Nothing);
            }
        }

        ASSERT_TRUE(mkObj->getNext() == PlanState::IS_EOF);
    }
                            
};

TEST_F(MkObjTest, TestAll) {
    std::vector<InclusionExclusion> incExcOptions{InclusionExclusion::Inclusion,
            InclusionExclusion::Exclusion};
    std::vector<InputType> inputTypeOptions{InputType::Bson, InputType::Object};

    const std::vector<std::string> fieldsToProject{"b"};

    const std::vector<std::string> fieldsKeptInclusion{"b"};
    const std::vector<std::string> fieldsKeptExclusion{"a", "c"};

    for (auto && inclusionExclusion : incExcOptions) {
        for (auto && inputType : inputTypeOptions) {
            const auto& expectedFieldsKept = inclusionExclusion == InclusionExclusion::Inclusion ? fieldsKeptInclusion : fieldsKeptExclusion;

            runTestWithOptions<MakeBSONObjStage>(inclusionExclusion, inputType,
                                                 fieldsToProject,
                                                 expectedFieldsKept);
            runTestWithOptions<MakeObjStage>(inclusionExclusion, inputType,
                                             fieldsToProject,
                                             expectedFieldsKept);
        }
    }
}
}  // namespace mongo::sbe
