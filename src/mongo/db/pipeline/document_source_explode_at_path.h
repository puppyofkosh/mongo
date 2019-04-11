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

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/field_path.h"

namespace mongo {

class DocumentSourceExplodeAtPath final : public DocumentSource {
public:
    // virtuals from DocumentSource
    GetNextResult getNext() final;

    const char* getSourceName() const final;

    Value serialize(boost::optional<ExplainOptions::Verbosity> explain = boost::none) const final;

    /**
     * TODO: ian
     * Should return the first component of the path given.
     * Eg exploding 'a.b' on the document {a: [{b: 1}, 42]} will create
     * {a: {b: {1}}}, modifying all of 'a'
     */
    GetModPathsReturn getModifiedPaths() const final;

    StageConstraints constraints(Pipeline::SplitState pipeState) const final {
        StageConstraints constraints(StreamType::kStreaming,
                                     PositionRequirement::kNone,
                                     HostTypeRequirement::kNone,
                                     DiskUseRequirement::kNoDiskUse,
                                     FacetRequirement::kAllowed,
                                     TransactionRequirement::kAllowed);

        constraints.canSwapWithMatch = true;
        return constraints;
    }

    boost::optional<MergingLogic> mergingLogic() final {
        return boost::none;
    }

    DepsTracker::State getDependencies(DepsTracker* deps) const final;

    /**
     * Creates a new $explodeAtPath DocumentSource from a BSON specification.
     */
    static boost::intrusive_ptr<DocumentSource> createFromBson(
        BSONElement elem, const boost::intrusive_ptr<ExpressionContext>& pExpCtx);

    static boost::intrusive_ptr<DocumentSourceExplodeAtPath> create(
        const boost::intrusive_ptr<ExpressionContext>& expCtx,
        const std::string& path);

    std::string getPath() const {
        return _path.fullPath();
    }
private:
    DocumentSourceExplodeAtPath(const boost::intrusive_ptr<ExpressionContext>& pExpCtx,
                                const FieldPath& fieldPath);

    // Configuration state.
    const FieldPath _path;

    // Iteration state.
    class Exploder;
    std::unique_ptr<Exploder> _exploder;
};

}  // namespace mongo
