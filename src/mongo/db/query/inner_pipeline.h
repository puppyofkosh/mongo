#pragma once

#include "mongo/base/status.h"
#include "mongo/db/namespace_string.h"

namespace mongo {

/**
 * Representation of agg pipeline stages which can be pushed into the "inner" find() layer.
 */
namespace inner_pipeline {
struct Stage {
    virtual StringData name() const = 0;
};

struct EqLookupStage : public Stage {
    EqLookupStage(NamespaceString nss, FieldPath localField, FieldPath foreignField, FieldPath as)
        : _nss(std::move(nss)),
          _localField(std::move(localField)),
          _foreignField(std::move(foreignField)),
          _as(std::move(as)) {}

    StringData name() const override {
        return "$lookup"_sd;
    }

    NamespaceString _nss;
    FieldPath _localField;
    FieldPath _foreignField;
    FieldPath _as;
};

struct GroupStage : public Stage {
    StringData name() const override {
        return "$group"_sd;
    }
};

struct InnerPipeline {
    std::vector<std::unique_ptr<Stage>> stages;
};
}  // namespace inner_pipeline
}  // namespace mongo
