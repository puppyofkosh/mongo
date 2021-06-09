#pragma once

#include <boost/intrusive_ptr.hpp>

#include "mongo/base/status.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/query/inner_pipeline.h"

namespace mongo {
    struct InnerPipelineStageImpl : public InnerPipelineStage {
        InnerPipelineStageImpl(boost::intrusive_ptr<DocumentSource> src) : _ds(src) {
        }
        
        DocumentSource* ds() override {
            return _ds.get();
        }

        boost::intrusive_ptr<DocumentSource> _ds;
    };
}
