/**
 * Copyright (C) 2017 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#include "mongo/db/pipeline/document_source.h"
#include "mongo/db/pipeline/document_source_change_stream.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/db/pipeline/document_sources_gen.h"
#include "mongo/db/pipeline/field_path.h"

namespace mongo {
class DocumentSourceOplogTransformation : public DocumentSource {
public:
    DocumentSourceOplogTransformation(const boost::intrusive_ptr<ExpressionContext>& expCtx,
                                      BSONObj changeStreamSpec);
    ~DocumentSourceOplogTransformation() = default;

    // TODO: remove isApplyOpsEntry and use a field instead
    Document applyTransformation(const Document& input, bool isApplyOpsEntry);
    boost::intrusive_ptr<DocumentSource> optimize() final {
        return this;
    }
    Document serializeStageOptions(boost::optional<ExplainOptions::Verbosity> explain) const;
    DocumentSource::GetDepsReturn getDependencies(DepsTracker* deps) const final;
    DocumentSource::GetModPathsReturn getModifiedPaths() const final;

    void doDispose() final {}
    Value serialize(boost::optional<ExplainOptions::Verbosity> explain) const;

    DocumentSource::StageConstraints constraints(Pipeline::SplitState pipeState) const final;

    DocumentSource::GetNextResult getNext();
    const char* getSourceName() const {
        return DocumentSourceChangeStream::kStageName.rawData();
    }

private:
    ResumeTokenData getResumeToken(const Document& doc, bool isApplyOpsEntry);
    Document extractNextApplyOpsEntry();
    bool isDocumentRelevant(const Document& d);

    BSONObj _changeStreamSpec;

    // Regex for matching the "ns" field in applyOps sub-entries. Only non-boost::none when we're
    // watching the entire DB.
    boost::optional<pcrecpp::RE> _nsRegex;

    // TODO: turn this into a boost::optional<some struct>.
    // also store lsid and txnNumber inside the struct.
    std::vector<Value> _currentApplyOps;
    size_t _applyOpsIndex = 0;
    boost::optional<TxnNumber> _txnNumber;
    boost::optional<Document> _lsid;

    // Fields of the document key, in order, including the shard key if the collection is
    // sharded, and anyway "_id". Empty until the first oplog entry with a uuid is encountered.
    // Needed for transforming 'insert' oplog entries.
    std::vector<FieldPath> _documentKeyFields;

    // Set to true if the collection is found to be sharded while retrieving _documentKeyFields.
    bool _documentKeyFieldsSharded = false;
};
}
