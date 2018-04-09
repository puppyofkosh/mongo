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

    Document applyTransformation(const Document& input);
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
    /*
     * Represents the DocumentSource's state if it's currently reading from an 'applyOps' entry
     * which was created as part of a transaction.
     */
    struct TransactionContext {
        MONGO_DISALLOW_COPYING(TransactionContext);

        // The applyOps representing the transaction. Only kept around so that the underlying
        // memory of 'arr' isn't freed.
        Value applyOps;

        // Array representation of the 'applyOps' field. Stored like this to avoid re-typechecking
        // each call to next(), or copying the entire array.
        const std::vector<Value>& arr;

        // Our current place in the applyOps array.
        size_t pos;

        // Fields that were taken from the 'applyOps' oplog entry.
        Document lsid;
        TxnNumber txnNumber;

        TransactionContext(const Value& applyOpsVal, const Document& lsidDoc, TxnNumber n)
            : applyOps(applyOpsVal),
              arr(applyOps.getArray()),
              pos(0),
              lsid(lsidDoc),
              txnNumber(n) {}
    };

    void initializeTransactionContext(const Document& input);

    Document extractNextApplyOpsEntry();
    bool isDocumentRelevant(const Document& d);

    BSONObj _changeStreamSpec;

    // Regex for matching the "ns" field in applyOps sub-entries. Only non-boost::none when we're
    // watching the entire DB.
    boost::optional<pcrecpp::RE> _nsRegex;

    // Represents if the current 'applyOps' we're unwinding, if any.
    boost::optional<TransactionContext> _txnContext;

    // Fields of the document key, in order, including the shard key if the collection is
    // sharded, and anyway "_id". Empty until the first oplog entry with a uuid is encountered.
    // Needed for transforming 'insert' oplog entries.
    std::vector<FieldPath> _documentKeyFields;

    // Set to true if the collection is found to be sharded while retrieving _documentKeyFields.
    bool _documentKeyFieldsSharded = false;
};
}
