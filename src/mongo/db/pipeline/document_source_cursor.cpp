/**
 * Copyright 2011 (c) 10gen Inc.
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
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_cursor.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"


namespace mongo {

using boost::intrusive_ptr;
using std::shared_ptr;
using std::string;

const char* DocumentSourceCursor::getSourceName() const {
    return "$cursor";
}

DocumentSource::GetNextResult DocumentSourceCursor::getNext() {
    pExpCtx->checkForInterrupt();

    if (_currentBatch.empty()) {
        loadBatch();

        if (_currentBatch.empty())
            return GetNextResult::makeEOF();
    }

    Document out = std::move(_currentBatch.front());
    _currentBatch.pop_front();
    return std::move(out);
}

void DocumentSourceCursor::loadBatch() {
    log() << "ian: In loadBatch()";
    if (!_exec) {
        // No more documents.
        dispose();
        return;
    }

    PlanExecutor::ExecState state;
    BSONObj resultObj;
    {
        AutoGetCollectionForRead autoColl(pExpCtx->opCtx, _exec->nss());
        uassertStatusOK(repl::ReplicationCoordinator::get(pExpCtx->opCtx)
                            ->checkCanServeReadsFor(pExpCtx->opCtx, _exec->nss(), true));

        uassertStatusOK(_exec->restoreState());

        int memUsageBytes = 0;
        {
            ON_BLOCK_EXIT([this] { recordPlanSummaryStats(); });

            while ((state = _exec->getNext(&resultObj, nullptr)) == PlanExecutor::ADVANCED) {
                if (_shouldProduceEmptyDocs) {
                    _currentBatch.push_back(Document());
                } else if (_dependencies) {
                    _currentBatch.push_back(_dependencies->extractFields(resultObj));
                } else {
                    _currentBatch.push_back(Document::fromBsonWithMetaData(resultObj));
                }

                if (_limit) {
                    if (++_docsAddedToBatches == _limit->getLimit()) {
                        break;
                    }
                    verify(_docsAddedToBatches < _limit->getLimit());
                }

                memUsageBytes += _currentBatch.back().getApproximateSize();

                // As long as we're waiting for inserts, we shouldn't do any batching at this level
                // we need the whole pipeline to see each document to see if we should stop waiting.
                // Furthermore, if we need to return the latest oplog time (in the tailable and
                // needs-merge case), batching will result in a wrong time.
                if (shouldWaitForInserts(pExpCtx->opCtx) ||
                    (pExpCtx->isTailableAwaitData() && pExpCtx->needsMerge) ||
                    memUsageBytes > internalDocumentSourceCursorBatchSizeBytes.load()) {
                    // End this batch and prepare PlanExecutor for yielding.
                    _exec->saveState();
                    return;
                }
            }
            // Special case for tailable cursor -- EOF doesn't preclude more results, so keep
            // the PlanExecutor alive.
            if (state == PlanExecutor::IS_EOF && pExpCtx->isTailableAwaitData()) {
                _exec->saveState();
                return;
            }
        }

        // If we got here, there won't be any more documents, so destroy our PlanExecutor. Note we
        // must hold a collection lock to destroy '_exec', but we can only assume that our locks
        // are still held if '_exec' did not end in an error. If '_exec' encountered an error
        // during a yield, the locks might be yielded.
        if (state != PlanExecutor::DEAD && state != PlanExecutor::FAILURE) {
            cleanupExecutor(autoColl);
        }
    }

    switch (state) {
        case PlanExecutor::ADVANCED:
        case PlanExecutor::IS_EOF:
            return;  // We've reached our limit or exhausted the cursor.
        case PlanExecutor::DEAD: {
            log() << "ian: uassert dead";
            // TODO: This case is never tested
            uasserted(ErrorCodes::QueryPlanKilled,
                      str::stream() << "collection or index disappeared when cursor yielded: "
                                    << WorkingSetCommon::toStatusString(resultObj));
        }
        case PlanExecutor::FAILURE: {
            log() << "ian: uassert failure";
            uasserted(17285,
                      str::stream() << "cursor encountered an error: "
                                    << WorkingSetCommon::toStatusString(resultObj));
        }
        default:
            MONGO_UNREACHABLE;
    }
}

Pipeline::SourceContainer::iterator DocumentSourceCursor::doOptimizeAt(
    Pipeline::SourceContainer::iterator itr, Pipeline::SourceContainer* container) {
    invariant(*itr == this);

    auto nextLimit = dynamic_cast<DocumentSourceLimit*>((*std::next(itr)).get());

    if (nextLimit) {
        if (_limit) {
            // We already have an internal limit, set it to the more restrictive of the two.
            _limit->setLimit(std::min(_limit->getLimit(), nextLimit->getLimit()));
        } else {
            _limit = nextLimit;
        }
        container->erase(std::next(itr));
        return itr;
    }
    return std::next(itr);
}

void DocumentSourceCursor::recordPlanSummaryStats() {
    invariant(_exec);
    // Aggregation handles in-memory sort outside of the query sub-system. Given that we need to
    // preserve the existing value of hasSortStage rather than overwrite with the underlying
    // PlanExecutor's value.
    auto hasSortStage = _planSummaryStats.hasSortStage;

    Explain::getSummaryStats(*_exec, &_planSummaryStats);

    _planSummaryStats.hasSortStage = hasSortStage;
}

Value DocumentSourceCursor::serialize(boost::optional<ExplainOptions::Verbosity> explain) const {
    // We never parse a DocumentSourceCursor, so we only serialize for explain.
    if (!explain)
        return Value();

    // When explain is run in QueryPlanner mode we will not execute the aggregation pipeline and
    // won't populate '_serializedExplain' with the explain plan. We must therefore generate the
    // serialized explain here.
    if (*explain == ExplainOptions::Verbosity::kQueryPlanner) {
        AutoGetCollectionForRead autoColl(pExpCtx->opCtx, _exec->nss());
        uassertStatusOK(_exec->restoreState());
        auto serializedExplain =
            serializeToExplain(explain.get(), autoColl.getCollection(), Status::OK(), _allStats);
        _exec->saveState();
        return serializedExplain;
    }

    MONGO_UNREACHABLE;
    // unique_ptr<PlanStageStats> execStatsOwned;
    // PlanStageStats* execStats = _executionStats.get();
    // if (!execStats) {
    //     invariant(_exec);
    //     // Dump the plannner's execution stats
    //     execStatsOwned = Explain::getWinningPlanStatsTree(_exec.get());
    //     execStats = execStatsOwned.get();
    // }

    // // TODO: this is probably wrong. Remove it and figure out what we should actually do.
    // AutoGetCollectionForRead autoColl(pExpCtx->opCtx, _exec->nss());
    // uassertStatusOK(_exec->restoreState());
    // auto serializedExplain =
    //     serializeToExplain(explain.get(), autoColl.getCollection(), Status::OK(), _allStats);
    // _exec->saveState();
    // return serializedExplain;

    // We've reached our limit or exhausted the cursor.
    // TODO: figure this out about execPlanStatus.

    // TODO: need to do something here

    // AutoGetCollectionForRead autoColl(pExpCtx->opCtx, _exec->nss());
    // Status execPlanStatus = Status::OK();
    // uassertStatusOK(_exec->restoreState());
    // auto serializedExplain = serializeToExplain(
    //     pExpCtx->explain.get(), autoColl.getCollection(), execPlanStatus, _allStats);
    // _exec->saveState();
    // return serializedExplain;

    // Is Dave saying that we use the executor here to get the executionStats?
    // do we have the collection lock here though?
}

Value DocumentSourceCursor::serializeToExplain(ExplainOptions::Verbosity explain,
                                               Collection* collection,
                                               Status executePlanStatus,
                                               const Explain::PreExecutionStats& allStats) const {

    // Get planner-level explain info from the underlying PlanExecutor.
    invariant(_exec);
    BSONObjBuilder explainBuilder;
    Explain::explainStagesPostExec(
        _exec.get(), collection, explain, &explainBuilder, executePlanStatus, allStats);

    // TODO: Maybe make this a BSONObjBuilder
    MutableDocument out;
    out["query"] = Value(_query);

    if (!_sort.isEmpty())
        out["sort"] = Value(_sort);

    if (_limit)
        out["limit"] = Value(_limit->getLimit());

    if (!_projection.isEmpty())
        out["fields"] = Value(_projection);

    // Add explain results from the query system into the agg explain output.
    BSONObj explainObj = explainBuilder.obj();
    invariant(explainObj.hasField("queryPlanner"));
    out["queryPlanner"] = Value(explainObj["queryPlanner"]);
    if (explain >= ExplainOptions::Verbosity::kExecStats) {
        BSONObjBuilder bob;
        Explain::getExecutionStats(_exec.get(), explain, _executionStats.get(), &bob, executePlanStatus, allStats);
        // FIXME: should be simpler if we use BSONObjBuilder instead
        BSONObj execObj = bob.obj();
        invariant(execObj.hasField("executionStats"));

        out["executionStats"] = Value(execObj["executionStats"]);
    }

    return Value(DOC(getSourceName() << out.freezeToValue()));
}

void DocumentSourceCursor::detachFromOperationContext() {
    if (_exec) {
        _exec->detachFromOperationContext();
    }
}

void DocumentSourceCursor::reattachToOperationContext(OperationContext* opCtx) {
    if (_exec) {
        _exec->reattachToOperationContext(opCtx);
    }
}

void DocumentSourceCursor::doDispose() {
    _currentBatch.clear();
    if (!_exec) {
        // We've already properly disposed of our PlanExecutor.
        return;
    }
    cleanupExecutor();
}

void DocumentSourceCursor::cleanupExecutor() {
    // TODO: Save executor stats that we care about here
    invariant(_exec);

    // Save our stats from _exec outside of the lock
    _executionStats = Explain::getWinningPlanStatsTree(_exec.get());
    
    auto* opCtx = pExpCtx->opCtx;
    // We need to be careful to not use AutoGetCollection here, since we only need the lock to
    // protect potential access to the Collection's CursorManager, and AutoGetCollection may throw
    // if this namespace has since turned into a view. Using Database::getCollection() will simply
    // return nullptr if the collection has since turned into a view. In this case, '_exec' will
    // already have been marked as killed when the collection was dropped, and we won't need to
    // access the CursorManager to properly dispose of it.
    AutoGetDb dbLock(opCtx, _exec->nss().db(), MODE_IS);
    Lock::CollectionLock collLock(opCtx->lockState(), _exec->nss().ns(), MODE_IS);
    auto collection = dbLock.getDb() ? dbLock.getDb()->getCollection(opCtx, _exec->nss()) : nullptr;
    auto cursorManager = collection ? collection->getCursorManager() : nullptr;
    _exec->dispose(opCtx, cursorManager);
    _exec.reset();
}

void DocumentSourceCursor::cleanupExecutor(const AutoGetCollectionForRead& readLock) {
    invariant(_exec);

    // Save our stats from _exec outside of the lock
    _executionStats = Explain::getWinningPlanStatsTree(_exec.get());

    auto cursorManager =
        readLock.getCollection() ? readLock.getCollection()->getCursorManager() : nullptr;
    _exec->dispose(pExpCtx->opCtx, cursorManager);
    _exec.reset();
}

DocumentSourceCursor::~DocumentSourceCursor() {
    invariant(!_exec);  // '_exec' should have been cleaned up via dispose() before destruction.
}

// TODO: Write test that has a rooted or to test the SubPlanner

DocumentSourceCursor::DocumentSourceCursor(
    Collection* collection,
    std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> exec,
    const intrusive_ptr<ExpressionContext>& pCtx)
    : DocumentSource(pCtx),
      _docsAddedToBatches(0),
      _exec(std::move(exec)),
      _outputSorts(_exec->getOutputSorts()) {

    _planSummary = Explain::getPlanSummary(_exec.get());
    recordPlanSummaryStats();
    invariant(!_shouldProduceEmptyDocs);

    if (pExpCtx->explain) {
        // TODO: put comment here saying its safe to access the executor even if we don't have the collection
        // lock since we're just going to call getStats() on it.
        _allStats = Explain::collectPreExecutionStats(_exec.get(), pExpCtx->explain.get());
    }

    if (collection) {
        collection->infoCache()->notifyOfQuery(pExpCtx->opCtx, _planSummaryStats.indexesUsed);
    }
}

intrusive_ptr<DocumentSourceCursor> DocumentSourceCursor::create(
    Collection* collection,
    std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> exec,
    const intrusive_ptr<ExpressionContext>& pExpCtx) {
    intrusive_ptr<DocumentSourceCursor> source(
        new DocumentSourceCursor(collection, std::move(exec), pExpCtx));
    return source;
}
}
