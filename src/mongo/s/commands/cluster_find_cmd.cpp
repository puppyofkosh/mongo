/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery

#include "mongo/platform/basic.h"

#include <boost/optional.hpp>

#include "mongo/client/read_preference.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/matcher/extensions_callback_noop.h"
#include "mongo/db/query/cursor_response.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/views/resolved_view.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/catalog_cache.h"
#include "mongo/s/commands/cluster_aggregate.h"
#include "mongo/s/commands/cluster_commands_helpers.h"
#include "mongo/s/commands/cluster_explain.h"
#include "mongo/s/grid.h"
#include "mongo/s/query/cluster_find.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

using std::unique_ptr;
using std::string;
using std::vector;

const char kTermField[] = "term";

/**
 * Implements the find command on mongos.
 */
class ClusterFindCmd : public BasicCommand {
    MONGO_DISALLOW_COPYING(ClusterFindCmd);

public:
    ClusterFindCmd() : BasicCommand("find") {}

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kOptIn;
    }

    bool maintenanceOk() const final {
        return false;
    }

    bool adminOnly() const final {
        return false;
    }

    bool supportsReadConcern(const std::string& dbName,
                             const BSONObj& cmdObj,
                             repl::ReadConcernLevel level) const final {
        return true;
    }

    bool shouldAffectCommandCounter() const final {
        return false;
    }

    std::string help() const override {
        return "query for documents";
    }

    /**
     * In order to run the find command, you must be authorized for the "find" action
     * type on the collection.
     */
    Status checkAuthForCommand(Client* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) const final {
        const NamespaceString nss(parseNs(dbname, cmdObj));
        auto hasTerm = cmdObj.hasField(kTermField);
        return AuthorizationSession::get(client)->checkAuthForFind(nss, hasTerm);
    }

    Status explain(OperationContext* opCtx,
                   const OpMsgRequest& request,
                   ExplainOptions::Verbosity verbosity,
                   BSONObjBuilder* out) const final {
        log() << "ian: in cluster find cmd explain";

        std::string dbname = request.getDatabase().toString();
        const BSONObj& originalCmdObj = request.body;
        const NamespaceString nss(
            CommandHelpers::parseNsCollectionRequired(dbname, originalCmdObj));
        // Parse the command BSON to a QueryRequest.
        bool isExplain = true;
        auto swQr = QueryRequest::makeFromFindCommand(std::move(nss), originalCmdObj, isExplain);
        if (!swQr.isOK()) {
            return swQr.getStatus();
        }
        auto& originalQr = *swQr.getValue();

        // Canonicalize the query.
        CanonicalQuery::canonicalize(opCtx, originalQr);
        

        /* TODO: replace false */
        auto qrForShards = ClusterFind::transformQueryForShards(originalQr, false);
        if (!qrForShards.isOK()) {
            return qrForShards.getStatus();
        }
        auto newCmdObj = qrForShards.getValue()->asFindCommand();

        log() << "ian: Probably want to do the transform here";
        log() << "ian: old cmd obj is " << originalCmdObj;
        log() << "ian: new cmd obj is " << newCmdObj;

        try {
            const auto explainCmd = ClusterExplain::wrapAsExplain(newCmdObj, verbosity);
            log() << "ian: explain obj is " << explainCmd;

            long long millisElapsed;
            std::vector<AsyncRequestsSender::Response> shardResponses;

            // We will time how long it takes to run the commands on the shards.
            Timer timer;
            const auto routingInfo =
                uassertStatusOK(Grid::get(opCtx)->catalogCache()->getCollectionRoutingInfo(
                    opCtx, originalQr.nss()));
            shardResponses =
                scatterGatherVersionedTargetByRoutingTable(opCtx,
                                                           originalQr.nss().db(),
                                                           originalQr.nss(),
                                                           routingInfo,
                                                           explainCmd,
                                                           ReadPreferenceSetting::get(opCtx),
                                                           Shard::RetryPolicy::kIdempotent,
                                                           originalQr.getFilter(),
                                                           originalQr.getCollation());
            millisElapsed = timer.millis();

            const char* mongosStageName =
                ClusterExplain::getStageNameForReadOp(shardResponses.size(), newCmdObj);

            uassertStatusOK(ClusterExplain::buildExplainResult(
                opCtx,
                ClusterExplain::downconvert(opCtx, shardResponses),
                mongosStageName,
                millisElapsed,
                originalQr.getSkip(),
                originalQr.getLimit(),
                out));

            return Status::OK();
        } catch (const ExceptionFor<ErrorCodes::CommandOnShardedViewNotSupportedOnMongod>& ex) {
            out->resetToEmpty();

            auto aggCmdOnView = originalQr.asAggregationCommand();
            if (!aggCmdOnView.isOK()) {
                return aggCmdOnView.getStatus();
            }

            auto aggRequestOnView =
                AggregationRequest::parseFromBSON(nss, aggCmdOnView.getValue(), verbosity);
            if (!aggRequestOnView.isOK()) {
                return aggRequestOnView.getStatus();
            }

            auto resolvedAggRequest = ex->asExpandedViewAggregation(aggRequestOnView.getValue());
            auto resolvedAggCmd = resolvedAggRequest.serializeToCommandObj().toBson();

            ClusterAggregate::Namespaces nsStruct;
            nsStruct.requestedNss = std::move(nss);
            nsStruct.executionNss = std::move(ex->getNamespace());

            auto status = ClusterAggregate::runAggregate(
                opCtx, nsStruct, resolvedAggRequest, resolvedAggCmd, out);
            uassertStatusOK(status);
            return status;
        }
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) final {
        // We count find command as a query op.
        globalOpCounters.gotQuery();

        const NamespaceString nss(CommandHelpers::parseNsCollectionRequired(dbname, cmdObj));

        const bool isExplain = false;
        auto qr = QueryRequest::makeFromFindCommand(nss, cmdObj, isExplain);
        uassertStatusOK(qr.getStatus());

        const boost::intrusive_ptr<ExpressionContext> expCtx;
        auto cq = CanonicalQuery::canonicalize(opCtx,
                                               std::move(qr.getValue()),
                                               expCtx,
                                               ExtensionsCallbackNoop(),
                                               MatchExpressionParser::kAllowAllSpecialFeatures);
        uassertStatusOK(cq.getStatus());

        try {
            // Do the work to generate the first batch of results. This blocks waiting to get
            // responses from the shard(s).
            std::vector<BSONObj> batch;
            auto cursorId = ClusterFind::runQuery(
                opCtx, *cq.getValue(), ReadPreferenceSetting::get(opCtx), &batch);
            // Build the response document.
            CursorResponseBuilder firstBatch(/*firstBatch*/ true, &result);
            for (const auto& obj : batch) {
                firstBatch.append(obj);
            }
            firstBatch.done(cursorId, nss.ns());
            return true;
        } catch (const ExceptionFor<ErrorCodes::CommandOnShardedViewNotSupportedOnMongod>& ex) {
            auto aggCmdOnView = cq.getValue()->getQueryRequest().asAggregationCommand();
            uassertStatusOK(aggCmdOnView.getStatus());

            auto aggRequestOnView = AggregationRequest::parseFromBSON(nss, aggCmdOnView.getValue());
            uassertStatusOK(aggRequestOnView.getStatus());

            auto resolvedAggRequest = ex->asExpandedViewAggregation(aggRequestOnView.getValue());
            auto resolvedAggCmd = resolvedAggRequest.serializeToCommandObj().toBson();

            // We pass both the underlying collection namespace and the view namespace here. The
            // underlying collection namespace is used to execute the aggregation on mongoD. Any
            // cursor returned will be registered under the view namespace so that subsequent
            // getMore and killCursors calls against the view have access.
            ClusterAggregate::Namespaces nsStruct;
            nsStruct.requestedNss = std::move(nss);
            nsStruct.executionNss = std::move(ex->getNamespace());

            auto status = ClusterAggregate::runAggregate(
                opCtx, nsStruct, resolvedAggRequest, resolvedAggCmd, &result);
            uassertStatusOK(status);
            return true;
        }
    }

} cmdFindCluster;

}  // namespace
}  // namespace mongo
