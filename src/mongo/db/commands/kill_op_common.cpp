/**
 * Copyright (C) 2018 MongoDB Inc.
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

#include "mongo/platform/basic.h"

#include "mongo/db/commands/kill_op_common.h"

#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

Status KillOpCmdBase::checkAuthForCommand(Client* client,
                                          const std::string& dbname,
                                          const BSONObj& cmdObj) const {
    AuthorizationSession* authzSession = AuthorizationSession::get(client);

    if (authzSession->isAuthorizedForActionsOnResource(ResourcePattern::forClusterResource(),
                                                       ActionType::killop)) {
        // If we have administrative permission to run killop, we don't need to traverse the
        // Client list to figure out if we own the operation which will be terminated.
        return Status::OK();
    }

    bool isAuthenticated = AuthorizationSession::get(client)->getAuthenticatedUserNames().more();
    if (isAuthenticated) {
        // A more fine-grained auth check, which will ensure that we're allowed to kill the
        // given opId, will be performed in the command body.
        return Status::OK();
    }
    return Status(ErrorCodes::Unauthorized, "Unauthorized");
}


StatusWith<std::tuple<stdx::unique_lock<Client>, OperationContext*>>
KillOpCmdBase::findOperationContext(ServiceContext* serviceContext, unsigned int opId) {
    for (ServiceContext::LockedClientsCursor cursor(serviceContext);
         Client* opClient = cursor.next();) {
        stdx::unique_lock<Client> lk(*opClient);

        OperationContext* opCtx = opClient->getOperationContext();
        if (opCtx && opCtx->getOpID() == opId) {
            return {std::make_tuple(std::move(lk), opCtx)};
        }
    }

    return Status(ErrorCodes::NoSuchKey, str::stream() << "Could not find opID: " << opId);
}

StatusWith<std::tuple<stdx::unique_lock<Client>, OperationContext*>>
KillOpCmdBase::findOpForKilling(Client* client, unsigned int opId) {
    AuthorizationSession* authzSession = AuthorizationSession::get(client);

    auto swLockAndOp = findOperationContext(client->getServiceContext(), opId);
    if (swLockAndOp.isOK()) {
        OperationContext* opToKill = std::get<1>(swLockAndOp.getValue());
        if (authzSession->isAuthorizedForActionsOnResource(ResourcePattern::forClusterResource(),
                                                           ActionType::killop) ||
            authzSession->isCoauthorizedWithClient(opToKill->getClient())) {
            return swLockAndOp;
        }
    }

    return Status(ErrorCodes::NoSuchKey, str::stream() << "Could not access opID: " << opId);
}

void KillOpCmdBase::killLocalOperation(OperationContext* opCtx,
                                       unsigned int opToKill,
                                       BSONObjBuilder& result) {
    auto lkAndOp = uassertStatusOK(findOpForKilling(opCtx->getClient(), opToKill));

    stdx::unique_lock<Client> lk;
    OperationContext* opCtxToKill;
    std::tie(lk, opCtxToKill) = std::move(lkAndOp);
    invariant(lk);
    opCtx->getServiceContext()->killOperation(opCtxToKill);
}

unsigned int KillOpCmdBase::convertOpId(long long op) {
    // Internally opid is an unsigned 32-bit int, but as BSON only has signed integer types,
    // we wrap values exceeding 2,147,483,647 to negative numbers. The following undoes this
    // transformation, so users can use killOp on the (negative) opid they received.
    uassert(26823,
            str::stream() << "invalid op : " << op,
            (op >= std::numeric_limits<int>::min()) && (op <= std::numeric_limits<int>::max()));

    return static_cast<unsigned int>(op);
}

}  // namespace mongo
