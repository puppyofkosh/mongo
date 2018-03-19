/**
 *    Copyright (C) 2015 MongoDB Inc.
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

#include "mongo/platform/basic.h"

#include "mongo/bson/util/bson_extract.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/client.h"
#include "mongo/db/commands/kill_op_common.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

StatusWith<std::tuple<stdx::unique_lock<Client>, OperationContext*>>
KillOpCmdBase::findOpForKilling(Client* client, unsigned int opId) {
    AuthorizationSession* authzSession = AuthorizationSession::get(client);

    auto swLockAndOp = client->getServiceContext()->findOperationContext(opId);
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

bool KillOpCmdBase::killLocalOperation(OperationContext* opCtx,
                                       unsigned int opToKill,
                                       BSONObjBuilder& result) {
    auto swLkAndOp = findOpForKilling(opCtx->getClient(), opToKill);
    if (!swLkAndOp.isOK()) {
        return false;
    }

    stdx::unique_lock<Client> lk;
    OperationContext* opCtxToKill;
    std::tie(lk, opCtxToKill) = std::move(swLkAndOp.getValue());
    opCtx->getServiceContext()->killOperation(opCtxToKill);
    return true;
}

unsigned int KillOpCmdBase::convertOpId(long long op) {
    // Internally opid is an unsigned 32-bit int, but as BSON only has signed integer types,
    // we wrap values exceeding 2,147,483,647 to negative numbers. The following undoes this
    // transformation, so users can use killOp on the (negative) opid they received.
    if (op >= std::numeric_limits<int>::min() && op < 0) {
        // Guaranteed not to overflow since op < 0.
        op += 1ull << 32;
    }

    uassert(26823,
            str::stream() << "invalid op : " << op,
            (op >= 0) && (op <= std::numeric_limits<unsigned int>::max()));

    return static_cast<unsigned int>(op);
}

}  // namespace mongo
