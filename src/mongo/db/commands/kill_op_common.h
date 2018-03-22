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

#include "mongo/db/commands.h"

namespace mongo {
class OperationContext;

/**
 * Base class for the killOp command, which attempts to kill a given operation. Contains code
 * common to mongos and mongod implementations.
 */
class KillOpCmdBase : public BasicCommand {
public:
    KillOpCmdBase() : BasicCommand("killOp") {}

    virtual ~KillOpCmdBase() = default;

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    bool adminOnly() const override {
        return true;
    }

    Status checkAuthForCommand(Client* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) const final;

protected:
    /**
     * Given an operation ID, search for an OperationContext with that ID. Returns either an error,
     * or the OperationContext found, along with the (acquired) lock for the associated Client.
     */
    static StatusWith<std::tuple<stdx::unique_lock<Client>, OperationContext*>>
    findOperationContext(ServiceContext* serviceContext, unsigned int opId);

    /**
     * Find the given operation, and check if we're authorized to kill it. On success, returns the
     * OperationContext as well as the acquired lock for the associated Client.
     */
    static StatusWith<std::tuple<stdx::unique_lock<Client>, OperationContext*>> findOpForKilling(
        Client* client, unsigned int opId);

    /**
     * Kill an operation running on this instance of mongod or mongos.
     */
    static void killLocalOperation(OperationContext* opCtx,
                                   unsigned int opToKill,
                                   BSONObjBuilder& result);

    /**
     * Extract the "op" field from cmdObj. Will also convert operation from signed long long to
     * unsigned int. Since BSON only supports signed ints, and an opId is unsigned, we deal with
     * the conversion from a negative signed int to an unsigned int here.
     */
    static unsigned int parseOpId(const BSONObj& cmdObj);

    static bool isKillingLocalOp(const BSONElement& opElem);
};

}  // namespace mongo
