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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kDefault

#include "mongo/platform/basic.h"

#include "mongo/client/dbcommandcursor.h"

#include "mongo/db/query/getmore_request.h"
#include "mongo/util/log.h"

namespace mongo {

DBCommandCursor::DBCommandCursor(DBClientBase* client, BSONObj command, const std::string& dbName)
    : _client(client), _initialCommand(command), _dbName(dbName) {
    invariant(_client);
}

DBCommandCursor::~DBCommandCursor() {
    kill();
}

void DBCommandCursor::requestMore() {
    BSONObj serverResponse;
    BSONObj commandToRun;

    if (!_lastResponse) {
        commandToRun = _initialCommand;
    } else {
        // TODO: Choose nss to pass in a better way.
        GetMoreRequest req(_lastResponse->getNSS(),
                           _lastResponse->getCursorId(),
                           boost::none,
                           boost::none,
                           boost::none,
                           boost::none);

        commandToRun = req.toBSON();
    }

    _client->runCommand(_dbName, commandToRun, serverResponse);
    auto swCursorResponse = CursorResponse::parseFromBSON(serverResponse);
    if (swCursorResponse.isOK()) {
        _lastResponse = std::move(swCursorResponse.getValue());
        _positionInBatch = 0;
    } else {
        _error = swCursorResponse.getStatus();
    }
}

bool DBCommandCursor::moreBuffered() {
    if (!_error.isOK()) {
        return true;
    }

    if (_lastResponse && _positionInBatch < _lastResponse->getBatch().size()) {
        return true;
    }

    return false;
}


bool DBCommandCursor::more() {
    if (!moreBuffered()) {
        if (_lastResponse && _lastResponse->getCursorId() == 0) {
            return false;
        }

        requestMore();

        // TODO: will the server ever troll us and return a non-zero cursor ID, followed by an empty
        // batch?
        // invariant(moreBuffered());
    }

    return moreBuffered();
}

StatusWith<BSONObj> DBCommandCursor::next() {
    invariant(more());
    if (!_error.isOK()) {
        return _error;
    }

    invariant(_lastResponse);
    return _lastResponse->getBatch()[_positionInBatch++];
}

void DBCommandCursor::kill() {
    if (_isKilled) {
        return;
    }

    if (!_lastResponse) {
        // Nothing was ever sent to the server.
        return;
    }

    if (_lastResponse->getCursorId() == 0) {
        // The cursor was exhausted.
        return;
    }

    _client->killCursor(_lastResponse->getNSS(), _lastResponse->getCursorId());
    _isKilled = true;
}
}
