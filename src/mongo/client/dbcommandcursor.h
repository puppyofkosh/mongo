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

#include "mongo/bson/bsonobj.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/cursor_response.h"


namespace mongo {

class DBCommandCursor {
    MONGO_DISALLOW_COPYING(DBCommandCursor);
public:
    DBCommandCursor(DBClientBase* client,
                    BSONObj command,
                    const std::string& dbName);

    virtual ~DBCommandCursor();
    
    /* Safe to call next() if true. May request more from the server. */
    bool more();

    /* Return next object in result cursor. */
    StatusWith<BSONObj> next();

    // TODO: add dtor which calls kill()

    /* 
     * Kill the cursor associated with this DBCommandCursor. Illegal to call if more() has not been
     * called yet. Once kill() has been called, it is illegal to call next() or more() again.
     */
    void kill();
    
private:
    bool moreBuffered();
    void requestMore();

    BSONObj getCommandToRun();
    
    boost::optional<CursorResponse> _lastResponse;
    size_t _positionInBatch = 0;

    Status _error = Status::OK();
    DBClientBase* _client = nullptr;

    BSONObj _initialCommand;
    std::string _dbName;

    bool _isKilled = false;
};

}
