/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <string>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/array_index_path.h"
#include "mongo/stdx/variant.h"

namespace mongo {

namespace doc_diff {

    /*
     * Diff format:
     *
     * diff := <entry>+ <null byte>
     * entry := <fieldId> <value>|<resize marker> <uint32>
     * fieldId := <name marker> <name C string>|<index marker> <arr index uint32>
     * value := <diff marker> <diff>|<'update' marker> <bsonelem>|<'insert' marker> <bsonelem>|
     *          <exclude marker>
     * bsonelem := <BSONElement with empty field name>
     * uint32 := <little endian unsigned int>
     */

    enum Marker : unsigned char {
        kNameMarker = 1,
        kIndexMarker = 2,

        kDiffMarker = 149,
        kUpdateMarker = 150,
        kInsertMarker = 151,
        kExcludeMarker = 152,
        kResizeMarker = 153
    };
    
    class OplogDiff {
    public:
        OplogDiff(ConstSharedBuffer buf, size_t len) :_data(buf), _len(len) {}

        const char* raw() const {
            return _data.get();
        }

        size_t len() const {
            return _len;
        }
        
    private:
        ConstSharedBuffer _data;
        size_t _len;
    };

    
class OplogDiffBuilder {
public:
    OplogDiffBuilder(BufBuilder& builder) :_builder(builder) {
    }

    BufBuilder& b() {
        return _builder;
    }

    // 's' MUST be null terminated
    void appendFieldName(StringData s) {
        _builder.appendChar(Marker::kNameMarker);
        _builder.appendStr(s, true);
    }

    void appendIndex(size_t ind) {
        _builder.appendChar(Marker::kIndexMarker);
        _builder.appendNum(static_cast<unsigned>(ind));
    }

    void appendElt(BSONElement elt) {
        // TODO: We could get away with just storing the value and not the empty field
        // name. for now, idc. Also the implementation could be better.
        BSONObj tmp(elt.wrap(""));
        _builder.appendBuf(tmp.firstElement().rawdata(), tmp.firstElement().size());
    }

    OplogDiff finish() {
        return OplogDiff(_builder.release(), _builder.len());
    }

    BufBuilder& subStart() {
        return _builder;
    }

    // TODO: abandon()/kill().

private:
    BufBuilder& _builder;
};

class OplogDiffReader {
public:
    OplogDiffReader(const char* start, size_t len)
        :_rest(start), _end(start + len)
    {
        // Should be null terminated. TODO: uassert.
        invariant(len > 0);
        invariant(*(start + (len - 1)) == 0);
    }

    unsigned char nextByte() {
        return *(reinterpret_cast<const unsigned char*>(_rest++));
    }

    const char* nextString() {
        auto ret = _rest;
        auto sz = strlen(ret) + 1;
        _rest += sz;
        return ret;
    }

    // TODO: endianness.......
    uint32_t nextUnsigned() {
        auto ret = *(reinterpret_cast<const uint32_t*>(_rest));
        _rest += 4;
        return ret;
    }

    BSONElement nextBsonElt() {
        BSONElement elt(_rest);
        invariant(elt.fieldNameStringData() == "");
        _rest += elt.size();
        return elt;
    }

private:
    const char* _rest;
    const char* _end;
};

// Stuff for converting it to a debug BSON output. Uses a made up format that doesn't disambiguate
// between array indexes and field names or anything.

void diffToDebugBSON(OplogDiffReader* reader, BSONObjBuilder* builder);

inline void valueHelper(OplogDiffReader* reader, BSONObjBuilder* builder, StringData fieldName) {
    const auto marker = reader->nextByte();

    if (marker == Marker::kDiffMarker) {
        BSONObjBuilder sub(builder->subobjStart(fieldName));
        diffToDebugBSON(reader, &sub);
    } else if (marker == Marker::kUpdateMarker) {
        auto elt = reader->nextBsonElt();
        builder->append(fieldName, elt.wrap("<update>"));
    } else if (marker == Marker::kInsertMarker) {
        auto elt = reader->nextBsonElt();
        builder->append(fieldName, elt.wrap("<insert>"));
    } else if (marker == Marker::kExcludeMarker) {
        builder->append(fieldName, "<exclude>");
    } else {
        std::cout << "encountered value " << static_cast<int>(marker) << " for field " << fieldName << std::endl;
        MONGO_UNREACHABLE;
    }
}
            
inline void diffToDebugBSON(OplogDiffReader* reader, BSONObjBuilder* builder) {
    auto marker = reader->nextByte();

    while (marker) {
        if (marker == Marker::kNameMarker) {
            auto str = reader->nextString();
            std::cout << "ian: name entry " << str << std::endl;
            
            valueHelper(reader, builder, str);
        } else if (marker == Marker::kIndexMarker) {
            const auto ind = reader->nextUnsigned();
            std::cout << "ian: ind entry " << ind << std::endl;
            
            std::string fieldName = std::to_string(ind);
            valueHelper(reader, builder, fieldName);
        } else if (marker == Marker::kResizeMarker) {
            const auto newSz = reader->nextUnsigned();
            std::cout << "ian: resize entry " << newSz << std::endl;
            
            builder->appendNumber("<resize>"_sd, static_cast<size_t>(newSz));
        } else {
            MONGO_UNREACHABLE;
        }
        marker = reader->nextByte();
    }
}

inline BSONObj diffToDebugBSON(const OplogDiff& d) {
    OplogDiffReader reader(d.raw(), d.len());
    BSONObjBuilder builder;
    diffToDebugBSON(&reader, &builder);
    return builder.obj();
}
}
}
