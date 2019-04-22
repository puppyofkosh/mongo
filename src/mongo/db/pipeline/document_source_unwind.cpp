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

#include "mongo/platform/basic.h"

#include "mongo/db/pipeline/document_source_unwind.h"

#include "mongo/db/jsobj.h"
#include "mongo/db/pipeline/document.h"
#include "mongo/db/pipeline/document_source_match.h"
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/lite_parsed_document_source.h"
#include "mongo/db/pipeline/value.h"

namespace mongo {

using boost::intrusive_ptr;
using std::string;
using std::vector;

/** Helper class to unwind array from a single document. */
class DocumentSourceUnwind::Unwinder {
public:
    Unwinder(const FieldPath& unwindPath,
             bool preserveNullAndEmptyArrays,
             const boost::optional<FieldPath>& indexPath);
    /** Reset the unwinder to unwind a new document. */
    void resetDocument(const Document& document);

    /**
     * @return the next document unwound from the document provided to resetDocument(), using
     * the current value in the array located at the provided unwindPath.
     *
     * Returns boost::none if the array is exhausted.
     */
    DocumentSource::GetNextResult getNext();

private:
    // Tracks whether or not we can possibly return any more documents. Note we may return
    // boost::none even if this is true.
    bool _haveNext = false;

    // Path to the array to unwind.
    const FieldPath _unwindPath;

    // Documents that have a nullish value, or an empty array for the field '_unwindPath', will pass
    // through the $unwind stage unmodified if '_preserveNullAndEmptyArrays' is true.
    const bool _preserveNullAndEmptyArrays;

    // If set, the $unwind stage will include the array index in the specified path, overwriting any
    // existing value, setting to null when the value was a non-array or empty array.
    const boost::optional<FieldPath> _indexPath;

    Value _inputArray;

    MutableDocument _output;

    // Document indexes of the field path components.
    vector<Position> _unwindPathFieldIndexes;

    // Index into the _inputArray to return next.
    size_t _index;
};

DocumentSourceUnwind::Unwinder::Unwinder(const FieldPath& unwindPath,
                                         bool preserveNullAndEmptyArrays,
                                         const boost::optional<FieldPath>& indexPath)
    : _unwindPath(unwindPath),
      _preserveNullAndEmptyArrays(preserveNullAndEmptyArrays),
      _indexPath(indexPath) {}

void DocumentSourceUnwind::Unwinder::resetDocument(const Document& document) {
    // Reset document specific attributes.
    _output.reset(document);
    _unwindPathFieldIndexes.clear();
    _index = 0;
    _inputArray = document.getNestedField(_unwindPath, &_unwindPathFieldIndexes);
    _haveNext = true;
}

DocumentSource::GetNextResult DocumentSourceUnwind::Unwinder::getNext() {
    // WARNING: Any functional changes to this method must also be implemented in the unwinding
    // implementation of the $lookup stage.
    if (!_haveNext) {
        return GetNextResult::makeEOF();
    }

    // Track which index this value came from. If 'includeArrayIndex' was specified, we will use
    // this index in the output document, or null if the value didn't come from an array.
    boost::optional<long long> indexForOutput;

    if (_inputArray.getType() == Array) {
        const size_t length = _inputArray.getArrayLength();
        invariant(_index == 0 || _index < length);

        if (length == 0) {
            // Preserve documents with empty arrays if asked to, otherwise skip them.
            _haveNext = false;
            if (!_preserveNullAndEmptyArrays) {
                return GetNextResult::makeEOF();
            }
            _output.removeNestedField(_unwindPathFieldIndexes);
        } else {
            // Set field to be the next element in the array. If needed, this will automatically
            // clone all the documents along the field path so that the end values are not shared
            // across documents that have come out of this pipeline operator. This is a partial deep
            // clone. Because the value at the end will be replaced, everything along the path
            // leading to that will be replaced in order not to share that change with any other
            // clones (or the original).
            _output.setNestedField(_unwindPathFieldIndexes, _inputArray[_index]);
            indexForOutput = _index;
            _index++;
            _haveNext = _index < length;
        }
    } else if (_inputArray.nullish()) {
        // Preserve a nullish value if asked to, otherwise skip it.
        _haveNext = false;
        if (!_preserveNullAndEmptyArrays) {
            return GetNextResult::makeEOF();
        }
    } else {
        // Any non-nullish, non-array type should pass through.
        _haveNext = false;
    }

    if (_indexPath) {
        _output.getNestedField(*_indexPath) =
            indexForOutput ? Value(*indexForOutput) : Value(BSONNULL);
    }

    return _haveNext ? _output.peek() : _output.freeze();
}

DocumentSourceUnwind::DocumentSourceUnwind(const intrusive_ptr<ExpressionContext>& pExpCtx,
                                           const FieldPath& fieldPath,
                                           bool preserveNullAndEmptyArrays,
                                           const boost::optional<FieldPath>& indexPath)
    : DocumentSource(pExpCtx),
      _unwindPath(fieldPath),
      _preserveNullAndEmptyArrays(preserveNullAndEmptyArrays),
      _indexPath(indexPath),
      _unwinder(new Unwinder(fieldPath, preserveNullAndEmptyArrays, indexPath)) {}

REGISTER_DOCUMENT_SOURCE(unwind,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceUnwind::createFromBson);

const char* DocumentSourceUnwind::getSourceName() const {
    return "$unwind";
}

intrusive_ptr<DocumentSourceUnwind> DocumentSourceUnwind::create(
    const intrusive_ptr<ExpressionContext>& expCtx,
    const string& unwindPath,
    bool preserveNullAndEmptyArrays,
    const boost::optional<string>& indexPath) {
    intrusive_ptr<DocumentSourceUnwind> source(
        new DocumentSourceUnwind(expCtx,
                                 FieldPath(unwindPath),
                                 preserveNullAndEmptyArrays,
                                 indexPath ? FieldPath(*indexPath) : boost::optional<FieldPath>()));
    return source;
}

DocumentSource::GetNextResult DocumentSourceUnwind::getNext() {
    pExpCtx->checkForInterrupt();

    auto nextOut = _unwinder->getNext();
    while (nextOut.isEOF()) {
        // No more elements in array currently being unwound. This will loop if the input
        // document is missing the unwind field or has an empty array.
        auto nextInput = pSource->getNext();
        if (!nextInput.isAdvanced()) {
            return nextInput;
        }

        // Try to extract an output document from the new input document.
        _unwinder->resetDocument(nextInput.releaseDocument());
        nextOut = _unwinder->getNext();
    }

    return nextOut;
}

DocumentSource::GetModPathsReturn DocumentSourceUnwind::getModifiedPaths() const {
    std::set<std::string> modifiedFields{_unwindPath.fullPath()};
    if (_indexPath) {
        modifiedFields.insert(_indexPath->fullPath());
    }
    return {GetModPathsReturn::Type::kFiniteSet, std::move(modifiedFields), {}};
}

Value DocumentSourceUnwind::serialize(boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(DOC(getSourceName() << DOC(
                         "path" << _unwindPath.fullPathWithPrefix() << "preserveNullAndEmptyArrays"
                                << (_preserveNullAndEmptyArrays ? Value(true) : Value())
                                << "includeArrayIndex"
                                << (_indexPath ? Value((*_indexPath).fullPath()) : Value()))));
}

DepsTracker::State DocumentSourceUnwind::getDependencies(DepsTracker* deps) const {
    deps->fields.insert(_unwindPath.fullPath());
    return DepsTracker::State::SEE_NEXT;
}

std::list<intrusive_ptr<DocumentSource>> DocumentSourceUnwind::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    // $unwind accepts either the legacy "{$unwind: '$path'}" syntax, or a nested document with
    // extra options.
    string prefixedPathString;
    bool preserveNullAndEmptyArrays = false;
    boost::optional<string> indexPath;
    bool nested = false;
    if (elem.type() == Object) {
        for (auto&& subElem : elem.Obj()) {
            if (subElem.fieldNameStringData() == "path") {
                uassert(28808,
                        str::stream() << "expected a string as the path for $unwind stage, got "
                                      << typeName(subElem.type()),
                        subElem.type() == String);
                prefixedPathString = subElem.str();
            } else if (subElem.fieldNameStringData() == "preserveNullAndEmptyArrays") {
                uassert(28809,
                        str::stream() << "expected a boolean for the preserveNullAndEmptyArrays "
                                         "option to $unwind stage, got "
                                      << typeName(subElem.type()),
                        subElem.type() == Bool);
                preserveNullAndEmptyArrays = subElem.Bool();
            } else if (subElem.fieldNameStringData() == "includeArrayIndex") {
                uassert(28810,
                        str::stream() << "expected a non-empty string for the includeArrayIndex "
                                         " option to $unwind stage, got "
                                      << typeName(subElem.type()),
                        subElem.type() == String && !subElem.String().empty());
                indexPath = subElem.String();
                uassert(28822,
                        str::stream() << "includeArrayIndex option to $unwind stage should not be "
                                         "prefixed with a '$': "
                                      << (*indexPath),
                        (*indexPath)[0] != '$');
            } else if (subElem.fieldNameStringData() == "nested") {
                uassert(31019,
                        str::stream()
                            << "expected a boolean for the nested option to $unwind stage, got "
                            << typeName(subElem.type()),
                        subElem.type() == Bool);
                nested = subElem.Bool();
            } else {
                uasserted(28811,
                          str::stream() << "unrecognized option to $unwind stage: "
                                        << subElem.fieldNameStringData());
            }
        }
    } else if (elem.type() == String) {
        prefixedPathString = elem.str();
    } else {
        uasserted(
            15981,
            str::stream()
                << "expected either a string or an object as specification for $unwind stage, got "
                << typeName(elem.type()));
    }
    uassert(28812, "no path specified to $unwind stage", !prefixedPathString.empty());

    uassert(28818,
            str::stream() << "path option to $unwind stage should be prefixed with a '$': "
                          << prefixedPathString,
            prefixedPathString[0] == '$');
    string pathString(Expression::removeFieldPrefix(prefixedPathString));

    if (nested) {
        std::list<intrusive_ptr<DocumentSource>> res;

        // Given 'unwindPath' 'a.b.c', build an unwinder for 'a',
        // 'a.b' and 'a.b.c'.
        std::string pathPrefix;
        FieldPath unwindPath(pathString);
        pathPrefix.reserve(unwindPath.getPathLength());
        for (size_t i = 0; i < unwindPath.getPathLength(); ++i) {
            StringData field = unwindPath.getFieldName(i);
            if (!pathPrefix.empty()) {
                pathPrefix += '.';
            }
            pathPrefix.insert(pathPrefix.end(), field.begin(), field.end());
            res.emplace_back(DocumentSourceUnwind::create(
                pExpCtx, pathPrefix, preserveNullAndEmptyArrays, indexPath));
            if (i != unwindPath.getPathLength() - 1) {
                // TODO: Is this justifiable?
                // Filter out any remaining arrays under the given path prefix. This is to avoid a
                // nested $unwind from examining arrays which are directly nested inside of another
                // array. Elsewhere in the query language, the "rule" for these doubly-nested
                // arrays is often to treat them as "blobs."

                // We only do this for the path prefixes (and not the full path) so that if we have
                // a document like {a: {b: [[1, 2]]}} and do a nested $unwind on "a.b", we'll still
                // get the document {a: {b: [1, 2]}}.

                BSONObj filter = BSON("" << BSON(pathPrefix << BSON("$not" << BSON("$type"
                                                                                   << "array"))));
                res.emplace_back(
                    DocumentSourceMatch::createFromBson(filter.firstElement(), pExpCtx));
            }
        }

        return res;
    }

    return {
        DocumentSourceUnwind::create(pExpCtx, pathString, preserveNullAndEmptyArrays, indexPath)};
}
}  // namespace mongo
