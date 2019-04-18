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
#include "mongo/db/pipeline/expression.h"
#include "mongo/db/pipeline/lite_parsed_document_source.h"
#include "mongo/db/pipeline/value.h"

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kQuery
#include "mongo/util/log.h"

namespace mongo {

using boost::intrusive_ptr;
using std::string;
using std::vector;

class DocumentSourceUnwind::Unwinder {
public:
    virtual ~Unwinder() = default;

    /** Reset the unwinder to unwind a new document. */
    virtual void resetDocument(const Document& document) = 0;

    /**
     * @return the next document unwound from the document provided to resetDocument(), using
     * the current value in the array located at the provided unwindPath.
     *
     * Returns boost::none if the array is exhausted.
     */
    virtual DocumentSource::GetNextResult getNext() = 0;
};

/** Helper class to unwind array from a single document. */
class DocumentSourceUnwind::StandardUnwinder : public DocumentSourceUnwind::Unwinder {
public:
    StandardUnwinder(const FieldPath& unwindPath,
                     bool preserveNullAndEmptyArrays,
                     const boost::optional<FieldPath>& indexPath);
    /** Reset the unwinder to unwind a new document. */
    void resetDocument(const Document& document) override;

    /**
     * @return the next document unwound from the document provided to resetDocument(), using
     * the current value in the array located at the provided unwindPath.
     *
     * Returns boost::none if the array is exhausted.
     */
    DocumentSource::GetNextResult getNext() override;

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

DocumentSourceUnwind::StandardUnwinder::StandardUnwinder(
    const FieldPath& unwindPath,
    bool preserveNullAndEmptyArrays,
    const boost::optional<FieldPath>& indexPath)
    : _unwindPath(unwindPath),
      _preserveNullAndEmptyArrays(preserveNullAndEmptyArrays),
      _indexPath(indexPath) {}

void DocumentSourceUnwind::StandardUnwinder::resetDocument(const Document& document) {
    // Reset document specific attributes.
    _output.reset(document);
    _unwindPathFieldIndexes.clear();
    _index = 0;
    _inputArray = document.getNestedField(_unwindPath, &_unwindPathFieldIndexes);
    _haveNext = true;
}

DocumentSource::GetNextResult DocumentSourceUnwind::StandardUnwinder::getNext() {
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

class DocumentSourceUnwind::NestedUnwinder : public DocumentSourceUnwind::Unwinder {
public:
    NestedUnwinder(const FieldPath& unwindPath,
                   bool preserveNullAndEmptyArrays,
                   const boost::optional<FieldPath>& indexPath) {
        _children.reserve(unwindPath.getPathLength());

        // Given 'unwindPath' 'a.b.c', build an unwinder for 'a',
        // 'a.b' and 'a.b.c'.
        std::string pathPrefix;
        pathPrefix.reserve(unwindPath.getPathLength());
        for (size_t i = 0; i < unwindPath.getPathLength(); ++i) {
            StringData field = unwindPath.getFieldName(i);
            if (!pathPrefix.empty()) {
                pathPrefix += '.';
            }
            pathPrefix.insert(pathPrefix.end(), field.begin(), field.end());
            _children.push_back(std::make_unique<StandardUnwinder>(
                FieldPath(pathPrefix), preserveNullAndEmptyArrays, indexPath));
        }
    }

    /** Reset the unwinder to unwind a new document. */
    void resetDocument(const Document& document) override {
        // There should be no more documents anywhere in this mini pipeline.
        for (auto&& child : _children) {
            invariant(child->getNext().isEOF());
        }

        // Set the very first child to look at this document.
        _children.front()->resetDocument(document);
    }

    /**
     * @return the next document unwound from the document provided to resetDocument(), using
     * the current value in the array located at the provided unwindPath.
     *
     * Returns boost::none if the array is exhausted.
     */
    DocumentSource::GetNextResult getNext() override {
        if (auto res = _children.back()->getNext(); !res.isEOF()) {
            return res;
        }

        // 1 = forward, -1 = backward.
        int direction = -1;
        size_t index = _children.size() - 1;
        boost::optional<Document> currentDocument;
        while (1) {
            if (direction > 0) {
                invariant(currentDocument);

                // We are moving towards the back of the pipeline, feeding documents
                // from stage i to stage i + 1.

                ++index;
                while (index < _children.size()) {
                    log() << "Reseting document for child " << index << " to "
                          << *currentDocument;
                    _children[index]->resetDocument(*currentDocument);
                    GetNextResult res = _children[index]->getNext();

                    if (res.isEOF()) {
                        // This stage 'consumed' its document. We have to go back and find
                        // an unwinder which has results ready.
                        direction = -1;
                        currentDocument = boost::none;
                        break;
                    }

                    if (res.isPaused()) {
                        return res;
                    }

                    currentDocument.emplace(res.getDocument());
                    ++index;
                }
            } else {
                log() << "moving backwards";
                invariant(direction == -1);
                invariant(!currentDocument);
                // Starting from 'index', go backwards and find an unwinder which has results ready.
                auto indexAndResult = findLastNonEof(index);
                index = indexAndResult.first;
                GetNextResult next = indexAndResult.second;
                if (next.isEOF()) {
                    invariant(index == 0);
                    return next;
                }

                if (next.isPaused()) {
                    return next;
                }

                currentDocument.emplace(next.getDocument());
                direction = 1;
            }
        }

        MONGO_UNREACHABLE;
    }

private:
    std::vector<std::unique_ptr<StandardUnwinder>> _children;

    /**
     * TODO
     */
    std::pair<size_t, DocumentSource::GetNextResult> findLastNonEof(size_t startIndex) {
        size_t i = startIndex;
        for (; i-- != 0;) {
            auto next = _children[i]->getNext();
            if (!next.isEOF()) {
                return std::make_pair(i, next);
            } else if (i == 0) {
                return std::make_pair(i, next);
            }
        }

        MONGO_UNREACHABLE;
    }
};

DocumentSourceUnwind::DocumentSourceUnwind(const intrusive_ptr<ExpressionContext>& pExpCtx,
                                           const FieldPath& fieldPath,
                                           bool preserveNullAndEmptyArrays,
                                           const boost::optional<FieldPath>& indexPath,
                                           bool nested)
    : DocumentSource(pExpCtx),
      _unwindPath(fieldPath),
      _preserveNullAndEmptyArrays(preserveNullAndEmptyArrays),
      _indexPath(indexPath),
      _nested(nested) {
    if (nested) {
        _unwinder.reset(new NestedUnwinder(fieldPath, preserveNullAndEmptyArrays, indexPath));
    } else {
        _unwinder.reset(new StandardUnwinder(fieldPath, preserveNullAndEmptyArrays, indexPath));
    }
}

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
    const boost::optional<string>& indexPath,
    bool nested) {
    intrusive_ptr<DocumentSourceUnwind> source(
        new DocumentSourceUnwind(expCtx,
                                 FieldPath(unwindPath),
                                 preserveNullAndEmptyArrays,
                                 indexPath ? FieldPath(*indexPath) : boost::optional<FieldPath>(),
                                 nested));
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

intrusive_ptr<DocumentSource> DocumentSourceUnwind::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    // $unwind accepts either the legacy "{$unwind: '$path'}" syntax, or a nested document with
    // extra options.
    string prefixedPathString;
    bool preserveNullAndEmptyArrays = false;
    bool nested = false;
    boost::optional<string> indexPath;
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
    return DocumentSourceUnwind::create(
        pExpCtx, pathString, preserveNullAndEmptyArrays, indexPath, nested);
}
}
