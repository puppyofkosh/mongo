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

#include "mongo/db/pipeline/document_source_explode_at_path.h"

#include "mongo/db/bson/dotted_path_support.h"
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

class DocumentSourceExplodeAtPath::Exploder {
public:
    Exploder(const FieldPath& fp) : _fieldPath(fp), _extractedElementsIndex(0) {}

    /** Reset the exploder to explode a new document. */
    void resetDocument(const Document& document);

    /**
     * @return the next document exploded from the document provided to resetDocument(), using
     * the current value in the array located at the provided unwindPath.
     */
    DocumentSource::GetNextResult getNext();

private:
    const FieldPath _fieldPath;

    std::vector<BSONElement> _extractedElements;
    size_t _extractedElementsIndex;
    BSONObj _currentDoc;

    MutableDocument _output;
};

void DocumentSourceExplodeAtPath::Exploder::resetDocument(const Document& d) {
    _output.reset(d);
    _extractedElements.clear();
    _currentDoc = d.toBson();
    dotted_path_support::extractAllElementsAlongPath(
        _currentDoc, _fieldPath.fullPath(), _extractedElements);
    _extractedElementsIndex = 0;
}

DocumentSource::GetNextResult DocumentSourceExplodeAtPath::Exploder::getNext() {
    if (_extractedElementsIndex == _extractedElements.size()) {
        return DocumentSource::GetNextResult::makeEOF();
    }

    const auto elt = _extractedElements[_extractedElementsIndex++];
    _output.setNestedField(_fieldPath, Value(elt));

    return _output.peek();
}

DocumentSourceExplodeAtPath::DocumentSourceExplodeAtPath(
    const intrusive_ptr<ExpressionContext>& pExpCtx, const FieldPath& fieldPath)
    : DocumentSource(pExpCtx), _path(fieldPath), _exploder(new Exploder(fieldPath)) {}

REGISTER_DOCUMENT_SOURCE(explodeAtPath,
                         LiteParsedDocumentSourceDefault::parse,
                         DocumentSourceExplodeAtPath::createFromBson);

const char* DocumentSourceExplodeAtPath::getSourceName() const {
    return "$explodeAtPath";
}

intrusive_ptr<DocumentSourceExplodeAtPath> DocumentSourceExplodeAtPath::create(
    const intrusive_ptr<ExpressionContext>& expCtx, const string& path) {
    intrusive_ptr<DocumentSourceExplodeAtPath> source(
        new DocumentSourceExplodeAtPath(expCtx, FieldPath(path)));
    return source;
}

DocumentSource::GetNextResult DocumentSourceExplodeAtPath::getNext() {
    pExpCtx->checkForInterrupt();

    auto nextOut = _exploder->getNext();
    while (nextOut.isEOF()) {
        // No more elements in array currently being unwound. This will loop if the input
        // document is missing the unwind field or has an empty array.
        auto nextInput = pSource->getNext();
        if (!nextInput.isAdvanced()) {
            return nextInput;
        }

        // Try to extract an output document from the new input document.
        _exploder->resetDocument(nextInput.releaseDocument());
        nextOut = _exploder->getNext();
    }

    return nextOut;
}

DocumentSource::GetModPathsReturn DocumentSourceExplodeAtPath::getModifiedPaths() const {
    // TODO: ian. Write a test for the $match optimization or disable it
    return {GetModPathsReturn::Type::kFiniteSet, {std::string(_path.getFieldName(0))}, {}};
}

Value DocumentSourceExplodeAtPath::serialize(
    boost::optional<ExplainOptions::Verbosity> explain) const {
    return Value(DOC(getSourceName() << DOC("path" << _path.fullPathWithPrefix())));
}

DepsTracker::State DocumentSourceExplodeAtPath::getDependencies(DepsTracker* deps) const {
    deps->fields.insert(_path.fullPath());
    return DepsTracker::State::SEE_NEXT;
}

intrusive_ptr<DocumentSource> DocumentSourceExplodeAtPath::createFromBson(
    BSONElement elem, const intrusive_ptr<ExpressionContext>& pExpCtx) {
    // $unwind accepts either the legacy "{$unwind: '$path'}" syntax, or a nested document with
    // extra options.
    string prefixedPathString;
    boost::optional<string> indexPath;
    if (elem.type() == String) {
        prefixedPathString = elem.str();
    } else {
        uasserted(31019,
                  str::stream() << "expected a string $explodeAtPath stage, got "
                                << typeName(elem.type()));
    }
    uassert(51173, "no path specified to $explodeAtPath stage", !prefixedPathString.empty());

    uassert(51174,
            str::stream() << "path option to $explodeAtPath stage should be prefixed with a '$': "
                          << prefixedPathString,
            prefixedPathString[0] == '$');
    string pathString(Expression::removeFieldPrefix(prefixedPathString));
    return DocumentSourceExplodeAtPath::create(pExpCtx, pathString);
}
}
