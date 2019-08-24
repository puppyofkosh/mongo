/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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

#include "mongo/db/query/projection.h"

#include "mongo/db/query/projection_ast_walker.h"

namespace mongo {
namespace projection_ast {

namespace {

// Does "broad" analysis on the projection, whether the entire document is needed to perform the
// projection.
class ProjectionAnalysisVisitor : public ProjectionASTVisitor {
public:
    void visit(MatchExpressionASTNode* node) {}
    void visit(ProjectionPathASTNode* node) {
        if (node->parent()) {
            _deps.hasDottedPath = true;
        }
    }
    void visit(ProjectionPositionalASTNode* node) {
        _deps.requiresMatchDetails = true;
    }

    void visit(ProjectionSliceASTNode* node) {}
    void visit(ProjectionElemMatchASTNode* node) {}
    void visit(ExpressionASTNode* node) {
        // A projection with any expression can't be covered

        const Expression* expr = node->expression();
        const ExpressionMeta* meta = dynamic_cast<const ExpressionMeta*>(expr);

        // Only {$meta: 'sortKey'} projections can be covered. Projections with any other expression
        // need to be covered. TODO: ian test this or ban it and do it in a follow up ticket.
        if (!(meta && meta->getMetaType() == DocumentMetadataFields::MetaType::kSortKey)) {
            _deps.requiresDocument = true;
        }
    }
    void visit(BooleanConstantASTNode* node) {}

    ProjectionDependencies extractResult() {
        return std::move(_deps);
    }

private:
    ProjectionDependencies _deps;
};

struct VisitorContext {
    std::stack<std::list<std::string>> fieldNames;
    std::string currentPath;
};

// Uses a DepsTracker to determine which fields are required from the projection.
class DepsAnalysisPreVisitor : public ProjectionASTVisitor {
public:
    DepsAnalysisPreVisitor(VisitorContext* ctx)
        : _fieldDependencyTracker(DepsTracker::kAllMetadataAvailable), _context(ctx) {}

    void visit(MatchExpressionASTNode* node) {}
    void visit(ProjectionPathASTNode* node) {
        if (node->parent()) {
            std::string path = _context->fieldNames.top().front();
            _context->fieldNames.top().pop_front();
            _context->currentPath = FieldPath::getFullyQualifiedPath(_context->currentPath, path);
        }

        _context->fieldNames.push(
            std::list<std::string>(node->fieldNames().begin(), node->fieldNames().end()));
    }


    void visit(ProjectionPositionalASTNode* node) {
        // Positional projection on a.b.c.$ may actually modify a, a.b, a.b.c, etc.
        // Treat all of these as dependencies.

        // TODO: This may not be strictly necessary.
        addAllSubPathsAsDependencies();
    }
    void visit(ProjectionSliceASTNode* node) {
        // find() $slice on a.b.c may modify a, a.b, and a.b.c if they're all arrays.
        // Add them all as dependencies.

        // TODO: This may not be strictly necessary.
        addAllSubPathsAsDependencies();
    }
    void visit(ProjectionElemMatchASTNode* node) {
        _fieldDependencyTracker.fields.insert(getFullFieldName());
    }
    void visit(ExpressionASTNode* node) {
        // The output of a computed field depends on whether that field is an array.
        _fieldDependencyTracker.fields.insert(getFullFieldName());
        node->expression()->addDependencies(&_fieldDependencyTracker);
    }
    void visit(BooleanConstantASTNode* node) {
        // For inclusions, we depend on the field.
        if (node->value()) {
            _fieldDependencyTracker.fields.insert(getFullFieldName());
        } else {
            // Still pop the field name from the stack.
            _context->fieldNames.top().pop_front();
        }
    }

    std::vector<std::string> requiredFields() {
        return std::vector<std::string>(_fieldDependencyTracker.fields.begin(),
                                        _fieldDependencyTracker.fields.end());
    }

    DepsTracker* depsTracker() {
        return &_fieldDependencyTracker;
    }

private:
    std::string getFullFieldName() {
        invariant(!_context->fieldNames.empty());
        invariant(!_context->fieldNames.top().empty());
        auto lastPart = _context->fieldNames.top().front();
        _context->fieldNames.top().pop_front();

        return FieldPath::getFullyQualifiedPath(_context->currentPath, lastPart);
    }

    void addAllSubPathsAsDependencies() {
        FieldPath fp(getFullFieldName());
        for (size_t i = 0; i < fp.getPathLength(); i++) {
            _fieldDependencyTracker.fields.insert(fp.getSubpath(i).toString());
        }
    }

    DepsTracker _fieldDependencyTracker;

    VisitorContext* _context;
};

// Visitor which helps maintain the field path context for the deps analysis.
class DepsAnalysisPostVisitor : public ProjectionASTVisitor {
public:
    DepsAnalysisPostVisitor(VisitorContext* context) : _context(context) {}

    virtual void visit(MatchExpressionASTNode* node) {}
    virtual void visit(ProjectionPathASTNode* node) {
        // Make sure all of the children used their field names.
        for (auto&& s : _context->fieldNames.top()) {
            std::cout << "ian: remaining field names " << s << std::endl;
        }
        invariant(_context->fieldNames.top().empty());
        _context->fieldNames.pop();

        if (!_context->currentPath.empty()) {
            FieldPath fp(_context->currentPath);
            if (fp.getPathLength() == 1) {
                _context->currentPath.clear();
            } else {
                _context->currentPath = fp.getSubpath(fp.getPathLength() - 2).toString();
            }
        }
    }
    virtual void visit(ProjectionPositionalASTNode* node) {}

    virtual void visit(ProjectionSliceASTNode* node) {}
    virtual void visit(ProjectionElemMatchASTNode* node) {}
    virtual void visit(ExpressionASTNode* node) {}
    virtual void visit(BooleanConstantASTNode* node) {}

private:
    VisitorContext* _context;
};

class DepsWalker {
public:
    DepsWalker(ProjectType type)
        : _preVisitor(&context), _postVisitor(&context), _projectionType(type) {}

    void preVisit(ASTNode* node) {
        node->acceptVisitor(&_generalAnalysisVisitor);

        // Only do this analysis on inclusion projections, as exclusion projections always require
        // the whole document.
        if (_projectionType == ProjectType::kInclusion) {
            node->acceptVisitor(&_preVisitor);
        }
    }

    void postVisit(ASTNode* node) {
        if (_projectionType == ProjectType::kInclusion) {
            node->acceptVisitor(&_postVisitor);
        }
    }

    void inVisit(long count, ASTNode* node) {}

    ProjectionDependencies done() {
        ProjectionDependencies res = _generalAnalysisVisitor.extractResult();
        if (_projectionType == ProjectType::kInclusion) {
            res.requiredFields = _preVisitor.requiredFields();

            auto* depsTracker = _preVisitor.depsTracker();
            res.needsTextScore =
                depsTracker->getNeedsMetadata(DepsTracker::MetadataType::TEXT_SCORE);
            res.needsGeoPoint =
                depsTracker->getNeedsMetadata(DepsTracker::MetadataType::GEO_NEAR_POINT);
            res.needsGeoDistance =
                depsTracker->getNeedsMetadata(DepsTracker::MetadataType::GEO_NEAR_DISTANCE);
            res.needsSortKey = depsTracker->getNeedsMetadata(DepsTracker::MetadataType::SORT_KEY);
        }

        return res;
    }

private:
    VisitorContext context;
    ProjectionAnalysisVisitor _generalAnalysisVisitor;

    DepsAnalysisPreVisitor _preVisitor;
    DepsAnalysisPostVisitor _postVisitor;

    ProjectType _projectionType;
};

}  // namespace


ProjectionDependencies Projection::analyzeProjection(ProjectionPathASTNode* root,
                                                     ProjectType type) {
    DepsWalker walker(type);
    projection_ast_walker::walk(&walker, root);
    ProjectionDependencies deps = walker.done();

    if (type == ProjectType::kExclusion) {
        deps.requiresDocument = true;
    }
    return deps;
}

Projection::Projection(ProjectionPathASTNode root, ProjectType type, const BSONObj& bson)
    : _root(std::move(root)), _type(type), _deps(analyzeProjection(&_root, type)), _bson(bson) {}

}  // namespace projection_ast
}  // namespace mongo
