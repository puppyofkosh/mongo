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

#include "mongo/db/query/find_projection_ast.h"

namespace mongo {
namespace {
bool isPositionalOperator(const char* fieldName) {
    return str::contains(fieldName, ".$") && !str::contains(fieldName, ".$ref") &&
        !str::contains(fieldName, ".$id") && !str::contains(fieldName, ".$db");
}

bool hasPositionalOperatorMatch(const MatchExpression* const query, StringData matchfield) {
    if (query->getCategory() == MatchExpression::MatchCategory::kLogical) {
        for (unsigned int i = 0; i < query->numChildren(); ++i) {
            if (hasPositionalOperatorMatch(query->getChild(i), matchfield)) {
                return true;
            }
        }
    } else {
        StringData queryPath = query->path();
        // We have to make a distinction between match expressions that are
        // initialized with an empty field/path name "" and match expressions
        // for which the path is not meaningful (eg. $where).
        if (!queryPath.rawData()) {
            return false;
        }
        StringData pathPrefix = str::before(queryPath, '.');
        return pathPrefix == matchfield;
    }
    return false;
}
}

namespace find_projection_ast {
namespace {
void addNodeAtPath(ProjectionASTNodeInternalBase* root,
                   const FieldPath& path,
                   const FieldPath& originalPath,
                   std::unique_ptr<find_projection_ast::ProjectionASTNode> newChild) {
    invariant(root);
    invariant(path.getPathLength() > 0);
    const auto nextComponent = path.getFieldName(0);

    ProjectionASTNode* child = root->getChild(nextComponent);

    if (path.getPathLength() == 1) {
        if (child) {
            uasserted(ErrorCodes::BadValue,
                      str::stream() << "path collision at " << originalPath.fullPath());
        }

        root->children.push_back(std::make_pair(nextComponent.toString(), std::move(newChild)));
        return;
    }

    if (!child) {
        // TODO: Figure out child ordering issue.
        auto newInternalChild = std::make_unique<ProjectionASTNodeInternal<ProjectionASTNode>>(
            Children<ProjectionASTNode>{});
        auto rawInternalChild = newInternalChild.get();
        root->children.push_back(
            std::make_pair(nextComponent.toString(), std::move(newInternalChild)));
        addNodeAtPath(rawInternalChild, path.tail(), originalPath, std::move(newChild));
        return;
    }

    // Either find or create an internal node.
    if (child->type() != NodeType::INTERNAL) {
        uasserted(ErrorCodes::BadValue,
                  str::stream() << "collision at " << originalPath.fullPath()
                                << " remaining portion "
                                << path.fullPath());
    }

    auto* childInternal = static_cast<ProjectionASTNodeInternal<ProjectionASTNode>*>(child);
    addNodeAtPath(childInternal, path.tail(), originalPath, std::move(newChild));
}
}

FindProjectionAST FindProjectionAST::fromBson(const BSONObj& b,
                                              const MatchExpression* const query) {

    // TODO: Support agg syntax with nesting.

    // It's unfortunate that we need this. It's not stored in the class since the expressions will
    // keep this alive as long as necessary.
    boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(nullptr, nullptr));

    ProjectionASTNodeInternal root(Children<ProjectionASTNode>{});

    bool hasPositional = false;
    bool hasElemMatch = false;
    boost::optional<ProjectType> type;
    for (auto&& elem : b) {
        if (elem.type() == BSONType::Object) {
            FieldPath path(elem.fieldNameStringData());

            BSONObj obj = elem.embeddedObject();
            BSONElement e2 = obj.firstElement();

            if (e2.fieldNameStringData() == "$slice") {
                if (e2.isNumber()) {
                    // This is A-OK.
                    addNodeAtPath(&root,
                                  path,
                                  path,
                                  std::make_unique<ProjectionASTNodeSlice>(0, e2.numberInt()));
                } else if (e2.type() == Array) {
                    BSONObj arr = e2.embeddedObject();
                    if (2 != arr.nFields()) {
                        uasserted(ErrorCodes::BadValue, "$slice array wrong size");
                    }

                    BSONObjIterator it(arr);
                    int skip = it.next().numberInt();
                    // Skip over 'skip'.
                    it.next();
                    int limit = it.next().numberInt();
                    if (limit <= 0) {
                        uasserted(ErrorCodes::BadValue, "$slice limit must be positive");
                    }
                    addNodeAtPath(
                        &root, path, path, std::make_unique<ProjectionASTNodeSlice>(skip, limit));
                } else {
                    uasserted(ErrorCodes::BadValue,
                              "$slice only supports numbers and [skip, limit] arrays");
                }
            } else if (e2.fieldNameStringData() == "$elemMatch") {
                // Validate $elemMatch arguments and dependencies.
                if (Object != e2.type()) {
                    uasserted(ErrorCodes::BadValue,
                              "elemMatch: Invalid argument, object required.");
                }

                if (hasPositional) {
                    uasserted(ErrorCodes::BadValue,
                              "Cannot specify positional operator and $elemMatch.");
                }

                if (str::contains(elem.fieldName(), '.')) {
                    uasserted(ErrorCodes::BadValue,
                              "Cannot use $elemMatch projection on a nested field.");
                }

                // Create a MatchExpression for the elemMatch.
                BSONObj elemMatchObj = elem.wrap();
                invariant(elemMatchObj.isOwned());

                // Not parsing the match expression itself because it would require an
                // expressionContext + operationContext.

                addNodeAtPath(
                    &root, path, path, std::make_unique<ProjectionASTNodeElemMatch>(elemMatchObj));
                hasElemMatch = true;
            } else {
                // Some other expression which will get parsed later. Ideally we'd parse it into
                // some kind of "expression syntax tree" here.
                std::cout << "attempting to parse " << obj << std::endl;
                auto expr = Expression::parseExpression(expCtx, obj, expCtx->variablesParseState);
                addNodeAtPath(&root,
                              path,
                              path,
                              std::make_unique<ProjectionASTNodeOtherExpression>(obj, expr));
            }

        } else if (elem.trueValue()) {
            if (!isPositionalOperator(elem.fieldName())) {
                FieldPath path(elem.fieldNameStringData());
                addNodeAtPath(&root, path, path, std::make_unique<ProjectionASTNodeInclusion>());
            } else {
                FieldPath path(str::before(elem.fieldNameStringData(), ".$"));
                addNodeAtPath(&root, path, path, std::make_unique<ProjectionASTNodePositional>());

                if (hasPositional) {
                    uasserted(ErrorCodes::BadValue,
                              "Cannot specify more than one positional proj. per query.");
                }

                if (hasElemMatch) {
                    uasserted(ErrorCodes::BadValue,
                              "Cannot specify positional operator and $elemMatch.");
                }

                StringData after = str::after(elem.fieldNameStringData(), ".$");
                if (after.find(".$"_sd) != std::string::npos) {
                    str::stream ss;
                    ss << "Positional projection '" << elem.fieldName() << "' contains "
                       << "the positional operator more than once.";
                    uasserted(ErrorCodes::BadValue, ss);
                }

                StringData matchfield = str::before(elem.fieldNameStringData(), '.');
                if (query && !hasPositionalOperatorMatch(query, matchfield)) {
                    str::stream ss;
                    ss << "Positional projection '" << elem.fieldName() << "' does not "
                       << "match the query document.";
                    uasserted(ErrorCodes::BadValue, ss);
                }

                hasPositional = true;
            }

            uassert(ErrorCodes::BadValue,
                    "Should be inclusion",
                    !type || *type == ProjectType::kInclusion);
            type = ProjectType::kInclusion;
        } else {
            invariant(!elem.trueValue());
            FieldPath path(elem.fieldNameStringData());
            addNodeAtPath(&root, path, path, std::make_unique<ProjectionASTNodeExclusion>());

            if (elem.fieldNameStringData() != "_id") {
                uassert(ErrorCodes::BadValue,
                        "Should be exclusion",
                        !type || *type == ProjectType::kExclusion);
                type = ProjectType::kExclusion;
            }
        }
    }

    return FindProjectionAST{std::move(root), type ? *type : ProjectType::kExclusion};
}

namespace {
std::unique_ptr<ProjectionASTNodeCommon> internalBaseToCommon(
    std::unique_ptr<ProjectionASTNode> b) {
    invariant(b->type() == NodeType::INTERNAL || b->commonToAggAndFind());

    return std::unique_ptr<ProjectionASTNodeCommon>(
        static_cast<ProjectionASTNodeCommon*>(b.release()));
}
}

void desugarHelper(const FindProjectionAST& originalAST,
                   std::vector<SliceInfo>* sliceInfo,
                   boost::optional<PositionalInfo>* positionalInfo,
                   std::string pathSoFar,
                   ProjectionASTNodeInternalBase* originalNode,
                   ProjectionASTNodeInternalCommon* newNode) {
    invariant(sliceInfo);
    invariant(positionalInfo);

    for (auto&& child : originalNode->children) {
        const auto& field = child.first;
        auto* node = child.second.get();

        std::string childPath = pathSoFar.empty() ? field : pathSoFar + "." + field;

        if (node->type() == NodeType::INTERNAL) {
            auto newChild = std::make_unique<ProjectionASTNodeInternalCommon>(
                Children<ProjectionASTNodeCommon>{});

            desugarHelper(originalAST,
                          sliceInfo,
                          positionalInfo,
                          childPath,
                          static_cast<ProjectionASTNodeInternalBase*>(node),
                          newChild.get());

            if (!newChild->children.empty()) {
                newNode->children.push_back(std::make_pair(field, std::move(newChild)));
            }

        } else if (node->commonToAggAndFind()) {
            // Common case. Keep the node and move on.
            newNode->children.push_back(
                std::make_pair(field, internalBaseToCommon(std::move(child.second))));
        } else if (node->type() == NodeType::INCLUSION_POSITIONAL) {
            invariant(originalAST.type == ProjectType::kInclusion);
            invariant(!*positionalInfo);

            // Replace the positional projection with an inclusion, and update the positional info.
            newNode->children.push_back(
                std::make_pair(field, std::make_unique<ProjectionASTNodeInclusion>()));

            *positionalInfo = PositionalInfo{childPath};
        } else if (node->type() == NodeType::EXPRESSION_SLICE) {
            auto* sliceNode = static_cast<ProjectionASTNodeSlice*>(node);
            // Update the sliceInfo
            sliceInfo->push_back(SliceInfo{childPath, sliceNode->skip, sliceNode->limit});

            // If this is an exclusion projection, then we don't add any nodes.
            if (originalAST.type == ProjectType::kExclusion) {
                continue;
            }

            // If it's an inclusion projection, replace the node with an inclusion.
            newNode->children.push_back(
                std::make_pair(field, std::make_unique<ProjectionASTNodeInclusion>()));
        } else if (node->type() == NodeType::EXPRESSION_ELEMMATCH) {
            // I don't feel like doing this case for skunkworks.
            MONGO_UNREACHABLE;
        } else {
            MONGO_UNREACHABLE;
        }
    }
}

ProjectionASTCommon desugar(FindProjectionAST ast) {
    std::vector<SliceInfo> sliceInfo;
    boost::optional<PositionalInfo> posInfo;
    ProjectionASTNodeInternal<ProjectionASTNodeCommon> root({});

    desugarHelper(ast, &sliceInfo, &posInfo, "", &ast.root, &root);

    return ProjectionASTCommon(std::move(root), ast.type, std::move(sliceInfo), std::move(posInfo));
}
}
}
