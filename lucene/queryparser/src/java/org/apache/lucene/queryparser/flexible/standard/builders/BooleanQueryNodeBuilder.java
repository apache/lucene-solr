/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.queryparser.flexible.standard.builders;

import java.util.List;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.IndexSearcher.TooManyClauses;
import org.apache.lucene.search.Query;

/**
 * Builds a {@link BooleanQuery} object from a {@link BooleanQueryNode} object. Every children in
 * the {@link BooleanQueryNode} object must be already tagged using {@link
 * QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} with a {@link Query} object. <br>
 * <br>
 * It takes in consideration if the children is a {@link ModifierQueryNode} to define the {@link
 * BooleanClause}.
 */
public class BooleanQueryNodeBuilder implements StandardQueryBuilder {

  public BooleanQueryNodeBuilder() {
    // empty constructor
  }

  @Override
  public BooleanQuery build(QueryNode queryNode) throws QueryNodeException {
    BooleanQueryNode booleanNode = (BooleanQueryNode) queryNode;

    BooleanQuery.Builder bQuery = new BooleanQuery.Builder();
    List<QueryNode> children = booleanNode.getChildren();

    if (children != null) {

      for (QueryNode child : children) {
        Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

        if (obj != null) {
          Query query = (Query) obj;

          try {
            bQuery.add(query, getModifierValue(child));

          } catch (TooManyClauses ex) {

            throw new QueryNodeException(
                new MessageImpl(
                    QueryParserMessages.TOO_MANY_BOOLEAN_CLAUSES,
                    IndexSearcher.getMaxClauseCount(),
                    queryNode.toQueryString(new EscapeQuerySyntaxImpl())),
                ex);
          }
        }
      }
    }

    return bQuery.build();
  }

  private static BooleanClause.Occur getModifierValue(QueryNode node) {

    if (node instanceof ModifierQueryNode) {
      ModifierQueryNode mNode = ((ModifierQueryNode) node);
      switch (mNode.getModifier()) {
        case MOD_REQ:
          return BooleanClause.Occur.MUST;

        case MOD_NOT:
          return BooleanClause.Occur.MUST_NOT;

        case MOD_NONE:
          return BooleanClause.Occur.SHOULD;
      }
    }

    return BooleanClause.Occur.SHOULD;
  }
}
