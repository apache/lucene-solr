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

import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode.Modifier;
import org.apache.lucene.queryparser.flexible.standard.nodes.StandardBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery.TooManyClauses;
import org.apache.lucene.search.similarities.Similarity;

/**
 * This builder does the same as the {@link BooleanQueryNodeBuilder}, but this
 * considers if the built {@link BooleanQuery} should have its coord disabled or
 * not. <br>
 * 
 * @see BooleanQueryNodeBuilder
 * @see BooleanQuery
 * @see Similarity#coord(int, int)
 */
public class StandardBooleanQueryNodeBuilder implements StandardQueryBuilder {

  public StandardBooleanQueryNodeBuilder() {
    // empty constructor
  }

  @Override
  public BooleanQuery build(QueryNode queryNode) throws QueryNodeException {
    StandardBooleanQueryNode booleanNode = (StandardBooleanQueryNode) queryNode;

    BooleanQuery.Builder bQuery = new BooleanQuery.Builder();
    bQuery.setDisableCoord(booleanNode.isDisableCoord());
    List<QueryNode> children = booleanNode.getChildren();

    if (children != null) {

      for (QueryNode child : children) {
        Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

        if (obj != null) {
          Query query = (Query) obj;

          try {
            bQuery.add(query, getModifierValue(child));
          } catch (TooManyClauses ex) {

            throw new QueryNodeException(new MessageImpl(
                QueryParserMessages.TOO_MANY_BOOLEAN_CLAUSES, BooleanQuery
                    .getMaxClauseCount(), queryNode
                    .toQueryString(new EscapeQuerySyntaxImpl())), ex);

          }

        }

      }

    }

    return bQuery.build();

  }

  private static BooleanClause.Occur getModifierValue(QueryNode node) {

    if (node instanceof ModifierQueryNode) {
      ModifierQueryNode mNode = ((ModifierQueryNode) node);
      Modifier modifier = mNode.getModifier();

      if (Modifier.MOD_NONE.equals(modifier)) {
        return BooleanClause.Occur.SHOULD;

      } else if (Modifier.MOD_NOT.equals(modifier)) {
        return BooleanClause.Occur.MUST_NOT;

      } else {
        return BooleanClause.Occur.MUST;
      }
    }

    return BooleanClause.Occur.SHOULD;

  }

}
