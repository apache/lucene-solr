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
package org.apache.lucene.queryparser.flexible.spans;

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryBuilder;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;

/**
 * This builder creates {@link SpanOrQuery}s from a {@link BooleanQueryNode}.<br>
 * <br>
 * 
 * It assumes that the {@link BooleanQueryNode} instance has at least one child.
 */
public class SpanOrQueryNodeBuilder implements StandardQueryBuilder {

  @Override
  public SpanOrQuery build(QueryNode node) throws QueryNodeException {

    // validates node
    BooleanQueryNode booleanNode = (BooleanQueryNode) node;

    List<QueryNode> children = booleanNode.getChildren();
    SpanQuery[] spanQueries = new SpanQuery[children.size()];

    int i = 0;
    for (QueryNode child : children) {
      spanQueries[i++] = (SpanQuery) child
          .getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
    }

    return new SpanOrQuery(spanQueries);

  }

}
