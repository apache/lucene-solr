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

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.SynonymQueryNode;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/** Builder for {@link SynonymQueryNode}. */
public class SynonymQueryNodeBuilder implements StandardQueryBuilder {

  /** Sole constructor. */
  public SynonymQueryNodeBuilder() {}

  @Override
  public Query build(QueryNode queryNode) throws QueryNodeException {
    // TODO: use SynonymQuery instead
    SynonymQueryNode node = (SynonymQueryNode) queryNode;
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (QueryNode child : node.getChildren()) {
      Object obj = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);

      if (obj != null) {
        Query query = (Query) obj;
        builder.add(query, Occur.SHOULD);
      }
    }
    return builder.build();
  }
}
