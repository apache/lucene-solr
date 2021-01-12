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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.MultiPhraseQueryNode;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.TermQuery;

/** Builds a {@link MultiPhraseQuery} object from a {@link MultiPhraseQueryNode} object. */
public class MultiPhraseQueryNodeBuilder implements StandardQueryBuilder {

  public MultiPhraseQueryNodeBuilder() {
    // empty constructor
  }

  @Override
  public MultiPhraseQuery build(QueryNode queryNode) throws QueryNodeException {
    MultiPhraseQueryNode phraseNode = (MultiPhraseQueryNode) queryNode;

    MultiPhraseQuery.Builder phraseQueryBuilder = new MultiPhraseQuery.Builder();

    List<QueryNode> children = phraseNode.getChildren();

    if (children != null) {
      TreeMap<Integer, List<Term>> positionTermMap = new TreeMap<>();

      for (QueryNode child : children) {
        FieldQueryNode termNode = (FieldQueryNode) child;
        TermQuery termQuery =
            (TermQuery) termNode.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
        List<Term> termList = positionTermMap.get(termNode.getPositionIncrement());

        if (termList == null) {
          termList = new LinkedList<>();
          positionTermMap.put(termNode.getPositionIncrement(), termList);
        }

        termList.add(termQuery.getTerm());
      }

      for (Map.Entry<Integer, List<Term>> entry : positionTermMap.entrySet()) {
        List<Term> termList = entry.getValue();
        phraseQueryBuilder.add(termList.toArray(new Term[termList.size()]), entry.getKey());
      }
    }

    return phraseQueryBuilder.build();
  }
}
