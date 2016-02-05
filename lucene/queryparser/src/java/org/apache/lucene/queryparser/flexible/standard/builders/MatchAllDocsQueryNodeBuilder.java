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

import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchAllDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;
import org.apache.lucene.search.MatchAllDocsQuery;

/**
 * Builds a {@link MatchAllDocsQuery} object from a
 * {@link MatchAllDocsQueryNode} object.
 */
public class MatchAllDocsQueryNodeBuilder implements StandardQueryBuilder {

  public MatchAllDocsQueryNodeBuilder() {
    // empty constructor
  }

  @Override
  public MatchAllDocsQuery build(QueryNode queryNode) throws QueryNodeException {

    // validates node
    if (!(queryNode instanceof MatchAllDocsQueryNode)) {
      throw new QueryNodeException(new MessageImpl(
          QueryParserMessages.LUCENE_QUERY_CONVERSION_ERROR, queryNode
              .toQueryString(new EscapeQuerySyntaxImpl()), queryNode.getClass()
              .getName()));
    }

    return new MatchAllDocsQuery();

  }

}
