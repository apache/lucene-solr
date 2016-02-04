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
package org.apache.lucene.queryparser.flexible.standard.processors;

import java.util.List;

import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;
import org.apache.lucene.queryparser.flexible.standard.parser.EscapeQuerySyntaxImpl;

/**
 * This processor verifies if
 * {@link ConfigurationKeys#ALLOW_LEADING_WILDCARD} is defined in the
 * {@link QueryConfigHandler}. If it is and leading wildcard is not allowed, it
 * looks for every {@link WildcardQueryNode} contained in the query node tree
 * and throws an exception if any of them has a leading wildcard ('*' or '?').
 * 
 * @see ConfigurationKeys#ALLOW_LEADING_WILDCARD
 */
public class AllowLeadingWildcardProcessor extends QueryNodeProcessorImpl {

  public AllowLeadingWildcardProcessor() {
    // empty constructor
  }

  @Override
  public QueryNode process(QueryNode queryTree) throws QueryNodeException {
    Boolean allowsLeadingWildcard = getQueryConfigHandler().get(ConfigurationKeys.ALLOW_LEADING_WILDCARD);

    if (allowsLeadingWildcard != null) {

      if (!allowsLeadingWildcard) {
        return super.process(queryTree);
      }

    }

    return queryTree;
  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    if (node instanceof WildcardQueryNode) {
      WildcardQueryNode wildcardNode = (WildcardQueryNode) node;

      if (wildcardNode.getText().length() > 0) {
        
        // Validate if the wildcard was escaped
        if (UnescapedCharSequence.wasEscaped(wildcardNode.getText(), 0))
          return node;
        
        switch (wildcardNode.getText().charAt(0)) {    
          case '*':
          case '?':
            throw new QueryNodeException(new MessageImpl(
                QueryParserMessages.LEADING_WILDCARD_NOT_ALLOWED, node
                    .toQueryString(new EscapeQuerySyntaxImpl())));    
        }
      }

    }

    return node;

  }

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

    return node;

  }

  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
      throws QueryNodeException {

    return children;

  }

}
