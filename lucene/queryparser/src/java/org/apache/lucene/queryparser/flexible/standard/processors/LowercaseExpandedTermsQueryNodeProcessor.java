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
import java.util.Locale;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.TextableQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.UnescapedCharSequence;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.WildcardQueryNode;

/**
 * This processor verifies if 
 * {@link ConfigurationKeys#LOWERCASE_EXPANDED_TERMS} is defined in the
 * {@link QueryConfigHandler}. If it is and the expanded terms should be
 * lower-cased, it looks for every {@link WildcardQueryNode},
 * {@link FuzzyQueryNode} and children of a {@link RangeQueryNode} and lower-case its
 * term.
 * 
 * @see ConfigurationKeys#LOWERCASE_EXPANDED_TERMS
 */
public class LowercaseExpandedTermsQueryNodeProcessor extends
    QueryNodeProcessorImpl {

  public LowercaseExpandedTermsQueryNodeProcessor() {
  }

  @Override
  public QueryNode process(QueryNode queryTree) throws QueryNodeException {
    Boolean lowercaseExpandedTerms = getQueryConfigHandler().get(ConfigurationKeys.LOWERCASE_EXPANDED_TERMS);

    if (lowercaseExpandedTerms != null && lowercaseExpandedTerms) {
      return super.process(queryTree);
    }

    return queryTree;

  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {
    
    Locale locale = getQueryConfigHandler().get(ConfigurationKeys.LOCALE);
    if (locale == null) {
      locale = Locale.getDefault();
    }

    if (node instanceof WildcardQueryNode
        || node instanceof FuzzyQueryNode
        || (node instanceof FieldQueryNode && node.getParent() instanceof RangeQueryNode)
        || node instanceof RegexpQueryNode) {

      TextableQueryNode txtNode = (TextableQueryNode) node;
      CharSequence text = txtNode.getText();
      txtNode.setText(text != null ? UnescapedCharSequence.toLowerCase(text, locale) : null);
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
