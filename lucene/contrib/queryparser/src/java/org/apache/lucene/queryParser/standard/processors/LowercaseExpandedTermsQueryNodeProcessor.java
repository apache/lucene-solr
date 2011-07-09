package org.apache.lucene.queryParser.standard.processors;

/**
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

import java.util.List;

import org.apache.lucene.queryParser.core.QueryNodeException;
import org.apache.lucene.queryParser.core.config.QueryConfigHandler;
import org.apache.lucene.queryParser.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryParser.core.nodes.ParametricQueryNode;
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.nodes.TextableQueryNode;
import org.apache.lucene.queryParser.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryParser.core.util.UnescapedCharSequence;
import org.apache.lucene.queryParser.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryParser.standard.nodes.RegexpQueryNode;
import org.apache.lucene.queryParser.standard.nodes.WildcardQueryNode;

/**
 * This processor verifies if 
 * {@link ConfigurationKeys#LOWERCASE_EXPANDED_TERMS} is defined in the
 * {@link QueryConfigHandler}. If it is and the expanded terms should be
 * lower-cased, it looks for every {@link WildcardQueryNode},
 * {@link FuzzyQueryNode} and {@link ParametricQueryNode} and lower-case its
 * term. <br/>
 * 
 * @see ConfigurationKeys#LOWERCASE_EXPANDED_TERMS
 */
public class LowercaseExpandedTermsQueryNodeProcessor extends
    QueryNodeProcessorImpl {

  public LowercaseExpandedTermsQueryNodeProcessor() {
    // empty constructor
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

    if (node instanceof WildcardQueryNode || node instanceof FuzzyQueryNode
        || node instanceof ParametricQueryNode || node instanceof RegexpQueryNode) {

      TextableQueryNode txtNode = (TextableQueryNode) node;
      txtNode.setText(UnescapedCharSequence.toLowerCase(txtNode.getText()));
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
