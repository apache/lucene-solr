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
import org.apache.lucene.queryParser.core.nodes.QueryNode;
import org.apache.lucene.queryParser.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryParser.standard.config.FuzzyAttribute;
import org.apache.lucene.search.FuzzyQuery;

/**
 * This processor iterates the query node tree looking for every
 * {@link FuzzyQueryNode}, when this kind of node is found, it checks on the
 * query configuration for {@link FuzzyAttribute}, gets the fuzzy prefix length
 * and default similarity from it and set to the fuzzy node. For more
 * information about fuzzy prefix length check: {@link FuzzyQuery}. <br/>
 * 
 * @see FuzzyAttribute
 * @see FuzzyQuery
 * @see FuzzyQueryNode
 */
public class FuzzyQueryNodeProcessor extends QueryNodeProcessorImpl {

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    return node;

  }

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

    if (node instanceof FuzzyQueryNode) {
      FuzzyQueryNode fuzzyNode = (FuzzyQueryNode) node;
      QueryConfigHandler config = getQueryConfigHandler();

      if (config != null && config.hasAttribute(FuzzyAttribute.class)) {
        FuzzyAttribute fuzzyAttr = config.getAttribute(FuzzyAttribute.class);
        fuzzyNode.setPrefixLength(fuzzyAttr.getPrefixLength());

        if (fuzzyNode.getSimilarity() < 0) {
          fuzzyNode.setSimilarity(fuzzyAttr.getFuzzyMinSimilarity());

        }

      } else if (fuzzyNode.getSimilarity() < 0) {
        throw new IllegalArgumentException("No "
            + FuzzyAttribute.class.getName() + " set in the config");
      }

    }

    return node;

  }

  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
      throws QueryNodeException {

    return children;

  }

}
