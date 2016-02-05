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

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.FuzzyQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.config.FuzzyConfig;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.search.FuzzyQuery;

/**
 * This processor iterates the query node tree looking for every
 * {@link FuzzyQueryNode}, when this kind of node is found, it checks on the
 * query configuration for
 * {@link ConfigurationKeys#FUZZY_CONFIG}, gets the
 * fuzzy prefix length and default similarity from it and set to the fuzzy node.
 * For more information about fuzzy prefix length check: {@link FuzzyQuery}.
 * 
 * @see ConfigurationKeys#FUZZY_CONFIG
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

      FuzzyConfig fuzzyConfig = null;
      
      if (config != null && (fuzzyConfig = config.get(ConfigurationKeys.FUZZY_CONFIG)) != null) {
        fuzzyNode.setPrefixLength(fuzzyConfig.getPrefixLength());

        if (fuzzyNode.getSimilarity() < 0) {
          fuzzyNode.setSimilarity(fuzzyConfig.getMinSimilarity());
        }
        
      } else if (fuzzyNode.getSimilarity() < 0) {
        throw new IllegalArgumentException("No FUZZY_CONFIG set in the config");
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
