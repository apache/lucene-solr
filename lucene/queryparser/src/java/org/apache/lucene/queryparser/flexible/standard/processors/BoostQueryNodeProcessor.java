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
import org.apache.lucene.queryparser.flexible.core.config.FieldConfig;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.BoostQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;

/**
 * This processor iterates the query node tree looking for every {@link FieldableNode} that has
 * {@link ConfigurationKeys#BOOST} in its config. If there is, the boost is applied to that {@link
 * FieldableNode}.
 *
 * @see ConfigurationKeys#BOOST
 * @see QueryConfigHandler
 * @see FieldableNode
 */
public class BoostQueryNodeProcessor extends QueryNodeProcessorImpl {

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    if (node instanceof FieldableNode
        && (node.getParent() == null || !(node.getParent() instanceof FieldableNode))) {

      FieldableNode fieldNode = (FieldableNode) node;
      QueryConfigHandler config = getQueryConfigHandler();

      if (config != null) {
        CharSequence field = fieldNode.getField();
        FieldConfig fieldConfig = config.getFieldConfig(StringUtils.toString(field));

        if (fieldConfig != null) {
          Float boost = fieldConfig.get(ConfigurationKeys.BOOST);

          if (boost != null) {
            return new BoostQueryNode(node, boost);
          }
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
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children) throws QueryNodeException {

    return children;
  }
}
