package org.apache.lucene.queryparser.flexible.core.processors;

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

import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.*;

/**
 * <p>
 * A {@link NoChildOptimizationQueryNodeProcessor} removes every
 * BooleanQueryNode, BoostQueryNode, TokenizedPhraseQueryNode or
 * ModifierQueryNode that do not have a valid children.
 * </p>
 * <p>
 * Example: When the children of these nodes are removed for any reason then the
 * nodes may become invalid.
 * </p>
 */
public class NoChildOptimizationQueryNodeProcessor extends
    QueryNodeProcessorImpl {

  public NoChildOptimizationQueryNodeProcessor() {
    // empty constructor
  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    if (node instanceof BooleanQueryNode || node instanceof BoostQueryNode
        || node instanceof TokenizedPhraseQueryNode
        || node instanceof ModifierQueryNode) {

      List<QueryNode> children = node.getChildren();

      if (children != null && children.size() > 0) {

        for (QueryNode child : children) {

          if (!(child instanceof DeletedQueryNode)) {
            return node;
          }

        }

      }

      return new MatchNoDocsQueryNode();

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
