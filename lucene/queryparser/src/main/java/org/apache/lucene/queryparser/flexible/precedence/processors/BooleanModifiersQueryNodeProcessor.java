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
package org.apache.lucene.queryparser.flexible.precedence.processors;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.precedence.PrecedenceQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.Operator;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.*;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode.Modifier;

/**
 * <p>
 * This processor is used to apply the correct {@link ModifierQueryNode} to {@link BooleanQueryNode}s children.
 * </p>
 * <p>
 * It walks through the query node tree looking for {@link BooleanQueryNode}s. If an {@link AndQueryNode} is found,
 * every child, which is not a {@link ModifierQueryNode} or the {@link ModifierQueryNode} 
 * is {@link Modifier#MOD_NONE}, becomes a {@link Modifier#MOD_REQ}. For any other
 * {@link BooleanQueryNode} which is not an {@link OrQueryNode}, it checks the default operator is {@link Operator#AND},
 * if it is, the same operation when an {@link AndQueryNode} is found is applied to it.
 * </p>
 * 
 * @see ConfigurationKeys#DEFAULT_OPERATOR
 * @see PrecedenceQueryParser#setDefaultOperator
 */
public class BooleanModifiersQueryNodeProcessor extends QueryNodeProcessorImpl {

  private ArrayList<QueryNode> childrenBuffer = new ArrayList<>();

  private Boolean usingAnd = false;

  public BooleanModifiersQueryNodeProcessor() {
    // empty constructor
  }

  @Override
  public QueryNode process(QueryNode queryTree) throws QueryNodeException {
    Operator op = getQueryConfigHandler().get(ConfigurationKeys.DEFAULT_OPERATOR);
    
    if (op == null) {
      throw new IllegalArgumentException(
          "StandardQueryConfigHandler.ConfigurationKeys.DEFAULT_OPERATOR should be set on the QueryConfigHandler");
    }

    this.usingAnd = StandardQueryConfigHandler.Operator.AND == op;

    return super.process(queryTree);

  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    if (node instanceof AndQueryNode) {
      this.childrenBuffer.clear();
      List<QueryNode> children = node.getChildren();

      for (QueryNode child : children) {
        this.childrenBuffer.add(applyModifier(child, ModifierQueryNode.Modifier.MOD_REQ));
      }

      node.set(this.childrenBuffer);

    } else if (this.usingAnd && node instanceof BooleanQueryNode
        && !(node instanceof OrQueryNode)) {

      this.childrenBuffer.clear();
      List<QueryNode> children = node.getChildren();

      for (QueryNode child : children) {
        this.childrenBuffer.add(applyModifier(child, ModifierQueryNode.Modifier.MOD_REQ));
      }

      node.set(this.childrenBuffer);

    }

    return node;

  }

  private QueryNode applyModifier(QueryNode node, ModifierQueryNode.Modifier mod) {

    // check if modifier is not already defined and is default
    if (!(node instanceof ModifierQueryNode)) {
      return new ModifierQueryNode(node, mod);

    } else {
      ModifierQueryNode modNode = (ModifierQueryNode) node;

      if (modNode.getModifier() == ModifierQueryNode.Modifier.MOD_NONE) {
        return new ModifierQueryNode(modNode.getChild(), mod);
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
