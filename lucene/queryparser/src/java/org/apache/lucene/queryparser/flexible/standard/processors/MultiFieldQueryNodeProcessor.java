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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.BooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;

/**
 * This processor is used to expand terms so the query looks for the same term
 * in different fields. It also boosts a query based on its field. <br>
 * <br>
 * This processor looks for every {@link FieldableNode} contained in the query
 * node tree. If a {@link FieldableNode} is found, it checks if there is a
 * {@link ConfigurationKeys#MULTI_FIELDS} defined in the {@link QueryConfigHandler}. If
 * there is, the {@link FieldableNode} is cloned N times and the clones are
 * added to a {@link BooleanQueryNode} together with the original node. N is
 * defined by the number of fields that it will be expanded to. The
 * {@link BooleanQueryNode} is returned.
 * 
 * @see ConfigurationKeys#MULTI_FIELDS
 */
public class MultiFieldQueryNodeProcessor extends QueryNodeProcessorImpl {

  private boolean processChildren = true;

  public MultiFieldQueryNodeProcessor() {
    // empty constructor
  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    return node;

  }

  @Override
  protected void processChildren(QueryNode queryTree) throws QueryNodeException {

    if (this.processChildren) {
      super.processChildren(queryTree);

    } else {
      this.processChildren = true;
    }

  }

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

    if (node instanceof FieldableNode) {
      this.processChildren = false;
      FieldableNode fieldNode = (FieldableNode) node;

      if (fieldNode.getField() == null) {
        CharSequence[] fields = getQueryConfigHandler().get(ConfigurationKeys.MULTI_FIELDS);

        if (fields == null) {
          throw new IllegalArgumentException(
              "StandardQueryConfigHandler.ConfigurationKeys.MULTI_FIELDS should be set on the QueryConfigHandler");
        }

        if (fields != null && fields.length > 0) {
          fieldNode.setField(fields[0]);

          if (fields.length == 1) {
            return fieldNode;
          } else {
            List<QueryNode> children = new ArrayList<>(fields.length);

            children.add(fieldNode);
            for (int i = 1; i < fields.length; i++) {
              try {
                fieldNode = (FieldableNode) fieldNode.cloneTree();
                fieldNode.setField(fields[i]);

                children.add(fieldNode);
              } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
              }
            }

            return new GroupQueryNode(new OrQueryNode(children));
          }
        }
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
