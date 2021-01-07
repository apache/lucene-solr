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
package org.apache.lucene.queryparser.flexible.core.processors;

import java.util.Iterator;
import java.util.List;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.DeletedQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * A {@link QueryNodeProcessorPipeline} class removes every instance of {@link DeletedQueryNode}
 * from a query node tree. If the resulting root node is a {@link DeletedQueryNode}, {@link
 * MatchNoDocsQueryNode} is returned.
 */
public class RemoveDeletedQueryNodesProcessor extends QueryNodeProcessorImpl {

  public RemoveDeletedQueryNodesProcessor() {
    // empty constructor
  }

  @Override
  public QueryNode process(QueryNode queryTree) throws QueryNodeException {
    queryTree = super.process(queryTree);

    if (queryTree instanceof DeletedQueryNode && !(queryTree instanceof MatchNoDocsQueryNode)) {

      return new MatchNoDocsQueryNode();
    }

    return queryTree;
  }

  @Override
  protected QueryNode postProcessNode(QueryNode node) throws QueryNodeException {

    if (!node.isLeaf()) {
      List<QueryNode> children = node.getChildren();
      boolean removeBoolean = false;

      if (children == null || children.size() == 0) {
        removeBoolean = true;

      } else {
        removeBoolean = true;

        for (Iterator<QueryNode> it = children.iterator(); it.hasNext(); ) {

          if (!(it.next() instanceof DeletedQueryNode)) {
            removeBoolean = false;
            break;
          }
        }
      }

      if (removeBoolean) {
        return new DeletedQueryNode();
      }
    }

    return node;
  }

  @Override
  protected List<QueryNode> setChildrenOrder(List<QueryNode> children) throws QueryNodeException {

    for (int i = 0; i < children.size(); i++) {

      if (children.get(i) instanceof DeletedQueryNode) {
        children.remove(i--);
      }
    }

    return children;
  }

  @Override
  protected QueryNode preProcessNode(QueryNode node) throws QueryNodeException {

    return node;
  }
}
