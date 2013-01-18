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

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * <p>
 * This is a default implementation for the {@link QueryNodeProcessor}
 * interface, it's an abstract class, so it should be extended by classes that
 * want to process a {@link QueryNode} tree.
 * </p>
 * <p>
 * This class process {@link QueryNode}s from left to right in the tree. While
 * it's walking down the tree, for every node,
 * {@link #preProcessNode(QueryNode)} is invoked. After a node's children are
 * processed, {@link #postProcessNode(QueryNode)} is invoked for that node.
 * {@link #setChildrenOrder(List)} is invoked before
 * {@link #postProcessNode(QueryNode)} only if the node has at least one child,
 * in {@link #setChildrenOrder(List)} the implementor might redefine the
 * children order or remove any children from the children list.
 * </p>
 * <p>
 * Here is an example about how it process the nodes:
 * </p>
 * 
 * <pre>
 *      a
 *     / \
 *    b   e
 *   / \
 *  c   d
 * </pre>
 * 
 * Here is the order the methods would be invoked for the tree described above:
 * 
 * <pre>
 *      preProcessNode( a );
 *      preProcessNode( b );
 *      preProcessNode( c );
 *      postProcessNode( c );
 *      preProcessNode( d );
 *      postProcessNode( d );
 *      setChildrenOrder( bChildrenList );
 *      postProcessNode( b );
 *      preProcessNode( e );
 *      postProcessNode( e );
 *      setChildrenOrder( aChildrenList );
 *      postProcessNode( a )
 * </pre>
 * 
 * @see org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor
 */
public abstract class QueryNodeProcessorImpl implements QueryNodeProcessor {

  private ArrayList<ChildrenList> childrenListPool = new ArrayList<ChildrenList>();

  private QueryConfigHandler queryConfig;

  public QueryNodeProcessorImpl() {
    // empty constructor
  }

  public QueryNodeProcessorImpl(QueryConfigHandler queryConfigHandler) {
    this.queryConfig = queryConfigHandler;
  }

  @Override
  public QueryNode process(QueryNode queryTree) throws QueryNodeException {
    return processIteration(queryTree);
  }

  private QueryNode processIteration(QueryNode queryTree)
      throws QueryNodeException {
    queryTree = preProcessNode(queryTree);

    processChildren(queryTree);

    queryTree = postProcessNode(queryTree);

    return queryTree;

  }

  /**
   * This method is called every time a child is processed.
   * 
   * @param queryTree
   *          the query node child to be processed
   * @throws QueryNodeException
   *           if something goes wrong during the query node processing
   */
  protected void processChildren(QueryNode queryTree) throws QueryNodeException {

    List<QueryNode> children = queryTree.getChildren();
    ChildrenList newChildren;

    if (children != null && children.size() > 0) {

      newChildren = allocateChildrenList();

      try {

        for (QueryNode child : children) {
          child = processIteration(child);

          if (child == null) {
            throw new NullPointerException();

          }

          newChildren.add(child);

        }

        List<QueryNode> orderedChildrenList = setChildrenOrder(newChildren);

        queryTree.set(orderedChildrenList);

      } finally {
        newChildren.beingUsed = false;
      }

    }

  }

  private ChildrenList allocateChildrenList() {
    ChildrenList list = null;

    for (ChildrenList auxList : this.childrenListPool) {

      if (!auxList.beingUsed) {
        list = auxList;
        list.clear();

        break;

      }

    }

    if (list == null) {
      list = new ChildrenList();
      this.childrenListPool.add(list);

    }

    list.beingUsed = true;

    return list;

  }

  /**
   * For reference about this method check:
   * {@link QueryNodeProcessor#setQueryConfigHandler(QueryConfigHandler)}.
   * 
   * @param queryConfigHandler
   *          the query configuration handler to be set.
   * 
   * @see QueryNodeProcessor#getQueryConfigHandler()
   * @see QueryConfigHandler
   */
  @Override
  public void setQueryConfigHandler(QueryConfigHandler queryConfigHandler) {
    this.queryConfig = queryConfigHandler;
  }

  /**
   * For reference about this method check:
   * {@link QueryNodeProcessor#getQueryConfigHandler()}.
   * 
   * @return QueryConfigHandler the query configuration handler to be set.
   * 
   * @see QueryNodeProcessor#setQueryConfigHandler(QueryConfigHandler)
   * @see QueryConfigHandler
   */
  @Override
  public QueryConfigHandler getQueryConfigHandler() {
    return this.queryConfig;
  }

  /**
   * This method is invoked for every node when walking down the tree.
   * 
   * @param node
   *          the query node to be pre-processed
   * 
   * @return a query node
   * 
   * @throws QueryNodeException
   *           if something goes wrong during the query node processing
   */
  abstract protected QueryNode preProcessNode(QueryNode node)
      throws QueryNodeException;

  /**
   * This method is invoked for every node when walking up the tree.
   * 
   * @param node
   *          node the query node to be post-processed
   * 
   * @return a query node
   * 
   * @throws QueryNodeException
   *           if something goes wrong during the query node processing
   */
  abstract protected QueryNode postProcessNode(QueryNode node)
      throws QueryNodeException;

  /**
   * This method is invoked for every node that has at least on child. It's
   * invoked right before {@link #postProcessNode(QueryNode)} is invoked.
   * 
   * @param children
   *          the list containing all current node's children
   * 
   * @return a new list containing all children that should be set to the
   *         current node
   * 
   * @throws QueryNodeException
   *           if something goes wrong during the query node processing
   */
  abstract protected List<QueryNode> setChildrenOrder(List<QueryNode> children)
      throws QueryNodeException;

  private static class ChildrenList extends ArrayList<QueryNode> {

    boolean beingUsed;

  }

}
