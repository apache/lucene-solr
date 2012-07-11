package org.apache.lucene.queryparser.flexible.core.util;

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

import org.apache.lucene.queryparser.flexible.core.QueryNodeError;
import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import java.util.ArrayList;
import java.util.List;


/**
 * Allow joining 2 QueryNode Trees, into one.
 */
public final class QueryNodeOperation {
  private QueryNodeOperation() {
    // Exists only to defeat instantiation.
  }

  private enum ANDOperation {
    BOTH, Q1, Q2, NONE
  }

  /**
   * perform a logical and of 2 QueryNode trees. if q1 and q2 are ANDQueryNode
   * nodes it uses head Node from q1 and adds the children of q2 to q1 if q1 is
   * a AND node and q2 is not, add q2 as a child of the head node of q1 if q2 is
   * a AND node and q1 is not, add q1 as a child of the head node of q2 if q1
   * and q2 are not ANDQueryNode nodes, create a AND node and make q1 and q2
   * children of that node if q1 or q2 is null it returns the not null node if
   * q1 = q2 = null it returns null
   */
  public final static QueryNode logicalAnd(QueryNode q1, QueryNode q2) {
    if (q1 == null)
      return q2;
    if (q2 == null)
      return q1;

    ANDOperation op = null;
    if (q1 instanceof AndQueryNode && q2 instanceof AndQueryNode)
      op = ANDOperation.BOTH;
    else if (q1 instanceof AndQueryNode)
      op = ANDOperation.Q1;
    else if (q1 instanceof AndQueryNode)
      op = ANDOperation.Q2;
    else
      op = ANDOperation.NONE;

    try {
      QueryNode result = null;
      switch (op) {
      case NONE:
        List<QueryNode> children = new ArrayList<QueryNode>();
        children.add(q1.cloneTree());
        children.add(q2.cloneTree());
        result = new AndQueryNode(children);
        return result;
      case Q1:
        result = q1.cloneTree();
        result.add(q2.cloneTree());
        return result;
      case Q2:
        result = q2.cloneTree();
        result.add(q1.cloneTree());
        return result;
      case BOTH:
        result = q1.cloneTree();
        result.add(q2.cloneTree().getChildren());
        return result;
      }
    } catch (CloneNotSupportedException e) {
      throw new QueryNodeError(e);
    }

    return null;

  }

}
