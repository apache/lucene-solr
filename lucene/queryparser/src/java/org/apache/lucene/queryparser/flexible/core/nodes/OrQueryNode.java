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
package org.apache.lucene.queryparser.flexible.core.nodes;

import java.util.Iterator;
import java.util.List;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

/** A {@link OrQueryNode} represents an OR boolean operation performed on a list of nodes. */
public class OrQueryNode extends BooleanQueryNode {

  /** @param clauses - the query nodes to be or'ed */
  public OrQueryNode(List<QueryNode> clauses) {
    super(clauses);
    if ((clauses == null) || (clauses.size() == 0)) {
      throw new IllegalArgumentException("OR query must have at least one clause");
    }
  }

  @Override
  public String toString() {
    if (getChildren() == null || getChildren().size() == 0) return "<boolean operation='or'/>";
    StringBuilder sb = new StringBuilder();
    sb.append("<boolean operation='or'>");
    for (QueryNode child : getChildren()) {
      sb.append("\n");
      sb.append(child.toString());
    }
    sb.append("\n</boolean>");
    return sb.toString();
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    if (getChildren() == null || getChildren().size() == 0) return "";

    StringBuilder sb = new StringBuilder();
    String filler = "";
    for (Iterator<QueryNode> it = getChildren().iterator(); it.hasNext(); ) {
      sb.append(filler).append(it.next().toQueryString(escapeSyntaxParser));
      filler = " OR ";
    }

    // in case is root or the parent is a group node avoid parenthesis
    if ((getParent() != null && getParent() instanceof GroupQueryNode) || isRoot())
      return sb.toString();
    else return "( " + sb.toString() + " )";
  }
}
