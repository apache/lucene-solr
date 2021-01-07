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
package org.apache.lucene.queryparser.flexible.standard.nodes;

import java.util.List;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;

/**
 * A {@link MultiPhraseQueryNode} indicates that its children should be used to build a {@link
 * MultiPhraseQuery} instead of {@link PhraseQuery}.
 */
public class MultiPhraseQueryNode extends QueryNodeImpl implements FieldableNode {

  public MultiPhraseQueryNode() {
    setLeaf(false);
    allocate();
  }

  @Override
  public String toString() {
    if (getChildren() == null || getChildren().size() == 0) return "<multiPhrase/>";
    StringBuilder sb = new StringBuilder();
    sb.append("<multiPhrase>");
    for (QueryNode child : getChildren()) {
      sb.append("\n");
      sb.append(child.toString());
    }
    sb.append("\n</multiPhrase>");
    return sb.toString();
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    if (getChildren() == null || getChildren().size() == 0) return "";

    StringBuilder sb = new StringBuilder();
    String filler = "";
    for (QueryNode child : getChildren()) {
      sb.append(filler).append(child.toQueryString(escapeSyntaxParser));
      filler = ",";
    }

    return "[MTP[" + sb.toString() + "]]";
  }

  @Override
  public QueryNode cloneTree() throws CloneNotSupportedException {
    MultiPhraseQueryNode clone = (MultiPhraseQueryNode) super.cloneTree();

    // nothing to do

    return clone;
  }

  @Override
  public CharSequence getField() {
    List<QueryNode> children = getChildren();

    if (children == null || children.size() == 0) {
      return null;

    } else {
      return ((FieldableNode) children.get(0)).getField();
    }
  }

  @Override
  public void setField(CharSequence fieldName) {
    List<QueryNode> children = getChildren();

    if (children != null) {

      for (QueryNode child : children) {

        if (child instanceof FieldableNode) {
          ((FieldableNode) child).setField(fieldName);
        }
      }
    }
  }
} // end class MultitermQueryNode
