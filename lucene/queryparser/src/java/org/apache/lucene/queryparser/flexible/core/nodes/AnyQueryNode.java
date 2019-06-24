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

import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

import java.util.List;


/**
 * A {@link AnyQueryNode} represents an ANY operator performed on a list of
 * nodes.
 */
public class AnyQueryNode extends AndQueryNode {
  private CharSequence field = null;
  private int minimumMatchingmElements = 0;

  /**
   * @param clauses
   *          - the query nodes to be or'ed
   */
  public AnyQueryNode(List<QueryNode> clauses, CharSequence field,
      int minimumMatchingElements) {
    super(clauses);
    this.field = field;
    this.minimumMatchingmElements = minimumMatchingElements;

    if (clauses != null) {

      for (QueryNode clause : clauses) {

        if (clause instanceof FieldQueryNode) {

          if (clause instanceof QueryNodeImpl) {
            ((QueryNodeImpl) clause).toQueryStringIgnoreFields = true;
          }

          if (clause instanceof FieldableNode) {
            ((FieldableNode) clause).setField(field);
          }

        }
      }

    }

  }

  public int getMinimumMatchingElements() {
    return this.minimumMatchingmElements;
  }

  /**
   * returns null if the field was not specified
   * 
   * @return the field
   */
  public CharSequence getField() {
    return this.field;
  }

  /**
   * returns - null if the field was not specified
   * 
   * @return the field as a String
   */
  public String getFieldAsString() {
    if (this.field == null)
      return null;
    else
      return this.field.toString();
  }

  /**
   * @param field
   *          - the field to set
   */
  public void setField(CharSequence field) {
    this.field = field;
  }

  @Override
  public QueryNode cloneTree() throws CloneNotSupportedException {
    AnyQueryNode clone = (AnyQueryNode) super.cloneTree();

    clone.field = this.field;
    clone.minimumMatchingmElements = this.minimumMatchingmElements;

    return clone;
  }

  @Override
  public String toString() {
    if (getChildren() == null || getChildren().size() == 0)
      return "<any field='" + this.field + "'  matchelements="
          + this.minimumMatchingmElements + "/>";
    StringBuilder sb = new StringBuilder();
    sb.append("<any field='").append(this.field).append("'  matchelements=").append(this.minimumMatchingmElements).append('>');
    for (QueryNode clause : getChildren()) {
      sb.append("\n");
      sb.append(clause.toString());
    }
    sb.append("\n</any>");
    return sb.toString();
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    String anySTR = "ANY " + this.minimumMatchingmElements;

    StringBuilder sb = new StringBuilder();
    if (getChildren() == null || getChildren().size() == 0) {
      // no childs case
    } else {
      String filler = "";
      for (QueryNode clause : getChildren()) {
        sb.append(filler).append(clause.toQueryString(escapeSyntaxParser));
        filler = " ";
      }
    }

    if (isDefaultField(this.field)) {
      return "( " + sb.toString() + " ) " + anySTR;
    } else {
      return this.field + ":(( " + sb.toString() + " ) " + anySTR + ")";
    }
  }

}
