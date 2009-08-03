package org.apache.lucene.queryParser.core.nodes;

/**
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

import org.apache.lucene.queryParser.core.parser.EscapeQuerySyntax;

/**
 * A {@link ParametricQueryNode} represents LE, LT, GE, GT, EQ, NE query.
 * Example: date >= "2009-10-10" OR price = 200
 */
public class ParametricQueryNode extends FieldQueryNode {

  private static final long serialVersionUID = -5770038129741218116L;

  private CompareOperator operator;

  public enum CompareOperator {
    LE, LT, GE, GT, EQ, NE;

    public String toString() {
      if (LE.equals(this)) {
        return "<=";
      } else if (LT.equals(this)) {
        return "<";
      } else if (GE.equals(this)) {
        return ">=";
      } else if (GT.equals(this)) {
        return ">";
      } else if (EQ.equals(this)) {
        return "=";
      } else if (NE.equals(this)) {
        return "!=";
      } else {
        throw new IllegalArgumentException("Unknown operator");
      }
    }
  }

  /**
   * @param field
   *          - field name
   * @param comp
   *          - CompareOperator
   * @param value
   *          - text value
   * @param begin
   *          - position in the query string
   * @param end
   *          - position in the query string
   */
  public ParametricQueryNode(CharSequence field, CompareOperator comp,
      CharSequence value, int begin, int end) {
    super(field, value, begin, end);
    this.operator = comp;
    setLeaf(true);
  }

  public CharSequence getOperand() {
    return getText();
  }

  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    return this.field + "" + this.operator.toString() + "\"" + this.text + "\"";
  }

  public String toString() {
    return "<parametric field='" + this.field + "' operator='"
        + this.operator.toString() + "' text='" + this.text + "'/>";
  }

  public ParametricQueryNode cloneTree() throws CloneNotSupportedException {
    ParametricQueryNode clone = (ParametricQueryNode) super.cloneTree();

    clone.operator = this.operator;

    return clone;
  }

  /**
   * @return the operator
   */
  public CompareOperator getOperator() {
    return this.operator;
  }
}
