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

import org.apache.lucene.queryparser.flexible.core.nodes.FieldableNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNodeImpl;
import org.apache.lucene.queryparser.flexible.core.nodes.TextableQueryNode;
import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.BytesRef;

/** A {@link RegexpQueryNode} represents {@link RegexpQuery} query Examples: /[a-z]|[0-9]/ */
public class RegexpQueryNode extends QueryNodeImpl implements TextableQueryNode, FieldableNode {
  private CharSequence text;
  private CharSequence field;

  /**
   * @param field - field name
   * @param text - value that contains a regular expression
   * @param begin - position in the query string
   * @param end - position in the query string
   */
  public RegexpQueryNode(CharSequence field, CharSequence text, int begin, int end) {
    this.field = field;
    this.text = text.subSequence(begin, end);
  }

  /**
   * @param field - field name
   * @param text - value that contains a regular expression
   */
  public RegexpQueryNode(CharSequence field, CharSequence text) {
    this(field, text, 0, text.length());
  }

  public BytesRef textToBytesRef() {
    return new BytesRef(text);
  }

  @Override
  public String toString() {
    return "<regexp field='" + this.field + "' term='" + this.text + "'/>";
  }

  @Override
  public RegexpQueryNode cloneTree() throws CloneNotSupportedException {
    RegexpQueryNode clone = (RegexpQueryNode) super.cloneTree();
    clone.field = this.field;
    clone.text = this.text;
    return clone;
  }

  @Override
  public CharSequence getText() {
    return text;
  }

  @Override
  public void setText(CharSequence text) {
    this.text = text;
  }

  @Override
  public CharSequence getField() {
    return field;
  }

  public String getFieldAsString() {
    return field.toString();
  }

  @Override
  public void setField(CharSequence field) {
    this.field = field;
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    return isDefaultField(field) ? "/" + text + "/" : field + ":/" + text + "/";
  }
}
