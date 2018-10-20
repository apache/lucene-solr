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

/**
 * A {@link OpaqueQueryNode} is used for specify values that are not supposed to
 * be parsed by the parser. For example: and XPATH query in the middle of a
 * query string a b @xpath:'/bookstore/book[1]/title' c d
 */
public class OpaqueQueryNode extends QueryNodeImpl {

  private CharSequence schema = null;

  private CharSequence value = null;

  /**
   * @param schema
   *          - schema identifier
   * @param value
   *          - value that was not parsed
   */
  public OpaqueQueryNode(CharSequence schema, CharSequence value) {
    this.setLeaf(true);

    this.schema = schema;
    this.value = value;

  }

  @Override
  public String toString() {
    return "<opaque schema='" + this.schema + "' value='" + this.value + "'/>";
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escapeSyntaxParser) {
    return "@" + this.schema + ":'" + this.value + "'";
  }

  @Override
  public QueryNode cloneTree() throws CloneNotSupportedException {
    OpaqueQueryNode clone = (OpaqueQueryNode) super.cloneTree();

    clone.schema = this.schema;
    clone.value = this.value;

    return clone;
  }

  /**
   * @return the schema
   */
  public CharSequence getSchema() {
    return this.schema;
  }

  /**
   * @return the value
   */
  public CharSequence getValue() {
    return this.value;
  }

}
