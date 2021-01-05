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

import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;

/**
 * A {@link PrefixWildcardQueryNode} represents wildcardquery that matches abc* or *. This does not
 * apply to phrases, this is a special case on the original lucene parser. TODO: refactor the code
 * to remove this special case from the parser. and probably do it on a Processor
 */
public class PrefixWildcardQueryNode extends WildcardQueryNode {

  /**
   * @param field - field name
   * @param text - value including the wildcard
   * @param begin - position in the query string
   * @param end - position in the query string
   */
  public PrefixWildcardQueryNode(CharSequence field, CharSequence text, int begin, int end) {
    super(field, text, begin, end);
  }

  public PrefixWildcardQueryNode(FieldQueryNode fqn) {
    this(fqn.getField(), fqn.getText(), fqn.getBegin(), fqn.getEnd());
  }

  @Override
  public String toString() {
    return "<prefixWildcard field='" + this.field + "' term='" + this.text + "'/>";
  }

  @Override
  public PrefixWildcardQueryNode cloneTree() throws CloneNotSupportedException {
    PrefixWildcardQueryNode clone = (PrefixWildcardQueryNode) super.cloneTree();

    // nothing to do here

    return clone;
  }
}
