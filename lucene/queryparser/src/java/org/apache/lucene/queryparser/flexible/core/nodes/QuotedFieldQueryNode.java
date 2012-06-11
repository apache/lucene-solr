package org.apache.lucene.queryparser.flexible.core.nodes;

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

import org.apache.lucene.queryparser.flexible.core.parser.EscapeQuerySyntax;

/**
 * A {@link QuotedFieldQueryNode} represents phrase query. Example:
 * "life is great"
 */
public class QuotedFieldQueryNode extends FieldQueryNode {

  /**
   * @param field
   *          - field name
   * @param text
   *          - value
   * @param begin
   *          - position in the query string
   * @param end
   *          - position in the query string
   */
  public QuotedFieldQueryNode(CharSequence field, CharSequence text, int begin,
      int end) {
    super(field, text, begin, end);
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escaper) {
    if (isDefaultField(this.field)) {
      return "\"" + getTermEscapeQuoted(escaper) + "\"";
    } else {
      return this.field + ":" + "\"" + getTermEscapeQuoted(escaper) + "\"";
    }
  }

  @Override
  public String toString() {
    return "<quotedfield start='" + this.begin + "' end='" + this.end
        + "' field='" + this.field + "' term='" + this.text + "'/>";
  }

  @Override
  public QuotedFieldQueryNode cloneTree() throws CloneNotSupportedException {
    QuotedFieldQueryNode clone = (QuotedFieldQueryNode) super.cloneTree();
    // nothing to do here
    return clone;
  }

}
