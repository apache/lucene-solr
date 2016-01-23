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
 * A {@link FuzzyQueryNode} represents a element that contains
 * field/text/similarity tuple
 */
public class FuzzyQueryNode extends FieldQueryNode {

  private float similarity;

  private int prefixLength;

  /**
   * @param field
   *          Name of the field query will use.
   * @param termStr
   *          Term token to use for building term for the query
   */
  /**
   * @param field
   *          - Field name
   * @param term
   *          - Value
   * @param minSimilarity
   *          - similarity value
   * @param begin
   *          - position in the query string
   * @param end
   *          - position in the query string
   */
  public FuzzyQueryNode(CharSequence field, CharSequence term,
      float minSimilarity, int begin, int end) {
    super(field, term, begin, end);
    this.similarity = minSimilarity;
    setLeaf(true);
  }

  public void setPrefixLength(int prefixLength) {
    this.prefixLength = prefixLength;
  }

  public int getPrefixLength() {
    return this.prefixLength;
  }

  @Override
  public CharSequence toQueryString(EscapeQuerySyntax escaper) {
    if (isDefaultField(this.field)) {
      return getTermEscaped(escaper) + "~" + this.similarity;
    } else {
      return this.field + ":" + getTermEscaped(escaper) + "~" + this.similarity;
    }
  }

  @Override
  public String toString() {
    return "<fuzzy field='" + this.field + "' similarity='" + this.similarity
        + "' term='" + this.text + "'/>";
  }

  public void setSimilarity(float similarity) {
    this.similarity = similarity;
  }

  @Override
  public FuzzyQueryNode cloneTree() throws CloneNotSupportedException {
    FuzzyQueryNode clone = (FuzzyQueryNode) super.cloneTree();

    clone.similarity = this.similarity;

    return clone;
  }

  /**
   * @return the similarity
   */
  public float getSimilarity() {
    return this.similarity;
  }
}
