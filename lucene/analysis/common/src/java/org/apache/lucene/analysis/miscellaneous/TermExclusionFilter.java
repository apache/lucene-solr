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

package org.apache.lucene.analysis.miscellaneous;

import java.util.function.Function;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * A ConditionalTokenFilter that only applies its wrapped filters to tokens that
 * are not contained in an exclusion set.
 */
public class TermExclusionFilter extends ConditionalTokenFilter {

  private final CharArraySet excludeTerms;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  /**
   * Creates a new TermExclusionFilter
   * @param excludeTerms  the set of terms to skip the wrapped filters for
   * @param input         the input TokenStream
   * @param inputFactory  a factory function to create the wrapped filters
   */
  public TermExclusionFilter(final CharArraySet excludeTerms, TokenStream input, Function<TokenStream, TokenStream> inputFactory) {
    super(input, inputFactory);
    this.excludeTerms = excludeTerms;
  }

  @Override
  protected boolean shouldFilter() {
    return excludeTerms.contains(termAtt.buffer(), 0, termAtt.length()) == false;
  }

}
