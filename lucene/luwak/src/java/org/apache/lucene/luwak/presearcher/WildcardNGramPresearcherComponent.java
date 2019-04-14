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

package org.apache.lucene.luwak.presearcher;

import java.util.Collections;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.luwak.analysis.SuffixingNGramTokenFilter;
import org.apache.lucene.luwak.termextractor.QueryTerm;
import org.apache.lucene.luwak.termextractor.treebuilder.RegexpNGramTermQueryTreeBuilder;
import org.apache.lucene.util.BytesRef;

/**
 * A Presearcher implementation that matches Wildcard queries by indexing regex
 * terms by their longest static substring, and generates ngrams from InputDocument
 * tokens to match them.
 * <p>
 * This implementation will filter out more wildcard queries than TermFilteredPresearcher,
 * at the expense of longer document build times.  Which one is more performant will depend
 * on the type and number of queries registered in the Monitor, and the size of documents
 * to be monitored.  Profiling is recommended.
 */
public class WildcardNGramPresearcherComponent extends PresearcherComponent {

  /**
   * The default suffix with which to mark ngrams
   */
  public static final String DEFAULT_NGRAM_SUFFIX = "XX";

  /**
   * The default maximum length of an input token before ANYTOKENS are generated
   */
  public static final int DEFAULT_MAX_TOKEN_SIZE = 30;

  /**
   * The default token to emit if a term is longer than MAX_TOKEN_SIZE
   */
  public static final String DEFAULT_WILDCARD_TOKEN = "__WILDCARD__";

  private final String ngramSuffix;

  private final String wildcardToken;

  private final int maxTokenSize;

  private final Set<String> excludedFields;

  /**
   * Create a new WildcardNGramPresearcherComponent
   *
   * @param ngramSuffix    the suffix with which to mark ngrams
   * @param maxTokenSize   the maximum length of an input token before WILDCARD tokens are generated
   * @param wildcardToken  the token to emit if a token is longer than maxTokenSize in length
   * @param excludedFields a Set of fields to ignore when generating ngrams
   */
  public WildcardNGramPresearcherComponent(String ngramSuffix, int maxTokenSize, String wildcardToken, Set<String> excludedFields) {
    super(new RegexpNGramTermQueryTreeBuilder(ngramSuffix, wildcardToken));
    this.ngramSuffix = ngramSuffix;
    this.maxTokenSize = maxTokenSize;
    this.wildcardToken = wildcardToken;
    this.excludedFields = excludedFields == null ? Collections.emptySet() : excludedFields;
  }

  /**
   * Create a new WildcardNGramPresearcherComponent using default settings
   */
  public WildcardNGramPresearcherComponent() {
    this(DEFAULT_NGRAM_SUFFIX, DEFAULT_MAX_TOKEN_SIZE, DEFAULT_WILDCARD_TOKEN, null);
  }

  /**
   * Create a new WildcardNGramPresearcherComponent with a maximum token size
   *
   * @param maxTokenSize the maximum length of an input token before WILDCARD tokens are generated
   */
  public WildcardNGramPresearcherComponent(int maxTokenSize) {
    this(DEFAULT_NGRAM_SUFFIX, maxTokenSize, DEFAULT_WILDCARD_TOKEN, null);
  }

  @Override
  public TokenStream filterDocumentTokens(String field, TokenStream ts) {
    if (excludedFields.contains(field))
      return ts;
    return new SuffixingNGramTokenFilter(ts, ngramSuffix, wildcardToken, maxTokenSize);
  }

  @Override
  public BytesRef extraToken(QueryTerm term) {
    if (term.type == QueryTerm.Type.CUSTOM && wildcardToken.equals(term.payload))
      return new BytesRef(wildcardToken);
    return null;
  }
}
