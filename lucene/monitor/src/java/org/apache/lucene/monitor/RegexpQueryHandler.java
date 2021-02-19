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

package org.apache.lucene.monitor;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.BytesRef;

/**
 * A query handler implementation that matches Regexp queries by indexing regex terms by their
 * longest static substring, and generates ngrams from Document tokens to match them.
 *
 * <p>This implementation will filter out more wildcard queries than TermFilteredPresearcher, at the
 * expense of longer document build times. Which one is more performant will depend on the type and
 * number of queries registered in the Monitor, and the size of documents to be monitored. Profiling
 * is recommended.
 */
public class RegexpQueryHandler implements CustomQueryHandler {

  /** The default suffix with which to mark ngrams */
  public static final String DEFAULT_NGRAM_SUFFIX = "XX";

  /** The default maximum length of an input token before ANYTOKENS are generated */
  public static final int DEFAULT_MAX_TOKEN_SIZE = 30;

  /** The default token to emit if a term is longer than MAX_TOKEN_SIZE */
  public static final String DEFAULT_WILDCARD_TOKEN = "__WILDCARD__";

  private final String ngramSuffix;

  private final String wildcardToken;
  private final BytesRef wildcardTokenBytes;

  private final int maxTokenSize;

  private final Set<String> excludedFields;

  /**
   * Creates a new RegexpQueryHandler
   *
   * @param ngramSuffix the suffix with which to mark ngrams
   * @param maxTokenSize the maximum length of an input token before WILDCARD tokens are generated
   * @param wildcardToken the token to emit if a token is longer than maxTokenSize in length
   * @param excludedFields a Set of fields to ignore when generating ngrams
   */
  public RegexpQueryHandler(
      String ngramSuffix, int maxTokenSize, String wildcardToken, Set<String> excludedFields) {
    this.ngramSuffix = ngramSuffix;
    this.maxTokenSize = maxTokenSize;
    this.wildcardTokenBytes = new BytesRef(wildcardToken);
    this.wildcardToken = wildcardToken;
    this.excludedFields = excludedFields == null ? Collections.emptySet() : excludedFields;
  }

  /** Creates a new RegexpQueryHandler using default settings */
  public RegexpQueryHandler() {
    this(DEFAULT_NGRAM_SUFFIX, DEFAULT_MAX_TOKEN_SIZE, DEFAULT_WILDCARD_TOKEN, null);
  }

  /**
   * Creates a new RegexpQueryHandler with a maximum token size
   *
   * @param maxTokenSize the maximum length of an input token before WILDCARD tokens are generated
   */
  public RegexpQueryHandler(int maxTokenSize) {
    this(DEFAULT_NGRAM_SUFFIX, maxTokenSize, DEFAULT_WILDCARD_TOKEN, null);
  }

  @Override
  public TokenStream wrapTermStream(String field, TokenStream ts) {
    if (excludedFields.contains(field)) return ts;
    return new SuffixingNGramTokenFilter(ts, ngramSuffix, wildcardToken, maxTokenSize);
  }

  @Override
  public QueryTree handleQuery(Query q, TermWeightor termWeightor) {
    if (q instanceof RegexpQuery == false) {
      return null;
    }
    RegexpQuery query = (RegexpQuery) q;
    String regexp = parseOutRegexp(query.toString(""));
    String selected = selectLongestSubstring(regexp);
    Term term = new Term(query.getField(), selected + ngramSuffix);
    double weight = termWeightor.applyAsDouble(term);
    return new QueryTree() {
      @Override
      public double weight() {
        return weight;
      }

      @Override
      public void collectTerms(BiConsumer<String, BytesRef> termCollector) {
        termCollector.accept(term.field(), term.bytes());
        termCollector.accept(term.field(), wildcardTokenBytes);
      }

      @Override
      public boolean advancePhase(double minWeight) {
        return false;
      }

      @Override
      public String toString(int depth) {
        return space(depth) + "WILDCARD_NGRAM[" + term.toString() + "]^" + weight;
      }
    };
  }

  private static String parseOutRegexp(String rep) {
    int fieldSepPos = rep.indexOf(":");
    int firstSlash = rep.indexOf("/", fieldSepPos);
    int lastSlash = rep.lastIndexOf("/");
    return rep.substring(firstSlash + 1, lastSlash);
  }

  private static String selectLongestSubstring(String regexp) {
    String selected = "";
    for (String substr : regexp.split("\\.|\\*|.\\?")) {
      if (substr.length() > selected.length()) {
        selected = substr;
      }
    }
    return selected;
  }
}
