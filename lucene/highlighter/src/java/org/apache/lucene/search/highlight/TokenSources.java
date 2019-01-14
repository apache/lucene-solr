/*
 * Created on 28-Oct-2004
 */
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
package org.apache.lucene.search.highlight;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Terms;

/**
 * Convenience methods for obtaining a {@link TokenStream} for use with the {@link Highlighter} - can obtain from
 * term vectors with offsets and positions or from an Analyzer re-parsing the stored content.
 *
 * @see TokenStreamFromTermVector
 */
public class TokenSources {

  private TokenSources() {}

  /**
   * Get a token stream from either un-inverting a term vector if possible, or by analyzing the text.
   *
   * WARNING: Don't call this if there is more than one value for this field.  If there are, and if there are term
   * vectors, then there is a single tokenstream with offsets suggesting all the field values were concatenated.
   *
   * @param field The field to either get term vectors from or to analyze the text from.
   * @param tvFields from {@link IndexReader#getTermVectors(int)}. Possibly null. For performance, this instance should
   *                 be re-used for the same document (e.g. when highlighting multiple fields).
   * @param text the text to analyze, failing term vector un-inversion
   * @param analyzer the analyzer to analyze {@code text} with, failing term vector un-inversion
   * @param maxStartOffset Terms with a startOffset greater than this aren't returned.  Use -1 for no limit.
   *                       Suggest using {@link Highlighter#getMaxDocCharsToAnalyze()} - 1.
   *
   * @return a token stream from either term vectors, or from analyzing the text. Never null.
   */
  public static TokenStream getTokenStream(String field, Fields tvFields, String text, Analyzer analyzer,
                                           int maxStartOffset) throws IOException {
    TokenStream tokenStream = getTermVectorTokenStreamOrNull(field, tvFields, maxStartOffset);
    if (tokenStream != null) {
      return tokenStream;
    }
    tokenStream = analyzer.tokenStream(field, text);
    if (maxStartOffset >= 0 && maxStartOffset < text.length() - 1) {
      tokenStream = new LimitTokenOffsetFilter(tokenStream, maxStartOffset);
    }
    return tokenStream;
  }

  /**
   * Get a token stream by un-inverting the term vector. This method returns null if {@code tvFields} is null
   * or if the field has no term vector, or if the term vector doesn't have offsets.  Positions are recommended on the
   * term vector but it isn't strictly required.
   *
   * @param field The field to get term vectors from.
   * @param tvFields from {@link IndexReader#getTermVectors(int)}. Possibly null. For performance, this instance should
   *                 be re-used for the same document (e.g. when highlighting multiple fields).
   * @param maxStartOffset Terms with a startOffset greater than this aren't returned.  Use -1 for no limit.
   *                       Suggest using {@link Highlighter#getMaxDocCharsToAnalyze()} - 1
   * @return a token stream from term vectors. Null if no term vectors with the right options.
   */
  public static TokenStream getTermVectorTokenStreamOrNull(String field, Fields tvFields, int maxStartOffset)
      throws IOException {
    if (tvFields == null) {
      return null;
    }
    final Terms tvTerms = tvFields.terms(field);
    if (tvTerms == null || !tvTerms.hasOffsets()) {
      return null;
    }
    return new TokenStreamFromTermVector(tvTerms, maxStartOffset);
  }

}
