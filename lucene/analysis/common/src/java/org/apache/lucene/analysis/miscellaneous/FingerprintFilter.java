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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;

/**
 * Filter outputs a single token which is a concatenation of the sorted and
 * de-duplicated set of input tokens. This can be useful for clustering/linking
 * use cases.
 */
public class FingerprintFilter extends TokenFilter {

  public static final int DEFAULT_MAX_OUTPUT_TOKEN_SIZE = 1024;
  public static final char DEFAULT_SEPARATOR = ' ';
  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLenAtt = addAttribute(PositionLengthAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  private CharArraySet uniqueTerms = null;
  private final int maxOutputTokenSize;
  private AttributeSource.State finalState;

  private final char separator;
  private boolean inputEnded = false;


  /**
   * Create a new FingerprintFilter with default settings
   */
  public FingerprintFilter(TokenStream input) {
    this(input, DEFAULT_MAX_OUTPUT_TOKEN_SIZE, DEFAULT_SEPARATOR);
  }

  /**
   * Create a new FingerprintFilter with control over all settings
   * 
   * @param input
   *          the source of tokens to be summarized into a single token
   * @param maxOutputTokenSize
   *          the maximum length of the summarized output token. If exceeded, no
   *          output token is emitted
   * @param separator
   *          the character used to separate tokens combined into the single
   *          output token
   */
  public FingerprintFilter(TokenStream input, int maxOutputTokenSize,
      char separator) {
    super(input);
    this.maxOutputTokenSize = maxOutputTokenSize;
    this.separator = separator;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (inputEnded) {
      return false;
    }
    boolean result = buildSingleOutputToken();
    finalState = captureState();
    return result;
  }

  /**
   * Gathers all tokens from input, de-duplicates, sorts then concatenates.
   * 
   * @return false for end of stream; true otherwise
   */
  private final boolean buildSingleOutputToken() throws IOException {
    inputEnded = false;

    char clonedLastTerm[] = null;
    uniqueTerms = new CharArraySet(8, false);
    int outputTokenSize = 0;
    while (input.incrementToken()) {
      if (outputTokenSize > maxOutputTokenSize) {
        continue;
      }

      final char term[] = termAttribute.buffer();
      final int length = termAttribute.length();

      if (!uniqueTerms.contains(term, 0, length)) {
        // clone the term, and add to the set of seen terms.
        clonedLastTerm = new char[length];
        System.arraycopy(term, 0, clonedLastTerm, 0, length);
        if (uniqueTerms.size() > 0) {
          outputTokenSize++; //Add 1 for the separator char we will output
        }
        uniqueTerms.add(clonedLastTerm);
        outputTokenSize += length;
      }
    }
    //Force end-of-stream operations to get the final state.
    input.end();
    inputEnded = true;

    //Gathering complete - now output exactly zero or one token:

    //Set the attributes for the single output token
    offsetAtt.setOffset(0, offsetAtt.endOffset());
    posLenAtt.setPositionLength(1);
    posIncrAtt.setPositionIncrement(1);
    typeAtt.setType("fingerprint");

    //No tokens gathered - no output
    if (uniqueTerms.size() < 1) {
      termAttribute.setEmpty();
      return false;
    }

    //Tokens gathered are too large - no output
    if (outputTokenSize > maxOutputTokenSize) {
      termAttribute.setEmpty();
      uniqueTerms.clear();
      return false;
    }

    // Special case - faster option when we have a single token
    if (uniqueTerms.size() == 1) {
      termAttribute.setEmpty().append(new String(clonedLastTerm));
      uniqueTerms.clear();
      return true;
    }

    // Sort the set of deduplicated tokens and combine 
    Object[] items = uniqueTerms.toArray();

    Arrays.sort(items, new Comparator<Object>() {
      @Override
      public int compare(Object o1, Object o2) {
        char v1[] = (char[]) o1;
        char v2[] = (char[]) o2;
        int len1 = v1.length;
        int len2 = v2.length;
        int lim = Math.min(len1, len2);

        int k = 0;
        while (k < lim) {
          char c1 = v1[k];
          char c2 = v2[k];
          if (c1 != c2) {
            return c1 - c2;
          }
          k++;
        }
        return len1 - len2;
      }
    });

    //TODO lets append directly to termAttribute?
    StringBuilder sb = new StringBuilder();
    for (Object item : items) {
      if (sb.length() >= 1) {
        sb.append(separator);
      }
      sb.append((char[]) item);
    }
    termAttribute.setEmpty().append(sb);
    uniqueTerms.clear();
    return true;

  }

  @Override
  public final void end() throws IOException {
    if (!inputEnded) {
      // Rare case - If an IOException occurs while performing buildSingleOutputToken
      // we may not have called input.end() already
      input.end();
      inputEnded = true;
    }

    if (finalState != null) {
      restoreState(finalState);
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    inputEnded = false;
    uniqueTerms = null;
  }

}
