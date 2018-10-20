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

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;


/**
 * Characters before the delimiter are the "token", the textual integer after is the term frequency.
 * To use this {@code TokenFilter} the field must be indexed with
 * {@link IndexOptions#DOCS_AND_FREQS} but no positions or offsets.
 * <p>
 * For example, if the delimiter is '|', then for the string "foo|5", "foo" is the token
 * and "5" is a term frequency. If there is no delimiter, the TokenFilter does not modify
 * the term frequency.
 * <p>
 * Note make sure your Tokenizer doesn't split on the delimiter, or this won't work
 */
public final class DelimitedTermFrequencyTokenFilter extends TokenFilter {
  public static final char DEFAULT_DELIMITER = '|';
  
  private final char delimiter;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final TermFrequencyAttribute tfAtt = addAttribute(TermFrequencyAttribute.class);


  public DelimitedTermFrequencyTokenFilter(TokenStream input) {
    this(input, DEFAULT_DELIMITER);
  }

  public DelimitedTermFrequencyTokenFilter(TokenStream input, char delimiter) {
    super(input);
    this.delimiter = delimiter;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      final char[] buffer = termAtt.buffer();
      final int length = termAtt.length();
      for (int i = 0; i < length; i++) {
        if (buffer[i] == delimiter) {
          termAtt.setLength(i); // simply set a new length
          i++;
          tfAtt.setTermFrequency(ArrayUtil.parseInt(buffer, i, length - i));
          return true;
        }
      }
      return true;
    }
    return false;
  }
}
