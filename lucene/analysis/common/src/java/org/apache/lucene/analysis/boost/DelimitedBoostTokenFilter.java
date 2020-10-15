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
package org.apache.lucene.analysis.boost;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.BoostAttribute;

import java.io.IOException;


/**
 * Characters before the delimiter are the "token", those after are the boost.
 * <p>
 * For example, if the delimiter is '|', then for the string "foo|0.7", foo is the token
 * and 0.7 is the boost.
 * <p>
 * Note make sure your Tokenizer doesn't split on the delimiter, or this won't work
 */
public final class DelimitedBoostTokenFilter extends TokenFilter {
  private final char delimiter;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final BoostAttribute boostAtt = addAttribute(BoostAttribute.class);

  public DelimitedBoostTokenFilter(TokenStream input, char delimiter) {
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
          float boost = Float.parseFloat(new String(buffer, i + 1, (length - (i + 1))));
          boostAtt.setBoost(boost);
          termAtt.setLength(i);
          return true;
        }
      }
      return true;
    } else {
      return false;
    }
  }
}
