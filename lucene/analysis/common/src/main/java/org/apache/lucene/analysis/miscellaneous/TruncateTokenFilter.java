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


import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

import java.io.IOException;

/**
 * A token filter for truncating the terms into a specific length.
 * Fixed prefix truncation, as a stemming method, produces good results on Turkish language.
 * It is reported that F5, using first 5 characters, produced best results in
 * <a href="http://www.users.muohio.edu/canf/papers/JASIST2008offPrint.pdf">
 * Information Retrieval on Turkish Texts</a>
 */
public final class TruncateTokenFilter extends TokenFilter {

  private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

  private final int length;

  public TruncateTokenFilter(TokenStream input, int length) {
    super(input);
    if (length < 1)
      throw new IllegalArgumentException("length parameter must be a positive number: " + length);
    this.length = length;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (!keywordAttr.isKeyword() && termAttribute.length() > length)
        termAttribute.setLength(length);
      return true;
    } else {
      return false;
    }
  }
}