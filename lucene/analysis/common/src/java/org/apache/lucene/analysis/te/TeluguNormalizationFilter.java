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
package org.apache.lucene.analysis.te;

import java.io.IOException;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

/**
 * A {@link TokenFilter} that applies {@link TeluguNormalizer} to normalize the orthography.
 *
 * <p>In some cases the normalization may cause unrelated terms to conflate, so to prevent terms
 * from being normalized use an instance of {@link SetKeywordMarkerFilter} or a custom {@link
 * TokenFilter} that sets the {@link KeywordAttribute} before this {@link TokenStream}.
 *
 * @since 9.0.0
 * @see TeluguNormalizer
 */
public final class TeluguNormalizationFilter extends TokenFilter {

  private final TeluguNormalizer normalizer = new TeluguNormalizer();
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final KeywordAttribute keywordAtt = addAttribute(KeywordAttribute.class);

  public TeluguNormalizationFilter(TokenStream input) {
    super(input);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (!keywordAtt.isKeyword())
        termAtt.setLength(normalizer.normalize(termAtt.buffer(), termAtt.length()));
      return true;
    }
    return false;
  }
}
