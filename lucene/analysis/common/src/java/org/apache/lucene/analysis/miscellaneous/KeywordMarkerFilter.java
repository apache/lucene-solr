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
import org.apache.lucene.analysis.tokenattributes.KeywordAttribute;

/**
 * Marks terms as keywords via the {@link KeywordAttribute}.
 *
 * @see KeywordAttribute
 */
public abstract class KeywordMarkerFilter extends TokenFilter {

  private final KeywordAttribute keywordAttr = addAttribute(KeywordAttribute.class);

  /**
   * Creates a new {@link KeywordMarkerFilter}
   *
   * @param in the input stream
   */
  protected KeywordMarkerFilter(TokenStream in) {
    super(in);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      if (isKeyword()) {
        keywordAttr.setKeyword(true);
      }
      return true;
    } else {
      return false;
    }
  }

  protected abstract boolean isKeyword();
}
