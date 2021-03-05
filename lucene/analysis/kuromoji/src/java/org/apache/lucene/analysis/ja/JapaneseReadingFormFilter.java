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
package org.apache.lucene.analysis.ja;

import java.io.IOException;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.tokenattributes.ReadingAttribute;
import org.apache.lucene.analysis.ja.util.ToStringUtil;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * A {@link org.apache.lucene.analysis.TokenFilter} that replaces the term attribute with the
 * reading of a token in either katakana or romaji form. The default reading form is katakana.
 */
public final class JapaneseReadingFormFilter extends TokenFilter {
  private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
  private final ReadingAttribute readingAttr = addAttribute(ReadingAttribute.class);

  private StringBuilder buffer = new StringBuilder();
  private boolean useRomaji;

  public JapaneseReadingFormFilter(TokenStream input, boolean useRomaji) {
    super(input);
    this.useRomaji = useRomaji;
  }

  public JapaneseReadingFormFilter(TokenStream input) {
    this(input, false);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      String reading = readingAttr.getReading();

      if (useRomaji) {
        if (reading == null) {
          // if it's an OOV term, just try the term text
          buffer.setLength(0);
          ToStringUtil.getRomanization(buffer, termAttr);
          termAttr.setEmpty().append(buffer);
        } else {
          ToStringUtil.getRomanization(termAttr.setEmpty(), reading);
        }
      } else {
        // just replace the term text with the reading, if it exists
        if (reading != null) {
          termAttr.setEmpty().append(reading);
        }
      }
      return true;
    } else {
      return false;
    }
  }
}
