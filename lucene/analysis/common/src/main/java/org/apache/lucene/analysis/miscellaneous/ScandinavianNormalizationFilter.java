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
import org.apache.lucene.analysis.util.StemmerUtil;

import java.io.IOException;

/**
 * This filter normalize use of the interchangeable Scandinavian characters æÆäÄöÖøØ
 * and folded variants (aa, ao, ae, oe and oo) by transforming them to åÅæÆøØ.
 * <p>
 * It's a semantically less destructive solution than {@link ScandinavianFoldingFilter},
 * most useful when a person with a Norwegian or Danish keyboard queries a Swedish index
 * and vice versa. This filter does <b>not</b>  the common Swedish folds of å and ä to a nor ö to o.
 * <p>
 * blåbærsyltetøj == blåbärsyltetöj == blaabaarsyltetoej but not blabarsyltetoj
 * räksmörgås == ræksmørgås == ræksmörgaos == raeksmoergaas but not raksmorgas
 * @see ScandinavianFoldingFilter
 */
public final class ScandinavianNormalizationFilter extends TokenFilter {

  public ScandinavianNormalizationFilter(TokenStream input) {
    super(input);
  }

  private final CharTermAttribute charTermAttribute = addAttribute(CharTermAttribute.class);

  private static final char AA = '\u00C5'; // Å
  private static final char aa = '\u00E5'; // å
  private static final char AE = '\u00C6'; // Æ
  private static final char ae = '\u00E6'; // æ
  private static final char AE_se = '\u00C4'; // Ä
  private static final char ae_se = '\u00E4'; // ä
  private static final char OE = '\u00D8'; // Ø
  private static final char oe = '\u00F8'; // ø
  private static final char OE_se = '\u00D6'; // Ö
  private static final char oe_se = '\u00F6'; //ö


  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }

    char[] buffer = charTermAttribute.buffer();
    int length = charTermAttribute.length();


    int i;
    for (i = 0; i < length; i++) {

      if (buffer[i] == ae_se) {
        buffer[i] = ae;

      } else if (buffer[i] == AE_se) {
        buffer[i] = AE;

      } else if (buffer[i] == oe_se) {
        buffer[i] = oe;

      } else if (buffer[i] == OE_se) {
        buffer[i] = OE;

      } else if (length - 1 > i) {

        if (buffer[i] == 'a' && (buffer[i + 1] == 'a' || buffer[i + 1] == 'o' || buffer[i + 1] == 'A' || buffer[i + 1] == 'O')) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = aa;

        } else if (buffer[i] == 'A' && (buffer[i + 1] == 'a' || buffer[i + 1] == 'A' || buffer[i + 1] == 'o' || buffer[i + 1] == 'O')) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = AA;

        } else if (buffer[i] == 'a' && (buffer[i + 1] == 'e' || buffer[i + 1] == 'E')) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = ae;

        } else if (buffer[i] == 'A' && (buffer[i + 1] == 'e' || buffer[i + 1] == 'E')) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = AE;

        } else if (buffer[i] == 'o' && (buffer[i + 1] == 'e' || buffer[i + 1] == 'E' || buffer[i + 1] == 'o' || buffer[i + 1] == 'O')) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = oe;

        } else if (buffer[i] == 'O' && (buffer[i + 1] == 'e' || buffer[i + 1] == 'E' || buffer[i + 1] == 'o' || buffer[i + 1] == 'O')) {
          length = StemmerUtil.delete(buffer, i + 1, length);
          buffer[i] = OE;

        }

      }
    }

    charTermAttribute.setLength(length);


    return true;
  }

}
