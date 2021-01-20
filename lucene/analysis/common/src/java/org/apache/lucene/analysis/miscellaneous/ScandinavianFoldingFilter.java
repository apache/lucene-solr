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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.StemmerUtil;

/**
 * This filter folds Scandinavian characters åÅäæÄÆ-&gt;a and öÖøØ-&gt;o. It also discriminate
 * against use of double vowels aa, ae, ao, oe and oo, leaving just the first one.
 *
 * <p>It's a semantically more destructive solution than {@link ScandinavianNormalizationFilter} but
 * can in addition help with matching raksmorgas as räksmörgås.
 *
 * <p>blåbærsyltetøj == blåbärsyltetöj == blaabaarsyltetoej == blabarsyltetoj räksmörgås ==
 * ræksmørgås == ræksmörgaos == raeksmoergaas == raksmorgas
 *
 * <p>Background: Swedish åäö are in fact the same letters as Norwegian and Danish åæø and thus
 * interchangeable when used between these languages. They are however folded differently when
 * people type them on a keyboard lacking these characters.
 *
 * <p>In that situation almost all Swedish people use a, a, o instead of å, ä, ö.
 *
 * <p>Norwegians and Danes on the other hand usually type aa, ae and oe instead of å, æ and ø. Some
 * do however use a, a, o, oo, ao and sometimes permutations of everything above.
 *
 * <p>This filter solves that mismatch problem, but might also cause new.
 *
 * @see ScandinavianNormalizationFilter
 */
public final class ScandinavianFoldingFilter extends TokenFilter {

  public ScandinavianFoldingFilter(TokenStream input) {
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
  private static final char oe_se = '\u00F6'; // ö

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) {
      return false;
    }

    char[] buffer = charTermAttribute.buffer();
    int length = charTermAttribute.length();

    int i;
    for (i = 0; i < length; i++) {

      if (buffer[i] == aa || buffer[i] == ae_se || buffer[i] == ae) {

        buffer[i] = 'a';

      } else if (buffer[i] == AA || buffer[i] == AE_se || buffer[i] == AE) {

        buffer[i] = 'A';

      } else if (buffer[i] == oe || buffer[i] == oe_se) {

        buffer[i] = 'o';

      } else if (buffer[i] == OE || buffer[i] == OE_se) {

        buffer[i] = 'O';

      } else if (length - 1 > i) {

        if ((buffer[i] == 'a' || buffer[i] == 'A')
            && (buffer[i + 1] == 'a'
                || buffer[i + 1] == 'A'
                || buffer[i + 1] == 'e'
                || buffer[i + 1] == 'E'
                || buffer[i + 1] == 'o'
                || buffer[i + 1] == 'O')) {

          length = StemmerUtil.delete(buffer, i + 1, length);

        } else if ((buffer[i] == 'o' || buffer[i] == 'O')
            && (buffer[i + 1] == 'e'
                || buffer[i + 1] == 'E'
                || buffer[i + 1] == 'o'
                || buffer[i + 1] == 'O')) {

          length = StemmerUtil.delete(buffer, i + 1, length);
        }
      }
    }

    charTermAttribute.setLength(length);

    return true;
  }
}
