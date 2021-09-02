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

import static org.apache.lucene.analysis.util.StemmerUtil.delete;

/**
 * Normalizer for Telugu.
 *
 * <p>Normalizes text to remove some differences in spelling variations.
 *
 * @since 9.0.0
 */
public class TeluguNormalizer {

  /**
   * Normalize an input buffer of Telugu text
   *
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int normalize(char s[], int len) {

    for (int i = 0; i < len; i++) {
      switch (s[i]) {
          // candrabindu (ఀ and ఁ) -> bindu (ం)
        case '\u0C00': // ఀ
        case '\u0C01': // ఁ
          s[i] = '\u0C02'; // ం
          break;
          // delete visarga (ః)
        case '\u0C03':
          len = delete(s, i, len);
          i--;
          break;

          // zwj/zwnj -> delete
        case '\u200D':
        case '\u200C':
          len = delete(s, i, len);
          i--;
          break;

          // long -> short vowels
        case '\u0C14': // ఔ
          s[i] = '\u0C13'; // ఓ
          break;
        case '\u0C10': // ఐ
          s[i] = '\u0C0F'; // ఏ
          break;
        case '\u0C06': // ఆ
          s[i] = '\u0C05'; // అ
          break;
        case '\u0C08': // ఈ
          s[i] = '\u0C07'; // ఇ
          break;
        case '\u0C0A': // ఊ
          s[i] = '\u0C09'; // ఉ
          break;

          // long -> short vowels matras
        case '\u0C40': // ీ
          s[i] = '\u0C3F'; // ి
          break;
        case '\u0C42': // ూ
          s[i] = '\u0C41'; // ు
          break;
        case '\u0C47': // ే
          s[i] = '\u0C46'; // ె
          break;
        case '\u0C4B': // ో
          s[i] = '\u0C4A'; // ొ
          break;
          // decomposed dipthong (ె + ౖ) -> precomposed diphthong vowel sign (ై)
        case '\u0C46':
          if (i + 1 < len && s[i + 1] == '\u0C56') {
            s[i] = '\u0C48';
            len = delete(s, i + 1, len);
          }
          break;
          // composed oo or au -> oo or au
        case '\u0C12':
          if (i + 1 < len && s[i + 1] == '\u0C55') {
            // (ఒ + ౕ) -> oo (ఓ)
            s[i] = '\u0C13';
            len = delete(s, i + 1, len);
          } else if (i + 1 < len && s[i + 1] == '\u0C4C') {
            // (ఒ + ౌ) -> au (ఔ)
            s[i] = '\u0C14';
            len = delete(s, i + 1, len);
          }
          break;

        default:
          break;
      }
    }

    return len;
  }
}
