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
package org.apache.lucene.analysis.ckb;

import static org.apache.lucene.analysis.util.StemmerUtil.delete;

/** 
 * Normalizes the Unicode representation of Sorani text.
 * <p>
 * Normalization consists of:
 * <ul>
 *   <li>Alternate forms of 'y' (0064, 0649) are converted to 06CC (FARSI YEH)
 *   <li>Alternate form of 'k' (0643) is converted to 06A9 (KEHEH)
 *   <li>Alternate forms of vowel 'e' (0647+200C, word-final 0647, 0629) are converted to 06D5 (AE)
 *   <li>Alternate (joining) form of 'h' (06BE) is converted to 0647
 *   <li>Alternate forms of 'rr' (0692, word-initial 0631) are converted to 0695 (REH WITH SMALL V BELOW)
 *   <li>Harakat, tatweel, and formatting characters such as directional controls are removed.
 * </ul>
 */
public class SoraniNormalizer {
  
  static final char YEH = '\u064A';
  static final char DOTLESS_YEH = '\u0649';
  static final char FARSI_YEH = '\u06CC';
  
  static final char KAF = '\u0643';
  static final char KEHEH = '\u06A9';
  
  static final char HEH = '\u0647';
  static final char AE = '\u06D5';
  static final char ZWNJ = '\u200C';
  static final char HEH_DOACHASHMEE = '\u06BE';
  static final char TEH_MARBUTA = '\u0629';
      
  static final char REH = '\u0631';
  static final char RREH = '\u0695';
  static final char RREH_ABOVE = '\u0692';
  
  static final char TATWEEL = '\u0640';
  static final char FATHATAN = '\u064B';
  static final char DAMMATAN = '\u064C';
  static final char KASRATAN = '\u064D';
  static final char FATHA = '\u064E';
  static final char DAMMA = '\u064F';
  static final char KASRA = '\u0650';
  static final char SHADDA = '\u0651';
  static final char SUKUN = '\u0652';

  /**
   * Normalize an input buffer of Sorani text
   * 
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int normalize(char s[], int len) {
    for (int i = 0; i < len; i++) {
      switch (s[i]) {
        case YEH:
        case DOTLESS_YEH:
          s[i] = FARSI_YEH;
          break;
        case KAF:
          s[i] = KEHEH;
          break;
        case ZWNJ:
          if (i > 0 && s[i-1] == HEH) {
            s[i-1] = AE;
          }
          len = delete(s, i, len);
          i--;
          break;
        case HEH:
          if (i == len-1) {
            s[i] = AE;
          }
          break;
        case TEH_MARBUTA:
          s[i] = AE;
          break;
        case HEH_DOACHASHMEE:
          s[i] = HEH;
          break;
        case REH:
          if (i == 0) {
            s[i] = RREH;
          }
          break;
        case RREH_ABOVE:
          s[i] = RREH;
          break;
        case TATWEEL:
        case KASRATAN:
        case DAMMATAN:
        case FATHATAN:
        case FATHA:
        case DAMMA:
        case KASRA:
        case SHADDA:
        case SUKUN:
          len = delete(s, i, len);
          i--;
          break;
        default:
          if (Character.getType(s[i]) == Character.FORMAT) {
            len = delete(s, i, len);
            i--;
          }
      }
    }
    return len;
  }
}
