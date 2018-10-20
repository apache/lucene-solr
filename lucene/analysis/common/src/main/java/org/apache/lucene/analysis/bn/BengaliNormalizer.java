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
package org.apache.lucene.analysis.bn;


import static org.apache.lucene.analysis.util.StemmerUtil.delete;

/**
 * Normalizer for Bengali.
 * <p>
 * Implements the Bengali-language specific algorithm specified in:
 * <i>A Double Metaphone encoding for Bangla and its application in spelling checker</i>
 * Naushad UzZaman and Mumit Khan.
 * http://www.panl10n.net/english/final%20reports/pdf%20files/Bangladesh/BAN16.pdf
 * </p>
 */
public class BengaliNormalizer {
  /**
   * Normalize an input buffer of Bengali text
   *
   * @param s   input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int normalize(char s[], int len) {

    for (int i = 0; i < len; i++) {
      switch (s[i]) {
        // delete Chandrabindu
        case '\u0981':
          len = delete(s, i, len);
          i--;
          break;

        // DirghoI kar -> RosshoI kar
        case '\u09C0':
          s[i] = '\u09BF';
          break;

        // DirghoU kar -> RosshoU kar
        case '\u09C2':
          s[i] = '\u09C1';
          break;

        // Khio (Ka + Hoshonto + Murdorno Sh)
        case '\u0995':
          if(i + 2 < len && s[i+1] == '\u09CD' && s[i+2] == '\u09BF') {
            if (i == 0) {
              s[i] = '\u0996';
              len = delete(s, i + 2, len);
              len = delete(s, i + 1, len);
            } else {
              s[i+1] = '\u0996';
              len = delete(s, i + 2, len);
            }
          }
          break;

        // Nga to Anusvara
        case '\u0999':
          s[i] = '\u0982';
          break;

        // Ja Phala
        case '\u09AF':
          if(i - 2 == 0 && s[i-1] == '\u09CD') {
            s[i - 1] = '\u09C7';

            if(i + 1 < len && s[i+1] == '\u09BE') {
              len = delete(s, i+1, len);
            }
            len = delete(s, i, len);
            i --;
          } else if(i - 1 >= 0 && s[i-1] == '\u09CD' ){
            len = delete(s, i, len);
            len = delete(s, i-1, len);
            i -=2;
          }
          break;

        // Ba Phalaa
        case '\u09AC':
          if((i >= 1 && s[i-1] != '\u09CD') || i == 0)
            break;
          if(i - 2 == 0) {
            len = delete(s, i, len);
            len = delete(s, i - 1, len);
            i -= 2;
          } else if(i - 5 >= 0 && s[i - 3] == '\u09CD') {
            len = delete(s, i, len);
            len = delete(s, i-1, len);
            i -=2;
          } else if(i - 2 >= 0){
            s[i - 1] = s[i - 2];
            len = delete(s, i, len);
            i --;
          }
          break;

        // Visarga
        case '\u0983':
          if(i == len -1) {
            if(len <= 3) {
              s[i] = '\u09B9';
            } else {
              len = delete(s, i, len);
            }
          } else {
            s[i] = s[i+1];
          }
          break;

        //All sh
        case '\u09B6':
        case '\u09B7':
          s[i] = '\u09B8';
          break;

        //check na
        case '\u09A3':
          s[i] = '\u09A8';
          break;

        //check ra
        case '\u09DC':
        case '\u09DD':
          s[i] = '\u09B0';
          break;

        case '\u09CE':
          s[i] = '\u09A4';
          break;

        default:
          break;
      }
    }

    return len;
  }
}
