package org.apache.lucene.analysis.hi;

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

import static org.apache.lucene.analysis.util.StemmerUtil.*;

/**
 * Normalizer for Hindi.
 * <p>
 * Normalizes text to remove some differences in spelling variations.
 * <p>
 * Implements the Hindi-language specific algorithm specified in:
 * <i>Word normalization in Indian languages</i>
 * Prasad Pingali and Vasudeva Varma.
 * http://web2py.iiit.ac.in/publications/default/download/inproceedings.pdf.3fe5b38c-02ee-41ce-9a8f-3e745670be32.pdf
 * <p>
 * with the following additions from <i>Hindi CLIR in Thirty Days</i>
 * Leah S. Larkey, Margaret E. Connell, and Nasreen AbdulJaleel.
 * http://maroo.cs.umass.edu/pub/web/getpdf.php?id=454:
 * <ul>
 *  <li>Internal Zero-width joiner and Zero-width non-joiners are removed
 *  <li>In addition to chandrabindu, NA+halant is normalized to anusvara
 * </ul>
 * 
 */
public class HindiNormalizer {
  /**
   * Normalize an input buffer of Hindi text
   * 
   * @param s input buffer
   * @param len length of input buffer
   * @return length of input buffer after normalization
   */
  public int normalize(char s[], int len) {

    for (int i = 0; i < len; i++) {
      switch (s[i]) {
        // dead n -> bindu
      case '\u0928':
        if (i + 1 < len && s[i + 1] == '\u094D') {
          s[i] = '\u0902';
          len = delete(s, i + 1, len);
        }
        break;
      // candrabindu -> bindu
      case '\u0901':
        s[i] = '\u0902';
        break;
      // nukta deletions
      case '\u093C':
        len = delete(s, i, len);
        i--;
        break;      
      case '\u0929':
        s[i] = '\u0928';
        break;
      case '\u0931':
        s[i] = '\u0930';
        break;
      case '\u0934':
        s[i] = '\u0933';
        break;
      case '\u0958':
        s[i] = '\u0915';
        break;
      case '\u0959':
        s[i] = '\u0916';
        break;
      case '\u095A':
        s[i] = '\u0917';
        break;
      case '\u095B':
        s[i] = '\u091C';
        break;
      case '\u095C':
        s[i] = '\u0921';
        break;
      case '\u095D':
        s[i] = '\u0922';
        break;
      case '\u095E':
        s[i] = '\u092B';
        break;
      case '\u095F':
        s[i] = '\u092F';
        break;
        // zwj/zwnj -> delete
      case '\u200D':
      case '\u200C':
        len = delete(s, i, len);
        i--;
        break;
        // virama -> delete
      case '\u094D':
        len = delete(s, i, len);
        i--;
        break;
        // chandra/short -> replace
      case '\u0945':
      case '\u0946':
        s[i] = '\u0947';
        break;
      case '\u0949':
      case '\u094A':
        s[i] = '\u094B';
        break;
      case '\u090D':
      case '\u090E':
        s[i] = '\u090F';
        break;
      case '\u0911':
      case '\u0912':
        s[i] = '\u0913';
        break;
      case '\u0972':
        s[i] = '\u0905';
        break;
        // long -> short ind. vowels
      case '\u0906':
        s[i] = '\u0905';
        break;
      case '\u0908':
        s[i] = '\u0907';
        break;
      case '\u090A':
        s[i] = '\u0909';
        break;
      case '\u0960':
        s[i] = '\u090B';
        break;
      case '\u0961':
        s[i] = '\u090C';
        break;
      case '\u0910':
        s[i] = '\u090F';
        break;
      case '\u0914':
        s[i] = '\u0913';
        break;
        // long -> short dep. vowels
      case '\u0940':
        s[i] = '\u093F';
        break;
      case '\u0942':
        s[i] = '\u0941';
        break;
      case '\u0944':
        s[i] = '\u0943';
        break;
      case '\u0963':
        s[i] = '\u0962';
        break;
      case '\u0948':
        s[i] = '\u0947';
        break;
      case '\u094C':
        s[i] = '\u094B';
        break;
      default:
        break;
      }
    }

    return len;
  }
}
