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

package org.apache.lucene.analysis.cjk;

import org.apache.lucene.analysis.charfilter.BaseCharFilter;

import java.io.IOException;
import java.io.Reader;

/**
 * A {@link org.apache.lucene.analysis.CharFilter} that normalizes CJK width differences:
 * <ul>
 *   <li>Folds fullwidth ASCII variants into the equivalent basic latin
 *   <li>Folds halfwidth Katakana variants into the equivalent kana
 * </ul>
 * <p>
 * NOTE: this char filter is the exact counterpart of {@link CJKWidthFilter}.
 */
public class CJKWidthCharFilter extends BaseCharFilter {

  /* halfwidth kana mappings: 0xFF65-0xFF9D
   *
   * note: 0xFF9C and 0xFF9D are only mapped to 0x3099 and 0x309A
   * as a fallback when they cannot properly combine with a preceding
   * character into a composed form.
   */
  private static final char KANA_NORM[] = new char[] {
    0x30fb, 0x30f2, 0x30a1, 0x30a3, 0x30a5, 0x30a7, 0x30a9, 0x30e3, 0x30e5,
    0x30e7, 0x30c3, 0x30fc, 0x30a2, 0x30a4, 0x30a6, 0x30a8, 0x30aa, 0x30ab,
    0x30ad, 0x30af, 0x30b1, 0x30b3, 0x30b5, 0x30b7, 0x30b9, 0x30bb, 0x30bd,
    0x30bf, 0x30c1, 0x30c4, 0x30c6, 0x30c8, 0x30ca, 0x30cb, 0x30cc, 0x30cd,
    0x30ce, 0x30cf, 0x30d2, 0x30d5, 0x30d8, 0x30db, 0x30de, 0x30df, 0x30e0,
    0x30e1, 0x30e2, 0x30e4, 0x30e6, 0x30e8, 0x30e9, 0x30ea, 0x30eb, 0x30ec,
    0x30ed, 0x30ef, 0x30f3, 0x3099, 0x309A
  };

  /* kana combining diffs: 0x30A6-0x30FD */
  private static final byte KANA_COMBINE_VOICED[] = new byte[] {
    78, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1,
    0, 1, 0, 1, 0, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 1,
    0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 8, 8, 8, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1
  };

  private static final byte KANA_COMBINE_SEMI_VOICED[] = new byte[] {
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 2, 0, 0, 2,
    0, 0, 2, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
  };

  private static final int HW_KATAKANA_VOICED_MARK = 0xFF9E;
  private static final int HW_KATAKANA_SEMI_VOICED_MARK = 0xFF9F;

  private int prevChar = -1;
  private int inputOff = 0;

  /** Default constructor that takes a {@link Reader}. */
  public CJKWidthCharFilter(Reader in) {
    super(in);
  }

  @Override
  public int read() throws IOException {
    while(true) {
      final int ch = input.read();
      if (ch == -1) {
        // reached end of the input
        int ret = prevChar;
        prevChar = ch;
        return ret;
      }

      inputOff++;
      int ret = -1;
      // if the current char is a voice mark, then try to combine it with the previous char.
      if (ch == HW_KATAKANA_SEMI_VOICED_MARK || ch == HW_KATAKANA_VOICED_MARK) {
        final int combinedChar = combineVoiceMark(prevChar, ch);
        if (prevChar != combinedChar) {
          // successfully combined. returns the combined char immediately
          prevChar = -1;
          // offset needs to be corrected
          final int prevCumulativeDiff = getLastCumulativeDiff();
          addOffCorrectMap(inputOff - 1 - prevCumulativeDiff, prevCumulativeDiff + 1);
          return combinedChar;
        }
      }

      if (prevChar != -1) {
        ret = prevChar;
      }

      if (ch >= 0xFF01 && ch <= 0xFF5E) {
        // Fullwidth ASCII variants
        prevChar = ch - 0xFEE0;
      } else if (ch >= 0xFF65 && ch <= 0xFF9F) {
        // Halfwidth Katakana variants
        prevChar = KANA_NORM[ch - 0xFF65];
      } else {
        // no need to normalize
        prevChar = ch;
      }

      if (ret != -1) {
        return ret;
      }
    }
  }

  /** returns combined char if we successfully combined the voice mark, otherwise original char */
  private int combineVoiceMark(int ch, int voiceMark) {
    assert voiceMark == HW_KATAKANA_SEMI_VOICED_MARK || voiceMark == HW_KATAKANA_VOICED_MARK;
    if (ch >= 0x30A6 && ch <= 0x30FD) {
      ch += (voiceMark == HW_KATAKANA_SEMI_VOICED_MARK)
        ? KANA_COMBINE_SEMI_VOICED[prevChar - 0x30A6]
        : KANA_COMBINE_VOICED[prevChar - 0x30A6];
    }
    return ch;
  }

  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    int numRead = 0;
    for(int i = off; i < off + len; i++) {
      int c = read();
      if (c == -1) break;
      cbuf[i] = (char) c;
      numRead++;
    }
    return numRead == 0 ? -1 : numRead;
  }

}
