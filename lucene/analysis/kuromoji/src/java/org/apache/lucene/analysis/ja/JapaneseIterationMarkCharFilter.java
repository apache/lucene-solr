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


import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.util.RollingCharBuffer;

import java.io.IOException;
import java.io.Reader;

/**
 * Normalizes Japanese horizontal iteration marks (odoriji) to their expanded form.
 * <p>
 * Sequences of iteration marks are supported.  In case an illegal sequence of iteration
 * marks is encountered, the implementation emits the illegal source character as-is
 * without considering its script.  For example, with input "?ゝ", we get
 * "??" even though the question mark isn't hiragana.
 * </p>
 * <p>
 * Note that a full stop punctuation character "。" (U+3002) can not be iterated
 * (see below). Iteration marks themselves can be emitted in case they are illegal,
 * i.e. if they go back past the beginning of the character stream.
 * </p>
 * <p>
 * The implementation buffers input until a full stop punctuation character (U+3002)
 * or EOF is reached in order to not keep a copy of the character stream in memory.
 * Vertical iteration marks, which are even rarer than horizontal iteration marks in
 * contemporary Japanese, are unsupported.
 * </p>
 */
public class JapaneseIterationMarkCharFilter extends CharFilter {

  /** Normalize kanji iteration marks by default */
  public static final boolean NORMALIZE_KANJI_DEFAULT = true; 

  /** Normalize kana iteration marks by default */
  public static final boolean NORMALIZE_KANA_DEFAULT = true;

  private static final char KANJI_ITERATION_MARK = '\u3005';           // 々

  private static final char HIRAGANA_ITERATION_MARK = '\u309d';        // ゝ

  private static final char HIRAGANA_VOICED_ITERATION_MARK = '\u309e'; // ゞ

  private static final char KATAKANA_ITERATION_MARK = '\u30fd';        // ヽ

  private static final char KATAKANA_VOICED_ITERATION_MARK = '\u30fe'; // ヾ

  private static final char FULL_STOP_PUNCTUATION = '\u3002';           // 。

  // Hiragana to dakuten map (lookup using code point - 0x30ab（か）*/
  private static char[] h2d = new char[50];

  // Katakana to dakuten map (lookup using code point - 0x30ab（カ
  private static char[] k2d = new char[50];

  private final RollingCharBuffer buffer = new RollingCharBuffer();

  private int bufferPosition = 0;

  private int iterationMarksSpanSize = 0;

  private int iterationMarkSpanEndPosition = 0;

  private boolean normalizeKanji;

  private boolean normalizeKana;

  static {
    // Hiragana dakuten map
    h2d[0] = '\u304c';  // か => が
    h2d[1] = '\u304c';  // が => が
    h2d[2] = '\u304e';  // き => ぎ
    h2d[3] = '\u304e';  // ぎ => ぎ
    h2d[4] = '\u3050';  // く => ぐ
    h2d[5] = '\u3050';  // ぐ => ぐ
    h2d[6] = '\u3052';  // け => げ
    h2d[7] = '\u3052';  // げ => げ
    h2d[8] = '\u3054';  // こ => ご
    h2d[9] = '\u3054';  // ご => ご
    h2d[10] = '\u3056'; // さ => ざ
    h2d[11] = '\u3056'; // ざ => ざ
    h2d[12] = '\u3058'; // し => じ
    h2d[13] = '\u3058'; // じ => じ
    h2d[14] = '\u305a'; // す => ず
    h2d[15] = '\u305a'; // ず => ず
    h2d[16] = '\u305c'; // せ => ぜ
    h2d[17] = '\u305c'; // ぜ => ぜ
    h2d[18] = '\u305e'; // そ => ぞ
    h2d[19] = '\u305e'; // ぞ => ぞ
    h2d[20] = '\u3060'; // た => だ
    h2d[21] = '\u3060'; // だ => だ
    h2d[22] = '\u3062'; // ち => ぢ
    h2d[23] = '\u3062'; // ぢ => ぢ
    h2d[24] = '\u3063';
    h2d[25] = '\u3065'; // つ => づ
    h2d[26] = '\u3065'; // づ => づ
    h2d[27] = '\u3067'; // て => で
    h2d[28] = '\u3067'; // で => で
    h2d[29] = '\u3069'; // と => ど
    h2d[30] = '\u3069'; // ど => ど
    h2d[31] = '\u306a';
    h2d[32] = '\u306b';
    h2d[33] = '\u306c';
    h2d[34] = '\u306d';
    h2d[35] = '\u306e';
    h2d[36] = '\u3070'; // は => ば
    h2d[37] = '\u3070'; // ば => ば
    h2d[38] = '\u3071';
    h2d[39] = '\u3073'; // ひ => び
    h2d[40] = '\u3073'; // び => び
    h2d[41] = '\u3074';
    h2d[42] = '\u3076'; // ふ => ぶ
    h2d[43] = '\u3076'; // ぶ => ぶ
    h2d[44] = '\u3077';
    h2d[45] = '\u3079'; // へ => べ
    h2d[46] = '\u3079'; // べ => べ
    h2d[47] = '\u307a';
    h2d[48] = '\u307c'; // ほ => ぼ
    h2d[49] = '\u307c'; // ぼ => ぼ

    // Make katakana dakuten map from hiragana map
    char codePointDifference = '\u30ab' - '\u304b'; // カ - か
    assert h2d.length == k2d.length;
    for (int i = 0; i < k2d.length; i++) {
      k2d[i] = (char) (h2d[i] + codePointDifference);
    }
  }

  /**
   * Constructor. Normalizes both kanji and kana iteration marks by default.
   *
   * @param input char stream
   */
  public JapaneseIterationMarkCharFilter(Reader input) {
    this(input, NORMALIZE_KANJI_DEFAULT, NORMALIZE_KANA_DEFAULT);
  }


  /**
   * Constructor
   *
   * @param input          char stream
   * @param normalizeKanji indicates whether kanji iteration marks should be normalized
   * @param normalizeKana indicates whether kana iteration marks should be normalized
   */
  public JapaneseIterationMarkCharFilter(Reader input, boolean normalizeKanji, boolean normalizeKana) {
    super(input);
    this.normalizeKanji = normalizeKanji;
    this.normalizeKana = normalizeKana;
    buffer.reset(input);
  }

  @Override
  public int read(char[] buffer, int offset, int length) throws IOException {
    int read = 0;

    for (int i = offset; i < offset + length; i++) {
      int c = read();
      if (c == -1) {
        break;
      }
      buffer[i] = (char) c;
      read++;
    }

    return read == 0 ? -1 : read;
  }

  @Override
  public int read() throws IOException {
    int ic = buffer.get(bufferPosition);

    // End of input
    if (ic == -1) {
      buffer.freeBefore(bufferPosition);
      return ic;
    }
    
    char c = (char) ic;

    // Skip surrogate pair characters
    if (Character.isHighSurrogate(c) || Character.isLowSurrogate(c)) {
      iterationMarkSpanEndPosition = bufferPosition + 1;
    }

    // Free rolling buffer on full stop
    if (c == FULL_STOP_PUNCTUATION) {
      buffer.freeBefore(bufferPosition);
      iterationMarkSpanEndPosition = bufferPosition + 1;
    }
    
    // Normalize iteration mark
    if (isIterationMark(c)) {
      c = normalizeIterationMark(c);
    }
    
    bufferPosition++;
    return c;
  }

  /**
   * Normalizes the iteration mark character c
   *
   * @param c iteration mark character to normalize
   * @return normalized iteration mark
   * @throws IOException If there is a low-level I/O error.
   */
  private char normalizeIterationMark(char c) throws IOException {

    // Case 1: Inside an iteration mark span
    if (bufferPosition < iterationMarkSpanEndPosition) {
      return normalize(sourceCharacter(bufferPosition, iterationMarksSpanSize), c);
    }

    // Case 2: New iteration mark spans starts where the previous one ended, which is illegal
    if (bufferPosition == iterationMarkSpanEndPosition) {
      // Emit the illegal iteration mark and increase end position to indicate that we can't
      // start a new span on the next position either
      iterationMarkSpanEndPosition++;
      return c;
    }

    // Case 3: New iteration mark span
    iterationMarksSpanSize = nextIterationMarkSpanSize();
    iterationMarkSpanEndPosition = bufferPosition + iterationMarksSpanSize;
    return normalize(sourceCharacter(bufferPosition, iterationMarksSpanSize), c);
  }

  /**
   * Finds the number of subsequent next iteration marks
   *
   * @return number of iteration marks starting at the current buffer position
   * @throws IOException If there is a low-level I/O error.
   */
  private int nextIterationMarkSpanSize() throws IOException {
    int spanSize = 0;
    for (int i = bufferPosition; buffer.get(i) != -1 && isIterationMark((char) (buffer.get(i))); i++) {
      spanSize++;
    }
    // Restrict span size so that we don't go past the previous end position
    if (bufferPosition - spanSize < iterationMarkSpanEndPosition) {
      spanSize = bufferPosition - iterationMarkSpanEndPosition;
    }
    return spanSize;
  }

  /**
   * Returns the source character for a given position and iteration mark span size
   *
   * @param position buffer position (should not exceed bufferPosition)
   * @param spanSize iteration mark span size
   * @return source character
   * @throws IOException If there is a low-level I/O error.
   */
  private char sourceCharacter(int position, int spanSize) throws IOException {
    return (char) buffer.get(position - spanSize);
  }

  /**
   * Normalize a character
   *
   * @param c character to normalize
   * @param m repetition mark referring to c
   * @return normalized character - return c on illegal iteration marks
   */
  private char normalize(char c, char m) {
    if (isHiraganaIterationMark(m)) {
      return normalizedHiragana(c, m);
    }

    if (isKatakanaIterationMark(m)) {
      return normalizedKatakana(c, m);
    }

    return c; // If m is not kana and we are to normalize it, we assume it is kanji and simply return it
  }

  /**
   * Normalize hiragana character
   *
   * @param c hiragana character
   * @param m repetition mark referring to c
   * @return normalized character - return c on illegal iteration marks
   */
  private char normalizedHiragana(char c, char m) {
    switch (m) {
      case HIRAGANA_ITERATION_MARK:
        return isHiraganaDakuten(c) ? (char) (c - 1) : c;
      case HIRAGANA_VOICED_ITERATION_MARK:
        return lookupHiraganaDakuten(c);
      default:
        return c;
    }
  }

  /**
   * Normalize katakana character
   *
   * @param c katakana character
   * @param m repetition mark referring to c
   * @return normalized character - return c on illegal iteration marks
   */
  private char normalizedKatakana(char c, char m) {
    switch (m) {
      case KATAKANA_ITERATION_MARK:
        return isKatakanaDakuten(c) ? (char) (c - 1) : c;
      case KATAKANA_VOICED_ITERATION_MARK:
        return lookupKatakanaDakuten(c);
      default:
        return c;
    }
  }

  /**
   * Iteration mark character predicate
   *
   * @param c character to test
   * @return true if c is an iteration mark character.  Otherwise false.
   */
  private boolean isIterationMark(char c) {
    return isKanjiIterationMark(c) || isHiraganaIterationMark(c) || isKatakanaIterationMark(c);
  }

  /**
   * Hiragana iteration mark character predicate
   *
   * @param c character to test
   * @return true if c is a hiragana iteration mark character.  Otherwise false.
   */
  private boolean isHiraganaIterationMark(char c) {
    if (normalizeKana) {
      return c == HIRAGANA_ITERATION_MARK || c == HIRAGANA_VOICED_ITERATION_MARK;
    } else {
      return false;
    }
  }

  /**
   * Katakana iteration mark character predicate
   *
   * @param c character to test
   * @return true if c is a katakana iteration mark character.  Otherwise false.
   */
  private boolean isKatakanaIterationMark(char c) {
    if (normalizeKana) {
      return c == KATAKANA_ITERATION_MARK || c == KATAKANA_VOICED_ITERATION_MARK;
    } else {
      return false;
    }
  }

  /**
   * Kanji iteration mark character predicate
   *
   * @param c character to test
   * @return true if c is a kanji iteration mark character.  Otherwise false.
   */
  private boolean isKanjiIterationMark(char c) {
    if (normalizeKanji) {
      return c == KANJI_ITERATION_MARK;
    } else {
      return false;
    }
  }

  /**
   * Look up hiragana dakuten
   *
   * @param c character to look up
   * @return hiragana dakuten variant of c or c itself if no dakuten variant exists
   */
  private char lookupHiraganaDakuten(char c) {
    return lookup(c, h2d, '\u304b'); // Code point is for か
  }

  /**
   * Look up katakana dakuten. Only full-width katakana are supported.
   *
   * @param c character to look up
   * @return katakana dakuten variant of c or c itself if no dakuten variant exists
   */
  private char lookupKatakanaDakuten(char c) {
    return lookup(c, k2d, '\u30ab'); // Code point is for カ
  }

  /**
   * Hiragana dakuten predicate
   *
   * @param c character to check
   * @return true if c is a hiragana dakuten and otherwise false
   */
  private boolean isHiraganaDakuten(char c) {
    return inside(c, h2d, '\u304b') && c == lookupHiraganaDakuten(c);
  }

  /**
   * Katakana dakuten predicate
   *
   * @param c character to check
   * @return true if c is a hiragana dakuten and otherwise false
   */
  private boolean isKatakanaDakuten(char c) {
    return inside(c, k2d, '\u30ab') && c == lookupKatakanaDakuten(c);
  }

  /**
   * Looks up a character in dakuten map and returns the dakuten variant if it exists.
   * Otherwise return the character being looked up itself
   *
   * @param c      character to look up
   * @param map    dakuten map
   * @param offset code point offset from c
   * @return mapped character or c if no mapping exists
   */
  private char lookup(char c, char[] map, char offset) {
    if (!inside(c, map, offset)) {
      return c;
    } else {
      return map[c - offset];
    }
  }

  /**
   * Predicate indicating if the lookup character is within dakuten map range
   *
   * @param c      character to look up
   * @param map    dakuten map
   * @param offset code point offset from c
   * @return true if c is mapped by map and otherwise false
   */
  private boolean inside(char c, char[] map, char offset) {
    return c >= offset && c < offset + map.length;
  }


  @Override
  protected int correct(int currentOff) {
    return currentOff; // this filter doesn't change the length of strings
  }
}
