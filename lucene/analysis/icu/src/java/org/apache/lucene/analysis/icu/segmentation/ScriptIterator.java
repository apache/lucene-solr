package org.apache.lucene.analysis.icu.segmentation;

/** 
 * Copyright (C) 1999-2010, International Business Machines
 * Corporation and others.  All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy 
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights 
 * to use, copy, modify, merge, publish, distribute, and/or sell copies of the 
 * Software, and to permit persons to whom the Software is furnished to do so, 
 * provided that the above copyright notice(s) and this permission notice appear 
 * in all copies of the Software and that both the above copyright notice(s) and
 * this permission notice appear in supporting documentation.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT OF THIRD PARTY RIGHTS. 
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR HOLDERS INCLUDED IN THIS NOTICE BE 
 * LIABLE FOR ANY CLAIM, OR ANY SPECIAL INDIRECT OR CONSEQUENTIAL DAMAGES, OR 
 * ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER 
 * IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT 
 * OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Except as contained in this notice, the name of a copyright holder shall not 
 * be used in advertising or otherwise to promote the sale, use or other 
 * dealings in this Software without prior written authorization of the 
 * copyright holder.
 */

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.lang.UCharacterEnums.ECharacterCategory;
import com.ibm.icu.lang.UScript;
import com.ibm.icu.text.UTF16;

/**
 * An iterator that locates ISO 15924 script boundaries in text. 
 * <p>
 * This is not the same as simply looking at the Unicode block, or even the 
 * Script property. Some characters are 'common' across multiple scripts, and
 * some 'inherit' the script value of text surrounding them.
 * <p>
 * This is similar to ICU (internal-only) UScriptRun, with the following
 * differences:
 * <ul>
 *  <li>Doesn't attempt to match paired punctuation. For tokenization purposes, this
 * is not necessary. Its also quite expensive. 
 *  <li>Non-spacing marks inherit the script of their base character, following 
 *  recommendations from UTR #24.
 * </ul>
 * @lucene.experimental
 */
final class ScriptIterator {
  private char text[];
  private int start;
  private int limit;
  private int index;

  private int scriptStart;
  private int scriptLimit;
  private int scriptCode;

  /**
   * Get the start of this script run
   * 
   * @return start position of script run
   */
  int getScriptStart() {
    return scriptStart;
  }

  /**
   * Get the index of the first character after the end of this script run
   * 
   * @return position of the first character after this script run
   */
  int getScriptLimit() {
    return scriptLimit;
  }

  /**
   * Get the UScript script code for this script run
   * 
   * @return code for the script of the current run
   */
  int getScriptCode() {
    return scriptCode;
  }

  /**
   * Iterates to the next script run, returning true if one exists.
   * 
   * @return true if there is another script run, false otherwise.
   */
  boolean next() {
    if (scriptLimit >= limit)
      return false;

    scriptCode = UScript.COMMON;
    scriptStart = scriptLimit;

    while (index < limit) {
      final int ch = UTF16.charAt(text, start, limit, index - start);
      final int sc = getScript(ch);

      /*
       * From UTR #24: Implementations that determine the boundaries between
       * characters of given scripts should never break between a non-spacing
       * mark and its base character. Thus for boundary determinations and
       * similar sorts of processing, a non-spacing mark — whatever its script
       * value — should inherit the script value of its base character.
       */
      if (isSameScript(scriptCode, sc)
          || UCharacter.getType(ch) == ECharacterCategory.NON_SPACING_MARK) {
        index += UTF16.getCharCount(ch);

        /*
         * Inherited or Common becomes the script code of the surrounding text.
         */
        if (scriptCode <= UScript.INHERITED && sc > UScript.INHERITED) {
          scriptCode = sc;
        }

      } else {
        break;
      }
    }

    scriptLimit = index;
    return true;
  }

  /** Determine if two scripts are compatible. */
  private static boolean isSameScript(int scriptOne, int scriptTwo) {
    return scriptOne <= UScript.INHERITED || scriptTwo <= UScript.INHERITED
        || scriptOne == scriptTwo;
  }

  /**
   * Set a new region of text to be examined by this iterator
   * 
   * @param text text buffer to examine
   * @param start offset into buffer
   * @param length maximum length to examine
   */
  void setText(char text[], int start, int length) {
    this.text = text;
    this.start = start;
    this.index = start;
    this.limit = start + length;
    this.scriptStart = start;
    this.scriptLimit = start;
    this.scriptCode = UScript.INVALID_CODE;
  }

  /** linear fast-path for basic latin case */
  private static final int basicLatin[] = new int[128];

  static {
    for (int i = 0; i < basicLatin.length; i++)
      basicLatin[i] = UScript.getScript(i);
  }

  /** fast version of UScript.getScript(). Basic Latin is an array lookup */
  private static int getScript(int codepoint) {
    if (0 <= codepoint && codepoint < basicLatin.length)
      return basicLatin[codepoint];
    else
      return UScript.getScript(codepoint);
  }
}
