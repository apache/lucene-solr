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
package org.apache.lucene.analysis.icu;


import java.io.IOException;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.ibm.icu.text.Replaceable;
import com.ibm.icu.text.Transliterator;
import com.ibm.icu.text.UTF16;
import com.ibm.icu.text.UnicodeSet;

/**
 * A {@link TokenFilter} that transforms text with ICU.
 * <p>
 * ICU provides text-transformation functionality via its Transliteration API.
 * Although script conversion is its most common use, a Transliterator can
 * actually perform a more general class of tasks. In fact, Transliterator
 * defines a very general API which specifies only that a segment of the input
 * text is replaced by new text. The particulars of this conversion are
 * determined entirely by subclasses of Transliterator.
 * </p>
 * <p>
 * Some useful transformations for search are built-in:
 * <ul>
 * <li>Conversion from Traditional to Simplified Chinese characters
 * <li>Conversion from Hiragana to Katakana
 * <li>Conversion from Fullwidth to Halfwidth forms.
 * <li>Script conversions, for example Serbian Cyrillic to Latin
 * </ul>
 * <p>
 * Example usage: <blockquote>stream = new ICUTransformFilter(stream,
 * Transliterator.getInstance("Traditional-Simplified"));</blockquote>
 * <br>
 * For more details, see the <a
 * href="http://userguide.icu-project.org/transforms/general">ICU User
 * Guide</a>.
 */
public final class ICUTransformFilter extends TokenFilter {
  // Transliterator to transform the text
  private final Transliterator transform;

  // Reusable position object
  private final Transliterator.Position position = new Transliterator.Position();

  // term attribute, will be updated with transformed text.
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  // Wraps a termAttribute around the replaceable interface.
  private final ReplaceableTermAttribute replaceableAttribute = new ReplaceableTermAttribute();

  /**
   * Create a new ICUTransformFilter that transforms text on the given stream.
   * 
   * @param input {@link TokenStream} to filter.
   * @param transform Transliterator to transform the text.
   */
  public ICUTransformFilter(TokenStream input, Transliterator transform) {
    super(input);
    this.transform = transform;

    /* 
     * This is cheating, but speeds things up a lot.
     * If we wanted to use pkg-private APIs we could probably do better.
     */
    if (transform.getFilter() == null && transform instanceof com.ibm.icu.text.RuleBasedTransliterator) {
      final UnicodeSet sourceSet = transform.getSourceSet();
      if (sourceSet != null && !sourceSet.isEmpty())
        transform.setFilter(sourceSet);
    }
  }

  @Override
  public boolean incrementToken() throws IOException {
    /*
     * Wrap around replaceable. clear the positions, and transliterate.
     */
    if (input.incrementToken()) {
      replaceableAttribute.setText(termAtt);
      
      final int length = termAtt.length(); 
      position.start = 0;
      position.limit = length;
      position.contextStart = 0;
      position.contextLimit = length;

      transform.filteredTransliterate(replaceableAttribute, position, false);
      return true;
    } else {
      return false;
    }
  }
  
  /**
   * Wrap a {@link CharTermAttribute} with the Replaceable API.
   */
  final class ReplaceableTermAttribute implements Replaceable {
    private char buffer[];
    private int length;
    private CharTermAttribute token;

    void setText(final CharTermAttribute token) {
      this.token = token;
      this.buffer = token.buffer();
      this.length = token.length();
    }

    @Override
    public int char32At(int pos) {
      return UTF16.charAt(buffer, 0, length, pos);
    }

    @Override
    public char charAt(int pos) {
      return buffer[pos];
    }

    @Override
    public void copy(int start, int limit, int dest) {
      char text[] = new char[limit - start];
      getChars(start, limit, text, 0);
      replace(dest, dest, text, 0, limit - start);
    }

    @Override
    public void getChars(int srcStart, int srcLimit, char[] dst, int dstStart) {
      System.arraycopy(buffer, srcStart, dst, dstStart, srcLimit - srcStart);
    }

    @Override
    public boolean hasMetaData() {
      return false;
    }

    @Override
    public int length() {
      return length;
    }

    @Override
    public void replace(int start, int limit, String text) {
      final int charsLen = text.length();
      final int newLength = shiftForReplace(start, limit, charsLen);
      // insert the replacement text
      text.getChars(0, charsLen, buffer, start);
      token.setLength(length = newLength);
    }

    @Override
    public void replace(int start, int limit, char[] text, int charsStart,
        int charsLen) {
      // shift text if necessary for the replacement
      final int newLength = shiftForReplace(start, limit, charsLen);
      // insert the replacement text
      System.arraycopy(text, charsStart, buffer, start, charsLen);
      token.setLength(length = newLength);
    }

    /** shift text (if necessary) for a replacement operation */
    private int shiftForReplace(int start, int limit, int charsLen) {
      final int replacementLength = limit - start;
      final int newLength = length - replacementLength + charsLen;
      // resize if necessary
      if (newLength > length)
        buffer = token.resizeBuffer(newLength);
      // if the substring being replaced is longer or shorter than the
      // replacement, need to shift things around
      if (replacementLength != charsLen && limit < length)
        System.arraycopy(buffer, limit, buffer, start + charsLen, length - limit);
      return newLength;
    }
  }
}
