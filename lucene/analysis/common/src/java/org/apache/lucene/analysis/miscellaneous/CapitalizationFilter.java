package org.apache.lucene.analysis.miscellaneous;

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

import java.io.IOException;
import java.util.Collection;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

/** 
 * A filter to apply normal capitalization rules to Tokens.  It will make the first letter
 * capital and the rest lower case.
 * <p/>
 * This filter is particularly useful to build nice looking facet parameters.  This filter
 * is not appropriate if you intend to use a prefix query.
 */
public final class CapitalizationFilter extends TokenFilter {
  public static final int DEFAULT_MAX_WORD_COUNT = Integer.MAX_VALUE;
  public static final int DEFAULT_MAX_TOKEN_LENGTH = Integer.MAX_VALUE;
  
  private final boolean onlyFirstWord;
  private final CharArraySet keep;
  private final boolean forceFirstLetter;
  private final Collection<char[]> okPrefix;

  private final int minWordLength;
  private final int maxWordCount;
  private final int maxTokenLength;
  
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  /**
   * Creates a CapitalizationFilter with the default parameters.
   * <p>
   * Calls {@link #CapitalizationFilter(TokenStream, boolean, CharArraySet, boolean, Collection, int, int, int)
   *   CapitalizationFilter(in, true, null, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH)}
   */
  public CapitalizationFilter(TokenStream in) {
    this(in, true, null, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);
  }
  
  /**
   * Creates a CapitalizationFilter with the specified parameters.
   * @param in input tokenstream 
   * @param onlyFirstWord should each word be capitalized or all of the words?
   * @param keep a keep word list.  Each word that should be kept separated by whitespace.
   * @param forceFirstLetter Force the first letter to be capitalized even if it is in the keep list.
   * @param okPrefix do not change word capitalization if a word begins with something in this list.
   * @param minWordLength how long the word needs to be to get capitalization applied.  If the
   *                      minWordLength is 3, "and" > "And" but "or" stays "or".
   * @param maxWordCount if the token contains more then maxWordCount words, the capitalization is
   *                     assumed to be correct.
   * @param maxTokenLength ???
   */
  public CapitalizationFilter(TokenStream in, boolean onlyFirstWord, CharArraySet keep, 
      boolean forceFirstLetter, Collection<char[]> okPrefix, int minWordLength, 
      int maxWordCount, int maxTokenLength) {
    super(in);
    this.onlyFirstWord = onlyFirstWord;
    this.keep = keep;
    this.forceFirstLetter = forceFirstLetter;
    this.okPrefix = okPrefix;
    this.minWordLength = minWordLength;
    this.maxWordCount = maxWordCount;
    this.maxTokenLength = maxTokenLength;
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) return false;

    char[] termBuffer = termAtt.buffer();
    int termBufferLength = termAtt.length();
    char[] backup = null;
    
    if (maxWordCount < DEFAULT_MAX_WORD_COUNT) {
      //make a backup in case we exceed the word count
      backup = new char[termBufferLength];
      System.arraycopy(termBuffer, 0, backup, 0, termBufferLength);
    }
    
    if (termBufferLength < maxTokenLength) {
      int wordCount = 0;

      int lastWordStart = 0;
      for (int i = 0; i < termBufferLength; i++) {
        char c = termBuffer[i];
        if (c <= ' ' || c == '.') {
          int len = i - lastWordStart;
          if (len > 0) {
            processWord(termBuffer, lastWordStart, len, wordCount++);
            lastWordStart = i + 1;
            i++;
          }
        }
      }

      // process the last word
      if (lastWordStart < termBufferLength) {
        processWord(termBuffer, lastWordStart, termBufferLength - lastWordStart, wordCount++);
      }

      if (wordCount > maxWordCount) {
        termAtt.copyBuffer(backup, 0, termBufferLength);
      }
    }

    return true;
  }
  
  private void processWord(char[] buffer, int offset, int length, int wordCount) {
    if (length < 1) {
      return;
    }
    
    if (onlyFirstWord && wordCount > 0) {
      for (int i = 0; i < length; i++) {
        buffer[offset + i] = Character.toLowerCase(buffer[offset + i]);

      }
      return;
    }

    if (keep != null && keep.contains(buffer, offset, length)) {
      if (wordCount == 0 && forceFirstLetter) {
        buffer[offset] = Character.toUpperCase(buffer[offset]);
      }
      return;
    }
    
    if (length < minWordLength) {
      return;
    }
    
    if (okPrefix != null) {
      for (char[] prefix : okPrefix) {
        if (length >= prefix.length) { //don't bother checking if the buffer length is less than the prefix
          boolean match = true;
          for (int i = 0; i < prefix.length; i++) {
            if (prefix[i] != buffer[offset + i]) {
              match = false;
              break;
            }
          }
          if (match == true) {
            return;
          }
        }
      }
    }

    // We know it has at least one character
    /*char[] chars = w.toCharArray();
    StringBuilder word = new StringBuilder( w.length() );
    word.append( Character.toUpperCase( chars[0] ) );*/
    buffer[offset] = Character.toUpperCase(buffer[offset]);

    for (int i = 1; i < length; i++) {
      buffer[offset + i] = Character.toLowerCase(buffer[offset + i]);
    }
    //return word.toString();
  }
}
