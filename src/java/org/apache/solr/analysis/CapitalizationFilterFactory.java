/**
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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * A filter to apply normal capitalization rules to Tokens.  It will make the first letter
 * capital and the rest lower case.
 * <p/>
 * This filter is particularly useful to build nice looking facet parameters.  This filter
 * is not appropriate if you intend to use a prefix query.
 * <p/>
 * The factory takes parameters:<br/>
 * "onlyFirstWord" - should each word be capitalized or all of the words?<br/>
 * "keep" - a keep word list.  Each word that should be kept separated by whitespace.<br/>
 * "keepIgnoreCase - true or false.  If true, the keep list will be considered case-insensitive.
 * "forceFirstLetter" - Force the first letter to be capitalized even if it is in the keep list<br/>
 * "okPrefix" - do not change word capitalization if a word begins with something in this list.
 * for example if "McK" is on the okPrefix list, the word "McKinley" should not be changed to
 * "Mckinley"<br/>
 * "minWordLength" - how long the word needs to be to get capitalization applied.  If the
 * minWordLength is 3, "and" > "And" but "or" stays "or"<br/>
 * "maxWordCount" - if the token contains more then maxWordCount words, the capitalization is
 * assumed to be correct.<br/>
 *
 * @version $Id$
 * @since solr 1.3
 */
public class CapitalizationFilterFactory extends BaseTokenFilterFactory {
  public static final int DEFAULT_MAX_WORD_COUNT = Integer.MAX_VALUE;
  public static final String KEEP = "keep";
  public static final String KEEP_IGNORE_CASE = "keepIgnoreCase";
  public static final String OK_PREFIX = "okPrefix";
  public static final String MIN_WORD_LENGTH = "minWordLength";
  public static final String MAX_WORD_COUNT = "maxWordCount";
  public static final String MAX_TOKEN_LENGTH = "maxTokenLength";
  public static final String ONLY_FIRST_WORD = "onlyFirstWord";
  public static final String FORCE_FIRST_LETTER = "forceFirstLetter";

  //Map<String,String> keep = new HashMap<String, String>(); // not synchronized because it is only initialized once
  CharArraySet keep;

  Collection<char[]> okPrefix = Collections.emptyList(); // for Example: McK

  int minWordLength = 0;  // don't modify capitalization for words shorter then this
  int maxWordCount = DEFAULT_MAX_WORD_COUNT;
  int maxTokenLength = DEFAULT_MAX_WORD_COUNT;
  boolean onlyFirstWord = true;
  boolean forceFirstLetter = true; // make sure the first letter is capitol even if it is in the keep list

  @Override
  public void init(Map<String, String> args) {
    super.init(args);

    String k = args.get(KEEP);
    if (k != null) {
      StringTokenizer st = new StringTokenizer(k);
      boolean ignoreCase = false;
      String ignoreStr = args.get(KEEP_IGNORE_CASE);
      if ("true".equalsIgnoreCase(ignoreStr)) {
        ignoreCase = true;
      }
      keep = new CharArraySet(10, ignoreCase);
      while (st.hasMoreTokens()) {
        k = st.nextToken().trim();
        keep.add(k.toCharArray());
      }
    }

    k = args.get(OK_PREFIX);
    if (k != null) {
      okPrefix = new ArrayList<char[]>();
      StringTokenizer st = new StringTokenizer(k);
      while (st.hasMoreTokens()) {
        okPrefix.add(st.nextToken().trim().toCharArray());
      }
    }

    k = args.get(MIN_WORD_LENGTH);
    if (k != null) {
      minWordLength = Integer.valueOf(k);
    }

    k = args.get(MAX_WORD_COUNT);
    if (k != null) {
      maxWordCount = Integer.valueOf(k);
    }

    k = args.get(MAX_TOKEN_LENGTH);
    if (k != null) {
      maxTokenLength = Integer.valueOf(k);
    }

    k = args.get(ONLY_FIRST_WORD);
    if (k != null) {
      onlyFirstWord = Boolean.valueOf(k);
    }

    k = args.get(FORCE_FIRST_LETTER);
    if (k != null) {
      forceFirstLetter = Boolean.valueOf(k);
    }
  }


  public void processWord(char[] buffer, int offset, int length, int wordCount) {
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

  public CapitalizationFilter create(TokenStream input) {
    return new CapitalizationFilter(input, this);
  }
}


/**
 * This relies on the Factory so that the difficult stuff does not need to be
 * re-initialized each time the filter runs.
 * <p/>
 * This is package protected since it is not useful without the Factory
 */
class CapitalizationFilter extends TokenFilter {
  private final CapitalizationFilterFactory factory;
  private final TermAttribute termAtt;

  public CapitalizationFilter(TokenStream in, final CapitalizationFilterFactory factory) {
    super(in);
    this.factory = factory;
    this.termAtt = (TermAttribute) addAttribute(TermAttribute.class);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (!input.incrementToken()) return false;

    char[] termBuffer = termAtt.termBuffer();
    int termBufferLength = termAtt.termLength();
    char[] backup = null;
    if (factory.maxWordCount < CapitalizationFilterFactory.DEFAULT_MAX_WORD_COUNT) {
      //make a backup in case we exceed the word count
      System.arraycopy(termBuffer, 0, backup, 0, termBufferLength);
    }
    if (termBufferLength < factory.maxTokenLength) {
      int wordCount = 0;

      int lastWordStart = 0;
      for (int i = 0; i < termBufferLength; i++) {
        char c = termBuffer[i];
        if (c <= ' ' || c == '.') {
          int len = i - lastWordStart;
          if (len > 0) {
            factory.processWord(termBuffer, lastWordStart, len, wordCount++);
            lastWordStart = i + 1;
            i++;
          }
        }
      }

      // process the last word
      if (lastWordStart < termBufferLength) {
        factory.processWord(termBuffer, lastWordStart, termBufferLength - lastWordStart, wordCount++);
      }

      if (wordCount > factory.maxWordCount) {
        termAtt.setTermBuffer(backup, 0, termBufferLength);
      }
    }

    return true;
  }

}

