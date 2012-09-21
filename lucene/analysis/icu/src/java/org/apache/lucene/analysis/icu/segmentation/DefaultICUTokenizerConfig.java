package org.apache.lucene.analysis.icu.segmentation;

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
import java.io.InputStream;

import org.apache.lucene.analysis.standard.StandardTokenizer;

import com.ibm.icu.lang.UScript;
import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.RuleBasedBreakIterator;
import com.ibm.icu.util.ULocale;

/**
 * Default {@link ICUTokenizerConfig} that is generally applicable
 * to many languages.
 * <p>
 * Generally tokenizes Unicode text according to UAX#29 
 * ({@link BreakIterator#getWordInstance(ULocale) BreakIterator.getWordInstance(ULocale.ROOT)}), 
 * but with the following tailorings:
 * <ul>
 *   <li>Thai text is broken into words with a 
 *   {@link com.ibm.icu.text.DictionaryBasedBreakIterator}
 *   <li>Lao, Myanmar, and Khmer text is broken into syllables
 *   based on custom BreakIterator rules.
 *   <li>Hebrew text has custom tailorings to handle special cases
 *   involving punctuation.
 * </ul>
 * @lucene.experimental
 */
public class DefaultICUTokenizerConfig extends ICUTokenizerConfig {
  /** Token type for words containing ideographic characters */
  public static final String WORD_IDEO = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.IDEOGRAPHIC];
  /** Token type for words containing Japanese hiragana */
  public static final String WORD_HIRAGANA = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.HIRAGANA];
  /** Token type for words containing Japanese katakana */
  public static final String WORD_KATAKANA = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.KATAKANA];
  /** Token type for words containing Korean hangul  */
  public static final String WORD_HANGUL = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.HANGUL];
  /** Token type for words that contain letters */
  public static final String WORD_LETTER = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.ALPHANUM];
  /** Token type for words that appear to be numbers */
  public static final String WORD_NUMBER = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.NUM];
  
  /*
   * the default breakiterators in use. these can be expensive to
   * instantiate, cheap to clone.
   */  
  private static final BreakIterator rootBreakIterator = 
    readBreakIterator("Default.brk");
  private static final BreakIterator thaiBreakIterator = 
    BreakIterator.getWordInstance(new ULocale("th_TH"));
  private static final BreakIterator hebrewBreakIterator = 
    readBreakIterator("Hebrew.brk");
  private static final BreakIterator khmerBreakIterator = 
    readBreakIterator("Khmer.brk");
  private static final BreakIterator laoBreakIterator = 
    new LaoBreakIterator(readBreakIterator("Lao.brk"));
  private static final BreakIterator myanmarBreakIterator = 
    readBreakIterator("Myanmar.brk");
  
  /** 
   * Creates a new config. This object is lightweight, but the first
   * time the class is referenced, breakiterators will be initialized.
   */
  public DefaultICUTokenizerConfig() {}

  @Override
  public BreakIterator getBreakIterator(int script) {
    switch(script) {
      case UScript.THAI: return (BreakIterator)thaiBreakIterator.clone();
      case UScript.HEBREW: return (BreakIterator)hebrewBreakIterator.clone();
      case UScript.KHMER: return (BreakIterator)khmerBreakIterator.clone();
      case UScript.LAO: return (BreakIterator)laoBreakIterator.clone();
      case UScript.MYANMAR: return (BreakIterator)myanmarBreakIterator.clone();
      default: return (BreakIterator)rootBreakIterator.clone();
    }
  }

  @Override
  public String getType(int script, int ruleStatus) {
    switch (ruleStatus) {
      case RuleBasedBreakIterator.WORD_IDEO:
        return WORD_IDEO;
      case RuleBasedBreakIterator.WORD_KANA:
        return script == UScript.HIRAGANA ? WORD_HIRAGANA : WORD_KATAKANA;
      case RuleBasedBreakIterator.WORD_LETTER:
        return script == UScript.HANGUL ? WORD_HANGUL : WORD_LETTER;
      case RuleBasedBreakIterator.WORD_NUMBER:
        return WORD_NUMBER;
      default: /* some other custom code */
        return "<OTHER>";
    }
  }

  private static RuleBasedBreakIterator readBreakIterator(String filename) {
    InputStream is = 
      DefaultICUTokenizerConfig.class.getResourceAsStream(filename);
    try {
      RuleBasedBreakIterator bi = 
        RuleBasedBreakIterator.getInstanceFromCompiledRules(is);
      is.close();
      return bi;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
