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
package org.apache.lucene.analysis.icu.segmentation;

import com.ibm.icu.lang.UScript;
import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.RuleBasedBreakIterator;
import com.ibm.icu.util.ULocale;
import java.io.IOException;
import java.io.InputStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Default {@link ICUTokenizerConfig} that is generally applicable to many languages.
 *
 * <p>Generally tokenizes Unicode text according to UAX#29 ({@link
 * BreakIterator#getWordInstance(ULocale) BreakIterator.getWordInstance(ULocale.ROOT)}), but with
 * the following tailorings:
 *
 * <ul>
 *   <li>Thai, Lao, Myanmar, Khmer, and CJK text is broken into words with a dictionary.
 * </ul>
 *
 * @lucene.experimental
 */
public class DefaultICUTokenizerConfig extends ICUTokenizerConfig {
  /** Token type for words containing ideographic characters */
  public static final String WORD_IDEO =
      StandardTokenizer.TOKEN_TYPES[StandardTokenizer.IDEOGRAPHIC];
  /** Token type for words containing Japanese hiragana */
  public static final String WORD_HIRAGANA =
      StandardTokenizer.TOKEN_TYPES[StandardTokenizer.HIRAGANA];
  /** Token type for words containing Japanese katakana */
  public static final String WORD_KATAKANA =
      StandardTokenizer.TOKEN_TYPES[StandardTokenizer.KATAKANA];
  /** Token type for words containing Korean hangul */
  public static final String WORD_HANGUL = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.HANGUL];
  /** Token type for words that contain letters */
  public static final String WORD_LETTER =
      StandardTokenizer.TOKEN_TYPES[StandardTokenizer.ALPHANUM];
  /** Token type for words that appear to be numbers */
  public static final String WORD_NUMBER = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.NUM];
  /** Token type for words that appear to be emoji sequences */
  public static final String WORD_EMOJI = StandardTokenizer.TOKEN_TYPES[StandardTokenizer.EMOJI];

  /*
   * the default breakiterators in use. these can be expensive to
   * instantiate, cheap to clone.
   */
  // we keep the cjk breaking separate, thats because it cannot be customized (because dictionary
  // is only triggered when kind = WORD, but kind = LINE by default and we have no non-evil way to
  // change it)
  private static final BreakIterator cjkBreakIterator = BreakIterator.getWordInstance(ULocale.ROOT);

  // TODO: if the wrong version of the ICU jar is used, loading these data files may give a strange
  // error.
  // maybe add an explicit check?
  // http://icu-project.org/apiref/icu4j/com/ibm/icu/util/VersionInfo.html

  // the same as ROOT, except no dictionary segmentation for cjk
  private static final RuleBasedBreakIterator defaultBreakIterator =
      readBreakIterator("Default.brk");
  private static final RuleBasedBreakIterator myanmarSyllableIterator =
      readBreakIterator("MyanmarSyllable.brk");

  // TODO: deprecate this boolean? you only care if you are doing super-expert stuff...
  private final boolean cjkAsWords;
  private final boolean myanmarAsWords;

  /**
   * Creates a new config. This object is lightweight, but the first time the class is referenced,
   * breakiterators will be initialized.
   *
   * @param cjkAsWords true if cjk text should undergo dictionary-based segmentation, otherwise text
   *     will be segmented according to UAX#29 defaults. If this is true, all Han+Hiragana+Katakana
   *     words will be tagged as IDEOGRAPHIC.
   * @param myanmarAsWords true if Myanmar text should undergo dictionary-based segmentation,
   *     otherwise it will be tokenized as syllables.
   */
  public DefaultICUTokenizerConfig(boolean cjkAsWords, boolean myanmarAsWords) {
    this.cjkAsWords = cjkAsWords;
    this.myanmarAsWords = myanmarAsWords;
  }

  @Override
  public boolean combineCJ() {
    return cjkAsWords;
  }

  @Override
  public RuleBasedBreakIterator getBreakIterator(int script) {
    switch (script) {
      case UScript.JAPANESE:
        return (RuleBasedBreakIterator) cjkBreakIterator.clone();
      case UScript.MYANMAR:
        if (myanmarAsWords) {
          return (RuleBasedBreakIterator) defaultBreakIterator.clone();
        } else {
          return (RuleBasedBreakIterator) myanmarSyllableIterator.clone();
        }
      default:
        return (RuleBasedBreakIterator) defaultBreakIterator.clone();
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
      case EMOJI_SEQUENCE_STATUS:
        return WORD_EMOJI;
      default: /* some other custom code */
        return "<OTHER>";
    }
  }

  private static RuleBasedBreakIterator readBreakIterator(String filename) {
    InputStream is = DefaultICUTokenizerConfig.class.getResourceAsStream(filename);
    try {
      RuleBasedBreakIterator bi = RuleBasedBreakIterator.getInstanceFromCompiledRules(is);
      is.close();
      return bi;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
