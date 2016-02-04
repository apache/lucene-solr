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
package org.apache.lucene.analysis.cn.smart;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

/**
 * <p>
 * SmartChineseAnalyzer is an analyzer for Chinese or mixed Chinese-English text.
 * The analyzer uses probabilistic knowledge to find the optimal word segmentation for Simplified Chinese text.
 * The text is first broken into sentences, then each sentence is segmented into words.
 * </p>
 * <p>
 * Segmentation is based upon the <a href="http://en.wikipedia.org/wiki/Hidden_Markov_Model">Hidden Markov Model</a>. 
 * A large training corpus was used to calculate Chinese word frequency probability.
 * </p>
 * <p>
 * This analyzer requires a dictionary to provide statistical data. 
 * SmartChineseAnalyzer has an included dictionary out-of-box.
 * </p>
 * <p>
 * The included dictionary data is from <a href="http://www.ictclas.org">ICTCLAS1.0</a>.
 * Thanks to ICTCLAS for their hard work, and for contributing the data under the Apache 2 License!
 * </p>
 * @lucene.experimental
 */
public final class SmartChineseAnalyzer extends Analyzer {

  private final CharArraySet stopWords;
  
  private static final String DEFAULT_STOPWORD_FILE = "stopwords.txt";
  
  private static final String STOPWORD_FILE_COMMENT = "//";
  
  /**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  /**
   * Atomically loads the DEFAULT_STOP_SET in a lazy fashion once the outer class 
   * accesses the static final set the first time.;
   */
  private static class DefaultSetHolder {
    static final CharArraySet DEFAULT_STOP_SET;

    static {
      try {
        DEFAULT_STOP_SET = loadDefaultStopWordSet();
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }

    static CharArraySet loadDefaultStopWordSet() throws IOException {
      // make sure it is unmodifiable as we expose it in the outer class
      return CharArraySet.unmodifiableSet(WordlistLoader.getWordSet(IOUtils
          .getDecodingReader(SmartChineseAnalyzer.class, DEFAULT_STOPWORD_FILE,
              StandardCharsets.UTF_8), STOPWORD_FILE_COMMENT));
    }
  }

  /**
   * Create a new SmartChineseAnalyzer, using the default stopword list.
   */
  public SmartChineseAnalyzer() {
    this(true);
  }

  /**
   * <p>
   * Create a new SmartChineseAnalyzer, optionally using the default stopword list.
   * </p>
   * <p>
   * The included default stopword list is simply a list of punctuation.
   * If you do not use this list, punctuation will not be removed from the text!
   * </p>
   * 
   * @param useDefaultStopWords true to use the default stopword list.
   */
  public SmartChineseAnalyzer(boolean useDefaultStopWords) {
    stopWords = useDefaultStopWords ? DefaultSetHolder.DEFAULT_STOP_SET
      : CharArraySet.EMPTY_SET;
  }

  /**
   * <p>
   * Create a new SmartChineseAnalyzer, using the provided {@link Set} of stopwords.
   * </p>
   * <p>
   * Note: the set should include punctuation, unless you want to index punctuation!
   * </p>
   * @param stopWords {@link Set} of stopwords to use.
   */
  public SmartChineseAnalyzer(CharArraySet stopWords) {
    this.stopWords = stopWords == null ? CharArraySet.EMPTY_SET : stopWords;
  }

  @Override
  public TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer tokenizer;
    TokenStream result;
    if (getVersion().onOrAfter(Version.LUCENE_4_8_0)) {
      tokenizer = new HMMChineseTokenizer();
      result = tokenizer;
    } else {
      tokenizer = new SentenceTokenizer();
      result = new WordTokenFilter(tokenizer);
    }
    // result = new LowerCaseFilter(result);
    // LowerCaseFilter is not needed, as SegTokenFilter lowercases Basic Latin text.
    // The porter stemming is too strict, this is not a bug, this is a feature:)
    result = new PorterStemFilter(result);
    if (!stopWords.isEmpty()) {
      result = new StopFilter(result, stopWords);
    }
    return new TokenStreamComponents(tokenizer, result);
  }
}
