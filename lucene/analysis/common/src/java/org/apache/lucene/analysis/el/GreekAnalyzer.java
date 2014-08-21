package org.apache.lucene.analysis.el;

/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for the Greek language. 
 * <p>
 * Supports an external list of stopwords (words
 * that will not be indexed at all).
 * A default set of stopwords is used unless an alternative list is specified.
 * </p>
 *
 * <a name="version"/>
 * <p>You may specify the {@link Version}
 * compatibility when creating GreekAnalyzer:
 * <ul>
 *   <li> As of 3.1, StandardFilter and GreekStemmer are used by default.
 * </ul>
 * 
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public final class GreekAnalyzer extends StopwordAnalyzerBase {
  /** File containing default Greek stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";
  
  /**
   * Returns a set of default Greek-stopwords 
   * @return a set of default Greek-stopwords 
   */
  public static final CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_SET;
  }
  
  private static class DefaultSetHolder {
    private static final CharArraySet DEFAULT_SET;
    
    static {
      try {
        DEFAULT_SET = loadStopwordSet(false, GreekAnalyzer.class, DEFAULT_STOPWORD_FILE, "#");
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }
  
  /**
   * Builds an analyzer with the default stop words.
   */
  public GreekAnalyzer() {
    this(DefaultSetHolder.DEFAULT_SET);
  }

  /**
   * @deprecated Use {@link #GreekAnalyzer()}
   */
  @Deprecated
  public GreekAnalyzer(Version matchVersion) {
    this(matchVersion, DefaultSetHolder.DEFAULT_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words. 
   * <p>
   * <b>NOTE:</b> The stopwords set should be pre-processed with the logic of 
   * {@link GreekLowerCaseFilter} for best results.
   * @param stopwords a stopword set
   */
  public GreekAnalyzer(CharArraySet stopwords) {
    super(stopwords);
  }

  /**
   * @deprecated Use {@link #GreekAnalyzer(CharArraySet)}
   */
  @Deprecated
  public GreekAnalyzer(Version matchVersion, CharArraySet stopwords) {
    super(matchVersion, stopwords);
  }
  
  /**
   * Creates
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from a {@link StandardTokenizer} filtered with
   *         {@link GreekLowerCaseFilter}, {@link StandardFilter},
   *         {@link StopFilter}, and {@link GreekStemFilter}
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {
    final Tokenizer source = new StandardTokenizer(getVersion(), reader);
    TokenStream result = new GreekLowerCaseFilter(getVersion(), source);
    if (getVersion().onOrAfter(Version.LUCENE_3_1))
      result = new StandardFilter(getVersion(), result);
    result = new StopFilter(getVersion(), result, stopwords);
    if (getVersion().onOrAfter(Version.LUCENE_3_1))
      result = new GreekStemFilter(result);
    return new TokenStreamComponents(source, result);
  }
}
