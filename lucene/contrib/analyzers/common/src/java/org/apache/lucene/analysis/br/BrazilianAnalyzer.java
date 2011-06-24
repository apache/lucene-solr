package org.apache.lucene.analysis.br;

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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.KeywordMarkerFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for Brazilian Portuguese language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (words that will
 * not be stemmed, but indexed).
 * </p>
 *
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public final class BrazilianAnalyzer extends StopwordAnalyzerBase {

	/**
	 * List of typical Brazilian Portuguese stopwords.
	 * @deprecated use {@link #getDefaultStopSet()} instead
	 */
	@Deprecated
	public final static String[] BRAZILIAN_STOP_WORDS = {
      "a","ainda","alem","ambas","ambos","antes",
      "ao","aonde","aos","apos","aquele","aqueles",
      "as","assim","com","como","contra","contudo",
      "cuja","cujas","cujo","cujos","da","das","de",
      "dela","dele","deles","demais","depois","desde",
      "desta","deste","dispoe","dispoem","diversa",
      "diversas","diversos","do","dos","durante","e",
      "ela","elas","ele","eles","em","entao","entre",
      "essa","essas","esse","esses","esta","estas",
      "este","estes","ha","isso","isto","logo","mais",
      "mas","mediante","menos","mesma","mesmas","mesmo",
      "mesmos","na","nas","nao","nas","nem","nesse","neste",
      "nos","o","os","ou","outra","outras","outro","outros",
      "pelas","pelas","pelo","pelos","perante","pois","por",
      "porque","portanto","proprio","propios","quais","qual",
      "qualquer","quando","quanto","que","quem","quer","se",
      "seja","sem","sendo","seu","seus","sob","sobre","sua",
      "suas","tal","tambem","teu","teus","toda","todas","todo",
      "todos","tua","tuas","tudo","um","uma","umas","uns"};

  /** File containing default Brazilian Portuguese stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";
  
	/**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static Set<?> getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  private static class DefaultSetHolder {
    static final Set<?> DEFAULT_STOP_SET;
    
    static {
      try {
        DEFAULT_STOP_SET = CharArraySet.unmodifiableSet(new CharArraySet(
            Version.LUCENE_CURRENT, WordlistLoader.getWordSet(BrazilianAnalyzer.class, 
                DEFAULT_STOPWORD_FILE, "#"), false));
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }


	/**
	 * Contains words that should be indexed but not stemmed.
	 */
	// TODO make this private in 3.1
	private Set<?> excltable = Collections.emptySet();
	
	/**
	 * Builds an analyzer with the default stop words ({@link #getDefaultStopSet()}).
	 */
	public BrazilianAnalyzer(Version matchVersion) {
    this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET);
	}
	
	/**
   * Builds an analyzer with the given stop words
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   */
  public BrazilianAnalyzer(Version matchVersion, Set<?> stopwords) {
     super(matchVersion, stopwords);
  }

  /**
   * Builds an analyzer with the given stop words and stemming exclusion words
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   */
  public BrazilianAnalyzer(Version matchVersion, Set<?> stopwords,
      Set<?> stemExclusionSet) {
    this(matchVersion, stopwords);
    excltable = CharArraySet.unmodifiableSet(CharArraySet
        .copy(matchVersion, stemExclusionSet));
  }

	/**
	 * Builds an analyzer with the given stop words.
	 * @deprecated use {@link #BrazilianAnalyzer(Version, Set)} instead
	 */
  @Deprecated
  public BrazilianAnalyzer(Version matchVersion, String... stopwords) {
    this(matchVersion, StopFilter.makeStopSet(matchVersion, stopwords));
  }

  /**
   * Builds an analyzer with the given stop words. 
   * @deprecated use {@link #BrazilianAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public BrazilianAnalyzer(Version matchVersion, Map<?,?> stopwords) {
    this(matchVersion, stopwords.keySet());
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #BrazilianAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public BrazilianAnalyzer(Version matchVersion, File stopwords)
      throws IOException {
    this(matchVersion, WordlistLoader.getWordSet(stopwords));
  }

	/**
	 * Builds an exclusionlist from an array of Strings.
	 * @deprecated use {@link #BrazilianAnalyzer(Version, Set, Set)} instead
	 */
	@Deprecated
	public void setStemExclusionTable( String... exclusionlist ) {
		excltable = StopFilter.makeStopSet( matchVersion, exclusionlist );
		setPreviousTokenStream(null); // force a new stemmer to be created
	}
	/**
	 * Builds an exclusionlist from a {@link Map}.
	 * @deprecated use {@link #BrazilianAnalyzer(Version, Set, Set)} instead
	 */
	@Deprecated
	public void setStemExclusionTable( Map<?,?> exclusionlist ) {
		excltable = new HashSet<Object>(exclusionlist.keySet());
		setPreviousTokenStream(null); // force a new stemmer to be created
	}
	/**
	 * Builds an exclusionlist from the words contained in the given file.
	 * @deprecated use {@link #BrazilianAnalyzer(Version, Set, Set)} instead
	 */
	@Deprecated
	public void setStemExclusionTable( File exclusionlist ) throws IOException {
		excltable = WordlistLoader.getWordSet( exclusionlist );
		setPreviousTokenStream(null); // force a new stemmer to be created
	}

  /**
   * Creates
   * {@link org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents}
   *         built from a {@link StandardTokenizer} filtered with
   *         {@link LowerCaseFilter}, {@link StandardFilter}, {@link StopFilter}
   *         , and {@link BrazilianStemFilter}.
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {
    Tokenizer source = new StandardTokenizer(matchVersion, reader);
    TokenStream result = new LowerCaseFilter(matchVersion, source);
    result = new StandardFilter(matchVersion, result);
    result = new StopFilter(matchVersion, result, stopwords);
    if(excltable != null && !excltable.isEmpty())
      result = new KeywordMarkerFilter(result, excltable);
    return new TokenStreamComponents(source, new BrazilianStemFilter(result));
  }
}

