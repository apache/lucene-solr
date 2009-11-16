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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardAnalyzer;  // for javadoc
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
public final class BrazilianAnalyzer extends Analyzer {

	/**
	 * List of typical Brazilian Portuguese stopwords.
	 * @deprecated use {@link #getDefaultStopSet()} instead
	 */
  // TODO make this private in 3.1
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

	/**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static Set<?> getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  private static class DefaultSetHolder {
    static final Set<?> DEFAULT_STOP_SET = CharArraySet
        .unmodifiableSet(new CharArraySet(Arrays.asList(BRAZILIAN_STOP_WORDS),
            false));
  }

	/**
	 * Contains the stopwords used with the {@link StopFilter}.
	 */
	private final Set<?> stoptable;
	
	/**
	 * Contains words that should be indexed but not stemmed.
	 */
	// TODO make this private in 3.1
	private Set<?> excltable = Collections.emptySet();
	
  private final Version matchVersion;

	/**
	 * Builds an analyzer with the default stop words ({@link #BRAZILIAN_STOP_WORDS}).
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
    stoptable = CharArraySet.unmodifiableSet(CharArraySet.copy(stopwords));
    this.matchVersion = matchVersion;
  }

  /**
   * Builds an analyzer with the given stop words and stemming exclusion words
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   * @param stemExclutionSet
   *          a stemming exclusion set
   */
  public BrazilianAnalyzer(Version matchVersion, Set<?> stopset,
      Set<?> stemExclusionSet) {
    this(matchVersion, stopset);
    excltable = CharArraySet.unmodifiableSet(CharArraySet
        .copy(stemExclusionSet));
  }

	/**
	 * Builds an analyzer with the given stop words.
	 * @deprecated use {@link #BrazilianAnalyzer(Version, Set)} instead
	 */
  public BrazilianAnalyzer(Version matchVersion, String... stopwords) {
    this(matchVersion, StopFilter.makeStopSet(stopwords));
  }

  /**
   * Builds an analyzer with the given stop words. 
   * @deprecated use {@link #BrazilianAnalyzer(Version, Set)} instead
   */
  public BrazilianAnalyzer(Version matchVersion, Map<?,?> stopwords) {
    this(matchVersion, stopwords.keySet());
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #BrazilianAnalyzer(Version, Set)} instead
   */
  public BrazilianAnalyzer(Version matchVersion, File stopwords)
      throws IOException {
    this(matchVersion, WordlistLoader.getWordSet(stopwords));
  }

	/**
	 * Builds an exclusionlist from an array of Strings.
	 * @deprecated use {@link #BrazilianAnalyzer(Version, Set, Set)} instead
	 */
	public void setStemExclusionTable( String... exclusionlist ) {
		excltable = StopFilter.makeStopSet( exclusionlist );
		setPreviousTokenStream(null); // force a new stemmer to be created
	}
	/**
	 * Builds an exclusionlist from a {@link Map}.
	 * @deprecated use {@link #BrazilianAnalyzer(Version, Set, Set)} instead
	 */
	public void setStemExclusionTable( Map<?,?> exclusionlist ) {
		excltable = new HashSet<Object>(exclusionlist.keySet());
		setPreviousTokenStream(null); // force a new stemmer to be created
	}
	/**
	 * Builds an exclusionlist from the words contained in the given file.
	 * @deprecated use {@link #BrazilianAnalyzer(Version, Set, Set)} instead
	 */
	public void setStemExclusionTable( File exclusionlist ) throws IOException {
		excltable = WordlistLoader.getWordSet( exclusionlist );
		setPreviousTokenStream(null); // force a new stemmer to be created
	}

	/**
	 * Creates a {@link TokenStream} which tokenizes all the text in the provided {@link Reader}.
	 *
	 * @return  A {@link TokenStream} built from a {@link StandardTokenizer} filtered with
	 * 			{@link LowerCaseFilter}, {@link StandardFilter}, {@link StopFilter}, and 
	 *          {@link BrazilianStemFilter}.
	 */
	@Override
	public final TokenStream tokenStream(String fieldName, Reader reader) {
                TokenStream result = new StandardTokenizer( matchVersion, reader );
		result = new LowerCaseFilter( result );
		result = new StandardFilter( result );
		result = new StopFilter( StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                         result, stoptable );
		result = new BrazilianStemFilter( result, excltable );
		return result;
	}
	
    private class SavedStreams {
      Tokenizer source;
      TokenStream result;
    };
    
    /**
     * Returns a (possibly reused) {@link TokenStream} which tokenizes all the text 
     * in the provided {@link Reader}.
     *
     * @return  A {@link TokenStream} built from a {@link StandardTokenizer} filtered with
     *          {@link LowerCaseFilter}, {@link StandardFilter}, {@link StopFilter}, and 
     *          {@link BrazilianStemFilter}.
     */
    @Override
    public TokenStream reusableTokenStream(String fieldName, Reader reader)
      throws IOException {
      SavedStreams streams = (SavedStreams) getPreviousTokenStream();
      if (streams == null) {
        streams = new SavedStreams();
        streams.source = new StandardTokenizer(matchVersion, reader);
        streams.result = new LowerCaseFilter(streams.source);
        streams.result = new StandardFilter(streams.result);
        streams.result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                        streams.result, stoptable);
        streams.result = new BrazilianStemFilter(streams.result, excltable);
        setPreviousTokenStream(streams);
      } else {
        streams.source.reset(reader);
      }
      return streams.result;
    }
}

