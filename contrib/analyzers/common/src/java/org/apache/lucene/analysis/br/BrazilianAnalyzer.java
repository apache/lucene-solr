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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Collections;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * {@link Analyzer} for Brazilian Portuguese language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (words that will
 * not be stemmed, but indexed).
 * </p>
 */
public final class BrazilianAnalyzer extends Analyzer {

	/**
	 * List of typical Brazilian Portuguese stopwords.
	 */
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
	 * Contains the stopwords used with the {@link StopFilter}.
	 */
	private Set stoptable = Collections.emptySet();
	
	/**
	 * Contains words that should be indexed but not stemmed.
	 */
	private Set excltable = Collections.emptySet();

	/**
	 * Builds an analyzer with the default stop words ({@link #BRAZILIAN_STOP_WORDS}).
	 */
	public BrazilianAnalyzer() {
		stoptable = StopFilter.makeStopSet( BRAZILIAN_STOP_WORDS );
	}

	/**
	 * Builds an analyzer with the given stop words.
	 */
	public BrazilianAnalyzer( String... stopwords ) {
		stoptable = StopFilter.makeStopSet( stopwords );
	}

	/**
	 * Builds an analyzer with the given stop words.
	 */
	public BrazilianAnalyzer( Map stopwords ) {
		stoptable = new HashSet(stopwords.keySet());
	}

	/**
	 * Builds an analyzer with the given stop words.
	 */
	public BrazilianAnalyzer( File stopwords ) throws IOException {
		stoptable = WordlistLoader.getWordSet( stopwords );
	}

	/**
	 * Builds an exclusionlist from an array of Strings.
	 */
	public void setStemExclusionTable( String... exclusionlist ) {
		excltable = StopFilter.makeStopSet( exclusionlist );
		setPreviousTokenStream(null); // force a new stemmer to be created
	}
	/**
	 * Builds an exclusionlist from a {@link Map}.
	 */
	public void setStemExclusionTable( Map exclusionlist ) {
		excltable = new HashSet(exclusionlist.keySet());
		setPreviousTokenStream(null); // force a new stemmer to be created
	}
	/**
	 * Builds an exclusionlist from the words contained in the given file.
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
	public final TokenStream tokenStream(String fieldName, Reader reader) {
		TokenStream result = new StandardTokenizer( reader );
		result = new LowerCaseFilter( result );
		result = new StandardFilter( result );
		result = new StopFilter( false, result, stoptable );
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
    public TokenStream reusableTokenStream(String fieldName, Reader reader)
      throws IOException {
      SavedStreams streams = (SavedStreams) getPreviousTokenStream();
      if (streams == null) {
        streams = new SavedStreams();
        streams.source = new StandardTokenizer(reader);
        streams.result = new LowerCaseFilter(streams.source);
        streams.result = new StandardFilter(streams.result);
        streams.result = new StopFilter(false, streams.result, stoptable);
        streams.result = new BrazilianStemFilter(streams.result, excltable);
        setPreviousTokenStream(streams);
      } else {
        streams.source.reset(reader);
      }
      return streams.result;
    }
}

