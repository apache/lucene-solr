package org.apache.lucene.analysis.br;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import java.io.File;
import java.io.Reader;
import java.util.Hashtable;

/**
 * Analyzer for brazilian language. Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (word that will
 * not be stemmed, but indexed).
 *
 * @author    João Kramer
 * @version   $Id: BrazilianAnalyzer.java,v 1.0 2001/02/13 21:29:04
 */
public final class BrazilianAnalyzer extends Analyzer {

	/**
	 * List of typical german stopwords.
	 */
	private String[] BRAZILIAN_STOP_WORDS = {
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
	 * Contains the stopwords used with the StopFilter.
	 */
	private Hashtable stoptable = new Hashtable();
	/**
	 * Contains words that should be indexed but not stemmed.
	 */
	private Hashtable excltable = new Hashtable();

	/**
	 * Builds an analyzer.
	 */
	public BrazilianAnalyzer() {
		stoptable = StopFilter.makeStopTable( BRAZILIAN_STOP_WORDS );
	}

	/**
	 * Builds an analyzer with the given stop words.
	 */
	public BrazilianAnalyzer( String[] stopwords ) {
		stoptable = StopFilter.makeStopTable( stopwords );
	}

	/**
	 * Builds an analyzer with the given stop words.
	 */
	public BrazilianAnalyzer( Hashtable stopwords ) {
		stoptable = stopwords;
	}

	/**
	 * Builds an analyzer with the given stop words.
	 */
	public BrazilianAnalyzer( File stopwords ) {
		stoptable = WordlistLoader.getWordtable( stopwords );
	}

	/**
	 * Builds an exclusionlist from an array of Strings.
	 */
	public void setStemExclusionTable( String[] exclusionlist ) {
		excltable = StopFilter.makeStopTable( exclusionlist );
	}
	/**
	 * Builds an exclusionlist from a Hashtable.
	 */
	public void setStemExclusionTable( Hashtable exclusionlist ) {
		excltable = exclusionlist;
	}
	/**
	 * Builds an exclusionlist from the words contained in the given file.
	 */
	public void setStemExclusionTable( File exclusionlist ) {
		excltable = WordlistLoader.getWordtable( exclusionlist );
	}
	
	/**
	 * Creates a TokenStream which tokenizes all the text in the provided Reader.
	 *
	 * @return  A TokenStream build from a StandardTokenizer filtered with
	 * 			StandardFilter, StopFilter, GermanStemFilter and LowerCaseFilter.
	 */
	public final TokenStream tokenStream(String fieldName, Reader reader) {
		TokenStream result = new StandardTokenizer( reader );
		result = new StandardFilter( result );
		result = new StopFilter( result, stoptable );
		result = new BrazilianStemFilter( result, excltable );
		// Convert to lowercase after stemming!
		result = new LowerCaseFilter( result );
		return result;
	}
}

