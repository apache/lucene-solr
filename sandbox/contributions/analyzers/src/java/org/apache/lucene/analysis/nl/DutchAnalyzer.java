package org.apache.lucene.analysis.nl;

/**
 * Copyright 2004 The Apache Software Foundation
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.Token;
import java.io.File;
import java.io.*;
import java.io.Reader;
import java.util.Hashtable;

/**
 *
 * @author Edwin de Jonge
 *
 * Analyzer for Dutch language. Supports an external list of stopwords (words that
 * will not be indexed at all), an external list of exclusions (word that will
 * not be stemmed, but indexed) and an external list of word-stem pairs that overrule
 * the algorithm (dictionary stemming).
 * A default set of stopwords is used unless an alternative list is specified, the
 * exclusion list is empty by default.
 * As start for the Analyzer the German Analyzer was used. The stemming algorithm
 * implemented can be found at @link
 */
public class DutchAnalyzer extends Analyzer
{
	/**
	 * List of typical Dutch stopwords.
	 */
	private String[] DUTCH_STOP_WORDS =
	{
	   "de","en","van","ik","te","dat","die","in","een",
	   "hij","het","niet","zijn","is","was","op","aan","met","als","voor","had",
	   "er","maar","om","hem","dan","zou","of","wat","mijn","men","dit","zo",
	   "door","over","ze","zich","bij","ook","tot","je","mij","uit","der","daar",
	   "haar","naar","heb","hoe","heeft","hebben","deze","u","want","nog","zal",
	   "me","zij","nu","ge","geen","omdat","iets","worden","toch","al","waren",
	   "veel","meer","doen","toen","moet","ben","zonder","kan","hun","dus",
	   "alles","onder","ja","eens","hier","wie","werd","altijd","doch","wordt",
	   "wezen","kunnen","ons","zelf","tegen","na","reeds","wil","kon","niets",
	   "uw","iemand","geweest","andere"
	};


	/**
	 * Contains the stopwords used with the StopFilter.
	 */
	private Hashtable stoptable = new Hashtable();

	/**
	 * Contains words that should be indexed but not stemmed.
	 */
	private Hashtable excltable = new Hashtable();

	private Hashtable _stemdict = new Hashtable();


	/**
	 * Builds an analyzer.
	 */
	public DutchAnalyzer()
	{
		stoptable = StopFilter.makeStopTable( DUTCH_STOP_WORDS );
		_stemdict.put("fiets","fiets"); //otherwise fiet
		_stemdict.put("bromfiets","bromfiets"); //otherwise bromfiet
		_stemdict.put("ei","eier");
		_stemdict.put("kind","kinder");
	}

	/**
	 * Builds an analyzer with the given stop words.
	 *
	 * @param stopwords
	 */
	public DutchAnalyzer( String[] stopwords )
	{
		stoptable = StopFilter.makeStopTable( stopwords );
	}

	/**
	 * Builds an analyzer with the given stop words.
	 *
	 * @param stopwords
	 */
	public DutchAnalyzer( Hashtable stopwords )
	{
		stoptable = stopwords;
	}

	/**
	 * Builds an analyzer with the given stop words.
	 *
	 *  @param stopwords
	 */
	public DutchAnalyzer( File stopwords )
	{
		stoptable = WordlistLoader.getWordtable( stopwords );
	}

	/**
	 * Builds an exclusionlist from an array of Strings.
	 *
	 * @param exclusionlist
	 */
	public void setStemExclusionTable( String[] exclusionlist )
	{
		excltable = StopFilter.makeStopTable( exclusionlist );
	}

	/**
	 * Builds an exclusionlist from a Hashtable.
	 */
	public void setStemExclusionTable( Hashtable exclusionlist )
	{
		excltable = exclusionlist;
	}

	/**
	 * Builds an exclusionlist from the words contained in the given file.
	 */
	public void setStemExclusionTable(File exclusionlist)
	{
		excltable = WordlistLoader.getWordtable(exclusionlist);
	}

	/**
	 * Reads a stemdictionary file , that overrules the stemming algorithm
	 * This is a textfile that contains per line
	 * word\tstem
	 * i.e: tabseperated
	 */
	public void setStemDictionary(File stemdict)
	{
		_stemdict = WordlistLoader.getStemDict(stemdict);
	}

	/**
	 * Creates a TokenStream which tokenizes all the text in the provided TextReader.
	 *
	 * @return A TokenStream build from a StandardTokenizer filtered with StandardFilter, StopFilter, GermanStemFilter
	 */
	public TokenStream tokenStream(String fieldName, Reader reader)
	{
		TokenStream result = new StandardTokenizer( reader );
		result = new StandardFilter( result );
		result = new StopFilter( result, stoptable );
		result = new DutchStemFilter( result, excltable, _stemdict);
		return result;
	}
}
