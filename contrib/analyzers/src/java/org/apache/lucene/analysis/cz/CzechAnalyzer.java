package org.apache.lucene.analysis.cz;

/**
 * Copyright 2004-2005 The Apache Software Foundation
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
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import java.io.*;
import java.util.Hashtable;
import java.util.HashSet;
import java.util.Set;

/**
 * Analyzer for Czech language. Supports an external list of stopwords (words that
 * will not be indexed at all).
 * A default set of stopwords is used unless an alternative list is specified, the
 * exclusion list is empty by default.
 *
 * @author    Lukas Zapletal [lzap@root.cz]
 */
public final class CzechAnalyzer extends Analyzer {

	/**
	 * List of typical stopwords.
	 */
	public final static String[] CZECH_STOP_WORDS = {
        "a","s","k","o","i","u","v","z","dnes","cz","t\u00edmto","bude\u0161","budem",
        "byli","jse\u0161","m\u016fj","sv\u00fdm","ta","tomto","tohle","tuto","tyto",
        "jej","zda","pro\u010d","m\u00e1te","tato","kam","tohoto","kdo","kte\u0159\u00ed",
        "mi","n\u00e1m","tom","tomuto","m\u00edt","nic","proto","kterou","byla",
        "toho","proto\u017ee","asi","ho","na\u0161i","napi\u0161te","re","co\u017e","t\u00edm",
        "tak\u017ee","sv\u00fdch","jej\u00ed","sv\u00fdmi","jste","aj","tu","tedy","teto",
        "bylo","kde","ke","prav\u00e9","ji","nad","nejsou","\u010di","pod","t\u00e9ma",
        "mezi","p\u0159es","ty","pak","v\u00e1m","ani","kdy\u017e","v\u0161ak","neg","jsem",
        "tento","\u010dl\u00e1nku","\u010dl\u00e1nky","aby","jsme","p\u0159ed","pta","jejich",
        "byl","je\u0161t\u011b","a\u017e","bez","tak\u00e9","pouze","prvn\u00ed","va\u0161e","kter\u00e1",
        "n\u00e1s","nov\u00fd","tipy","pokud","m\u016f\u017ee","strana","jeho","sv\u00e9","jin\u00e9",
        "zpr\u00e1vy","nov\u00e9","nen\u00ed","v\u00e1s","jen","podle","zde","u\u017e","b\u00fdt","v\u00edce",
        "bude","ji\u017e","ne\u017e","kter\u00fd","by","kter\u00e9","co","nebo","ten","tak",
        "m\u00e1","p\u0159i","od","po","jsou","jak","dal\u0161\u00ed","ale","si","se","ve",
        "to","jako","za","zp\u011bt","ze","do","pro","je","na","atd","atp",
        "jakmile","p\u0159i\u010dem\u017e","j\u00e1","on","ona","ono","oni","ony","my","vy",
        "j\u00ed","ji","m\u011b","mne","jemu","tomu","t\u011bm","t\u011bmu","n\u011bmu","n\u011bmu\u017e",
        "jeho\u017e","j\u00ed\u017e","jeliko\u017e","je\u017e","jako\u017e","na\u010de\u017e",
    };

	/**
	 * Contains the stopwords used with the StopFilter.
	 */
	private Set stoptable;

	/**
	 * Builds an analyzer with the default stop words ({@link #CZECH_STOP_WORDS}).
	 */
	public CzechAnalyzer() {
		stoptable = StopFilter.makeStopSet( CZECH_STOP_WORDS );
	}

	/**
	 * Builds an analyzer with the given stop words.
	 */
	public CzechAnalyzer( String[] stopwords ) {
		stoptable = StopFilter.makeStopSet( stopwords );
	}

	public CzechAnalyzer( HashSet stopwords ) {
		stoptable = stopwords;
	}

	/**
	 * Builds an analyzer with the given stop words.
	 */
	public CzechAnalyzer( File stopwords ) throws IOException {
		stoptable = WordlistLoader.getWordSet( stopwords );
	}

    /**
     * Loads stopwords hash from resource stream (file, database...).
     * @param   wordfile    File containing the wordlist
     * @param   encoding    Encoding used (win-1250, iso-8859-2, ...), null for default system encoding
     */
    public void loadStopWords( InputStream wordfile, String encoding ) {
        if ( wordfile == null ) {
            stoptable = new HashSet();
            return;
        }
        try {
            // clear any previous table (if present)
            stoptable = new HashSet();

            InputStreamReader isr;
            if (encoding == null)
                isr = new InputStreamReader(wordfile);
            else
                isr = new InputStreamReader(wordfile, encoding);

            LineNumberReader lnr = new LineNumberReader(isr);
            String word;
            while ( ( word = lnr.readLine() ) != null ) {
                stoptable.add(word);
            }

        } catch ( IOException e ) {
            stoptable = null;
        }
    }

	/**
	 * Creates a TokenStream which tokenizes all the text in the provided Reader.
	 *
	 * @return  A TokenStream build from a StandardTokenizer filtered with
	 * 			StandardFilter, LowerCaseFilter, and StopFilter
	 */
	public final TokenStream tokenStream( String fieldName, Reader reader ) {
		TokenStream result = new StandardTokenizer( reader );
		result = new StandardFilter( result );
		result = new LowerCaseFilter( result );
		result = new StopFilter( result, stoptable );
		return result;
	}
}

