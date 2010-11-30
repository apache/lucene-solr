package org.apache.lucene.analysis.cz;

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

import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.KeywordMarkerFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

import java.io.*;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link Analyzer} for Czech language.
 * <p>
 * Supports an external list of stopwords (words that will not be indexed at
 * all). A default set of stopwords is used unless an alternative list is
 * specified.
 * </p>
 * 
 * <a name="version"/>
 * <p>
 * You must specify the required {@link Version} compatibility when creating
 * CzechAnalyzer:
 * <ul>
 * <li>As of 3.1, words are stemmed with {@link CzechStemFilter}
 * <li>As of 2.9, StopFilter preserves position increments
 * <li>As of 2.4, Tokens incorrectly identified as acronyms are corrected (see
 * <a href="https://issues.apache.org/jira/browse/LUCENE-1068">LUCENE-1068</a>)
 * </ul>
 */
public final class CzechAnalyzer extends ReusableAnalyzerBase {

  /**
	 * List of typical stopwords.
	 * @deprecated use {@link #getDefaultStopSet()} instead
	 */
	@Deprecated
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
	
  /** File containing default Czech stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";
  
  /**
   * Returns a set of default Czech-stopwords
   * 
   * @return a set of default Czech-stopwords
   */
	public static final Set<?> getDefaultStopSet(){
	  return DefaultSetHolder.DEFAULT_SET;
	}
	
	private static class DefaultSetHolder {
	  private static final Set<?> DEFAULT_SET;
	  
	  static {
	    try {
	      DEFAULT_SET = CharArraySet.unmodifiableSet(new CharArraySet(
	          Version.LUCENE_CURRENT, WordlistLoader.getWordSet(CzechAnalyzer.class, 
	              DEFAULT_STOPWORD_FILE, "#"), false));
	    } catch (IOException ex) {
	      // default set should always be present as it is part of the
	      // distribution (JAR)
	      throw new RuntimeException("Unable to load default stopword set");
	    }
	  }
	}

 
  /**
   * Contains the stopwords used with the {@link StopFilter}.
   */
	// TODO once loadStopWords is gone those member should be removed too in favor of StopwordAnalyzerBase
	private Set<?> stoptable;
  private final Version matchVersion;
  private final Set<?> stemExclusionTable;

  /**
   * Builds an analyzer with the default stop words ({@link #CZECH_STOP_WORDS}).
   * 
   * @param matchVersion Lucene version to match See
   *          {@link <a href="#version">above</a>}
   */
	public CzechAnalyzer(Version matchVersion) {
    this(matchVersion, DefaultSetHolder.DEFAULT_SET);
	}
	
  /**
   * Builds an analyzer with the given stop words.
   * 
   * @param matchVersion Lucene version to match See
   *          {@link <a href="#version">above</a>}
   * @param stopwords a stopword set
   */
  public CzechAnalyzer(Version matchVersion, Set<?> stopwords) {
    this(matchVersion, stopwords, CharArraySet.EMPTY_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words and a set of work to be
   * excluded from the {@link CzechStemFilter}.
   * 
   * @param matchVersion Lucene version to match See
   *          {@link <a href="#version">above</a>}
   * @param stopwords a stopword set
   * @param stemExclusionTable a stemming exclusion set
   */
  public CzechAnalyzer(Version matchVersion, Set<?> stopwords, Set<?> stemExclusionTable) {
    this.matchVersion = matchVersion;
    this.stoptable = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion, stopwords));
    this.stemExclusionTable = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion, stemExclusionTable));
  }


  /**
   * Builds an analyzer with the given stop words.
   * 
   * @param matchVersion Lucene version to match See
   *          {@link <a href="#version">above</a>}
   * @param stopwords a stopword set
   * @deprecated use {@link #CzechAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public CzechAnalyzer(Version matchVersion, String... stopwords) {
    this(matchVersion, StopFilter.makeStopSet( matchVersion, stopwords ));
	}

  /**
   * Builds an analyzer with the given stop words.
   * 
   * @param matchVersion Lucene version to match See
   *          {@link <a href="#version">above</a>}
   * @param stopwords a stopword set
   * @deprecated use {@link #CzechAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public CzechAnalyzer(Version matchVersion, HashSet<?> stopwords) {
    this(matchVersion, (Set<?>)stopwords);
	}

  /**
   * Builds an analyzer with the given stop words.
   * 
   * @param matchVersion Lucene version to match See
   *          {@link <a href="#version">above</a>}
   * @param stopwords a file containing stopwords
   * @deprecated use {@link #CzechAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public CzechAnalyzer(Version matchVersion, File stopwords ) throws IOException {
    this(matchVersion, (Set<?>)WordlistLoader.getWordSet( stopwords ));
	}

    /**
     * Loads stopwords hash from resource stream (file, database...).
     * @param   wordfile    File containing the wordlist
     * @param   encoding    Encoding used (win-1250, iso-8859-2, ...), null for default system encoding
     * @deprecated use {@link WordlistLoader#getWordSet(Reader, String) }
     *             and {@link #CzechAnalyzer(Version, Set)} instead
     */
    // TODO extend StopwordAnalyzerBase once this method is gone!
    @Deprecated
    public void loadStopWords( InputStream wordfile, String encoding ) {
        setPreviousTokenStream(null); // force a new stopfilter to be created
        if ( wordfile == null ) {
            stoptable = Collections.emptySet();
            return;
        }
        try {
            // clear any previous table (if present)
            stoptable = Collections.emptySet();

            InputStreamReader isr;
            if (encoding == null)
                isr = new InputStreamReader(wordfile);
            else
                isr = new InputStreamReader(wordfile, encoding);

            stoptable = WordlistLoader.getWordSet(isr);
        } catch ( IOException e ) {
          // clear any previous table (if present)
          // TODO: throw IOException
          stoptable = Collections.emptySet();
        }
    }

  /**
   * Creates
   * {@link org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents}
   *         built from a {@link StandardTokenizer} filtered with
   *         {@link StandardFilter}, {@link LowerCaseFilter}, {@link StopFilter}
   *         , and {@link CzechStemFilter} (only if version is >= LUCENE_31). If
   *         a version is >= LUCENE_31 and a stem exclusion set is provided via
   *         {@link #CzechAnalyzer(Version, Set, Set)} a
   *         {@link KeywordMarkerFilter} is added before
   *         {@link CzechStemFilter}.
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {
    final Tokenizer source = new StandardTokenizer(matchVersion, reader);
    TokenStream result = new StandardFilter(matchVersion, source);
    result = new LowerCaseFilter(matchVersion, result);
    result = new StopFilter( matchVersion, result, stoptable);
    if (matchVersion.onOrAfter(Version.LUCENE_31)) {
      if(!this.stemExclusionTable.isEmpty())
        result = new KeywordMarkerFilter(result, stemExclusionTable);
      result = new CzechStemFilter(result);
    }
    return new TokenStreamComponents(source, result);
  }
}

