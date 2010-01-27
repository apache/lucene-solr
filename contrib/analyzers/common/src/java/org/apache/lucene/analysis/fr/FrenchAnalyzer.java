package org.apache.lucene.analysis.fr;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents; // javadoc @link
import org.apache.lucene.analysis.KeywordMarkerTokenFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;  // for javadoc
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * {@link Analyzer} for French language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (word that will
 * not be stemmed, but indexed).
 * A default set of stopwords is used unless an alternative list is specified, but the
 * exclusion list is empty by default.
 * </p>
 *
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating FrenchAnalyzer:
 * <ul>
 *   <li> As of 2.9, StopFilter preserves position
 *        increments
 * </ul>
 *
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public final class FrenchAnalyzer extends StopwordAnalyzerBase {

  /**
   * Extended list of typical French stopwords.
   * @deprecated use {@link #getDefaultStopSet()} instead
   */
  // TODO make this private in 3.1
  @Deprecated
  public final static String[] FRENCH_STOP_WORDS = {
    "a", "afin", "ai", "ainsi", "après", "attendu", "au", "aujourd", "auquel", "aussi",
    "autre", "autres", "aux", "auxquelles", "auxquels", "avait", "avant", "avec", "avoir",
    "c", "car", "ce", "ceci", "cela", "celle", "celles", "celui", "cependant", "certain",
    "certaine", "certaines", "certains", "ces", "cet", "cette", "ceux", "chez", "ci",
    "combien", "comme", "comment", "concernant", "contre", "d", "dans", "de", "debout",
    "dedans", "dehors", "delà", "depuis", "derrière", "des", "désormais", "desquelles",
    "desquels", "dessous", "dessus", "devant", "devers", "devra", "divers", "diverse",
    "diverses", "doit", "donc", "dont", "du", "duquel", "durant", "dès", "elle", "elles",
    "en", "entre", "environ", "est", "et", "etc", "etre", "eu", "eux", "excepté", "hormis",
    "hors", "hélas", "hui", "il", "ils", "j", "je", "jusqu", "jusque", "l", "la", "laquelle",
    "le", "lequel", "les", "lesquelles", "lesquels", "leur", "leurs", "lorsque", "lui", "là",
    "ma", "mais", "malgré", "me", "merci", "mes", "mien", "mienne", "miennes", "miens", "moi",
    "moins", "mon", "moyennant", "même", "mêmes", "n", "ne", "ni", "non", "nos", "notre",
    "nous", "néanmoins", "nôtre", "nôtres", "on", "ont", "ou", "outre", "où", "par", "parmi",
    "partant", "pas", "passé", "pendant", "plein", "plus", "plusieurs", "pour", "pourquoi",
    "proche", "près", "puisque", "qu", "quand", "que", "quel", "quelle", "quelles", "quels",
    "qui", "quoi", "quoique", "revoici", "revoilà", "s", "sa", "sans", "sauf", "se", "selon",
    "seront", "ses", "si", "sien", "sienne", "siennes", "siens", "sinon", "soi", "soit",
    "son", "sont", "sous", "suivant", "sur", "ta", "te", "tes", "tien", "tienne", "tiennes",
    "tiens", "toi", "ton", "tous", "tout", "toute", "toutes", "tu", "un", "une", "va", "vers",
    "voici", "voilà", "vos", "votre", "vous", "vu", "vôtre", "vôtres", "y", "à", "ça", "ès",
    "été", "être", "ô"
  };

  /**
   * Contains words that should be indexed but not stemmed.
   */
  //TODO make this final in 3.0
  private Set<?> excltable = Collections.<Object>emptySet();

  /**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static Set<?> getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  private static class DefaultSetHolder {
    static final Set<?> DEFAULT_STOP_SET = CharArraySet
        .unmodifiableSet(new CharArraySet(Version.LUCENE_CURRENT, Arrays.asList(FRENCH_STOP_WORDS),
            false));
  }

  /**
   * Builds an analyzer with the default stop words ({@link #FRENCH_STOP_WORDS}).
   */
  public FrenchAnalyzer(Version matchVersion) {
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
  public FrenchAnalyzer(Version matchVersion, Set<?> stopwords){
    this(matchVersion, stopwords, CharArraySet.EMPTY_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   * @param stemExclutionSet
   *          a stemming exclusion set
   */
  public FrenchAnalyzer(Version matchVersion, Set<?> stopwords,
      Set<?> stemExclutionSet) {
    super(matchVersion, stopwords);
    this.excltable = CharArraySet.unmodifiableSet(CharArraySet
        .copy(matchVersion, stemExclutionSet));
  }
 

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #FrenchAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public FrenchAnalyzer(Version matchVersion, String... stopwords) {
    this(matchVersion, StopFilter.makeStopSet(matchVersion, stopwords));
  }

  /**
   * Builds an analyzer with the given stop words.
   * @throws IOException
   * @deprecated use {@link #FrenchAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public FrenchAnalyzer(Version matchVersion, File stopwords) throws IOException {
    this(matchVersion, WordlistLoader.getWordSet(stopwords));
  }

  /**
   * Builds an exclusionlist from an array of Strings.
   * @deprecated use {@link #FrenchAnalyzer(Version, Set, Set)} instead
   */
  @Deprecated
  public void setStemExclusionTable(String... exclusionlist) {
    excltable = StopFilter.makeStopSet(matchVersion, exclusionlist);
    setPreviousTokenStream(null); // force a new stemmer to be created
  }

  /**
   * Builds an exclusionlist from a Map.
   * @deprecated use {@link #FrenchAnalyzer(Version, Set, Set)} instead
   */
  @Deprecated
  public void setStemExclusionTable(Map<?,?> exclusionlist) {
    excltable = new HashSet<Object>(exclusionlist.keySet());
    setPreviousTokenStream(null); // force a new stemmer to be created
  }

  /**
   * Builds an exclusionlist from the words contained in the given file.
   * @throws IOException
   * @deprecated use {@link #FrenchAnalyzer(Version, Set, Set)} instead
   */
  @Deprecated
  public void setStemExclusionTable(File exclusionlist) throws IOException {
    excltable = new HashSet<Object>(WordlistLoader.getWordSet(exclusionlist));
    setPreviousTokenStream(null); // force a new stemmer to be created
  }

  /**
   * Creates {@link TokenStreamComponents} used to tokenize all the text in the provided
   * {@link Reader}.
   *
   * @return {@link TokenStreamComponents} built from a {@link StandardTokenizer} 
   *         filtered with {@link StandardFilter}, {@link StopFilter}, 
   *         {@link FrenchStemFilter} and {@link LowerCaseFilter}
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {
    final Tokenizer source = new StandardTokenizer(matchVersion, reader);
    TokenStream result = new StandardFilter(source);
    result = new StopFilter(matchVersion, result, stopwords);
    if(!excltable.isEmpty())
      result = new KeywordMarkerTokenFilter(result, excltable);
    result = new FrenchStemFilter(result);
    // Convert to lowercase after stemming!
    return new TokenStreamComponents(source, new LowerCaseFilter(matchVersion, result));
  }
}

