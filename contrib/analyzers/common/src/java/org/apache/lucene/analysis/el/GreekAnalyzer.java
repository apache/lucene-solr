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


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents; // javadoc @link
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;  // for javadoc
import org.apache.lucene.util.Version;

import java.io.Reader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * {@link Analyzer} for the Greek language. 
 * <p>
 * Supports an external list of stopwords (words
 * that will not be indexed at all).
 * A default set of stopwords is used unless an alternative list is specified.
 * </p>
 *
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public final class GreekAnalyzer extends StopwordAnalyzerBase
{
    /**
     * List of typical Greek stopwords.
     */
    private static final String[] GREEK_STOP_WORDS = {
      "ο", "η", "το", "οι", "τα", "του", "τησ", "των", "τον", "την", "και", 
      "κι", "κ", "ειμαι", "εισαι", "ειναι", "ειμαστε", "ειστε", "στο", "στον",
      "στη", "στην", "μα", "αλλα", "απο", "για", "προσ", "με", "σε", "ωσ",
      "παρα", "αντι", "κατα", "μετα", "θα", "να", "δε", "δεν", "μη", "μην",
      "επι", "ενω", "εαν", "αν", "τοτε", "που", "πωσ", "ποιοσ", "ποια", "ποιο",
      "ποιοι", "ποιεσ", "ποιων", "ποιουσ", "αυτοσ", "αυτη", "αυτο", "αυτοι",
      "αυτων", "αυτουσ", "αυτεσ", "αυτα", "εκεινοσ", "εκεινη", "εκεινο",
      "εκεινοι", "εκεινεσ", "εκεινα", "εκεινων", "εκεινουσ", "οπωσ", "ομωσ",
      "ισωσ", "οσο", "οτι"
    };
    
    /**
     * Returns a set of default Greek-stopwords 
     * @return a set of default Greek-stopwords 
     */
    public static final Set<?> getDefaultStopSet(){
      return DefaultSetHolder.DEFAULT_SET;
    }
    
    private static class DefaultSetHolder {
      private static final Set<?> DEFAULT_SET = CharArraySet.unmodifiableSet(new CharArraySet(
          Version.LUCENE_CURRENT, Arrays.asList(GREEK_STOP_WORDS), false));
    }

    public GreekAnalyzer(Version matchVersion) {
      this(matchVersion, DefaultSetHolder.DEFAULT_SET);
    }
    
    /**
     * Builds an analyzer with the given stop words 
     * 
     * @param matchVersion
     *          lucene compatibility version
     * @param stopwords
     *          a stopword set
     */
    public GreekAnalyzer(Version matchVersion, Set<?> stopwords) {
      super(matchVersion, stopwords);
    }

    /**
     * Builds an analyzer with the given stop words.
     * @param stopwords Array of stopwords to use.
     * @deprecated use {@link #GreekAnalyzer(Version, Set)} instead
     */
    @Deprecated
    public GreekAnalyzer(Version matchVersion, String... stopwords)
    {
      this(matchVersion, StopFilter.makeStopSet(matchVersion, stopwords));
    }

    /**
     * Builds an analyzer with the given stop words.
     * @deprecated use {@link #GreekAnalyzer(Version, Set)} instead
     */
    @Deprecated
    public GreekAnalyzer(Version matchVersion, Map<?,?> stopwords)
    {
      this(matchVersion, stopwords.keySet());
    }
  
   /**
    * Creates {@link TokenStreamComponents} used to tokenize all the text in the
    * provided {@link Reader}.
    * 
    * @return {@link TokenStreamComponents} built from a
    *         {@link StandardTokenizer} filtered with
    *         {@link GreekLowerCaseFilter} and {@link StopFilter}
    */
    @Override
    protected TokenStreamComponents createComponents(String fieldName,
        Reader reader) {
      final Tokenizer source = new StandardTokenizer(matchVersion, reader);
      final TokenStream result = new GreekLowerCaseFilter(source);
      return new TokenStreamComponents(source, new StopFilter(matchVersion, result, stopwords));
    }
}
