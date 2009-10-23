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
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
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
public final class GreekAnalyzer extends Analyzer
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
     * Contains the stopwords used with the {@link StopFilter}.
     */
    private Set stopSet = new HashSet();

    private final Version matchVersion;

    public GreekAnalyzer(Version matchVersion) {
      super();
      stopSet = StopFilter.makeStopSet(GREEK_STOP_WORDS);
      this.matchVersion = matchVersion;
    }

    /**
     * Builds an analyzer with the given stop words.
     * @param stopwords Array of stopwords to use.
     */
    public GreekAnalyzer(Version matchVersion, String... stopwords)
    {
      super();
      stopSet = StopFilter.makeStopSet(stopwords);
      this.matchVersion = matchVersion;
    }

    /**
     * Builds an analyzer with the given stop words.
     */
    public GreekAnalyzer(Version matchVersion, Map stopwords)
    {
      super();
      stopSet = new HashSet(stopwords.keySet());
      this.matchVersion = matchVersion;
    }

    /**
     * Creates a {@link TokenStream} which tokenizes all the text in the provided {@link Reader}.
     *
     * @return  A {@link TokenStream} built from a {@link StandardTokenizer} filtered with
     *                  {@link GreekLowerCaseFilter} and {@link StopFilter}
     */
    public TokenStream tokenStream(String fieldName, Reader reader)
    {
        TokenStream result = new StandardTokenizer(matchVersion, reader);
        result = new GreekLowerCaseFilter(result);
        result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                result, stopSet);
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
     *                  {@link GreekLowerCaseFilter} and {@link StopFilter}
     */
    public TokenStream reusableTokenStream(String fieldName, Reader reader) 
      throws IOException {
      SavedStreams streams = (SavedStreams) getPreviousTokenStream();
      if (streams == null) {
        streams = new SavedStreams();
        streams.source = new StandardTokenizer(matchVersion, reader);
        streams.result = new GreekLowerCaseFilter(streams.source);
        streams.result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                        streams.result, stopSet);
        setPreviousTokenStream(streams);
      } else {
        streams.source.reset(reader);
      }
      return streams.result;
    }
}
