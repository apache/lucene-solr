package org.apache.lucene.analysis.ru;

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

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * {@link Analyzer} for Russian language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all).
 * A default set of stopwords is used unless an alternative list is specified.
 * </p>
 *
 * @version $Id$
 */
public final class RussianAnalyzer extends Analyzer
{
    // letters (currently unused letters are commented out)
    private final static char A = 0;
    private final static char B = 1;
    private final static char V = 2;
    private final static char G = 3;
    private final static char D = 4;
    private final static char E = 5;
    private final static char ZH = 6;
    private final static char Z = 7;
    private final static char I = 8;
    private final static char I_ = 9;
    private final static char K = 10;
    private final static char L = 11;
    private final static char M = 12;
    private final static char N = 13;
    private final static char O = 14;
    private final static char P = 15;
    private final static char R = 16;
    private final static char S = 17;
    private final static char T = 18;
    private final static char U = 19;
    //private final static char F = 20;
    private final static char X = 21;
    //private final static char TS = 22;
    private final static char CH = 23;
    private final static char SH = 24;
    private final static char SHCH = 25;
    //private final static char HARD = 26;
    private final static char Y = 27;
    private final static char SOFT = 28;
    private final static char AE = 29;
    private final static char IU = 30;
    private final static char IA = 31;

    /**
     * List of typical Russian stopwords.
     */
    private static char[][] RUSSIAN_STOP_WORDS = {
        {A},
        {B, E, Z},
        {B, O, L, E, E},
        {B, Y},
        {B, Y, L},
        {B, Y, L, A},
        {B, Y, L, I},
        {B, Y, L, O},
        {B, Y, T, SOFT},
        {V},
        {V, A, M},
        {V, A, S},
        {V, E, S, SOFT},
        {V, O},
        {V, O, T},
        {V, S, E},
        {V, S, E, G, O},
        {V, S, E, X},
        {V, Y},
        {G, D, E},
        {D, A},
        {D, A, ZH, E},
        {D, L, IA},
        {D, O},
        {E, G, O},
        {E, E},
        {E, I_,},
        {E, IU},
        {E, S, L, I},
        {E, S, T, SOFT},
        {E, SHCH, E},
        {ZH, E},
        {Z, A},
        {Z, D, E, S, SOFT},
        {I},
        {I, Z},
        {I, L, I},
        {I, M},
        {I, X},
        {K},
        {K, A, K},
        {K, O},
        {K, O, G, D, A},
        {K, T, O},
        {L, I},
        {L, I, B, O},
        {M, N, E},
        {M, O, ZH, E, T},
        {M, Y},
        {N, A},
        {N, A, D, O},
        {N, A, SH},
        {N, E},
        {N, E, G, O},
        {N, E, E},
        {N, E, T},
        {N, I},
        {N, I, X},
        {N, O},
        {N, U},
        {O},
        {O, B},
        {O, D, N, A, K, O},
        {O, N},
        {O, N, A},
        {O, N, I},
        {O, N, O},
        {O, T},
        {O, CH, E, N, SOFT},
        {P, O},
        {P, O, D},
        {P, R, I},
        {S},
        {S, O},
        {T, A, K},
        {T, A, K, ZH, E},
        {T, A, K, O, I_},
        {T, A, M},
        {T, E},
        {T, E, M},
        {T, O},
        {T, O, G, O},
        {T, O, ZH, E},
        {T, O, I_},
        {T, O, L, SOFT, K, O},
        {T, O, M},
        {T, Y},
        {U},
        {U, ZH, E},
        {X, O, T, IA},
        {CH, E, G, O},
        {CH, E, I_},
        {CH, E, M},
        {CH, T, O},
        {CH, T, O, B, Y},
        {CH, SOFT, E},
        {CH, SOFT, IA},
        {AE, T, A},
        {AE, T, I},
        {AE, T, O},
        {IA}
    };

    /**
     * Contains the stopwords used with the StopFilter.
     */
    private Set stopSet = new HashSet();

    /**
     * Charset for Russian letters.
     * Represents encoding for 32 lowercase Russian letters.
     * Predefined charsets can be taken from RussianCharSets class
     * @deprecated Support for non-Unicode encodings will be removed in Lucene 3.0
     */
    private char[] charset;


    public RussianAnalyzer() {
        charset = RussianCharsets.UnicodeRussian;
        stopSet = StopFilter.makeStopSet(
                    makeStopWords(RussianCharsets.UnicodeRussian));
    }

    /**
     * Builds an analyzer.
     * @deprecated Use {@link #RussianAnalyzer()} instead.
     */
    public RussianAnalyzer(char[] charset)
    {
        this.charset = charset;
        stopSet = StopFilter.makeStopSet(makeStopWords(charset));
    }

    /**
     * Builds an analyzer with the given stop words.
     * @deprecated Use {@link #RussianAnalyzer(String[])} instead.
     */
    public RussianAnalyzer(char[] charset, String[] stopwords)
    {
        this.charset = charset;
        stopSet = StopFilter.makeStopSet(stopwords);
    }
    
    /**
     * Builds an analyzer with the given stop words.
     */
    public RussianAnalyzer(String[] stopwords)
    {
    	this.charset = RussianCharsets.UnicodeRussian;
    	stopSet = StopFilter.makeStopSet(stopwords);
    }

    /** Takes russian stop words and translates them to a String array, using
     * the given charset.
     * @deprecated Support for non-Unicode encodings will be removed in Lucene 3.0
     */
    private static String[] makeStopWords(char[] charset)
    {
        String[] res = new String[RUSSIAN_STOP_WORDS.length];
        for (int i = 0; i < res.length; i++)
        {
            char[] theStopWord = RUSSIAN_STOP_WORDS[i];
            // translate the word, using the charset
            StringBuffer theWord = new StringBuffer();
            for (int j = 0; j < theStopWord.length; j++)
            {
                theWord.append(charset[theStopWord[j]]);
            }
            res[i] = theWord.toString();
        }
        return res;
    }

    /**
     * Builds an analyzer with the given stop words.
     * TODO: create a Set version of this ctor
     * @deprecated Use {@link #RussianAnalyzer(Map)} instead.
     */
    public RussianAnalyzer(char[] charset, Map stopwords)
    {
        this.charset = charset;
        stopSet = new HashSet(stopwords.keySet());
    }
    
    /**
     * Builds an analyzer with the given stop words.
     * TODO: create a Set version of this ctor
     */
    public RussianAnalyzer(Map stopwords)
    {
    	charset = RussianCharsets.UnicodeRussian;
    	stopSet = new HashSet(stopwords.keySet());
    }

    /**
     * Creates a {@link TokenStream} which tokenizes all the text in the 
     * provided {@link Reader}.
     *
     * @return  A {@link TokenStream} built from a 
     *   {@link RussianLetterTokenizer} filtered with 
     *   {@link RussianLowerCaseFilter}, {@link StopFilter}, 
     *   and {@link RussianStemFilter}
     */
    public TokenStream tokenStream(String fieldName, Reader reader)
    {
        TokenStream result = new RussianLetterTokenizer(reader, charset);
        result = new RussianLowerCaseFilter(result, charset);
        result = new StopFilter(result, stopSet);
        result = new RussianStemFilter(result, charset);
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
     * @return  A {@link TokenStream} built from a 
     *   {@link RussianLetterTokenizer} filtered with 
     *   {@link RussianLowerCaseFilter}, {@link StopFilter}, 
     *   and {@link RussianStemFilter}
     */
    public TokenStream reusableTokenStream(String fieldName, Reader reader) 
      throws IOException {
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.source = new RussianLetterTokenizer(reader, charset);
      streams.result = new RussianLowerCaseFilter(streams.source, charset);
      streams.result = new StopFilter(streams.result, stopSet);
      streams.result = new RussianStemFilter(streams.result, charset);
      setPreviousTokenStream(streams);
    } else {
      streams.source.reset(reader);
    }
    return streams.result;
  }
}
