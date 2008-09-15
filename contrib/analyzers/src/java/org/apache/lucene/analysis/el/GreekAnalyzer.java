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
import org.apache.lucene.analysis.standard.StandardTokenizer;

import java.io.Reader;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Analyzer for the Greek language. Supports an external list of stopwords (words
 * that will not be indexed at all).
 * A default set of stopwords is used unless an alternative list is specified.
 *
 */
public final class GreekAnalyzer extends Analyzer
{
    // the letters are indexes to the charset array (see GreekCharsets.java)
    private static char A = 6;
    private static char B = 7;
    private static char G = 8;
    private static char D = 9;
    private static char E = 10;
    private static char Z = 11;
    private static char H = 12;
    private static char TH = 13;
    private static char I = 14;
    private static char K = 15;
    private static char L = 16;
    private static char M = 17;
    private static char N = 18;
    private static char KS = 19;
    private static char O = 20;
    private static char P = 21;
    private static char R = 22;
    private static char S = 24;	// skip final sigma
    private static char T = 25;
    private static char Y = 26;
    private static char F = 27;
    private static char X = 28;
    private static char PS = 29;
    private static char W = 30;

    /**
     * List of typical Greek stopwords.
     */
    private static char[][] GREEK_STOP_WORDS = {
        {O},
		{H},
		{T, O},
        {O, I},
		{T, A},
		{T, O, Y},
		{T, H, S},
		{T, W, N},
		{T, O, N},
		{T, H, N},
		{K, A, I},
		{K, I},
		{K},
		{E, I, M, A, I},
		{E, I, S, A, I},
		{E, I, N, A, I},
		{E, I, M, A, S, T, E},
		{E, I, S, T, E},
		{S, T, O},
		{S, T, O, N},
		{S, T, H},
		{S, T, H, N},
		{M, A},
		{A, L, L, A},
		{A, P, O},
		{G, I, A},
		{P, R, O, S},
		{M, E},
		{S, E},
		{W, S},
		{P, A, R, A},
		{A, N, T, I},
		{K, A, T, A},
		{M, E, T, A},
		{TH, A},
		{N, A},
		{D, E},
		{D, E, N},
		{M, H},
		{M, H, N},
		{E, P, I},
		{E, N, W},
		{E, A, N},
		{A, N},
		{T, O, T, E},
		{P, O, Y},
		{P, W, S},
		{P, O, I, O, S},
		{P, O, I, A},
		{P, O, I, O},
		{P, O, I, O, I},
		{P, O, I, E, S},
		{P, O, I, W, N},
		{P, O, I, O, Y, S},
		{A, Y, T, O, S},
		{A, Y, T, H},
		{A, Y, T, O},
		{A, Y, T, O, I},
		{A, Y, T, W, N},
		{A, Y, T, O, Y, S},
		{A, Y, T, E, S},
		{A, Y, T, A},
		{E, K, E, I, N, O, S},
		{E, K, E, I, N, H},
		{E, K, E, I, N, O},
		{E, K, E, I, N, O, I},
		{E, K, E, I, N, E, S},
		{E, K, E, I, N, A},
		{E, K, E, I, N, W, N},
		{E, K, E, I, N, O, Y, S},
		{O, P, W, S},
		{O, M, W, S},
		{I, S, W, S},
		{O, S, O},
		{O, T, I}
    };

    /**
     * Contains the stopwords used with the StopFilter.
     */
    private Set stopSet = new HashSet();

    /**
     * Charset for Greek letters.
     * Represents encoding for 24 lowercase Greek letters.
     * Predefined charsets can be taken from GreekCharSets class
     */
    private char[] charset;

    public GreekAnalyzer() {
        charset = GreekCharsets.UnicodeGreek;
        stopSet = StopFilter.makeStopSet(
                    makeStopWords(GreekCharsets.UnicodeGreek));
    }

    /**
     * Builds an analyzer.
     */
    public GreekAnalyzer(char[] charset)
    {
        this.charset = charset;
        stopSet = StopFilter.makeStopSet(makeStopWords(charset));
    }

    /**
     * Builds an analyzer with the given stop words.
     */
    public GreekAnalyzer(char[] charset, String[] stopwords)
    {
        this.charset = charset;
        stopSet = StopFilter.makeStopSet(stopwords);
    }

    // Takes greek stop words and translates them to a String array, using
    // the given charset
    private static String[] makeStopWords(char[] charset)
    {
        String[] res = new String[GREEK_STOP_WORDS.length];
        for (int i = 0; i < res.length; i++)
        {
            char[] theStopWord = GREEK_STOP_WORDS[i];
            // translate the word,using the charset
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
     */
    public GreekAnalyzer(char[] charset, Map stopwords)
    {
        this.charset = charset;
        stopSet = new HashSet(stopwords.keySet());
    }

    /**
     * Creates a TokenStream which tokenizes all the text in the provided Reader.
     *
     * @return  A TokenStream build from a StandardTokenizer filtered with
     *                  GreekLowerCaseFilter and StopFilter
     */
    public TokenStream tokenStream(String fieldName, Reader reader)
    {
    	TokenStream result = new StandardTokenizer(reader);
        result = new GreekLowerCaseFilter(result, charset);
        result = new StopFilter(result, stopSet);
        return result;
    }
}
