package org.apache.lucene.analysis.ru;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;

import java.io.Reader;
import java.util.Hashtable;

/**
 * Analyzer for Russian language. Supports an external list of stopwords (words that
 * will not be indexed at all).
 * A default set of stopwords is used unless an alternative list is specified.
 *
 * @author  Boris Okner, b.okner@rogers.com
 * @version $Id$
 */
public final class RussianAnalyzer extends Analyzer
{
    // letters
    private static char A = 0;
    private static char B = 1;
    private static char V = 2;
    private static char G = 3;
    private static char D = 4;
    private static char E = 5;
    private static char ZH = 6;
    private static char Z = 7;
    private static char I = 8;
    private static char I_ = 9;
    private static char K = 10;
    private static char L = 11;
    private static char M = 12;
    private static char N = 13;
    private static char O = 14;
    private static char P = 15;
    private static char R = 16;
    private static char S = 17;
    private static char T = 18;
    private static char U = 19;
    private static char F = 20;
    private static char X = 21;
    private static char TS = 22;
    private static char CH = 23;
    private static char SH = 24;
    private static char SHCH = 25;
    private static char HARD = 26;
    private static char Y = 27;
    private static char SOFT = 28;
    private static char AE = 29;
    private static char IU = 30;
    private static char IA = 31;

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
    private Hashtable stoptable = new Hashtable();

    /**
     * Charset for Russian letters.
     * Represents encoding for 32 lowercase Russian letters.
     * Predefined charsets can be taken from RussianCharSets class
     */
    private char[] charset;

    /**
     * Builds an analyzer.
     */
    public RussianAnalyzer(char[] charset)
    {
        this.charset = charset;
        stoptable = StopFilter.makeStopTable(makeStopWords(charset));
    }

    /**
     * Builds an analyzer with the given stop words.
     */
    public RussianAnalyzer(char[] charset, String[] stopwords)
    {
        this.charset = charset;
        stoptable = StopFilter.makeStopTable(stopwords);
    }

    // Takes russian stop words and translates them to a String array, using
    // the given charset
    private static String[] makeStopWords(char[] charset)
    {
        String[] res = new String[RUSSIAN_STOP_WORDS.length];
        for (int i = 0; i < res.length; i++)
        {
            char[] theStopWord = RUSSIAN_STOP_WORDS[i];
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
    public RussianAnalyzer(char[] charset, Hashtable stopwords)
    {
        this.charset = charset;
        stoptable = stopwords;
    }

    /**
     * Creates a TokenStream which tokenizes all the text in the provided Reader.
     *
     * @return  A TokenStream build from a RussianLetterTokenizer filtered with
     *                  RussianLowerCaseFilter, StopFilter, and RussianStemFilter
     */
    public TokenStream tokenStream(String fieldName, Reader reader)
    {
        TokenStream result = new RussianLetterTokenizer(reader, charset);
        result = new RussianLowerCaseFilter(result, charset);
        result = new StopFilter(result, stoptable);
        result = new RussianStemFilter(result, charset);
        return result;
    }
}
