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

import junit.framework.TestCase;

import java.io.FileReader;

import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;

/**
 * Test case for RussianAnalyzer.
 *
 * @author    Boris Okner
 * @version   $Id$
 */

public class TestRussianAnalyzer extends TestCase
{
    private InputStreamReader inWords;

    private InputStreamReader sampleUnicode;

    private FileReader inWordsKOI8;

    private FileReader sampleKOI8;

    private FileReader inWords1251;

    private FileReader sample1251;

    public TestRussianAnalyzer(String name)
    {
        super(name);
    }

    /**
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception
    {
        super.setUp();

    }

    /**
     * @see TestCase#tearDown()
     */
    protected void tearDown() throws Exception
    {
        super.tearDown();
    }

    public void testUnicode() throws IOException
    {
        RussianAnalyzer ra = new RussianAnalyzer(RussianCharsets.UnicodeRussian);
        inWords =
            new InputStreamReader(
                new FileInputStream("src/test/org/apache/lucene/analysis/ru/testUnicode.txt"),
                "Unicode");

        sampleUnicode =
            new InputStreamReader(
                new FileInputStream("src/test/org/apache/lucene/analysis/ru/resUnicode.htm"),
                "Unicode");

        TokenStream in = ra.tokenStream("all", inWords);

        RussianLetterTokenizer sample =
            new RussianLetterTokenizer(
                sampleUnicode,
                RussianCharsets.UnicodeRussian);

        for (;;)
        {
            Token token = in.next();

            if (token == null)
            {
                break;
            }

            Token sampleToken = sample.next();
            assertEquals(
                "Unicode",
                token.termText(),
                sampleToken == null
                ? null
                : sampleToken.termText());
        }

        inWords.close();
        sampleUnicode.close();
    }

    public void testKOI8() throws IOException
    {
        //System.out.println(new java.util.Date());
        RussianAnalyzer ra = new RussianAnalyzer(RussianCharsets.KOI8);
        // KOI8
        inWordsKOI8 = new FileReader("src/test/org/apache/lucene/analysis/ru/testKOI8.txt");

        sampleKOI8 = new FileReader("src/test/org/apache/lucene/analysis/ru/resKOI8.htm");

        TokenStream in = ra.tokenStream("all", inWordsKOI8);
        RussianLetterTokenizer sample =
            new RussianLetterTokenizer(
                sampleKOI8,
                RussianCharsets.KOI8);

        for (;;)
        {
            Token token = in.next();

            if (token == null)
            {
                break;
            }

            Token sampleToken = sample.next();
            assertEquals(
                "KOI8",
                token.termText(),
                sampleToken == null
                ? null
                : sampleToken.termText());

        }

        inWordsKOI8.close();
        sampleKOI8.close();
    }

    public void test1251() throws IOException
    {
        // 1251
        inWords1251 = new FileReader("src/test/org/apache/lucene/analysis/ru/test1251.txt");

        sample1251 = new FileReader("src/test/org/apache/lucene/analysis/ru/res1251.htm");

        RussianAnalyzer ra = new RussianAnalyzer(RussianCharsets.CP1251);
        TokenStream in = ra.tokenStream("", inWords1251);
        RussianLetterTokenizer sample =
            new RussianLetterTokenizer(
                sample1251,
                RussianCharsets.CP1251);

        for (;;)
        {
            Token token = in.next();

            if (token == null)
            {
                break;
            }

            Token sampleToken = sample.next();
            assertEquals(
                "1251",
                token.termText(),
                sampleToken == null
                ? null
                : sampleToken.termText());

        }

        inWords1251.close();
        sample1251.close();
    }
}
