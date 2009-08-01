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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/**
 * Test case for RussianAnalyzer.
 *
 *
 * @version   $Id$
 */

public class TestRussianAnalyzer extends TestCase
{
    private InputStreamReader inWords;

    private InputStreamReader sampleUnicode;

    private Reader inWordsKOI8;

    private Reader sampleKOI8;

    private Reader inWords1251;

    private Reader sample1251;

    private File dataDir;

    protected void setUp() throws Exception
    {
      dataDir = new File(System.getProperty("dataDir", "./bin"));
    }

    public void testUnicode() throws IOException
    {
        RussianAnalyzer ra = new RussianAnalyzer(RussianCharsets.UnicodeRussian);
        inWords =
            new InputStreamReader(
                new FileInputStream(new File(dataDir, "/org/apache/lucene/analysis/ru/testUTF8.txt")),
                "UTF-8");

        sampleUnicode =
            new InputStreamReader(
                new FileInputStream(new File(dataDir, "/org/apache/lucene/analysis/ru/resUTF8.htm")),
                "UTF-8");

        TokenStream in = ra.tokenStream("all", inWords);

        RussianLetterTokenizer sample =
            new RussianLetterTokenizer(
                sampleUnicode,
                RussianCharsets.UnicodeRussian);

        TermAttribute text = (TermAttribute) in.getAttribute(TermAttribute.class);
        TermAttribute sampleText = (TermAttribute) sample.getAttribute(TermAttribute.class);

        for (;;)
        {
          if (in.incrementToken() == false)
            break;

            boolean nextSampleToken = sample.incrementToken();
            assertEquals(
                "Unicode",
                text.term(),
                nextSampleToken == false
                ? null
                : sampleText.term());
        }

        inWords.close();
        sampleUnicode.close();
    }

    public void testKOI8() throws IOException
    {
        //System.out.println(new java.util.Date());
        RussianAnalyzer ra = new RussianAnalyzer(RussianCharsets.KOI8);
        // KOI8
        inWordsKOI8 = new InputStreamReader(new FileInputStream(new File(dataDir, "/org/apache/lucene/analysis/ru/testKOI8.txt")), "iso-8859-1");

        sampleKOI8 = new InputStreamReader(new FileInputStream(new File(dataDir, "/org/apache/lucene/analysis/ru/resKOI8.htm")), "iso-8859-1");

        TokenStream in = ra.tokenStream("all", inWordsKOI8);
        RussianLetterTokenizer sample =
            new RussianLetterTokenizer(
                sampleKOI8,
                RussianCharsets.KOI8);

        TermAttribute text = (TermAttribute) in.getAttribute(TermAttribute.class);
        TermAttribute sampleText = (TermAttribute) sample.getAttribute(TermAttribute.class);

        for (;;)
        {
          if (in.incrementToken() == false)
            break;

            boolean nextSampleToken = sample.incrementToken();
            assertEquals(
                "KOI8",
                text.term(),
                nextSampleToken == false
                ? null
                : sampleText.term());
        }
        inWordsKOI8.close();
        sampleKOI8.close();
    }

    public void test1251() throws IOException
    {
        // 1251
        inWords1251 = new InputStreamReader(new FileInputStream(new File(dataDir, "/org/apache/lucene/analysis/ru/test1251.txt")), "iso-8859-1");

        sample1251 = new InputStreamReader(new FileInputStream(new File(dataDir, "/org/apache/lucene/analysis/ru/res1251.htm")), "iso-8859-1");

        RussianAnalyzer ra = new RussianAnalyzer(RussianCharsets.CP1251);
        TokenStream in = ra.tokenStream("", inWords1251);
        RussianLetterTokenizer sample =
            new RussianLetterTokenizer(
                sample1251,
                RussianCharsets.CP1251);

        TermAttribute text = (TermAttribute) in.getAttribute(TermAttribute.class);
        TermAttribute sampleText = (TermAttribute) sample.getAttribute(TermAttribute.class);

        for (;;)
        {
          if (in.incrementToken() == false)
            break;

            boolean nextSampleToken = sample.incrementToken();
            assertEquals(
                "1251",
                text.term(),
                nextSampleToken == false
                ? null
                : sampleText.term());
        }

        inWords1251.close();
        sample1251.close();
    }
    
    public void testDigitsInRussianCharset() 
    {
        Reader reader = new StringReader("text 1000");
        RussianAnalyzer ra = new RussianAnalyzer();
        TokenStream stream = ra.tokenStream("", reader);

        TermAttribute termText = (TermAttribute) stream.getAttribute(TermAttribute.class);
        try {
            assertTrue(stream.incrementToken());
            assertEquals("text", termText.term());
            assertTrue(stream.incrementToken());
            assertEquals("RussianAnalyzer's tokenizer skips numbers from input text", "1000", termText.term());
            assertFalse(stream.incrementToken());
        }
        catch (IOException e)
        {
            fail("unexpected IOException");
        }
    }

}
