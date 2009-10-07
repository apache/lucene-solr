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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/**
 * Test case for RussianAnalyzer.
 */

public class TestRussianAnalyzer extends BaseTokenStreamTestCase
{
    private InputStreamReader inWords;

    private InputStreamReader sampleUnicode;

    private File dataDir;

    protected void setUp() throws Exception
    {
      super.setUp();
      dataDir = new File(System.getProperty("dataDir", "./bin"));
    }

    public void testUnicode() throws IOException
    {
        RussianAnalyzer ra = new RussianAnalyzer();
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
                sampleUnicode);

        TermAttribute text = in.getAttribute(TermAttribute.class);
        TermAttribute sampleText = sample.getAttribute(TermAttribute.class);

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
    
    public void testDigitsInRussianCharset() 
    {
        Reader reader = new StringReader("text 1000");
        RussianAnalyzer ra = new RussianAnalyzer();
        TokenStream stream = ra.tokenStream("", reader);

        TermAttribute termText = stream.getAttribute(TermAttribute.class);
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
    
    public void testReusableTokenStream() throws Exception {
      Analyzer a = new RussianAnalyzer();
      assertAnalyzesToReuse(a, "Вместе с тем о силе электромагнитной энергии имели представление еще",
          new String[] { "вмест", "сил", "электромагнитн", "энерг", "имел", "представлен" });
      assertAnalyzesToReuse(a, "Но знание это хранилось в тайне",
          new String[] { "знан", "хран", "тайн" });
    }
}
