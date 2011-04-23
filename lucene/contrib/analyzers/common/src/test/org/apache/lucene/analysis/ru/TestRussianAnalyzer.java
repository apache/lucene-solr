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
import java.io.InputStreamReader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

/**
 * Test case for RussianAnalyzer.
 */

public class TestRussianAnalyzer extends BaseTokenStreamTestCase
{
    private InputStreamReader inWords;

    private InputStreamReader sampleUnicode;

    /**
     * @deprecated remove this test and its datafiles in Lucene 4.0
     * the Snowball version has its own data tests.
     */
    @Deprecated
    public void testUnicode30() throws IOException
    {
        RussianAnalyzer ra = new RussianAnalyzer(Version.LUCENE_30);
        inWords =
            new InputStreamReader(
                getClass().getResourceAsStream("testUTF8.txt"),
                "UTF-8");

        sampleUnicode =
            new InputStreamReader(
                getClass().getResourceAsStream("resUTF8.htm"),
                "UTF-8");

        TokenStream in = ra.tokenStream("all", inWords);

        RussianLetterTokenizer sample =
            new RussianLetterTokenizer(TEST_VERSION_CURRENT,
                sampleUnicode);

        CharTermAttribute text = in.getAttribute(CharTermAttribute.class);
        CharTermAttribute sampleText = sample.getAttribute(CharTermAttribute.class);

        for (;;)
        {
          if (in.incrementToken() == false)
            break;

            boolean nextSampleToken = sample.incrementToken();
            assertEquals(
                "Unicode",
                text.toString(),
                nextSampleToken == false
                ? null
                : sampleText.toString());
        }

        inWords.close();
        sampleUnicode.close();
    }
    
    /** Check that RussianAnalyzer doesnt discard any numbers */
    public void testDigitsInRussianCharset() throws IOException
    {
      RussianAnalyzer ra = new RussianAnalyzer(TEST_VERSION_CURRENT);
      assertAnalyzesTo(ra, "text 1000", new String[] { "text", "1000" });
    }
    
    /** @deprecated remove this test in Lucene 4.0: stopwords changed */
    @Deprecated
    public void testReusableTokenStream30() throws Exception {
      Analyzer a = new RussianAnalyzer(Version.LUCENE_30);
      assertAnalyzesToReuse(a, "Вместе с тем о силе электромагнитной энергии имели представление еще",
          new String[] { "вмест", "сил", "электромагнитн", "энерг", "имел", "представлен" });
      assertAnalyzesToReuse(a, "Но знание это хранилось в тайне",
          new String[] { "знан", "хран", "тайн" });
    }
    
    public void testReusableTokenStream() throws Exception {
      Analyzer a = new RussianAnalyzer(TEST_VERSION_CURRENT);
      assertAnalyzesToReuse(a, "Вместе с тем о силе электромагнитной энергии имели представление еще",
          new String[] { "вмест", "сил", "электромагнитн", "энерг", "имел", "представлен" });
      assertAnalyzesToReuse(a, "Но знание это хранилось в тайне",
          new String[] { "знан", "эт", "хран", "тайн" });
    }
    
    
    public void testWithStemExclusionSet() throws Exception {
      CharArraySet set = new CharArraySet(TEST_VERSION_CURRENT, 1, true);
      set.add("представление");
      Analyzer a = new RussianAnalyzer(TEST_VERSION_CURRENT, RussianAnalyzer.getDefaultStopSet() , set);
      assertAnalyzesToReuse(a, "Вместе с тем о силе электромагнитной энергии имели представление еще",
          new String[] { "вмест", "сил", "электромагнитн", "энерг", "имел", "представление" });
     
    }
    
    /** blast some random strings through the analyzer */
    public void testRandomStrings() throws Exception {
      checkRandomData(random, new RussianAnalyzer(TEST_VERSION_CURRENT), 10000*RANDOM_MULTIPLIER);
    }
}
