package org.apache.lucene.analysis.ru;

/*
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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.util.Version;

/**
 * Test case for RussianAnalyzer.
 */

public class TestRussianAnalyzer extends BaseTokenStreamTestCase {

     /** Check that RussianAnalyzer doesnt discard any numbers */
    public void testDigitsInRussianCharset() throws IOException
    {
      RussianAnalyzer ra = new RussianAnalyzer();
      assertAnalyzesTo(ra, "text 1000", new String[] { "text", "1000" });
    }
    
    /** @deprecated (3.1) remove this test in Lucene 5.0: stopwords changed */
    @Deprecated
    public void testReusableTokenStream30() throws Exception {
      Analyzer a = new RussianAnalyzer(Version.LUCENE_3_0);
      assertAnalyzesTo(a, "Вместе с тем о силе электромагнитной энергии имели представление еще",
          new String[] { "вмест", "сил", "электромагнитн", "энерг", "имел", "представлен" });
      assertAnalyzesTo(a, "Но знание это хранилось в тайне",
          new String[] { "знан", "хран", "тайн" });
    }
    
    public void testReusableTokenStream() throws Exception {
      Analyzer a = new RussianAnalyzer();
      assertAnalyzesTo(a, "Вместе с тем о силе электромагнитной энергии имели представление еще",
          new String[] { "вмест", "сил", "электромагнитн", "энерг", "имел", "представлен" });
      assertAnalyzesTo(a, "Но знание это хранилось в тайне",
          new String[] { "знан", "эт", "хран", "тайн" });
    }
    
    
    public void testWithStemExclusionSet() throws Exception {
      CharArraySet set = new CharArraySet( 1, true);
      set.add("представление");
      Analyzer a = new RussianAnalyzer( RussianAnalyzer.getDefaultStopSet() , set);
      assertAnalyzesTo(a, "Вместе с тем о силе электромагнитной энергии имели представление еще",
          new String[] { "вмест", "сил", "электромагнитн", "энерг", "имел", "представление" });
     
    }
    
    /** blast some random strings through the analyzer */
    public void testRandomStrings() throws Exception {
      checkRandomData(random(), new RussianAnalyzer(), 1000*RANDOM_MULTIPLIER);
    }
}
