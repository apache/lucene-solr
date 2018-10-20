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
package org.apache.lucene.analysis.synonym;

import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;

public class TestWordnetSynonymParser extends BaseTokenStreamTestCase {

  String synonymsFile = 
    "s(100000001,1,'woods',n,1,0).\n" +
    "s(100000001,2,'wood',n,1,0).\n" +
    "s(100000001,3,'forest',n,1,0).\n" +
    "s(100000002,1,'wolfish',n,1,0).\n" +
    "s(100000002,2,'ravenous',n,1,0).\n" +
    "s(100000003,1,'king',n,1,1).\n" +
    "s(100000003,2,'baron',n,1,1).\n" +
    "s(100000004,1,'king''s evil',n,1,1).\n" +
    "s(100000004,2,'king''s meany',n,1,1).\n";
  
  public void testSynonyms() throws Exception {
    Analyzer analyzer = new MockAnalyzer(random());
    WordnetSynonymParser parser = new WordnetSynonymParser(true, true, analyzer);
    parser.parse(new StringReader(synonymsFile));
    final SynonymMap map = parser.build();
    analyzer.close();
    
    analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new SynonymFilter(tokenizer, map, false));
      }
    };
    
    /* all expansions */
    assertAnalyzesTo(analyzer, "Lost in the woods",
        new String[] { "Lost", "in", "the", "woods", "wood", "forest" },
        new int[] { 0, 5, 8, 12, 12, 12 },
        new int[] { 4, 7, 11, 17, 17, 17 },
        new int[] { 1, 1, 1, 1, 0, 0 });
    
    /* single quote */
    assertAnalyzesTo(analyzer, "king",
        new String[] { "king", "baron" });
    
    /* multi words */
    assertAnalyzesTo(analyzer, "king's evil",
        new String[] { "king's", "king's", "evil", "meany" });
    analyzer.close();
  }
}
