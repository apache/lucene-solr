package org.apache.lucene.index.memory;

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
import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;

public class TestSynonymTokenFilter extends BaseTokenStreamTestCase {
  File dataDir = new File(System.getProperty("dataDir", "./bin"));
  File testFile = new File(dataDir, "org/apache/lucene/index/memory/testSynonyms.txt");
  
  public void testSynonyms() throws Exception {
    SynonymMap map = new SynonymMap(new FileInputStream(testFile));
    /* all expansions */
    Analyzer analyzer = new SynonymWhitespaceAnalyzer(map, Integer.MAX_VALUE);
    assertAnalyzesTo(analyzer, "Lost in the woods",
        new String[] { "lost", "in", "the", "woods", "forest", "wood" },
        new int[] { 0, 5, 8, 12, 12, 12 },
        new int[] { 4, 7, 11, 17, 17, 17 },
        new int[] { 1, 1, 1, 1, 0, 0 });
  }
  
  public void testSynonymsLimitedAmount() throws Exception {
    SynonymMap map = new SynonymMap(new FileInputStream(testFile));
    /* limit to one synonym expansion */
    Analyzer analyzer = new SynonymWhitespaceAnalyzer(map, 1);
    assertAnalyzesTo(analyzer, "Lost in the woods",
        /* wood comes before forest due to 
         * the input file, not lexicographic order
         */
        new String[] { "lost", "in", "the", "woods", "wood" },
        new int[] { 0, 5, 8, 12, 12 },
        new int[] { 4, 7, 11, 17, 17 },
        new int[] { 1, 1, 1, 1, 0 });
  }
  
  public void testReusableTokenStream() throws Exception {
    SynonymMap map = new SynonymMap(new FileInputStream(testFile));
    /* limit to one synonym expansion */
    Analyzer analyzer = new SynonymWhitespaceAnalyzer(map, 1);
    assertAnalyzesToReuse(analyzer, "Lost in the woods",
        new String[] { "lost", "in", "the", "woods", "wood" },
        new int[] { 0, 5, 8, 12, 12 },
        new int[] { 4, 7, 11, 17, 17 },
        new int[] { 1, 1, 1, 1, 0 });
    assertAnalyzesToReuse(analyzer, "My wolfish dog went to the forest",
        new String[] { "my", "wolfish", "ravenous", "dog", "went", "to",
          "the", "forest", "woods" },
        new int[] { 0, 3, 3, 11, 15, 20, 23, 27, 27 },
        new int[] { 2, 10, 10, 14, 19, 22, 26, 33, 33 },
        new int[] { 1, 1, 0, 1, 1, 1, 1, 1, 0 });
  }
  
  private class SynonymWhitespaceAnalyzer extends Analyzer {
    private SynonymMap synonyms;
    private int maxSynonyms;
    
    public SynonymWhitespaceAnalyzer(SynonymMap synonyms, int maxSynonyms) {
      this.synonyms = synonyms;
      this.maxSynonyms = maxSynonyms;
    }
    
    public TokenStream tokenStream(String fieldName, Reader reader) {
      TokenStream ts = new WhitespaceTokenizer(reader);
      ts = new LowerCaseFilter(ts);
      ts = new SynonymTokenFilter(ts, synonyms, maxSynonyms);
      return ts;
    }
    
    private class SavedStreams {
      Tokenizer source;
      TokenStream result;
    };
    
    public TokenStream reusableTokenStream(String fieldName, Reader reader)
        throws IOException {
      SavedStreams streams = (SavedStreams) getPreviousTokenStream();
      if (streams == null) {
        streams = new SavedStreams();
        streams.source = new WhitespaceTokenizer(reader);
        streams.result = new LowerCaseFilter(streams.source);
        streams.result = new SynonymTokenFilter(streams.result, synonyms, maxSynonyms);
        setPreviousTokenStream(streams);
      } else {
        streams.source.reset(reader);
        streams.result.reset(); // reset the SynonymTokenFilter
      }
      return streams.result;
    }
  }
  
}
