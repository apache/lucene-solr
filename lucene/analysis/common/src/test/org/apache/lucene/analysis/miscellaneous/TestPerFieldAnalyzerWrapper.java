package org.apache.lucene.analysis.miscellaneous;

import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

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

public class TestPerFieldAnalyzerWrapper extends BaseTokenStreamTestCase {
  public void testPerField() throws Exception {
    String text = "Qwerty";

    Map<String, Analyzer> analyzerPerField = new HashMap<String, Analyzer>();
    analyzerPerField.put("special", new SimpleAnalyzer(TEST_VERSION_CURRENT));

    PerFieldAnalyzerWrapper analyzer =
              new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(TEST_VERSION_CURRENT), analyzerPerField);

    TokenStream tokenStream = analyzer.tokenStream("field",
        new StringReader(text));
    CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.reset();

    assertTrue(tokenStream.incrementToken());
    assertEquals("WhitespaceAnalyzer does not lowercase",
                 "Qwerty",
                 termAtt.toString());

    tokenStream = analyzer.tokenStream("special",
        new StringReader(text));
    termAtt = tokenStream.getAttribute(CharTermAttribute.class);
    tokenStream.reset();

    assertTrue(tokenStream.incrementToken());
    assertEquals("SimpleAnalyzer lowercases",
                 "qwerty",
                 termAtt.toString());
  }
  
  public void testCharFilters() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        return new TokenStreamComponents(new MockTokenizer(reader));
      }

      @Override
      protected Reader initReader(String fieldName, Reader reader) {
        return new MockCharFilter(reader, 7);
      }
    };
    assertAnalyzesTo(a, "ab",
        new String[] { "aab" },
        new int[] { 0 },
        new int[] { 2 }
    );
    
    // now wrap in PFAW
    PerFieldAnalyzerWrapper p = new PerFieldAnalyzerWrapper(a, Collections.<String,Analyzer>emptyMap());
    
    assertAnalyzesTo(p, "ab",
        new String[] { "aab" },
        new int[] { 0 },
        new int[] { 2 }
    );
  }
}
