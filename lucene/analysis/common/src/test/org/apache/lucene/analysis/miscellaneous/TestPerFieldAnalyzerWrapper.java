package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockCharFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Rethrow;

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

    Map<String,Analyzer> analyzerPerField =
        Collections.<String,Analyzer>singletonMap("special", new SimpleAnalyzer());

    PerFieldAnalyzerWrapper analyzer =
              new PerFieldAnalyzerWrapper(new WhitespaceAnalyzer(), analyzerPerField);

    TokenStream tokenStream = analyzer.tokenStream("field", text);
    try {
      CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
      tokenStream.reset();

      assertTrue(tokenStream.incrementToken());
      assertEquals("WhitespaceAnalyzer does not lowercase",
                 "Qwerty",
                 termAtt.toString());
      assertFalse(tokenStream.incrementToken());
      tokenStream.end();
    } finally {
      IOUtils.closeWhileHandlingException(tokenStream);
    }

    tokenStream = analyzer.tokenStream("special", text);
    try {
      CharTermAttribute termAtt = tokenStream.getAttribute(CharTermAttribute.class);
      tokenStream.reset();

      assertTrue(tokenStream.incrementToken());
      assertEquals("SimpleAnalyzer lowercases",
                 "qwerty",
                 termAtt.toString());
      assertFalse(tokenStream.incrementToken());
      tokenStream.end();
    } finally {
      IOUtils.closeWhileHandlingException(tokenStream);
    }
  }
  
  public void testReuseWrapped() throws Exception {
    final String text = "Qwerty";

    final Analyzer specialAnalyzer = new SimpleAnalyzer();
    final Analyzer defaultAnalyzer = new WhitespaceAnalyzer();

    TokenStream ts1, ts2, ts3, ts4;

    final PerFieldAnalyzerWrapper wrapper1 = new PerFieldAnalyzerWrapper(defaultAnalyzer,
        Collections.<String,Analyzer>singletonMap("special", specialAnalyzer));

    // test that the PerFieldWrapper returns the same instance as original Analyzer:
    ts1 = defaultAnalyzer.tokenStream("something", text);
    ts2 = wrapper1.tokenStream("something", text);
    assertSame(ts1, ts2);

    ts1 = specialAnalyzer.tokenStream("special", text);
    ts2 = wrapper1.tokenStream("special", text);
    assertSame(ts1, ts2);

    // Wrap with another wrapper, which does *not* extend DelegatingAnalyzerWrapper:
    final AnalyzerWrapper wrapper2 = new AnalyzerWrapper(wrapper1.getReuseStrategy()) {
      @Override
      protected Analyzer getWrappedAnalyzer(String fieldName) {
        return wrapper1;
      }

      @Override
      protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
        try {
          assertNotSame(specialAnalyzer.tokenStream("special", text), components.getTokenStream());
        } catch (IOException e) {
          Rethrow.rethrow(e);
        }
        TokenFilter filter = new ASCIIFoldingFilter(components.getTokenStream());
        return new TokenStreamComponents(components.getTokenizer(), filter);
      }
    };
    ts3 = wrapper2.tokenStream("special", text);
    assertNotSame(ts1, ts3);
    assertTrue(ts3 instanceof ASCIIFoldingFilter);
    // check that cache did not get corrumpted:
    ts2 = wrapper1.tokenStream("special", text);
    assertSame(ts1, ts2);
    
    // Wrap PerField with another PerField. In that case all TokenStreams returned must be the same:
    final PerFieldAnalyzerWrapper wrapper3 = new PerFieldAnalyzerWrapper(wrapper1,
        Collections.<String,Analyzer>singletonMap("moreSpecial", specialAnalyzer));
    ts1 = specialAnalyzer.tokenStream("special", text);
    ts2 = wrapper3.tokenStream("special", text);
    assertSame(ts1, ts2);
    ts3 = specialAnalyzer.tokenStream("moreSpecial", text);
    ts4 = wrapper3.tokenStream("moreSpecial", text);
    assertSame(ts3, ts4);
    assertSame(ts2, ts3);
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
