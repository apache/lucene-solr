package org.apache.lucene.analysis.cz;

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

import java.io.StringReader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

/**
 * Test the CzechAnalyzer
 * 
 * CzechAnalyzer is like a StandardAnalyzer with a custom stopword list.
 *
 */
public class TestCzechAnalyzer extends TestCase {

  public void testStopWord() throws Exception {
    assertAnalyzesTo(new CzechAnalyzer(), "Pokud mluvime o volnem", new String[] { "mluvime", "volnem" });
  }

  private void assertAnalyzesTo(Analyzer a, String input, String[] output) throws Exception {
    TokenStream ts = a.tokenStream("dummy", new StringReader(input));
    TermAttribute text = (TermAttribute) ts.getAttribute(TermAttribute.class);
    for (int i=0; i<output.length; i++) {
      assertTrue(ts.incrementToken());
      assertEquals(text.term(), output[i]);
    }
    assertFalse(ts.incrementToken());
    ts.close();
  }
}
