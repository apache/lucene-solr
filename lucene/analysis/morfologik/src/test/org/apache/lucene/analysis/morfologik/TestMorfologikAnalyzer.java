// -*- c-basic-offset: 2 -*-
package org.apache.lucene.analysis.morfologik;

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
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 * TODO: The tests below rely on the order of returned lemmas, which is probably not good. 
 */
public class TestMorfologikAnalyzer extends BaseTokenStreamTestCase {

  private Analyzer getTestAnalyzer() {
    return new MorfologikAnalyzer(TEST_VERSION_CURRENT);
  }

  /** Test stemming of single tokens with Morfologik library. */
  public final void testSingleTokens() throws IOException {
    Analyzer a = getTestAnalyzer();
    assertAnalyzesToReuse(a, "a", new String[] { "a" });
    assertAnalyzesToReuse(a, "liście", new String[] { "liść", "list", "lista", });
    assertAnalyzesToReuse(a, "danych", new String[] { "dany", "dane", "dać" });
    assertAnalyzesToReuse(a, "ęóąśłżźćń", new String[] { "ęóąśłżźćń" });
  }

  /** Test stemming of multiple tokens and proper term metrics. */
  public final void testMultipleTokens() throws IOException {
    Analyzer a = getTestAnalyzer();
    assertAnalyzesToReuse(
      a,
      "liście danych",
      new String[] { "liść", "list", "lista", "dany", "dane", "dać" },
      new int[] { 0, 0, 0, 7, 7, 7 },
      new int[] { 6, 6, 6, 13, 13, 13 },
      new int[] { 1, 0, 0, 1, 0, 0 });
  }

  /** Test reuse of MorfologikFilter with leftover stems. */
  public final void testLeftoverStems() throws IOException {
    Analyzer a = getTestAnalyzer();
    TokenStream ts_1 = a.tokenStream("dummy", new StringReader("liście"));
    CharTermAttribute termAtt_1 = ts_1.getAttribute(CharTermAttribute.class);
    ts_1.reset();
    ts_1.incrementToken();
    assertEquals("first stream", "liść", termAtt_1.toString());

    TokenStream ts_2 = a.tokenStream("dummy", new StringReader("danych"));
    CharTermAttribute termAtt_2 = ts_2.getAttribute(CharTermAttribute.class);
    ts_2.reset();
    ts_2.incrementToken();
    assertEquals("second stream", "dany", termAtt_2.toString());
  }

  /** Test stemming of mixed-case tokens. */
  public final void testCase() throws IOException {
    Analyzer a = getTestAnalyzer();

    assertAnalyzesToReuse(a, "AGD",      new String[] { "artykuły gospodarstwa domowego" });
    assertAnalyzesToReuse(a, "agd",      new String[] { "artykuły gospodarstwa domowego" });

    assertAnalyzesToReuse(a, "Poznania", new String[] { "Poznań" });
    assertAnalyzesToReuse(a, "poznania", new String[] { "poznać" });

    assertAnalyzesToReuse(a, "Aarona",   new String[] { "Aaron" });
    assertAnalyzesToReuse(a, "aarona",   new String[] { "aarona" });

    assertAnalyzesToReuse(a, "Liście",   new String[] { "liść", "list", "lista" });
  }

  private void assertPOSToken(TokenStream ts, String term, String pos) throws IOException {
    ts.incrementToken();
    assertEquals(term, ts.getAttribute(CharTermAttribute.class).toString());
    assertEquals(pos,  ts.getAttribute(MorphosyntacticTagAttribute.class).getTag().toString());
  }

  /** Test morphosyntactic annotations. */
  public final void testPOSAttribute() throws IOException {
    TokenStream ts = getTestAnalyzer().tokenStream("dummy", new StringReader("liście"));

    assertPOSToken(ts, "liść",  "subst:pl:acc.nom.voc:m3");
    assertPOSToken(ts, "list",  "subst:sg:loc.voc:m3");
    assertPOSToken(ts, "lista", "subst:sg:dat.loc:f");
  }
  
  /** blast some random strings through the analyzer */
  public void testRandom() throws Exception {
    checkRandomData(random(), getTestAnalyzer(), 10000 * RANDOM_MULTIPLIER); 
  }
}
