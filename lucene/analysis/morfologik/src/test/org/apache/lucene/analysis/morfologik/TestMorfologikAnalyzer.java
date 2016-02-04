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
package org.apache.lucene.analysis.morfologik;


import java.io.IOException;
import java.util.TreeSet;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * TODO: The tests below rely on the order of returned lemmas, which is probably not good. 
 */
public class TestMorfologikAnalyzer extends BaseTokenStreamTestCase {

  private Analyzer getTestAnalyzer() {
    return new MorfologikAnalyzer();
  }

  /** Test stemming of single tokens with Morfologik library. */
  public final void testSingleTokens() throws IOException {
    Analyzer a = getTestAnalyzer();
    assertAnalyzesTo(a, "a", new String[] { "a" });
    assertAnalyzesTo(a, "liście", new String[] { "liście", "liść", "list", "lista" });
    assertAnalyzesTo(a, "danych", new String[] { "dany", "dana", "dane", "dać" });
    assertAnalyzesTo(a, "ęóąśłżźćń", new String[] { "ęóąśłżźćń" });
    a.close();
  }

  /** Test stemming of multiple tokens and proper term metrics. */
  public final void testMultipleTokens() throws IOException {
    Analyzer a = getTestAnalyzer();
    assertAnalyzesTo(
      a,
      "liście danych",
      new String[] { "liście", "liść", "list", "lista", "dany", "dana", "dane", "dać" },
      new int[] { 0, 0, 0, 0, 7, 7, 7, 7 },
      new int[] { 6, 6, 6, 6, 13, 13, 13, 13 },
      new int[] { 1, 0, 0, 0, 1, 0, 0, 0 });

    assertAnalyzesTo(
        a,
        "T. Gl\u00FCcksberg",
        new String[] { "tom", "tona", "Gl\u00FCcksberg" },
        new int[] { 0, 0, 3  },
        new int[] { 1, 1, 13 },
        new int[] { 1, 0, 1  });
    a.close();
  }

  @SuppressWarnings("unused")
  private void dumpTokens(String input) throws IOException {
    try (Analyzer a = getTestAnalyzer();
        TokenStream ts = a.tokenStream("dummy", input)) {
      ts.reset();

      MorphosyntacticTagsAttribute attribute = ts.getAttribute(MorphosyntacticTagsAttribute.class);
      CharTermAttribute charTerm = ts.getAttribute(CharTermAttribute.class);
      while (ts.incrementToken()) {
        System.out.println(charTerm.toString() + " => " + attribute.getTags());
      }
      ts.end();
    }
  }

  /** Test reuse of MorfologikFilter with leftover stems. */
  public final void testLeftoverStems() throws IOException {
    Analyzer a = getTestAnalyzer();
    try (TokenStream ts_1 = a.tokenStream("dummy", "liście")) {
      CharTermAttribute termAtt_1 = ts_1.getAttribute(CharTermAttribute.class);
      ts_1.reset();
      ts_1.incrementToken();
      assertEquals("first stream", "liście", termAtt_1.toString());
      ts_1.end();
    }

    try (TokenStream ts_2 = a.tokenStream("dummy", "danych")) {
      CharTermAttribute termAtt_2 = ts_2.getAttribute(CharTermAttribute.class);
      ts_2.reset();
      ts_2.incrementToken();
      assertEquals("second stream", "dany", termAtt_2.toString());
      ts_2.end();
    }
    a.close();
  }

  /** Test stemming of mixed-case tokens. */
  public final void testCase() throws IOException {
    Analyzer a = getTestAnalyzer();

    assertAnalyzesTo(a, "AGD",      new String[] { "AGD", "artykuły gospodarstwa domowego" });
    assertAnalyzesTo(a, "agd",      new String[] { "artykuły gospodarstwa domowego" });

    assertAnalyzesTo(a, "Poznania", new String[] { "Poznań" });
    assertAnalyzesTo(a, "poznania", new String[] { "poznanie", "poznać" });

    assertAnalyzesTo(a, "Aarona",   new String[] { "Aaron" });
    assertAnalyzesTo(a, "aarona",   new String[] { "aarona" });

    assertAnalyzesTo(a, "Liście",   new String[] { "liście", "liść", "list", "lista" });
    a.close();
  }

  private void assertPOSToken(TokenStream ts, String term, String... tags) throws IOException {
    ts.incrementToken();
    assertEquals(term, ts.getAttribute(CharTermAttribute.class).toString());
    
    TreeSet<String> actual = new TreeSet<>();
    TreeSet<String> expected = new TreeSet<>();
    for (StringBuilder b : ts.getAttribute(MorphosyntacticTagsAttribute.class).getTags()) {
      actual.add(b.toString());
    }
    for (String s : tags) {
      expected.add(s);
    }
    
    if (!expected.equals(actual)) {
      System.out.println("Expected:\n" + expected);
      System.out.println("Actual:\n" + actual);
      assertEquals(expected, actual);
    }
  }

  /** Test morphosyntactic annotations. */
  public final void testPOSAttribute() throws IOException {
    try (Analyzer a = getTestAnalyzer();
         TokenStream ts = a.tokenStream("dummy", "liście")) {
      ts.reset();
      assertPOSToken(ts, "liście",  
        "subst:sg:acc:n2",
        "subst:sg:nom:n2",
        "subst:sg:voc:n2");

      assertPOSToken(ts, "liść",  
        "subst:pl:acc:m3",
        "subst:pl:nom:m3",
        "subst:pl:voc:m3");

      assertPOSToken(ts, "list",  
        "subst:sg:loc:m3",
        "subst:sg:voc:m3");

      assertPOSToken(ts, "lista", 
        "subst:sg:dat:f",
        "subst:sg:loc:f");
      ts.end();
    }
  }

  /** */
  public final void testKeywordAttrTokens() throws IOException {
    Analyzer a = new MorfologikAnalyzer() {
      @Override
      protected TokenStreamComponents createComponents(String field) {
        final CharArraySet keywords = new CharArraySet(1, false);
        keywords.add("liście");

        final Tokenizer src = new StandardTokenizer();
        TokenStream result = new StandardFilter(src);
        result = new SetKeywordMarkerFilter(result, keywords);
        result = new MorfologikFilter(result); 

        return new TokenStreamComponents(src, result);
      }
    };

    assertAnalyzesTo(
      a,
      "liście danych",
      new String[] { "liście", "dany", "dana", "dane", "dać" },
      new int[] { 0, 7, 7, 7, 7 },
      new int[] { 6, 13, 13, 13, 13 },
      new int[] { 1, 1, 0, 0, 0 });
    a.close();
  }

  /** blast some random strings through the analyzer */
  public void testRandom() throws Exception {
    Analyzer a = getTestAnalyzer();
    checkRandomData(random(), a, 1000 * RANDOM_MULTIPLIER);
    a.close();
  }
}
