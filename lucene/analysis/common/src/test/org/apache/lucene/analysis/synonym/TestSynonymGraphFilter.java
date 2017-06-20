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

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockGraphTokenFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.FlattenGraphFilter;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonTestUtil;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.Util;

public class TestSynonymGraphFilter extends BaseTokenStreamTestCase {

  /** Set as a side effect by {@link #getAnalyzer} and {@link #getFlattenAnalyzer}. */
  private SynonymGraphFilter synFilter;
  private FlattenGraphFilter flattenFilter;

  public void testBasicKeepOrigOneOutput() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x", true);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a b",
                     new String[] {"c", "x", "a", "b"},
                     new int[]    { 0,   2,   2,   4},
                     new int[]    { 1,   5,   3,   5},
                     new String[] {"word", "SYNONYM", "word", "word"},
                     new int[]    { 1,   1,   0,   1},
                     new int[]    { 1,   2,   1,   1});
    a.close();
  }

  public void testMixedKeepOrig() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x", true);
    add(b, "e f", "y", false);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a b c e f g",
                     new String[] {"c", "x", "a", "b", "c", "y", "g"},
                     new int[]    { 0,   2,   2,   4,   6,   8,   12},
                     new int[]    { 1,   5,   3,   5,   7,   11,  13},
                     new String[] {"word", "SYNONYM", "word", "word", "word", "SYNONYM", "word"},
                     new int[]    { 1,   1,   0,   1,   1,   1,   1},
                     new int[]    { 1,   2,   1,   1,   1,   1,   1});
    a.close();
  }

  public void testNoParseAfterBuffer() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "b a", "x", true);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "b b b",
                     new String[] {"b", "b", "b"},
                     new int[]    { 0,   2,   4},
                     new int[]    { 1,   3,   5},
                     new String[] {"word", "word", "word"},
                     new int[]    { 1,   1,   1},
                     new int[]    { 1,   1,   1});
    a.close();
  }

  public void testOneInputMultipleOutputKeepOrig() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x", true);
    add(b, "a b", "y", true);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a b c",
                     new String[] {"c", "x", "y",  "a", "b", "c"},
                     new int[]    { 0,   2,   2,   2,   4,   6},
                     new int[]    { 1,   5,   5,   3,   5,   7},
                     new String[] {"word", "SYNONYM", "SYNONYM", "word", "word", "word"},
                     new int[]    { 1,   1,   0,   0,   1,   1,   1,   1},
                     new int[]    { 1,   2,   2,   1,   1,   1,   1,   1});
    a.close();
  }

  /**
   * Verify type of token and positionLength after analyzer.
   */
  public void testPositionLengthAndTypeSimple() throws Exception {
    String testFile =
        "spider man, spiderman";

    Analyzer analyzer = solrSynsToAnalyzer(testFile);

    assertAnalyzesToPositions(analyzer, "spider man",
        new String[]{"spiderman", "spider", "man"},
        new String[]{"SYNONYM", "word", "word"},
        new int[]{1, 0, 1},
        new int[]{2, 1, 1});
  }

  /**
   * parse a syn file with some escaped syntax chars
   */
  public void testEscapedStuff() throws Exception {
    String testFile =
        "a\\=>a => b\\=>b\n" +
            "a\\,a => b\\,b";
    Analyzer analyzer = solrSynsToAnalyzer(testFile);

    assertAnalyzesTo(analyzer, "ball",
        new String[]{"ball"},
        new int[]{1});

    assertAnalyzesTo(analyzer, "a=>a",
        new String[]{"b=>b"},
        new int[]{1});

    assertAnalyzesTo(analyzer, "a,a",
                     new String[]{"b,b"},
                     new int[]{1});
    analyzer.close();
  }

  /**
   * parse a syn file with bad syntax
   */
  public void testInvalidAnalyzesToNothingOutput() throws Exception {
    String testFile = "a => 1";
    Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.SIMPLE, false);
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
    try {
      parser.parse(new StringReader(testFile));
      fail("didn't get expected exception");
    } catch (ParseException expected) {
      // expected exc
    }
    analyzer.close();
  }

  /**
   * parse a syn file with bad syntax
   */
  public void testInvalidDoubleMap() throws Exception {
    String testFile = "a => b => c";
    Analyzer analyzer = new MockAnalyzer(random());
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
    try {
      parser.parse(new StringReader(testFile));
      fail("didn't get expected exception");
    } catch (ParseException expected) {
     // expected exc
    }
    analyzer.close();
  }

  /**
   * Tests some simple examples from the solr wiki
   */
  public void testSimple() throws Exception {
    String testFile =
        "i-pod, ipod, ipoooood\n" +
            "foo => foo bar\n" +
            "foo => baz\n" +
            "this test, that testing";

    Analyzer analyzer = solrSynsToAnalyzer(testFile);

    assertAnalyzesTo(analyzer, "ball",
        new String[]{"ball"},
        new int[]{1});

    assertAnalyzesTo(analyzer, "i-pod",
                     new String[]{"ipod", "ipoooood", "i-pod"},
                     new int[]{1, 0, 0});

    assertAnalyzesTo(analyzer, "foo",
        new String[]{"foo", "baz", "bar"},
        new int[]{1, 0, 1});

    assertAnalyzesTo(analyzer, "this test",
        new String[]{"that", "this", "testing", "test"},
        new int[]{1, 0, 1, 0});
    analyzer.close();
  }

  public void testBufferLength() throws Exception {
    String testFile =
        "c => 8 2 5 6 7\n" +
            "f c e d f, 1\n" +
            "c g a f d, 6 5 5\n" +
            "e c => 4\n" +
            "g => 5\n" +
            "a g b f e => 5 0 7 7\n" +
            "b => 1";
    Analyzer analyzer = solrSynsToAnalyzer(testFile);

    String doc = "b c g a f b d";
    String[] expected = new String[]{"1", "8", "2", "5", "6", "7", "5", "a", "f", "1", "d"};
    assertAnalyzesTo(analyzer, doc, expected);
  }

  private Analyzer solrSynsToAnalyzer(String syns) throws IOException, ParseException {
    Analyzer analyzer = new MockAnalyzer(random());
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);
    parser.parse(new StringReader(syns));
    analyzer.close();
    return getFlattenAnalyzer(parser, true);
  }

  public void testMoreThanOneLookAhead() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b c d", "x", true);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "a b c e",
                     new String[] {"a", "b", "c", "e"},
                     new int[]    { 0,   2,   4,   6},
                     new int[]    { 1,   3,   5,   7},
                     new String[] {"word", "word", "word", "word"},
                     new int[]    { 1,   1,   1,   1},
                     new int[]    { 1,   1,   1,   1});
    a.close();
  }

  public void testLookaheadAfterParse() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "b b", "x", true);
    add(b, "b", "y", true);

    Analyzer a = getAnalyzer(b, true);

    assertAnalyzesTo(a, "b a b b",
                     new String[] {"y", "b", "a", "x", "b", "b"},
                     new int[]    {0,    0,   2,   4,   4,   6},  
                     new int[]    {1,    1,   3,   7,   5,   7},  
                     null,
                     new int[]    {1,    0,   1,   1,   0,   1},  
                     new int[]    {1,    1,   1,   2,   1,   1},
                     true);
  }

  public void testLookaheadSecondParse() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "b b b", "x", true);
    add(b, "b", "y", true);

    Analyzer a = getAnalyzer(b, true);

    assertAnalyzesTo(a, "b b",
                     new String[] {"y", "b", "y", "b"},
                     new int[]    { 0,   0,   2,   2},  
                     new int[]    { 1,   1,   3,   3},  
                     null,
                     new int[]    { 1,    0,   1,   0},  
                     new int[]    { 1,    1,   1,   1},
                     true);
  }

  public void testOneInputMultipleOutputNoKeepOrig() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x", false);
    add(b, "a b", "y", false);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a b c",
                     new String[] {"c", "x", "y", "c"},
                     new int[]    { 0,   2,   2,   6},
                     new int[]    { 1,   5,   5,   7},
                     new String[] {"word", "SYNONYM", "SYNONYM", "word"},
                     new int[]    { 1,   1,   0,   1},
                     new int[]    { 1,   1,   1,   1});
    a.close();
  }

  public void testOneInputMultipleOutputMixedKeepOrig() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x", true);
    add(b, "a b", "y", false);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a b c",
                     new String[] {"c", "x", "y",  "a", "b", "c"},
                     new int[]    { 0,   2,   2,   2,   4,   6},
                     new int[]    { 1,   5,   5,   3,   5,   7},
                     new String[] {"word", "SYNONYM", "SYNONYM", "word", "word", "word"},
                     new int[]    { 1,   1,   0,   0,   1,   1,   1,   1},
                     new int[]    { 1,   2,   2,   1,   1,   1,   1,   1});
    a.close();
  }

  public void testSynAtEnd() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x", true);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c d e a b",
                     new String[] {"c", "d", "e", "x", "a", "b"},
                     new int[]    { 0,   2,   4,   6,   6,   8},
                     new int[]    { 1,   3,   5,   9,   7,   9},
                     new String[] {"word", "word", "word", "SYNONYM", "word", "word"},
                     new int[]    { 1,   1,   1,   1,   0,   1},
                     new int[]    { 1,   1,   1,   2,   1,   1});
    a.close();
  }

  public void testTwoSynsInARow() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a", "x", false);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a a b",
                     new String[] {"c", "x", "x", "b"},
                     new int[]    { 0,   2,   4,   6},
                     new int[]    { 1,   3,   5,   7},
                     new String[] {"word", "SYNONYM", "SYNONYM", "word"},
                     new int[]    { 1,   1,   1,   1},
                     new int[]    { 1,   1,   1,   1});
    a.close();
  }

  public void testBasicKeepOrigTwoOutputs() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x y", true);
    add(b, "a b", "m n o", true);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a b d",
                     new String[] {"c", "x", "m", "a", "y", "n", "o", "b", "d"},
                     new int[]    { 0,   2,   2,   2,   2,   2,   2,   4,   6},
                     new int[]    { 1,   5,   5,   3,   5,   5,   5,   5,   7},
                     new String[] {"word", "SYNONYM", "SYNONYM", "word", "SYNONYM", "SYNONYM", "SYNONYM", "word", "word"},
                     new int[]    { 1,   1,   0,   0,   1,   1,   1,   1,   1},
                     new int[]    { 1,   1,   2,   4,   4,   1,   2,   1,   1});
    a.close();
  }

  public void testNoCaptureIfNoMatch() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x y", true);

    Analyzer a = getAnalyzer(b, true);

    assertAnalyzesTo(a,
                     "c d d",
                     new String[] {"c", "d", "d"},
                     new int[]    { 0,   2,   4},
                     new int[]    { 1,   3,   5},
                     new String[] {"word", "word", "word"},
                     new int[]    { 1,   1,   1},
                     new int[]    { 1,   1,   1});
    assertEquals(0, synFilter.getCaptureCount());
    a.close();
  }

  public void testBasicNotKeepOrigOneOutput() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x", false);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a b",
                     new String[] {"c", "x"},
                     new int[] {0, 2},
                     new int[] {1, 5},
                     new String[] {"word", "SYNONYM"},
                     new int[] {1, 1},
                     new int[] {1, 1});
    a.close();
  }

  public void testBasicNoKeepOrigTwoOutputs() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x y", false);
    add(b, "a b", "m n o", false);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a b d",
                     new String[] {"c", "x", "m", "y", "n", "o", "d"},
                     new int[]    { 0,   2,   2,   2,   2,   2,   6},
                     new int[]    { 1,   5,   5,   5,   5,   5,   7},
                     new String[] {"word", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "word"},
                     new int[]    { 1,   1,   0,   1,   1,   1,   1},
                     new int[]    { 1,   1,   2,   3,   1,   1,   1});
    a.close();
  }

  public void testIgnoreCase() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x y", false);
    add(b, "a b", "m n o", false);
    
    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c A B D",
                     new String[] {"c", "x", "m", "y", "n", "o", "D"},
                     new int[]    { 0,   2,   2,   2,   2,   2,   6},
                     new int[]    { 1,   5,   5,   5,   5,   5,   7},
                     new String[] {"word", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "word"},
                     new int[]    { 1,   1,   0,   1,   1,   1,   1},
                     new int[]    { 1,   1,   2,   3,   1,   1,   1});
    a.close();
  }

  public void testDoNotIgnoreCase() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x y", false);
    add(b, "a b", "m n o", false);
    
    Analyzer a = getAnalyzer(b, false);
    assertAnalyzesTo(a,
                     "c A B D",
                     new String[] {"c", "A", "B", "D"},
                     new int[]    { 0,   2,   4,   6},
                     new int[]    { 1,   3,   5,   7},
                     new String[] {"word", "word", "word", "word"},
                     new int[]    { 1,   1,   1,   1},
                     new int[]    { 1,   1,   1,   1});
    a.close();
  }

  public void testBufferedFinish1() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b c", "m n o", false);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a b",
                     new String[] {"c", "a", "b"},
                     new int[]    { 0,   2,   4},
                     new int[]    { 1,   3,   5},
                     new String[] {"word", "word", "word"},
                     new int[]    { 1,   1,   1},
                     new int[]    { 1,   1,   1});
    a.close();
  }

  public void testBufferedFinish2() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "m n o", false);
    add(b, "d e", "m n o", false);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "c a d",
                     new String[] {"c", "a", "d"},
                     new int[]    { 0,   2,   4},
                     new int[]    { 1,   3,   5},
                     new String[] {"word", "word", "word"},
                     new int[]    { 1,   1,   1},
                     new int[]    { 1,   1,   1});
    a.close();
  }

  public void testCanReuse() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b", "x", true);
    Analyzer a = getAnalyzer(b, true);
    for(int i=0;i<10;i++) {
      assertAnalyzesTo(a,
                       "c a b",
                       new String[] {"c", "x", "a", "b"},
                       new int[]    { 0,   2,   2,   4},
                       new int[]    { 1,   5,   3,   5},
                       new String[] {"word", "SYNONYM", "word", "word"},
                       new int[]    { 1,   1,   0,   1},
                       new int[]    { 1,   2,   1,   1});
    }
    a.close();
  }

  /** Multiple input tokens map to a single output token */
  public void testManyToOne() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b c", "z", true);

    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "a b c d",
                     new String[] {"z", "a", "b", "c", "d"},
                     new int[]    { 0,   0,   2,   4,   6},
                     new int[]    { 5,   1,   3,   5,   7},
                     new String[] {"SYNONYM", "word", "word", "word", "word"},
                     new int[]    { 1,   0,   1,   1,   1},
                     new int[]    { 3,   1,   1,   1,   1});
    a.close();
  }
  
  public void testBufferAfterMatch() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "a b c d", "x", true);
    add(b, "a b", "y", false);

    // The 'c' token has to be buffered because SynGraphFilter
    // needs to know whether a b c d -> x matches:
    Analyzer a = getAnalyzer(b, true);
    assertAnalyzesTo(a,
                     "f a b c e",
                     new String[] {"f", "y", "c", "e"},
                     new int[]    { 0,   2,   6,   8},
                     new int[]    { 1,   5,   7,   9},
                     new String[] {"word", "SYNONYM", "word", "word"},
                     new int[]    { 1,   1,   1,   1},
                     new int[]    { 1,   1,   1,   1});
    a.close();
  }

  public void testZeroSyns() throws Exception {
    Tokenizer tokenizer = new MockTokenizer();
    tokenizer.setReader(new StringReader("aa bb"));
    try {
      new SynonymGraphFilter(tokenizer, new SynonymMap.Builder(true).build(), true);
      fail("did not hit expected exception");
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("fst must be non-null", iae.getMessage());
    }
  }

  public void testOutputHangsOffEnd() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = false;
    // b hangs off the end (no input token under it):
    add(b, "a", "a b", keepOrig);
    Analyzer a = getFlattenAnalyzer(b, true);
    assertAnalyzesTo(a, "a",
                     new String[] {"a", "b"},
                     new int[]    { 0,   0},  
                     new int[]    { 1,   1},
                     null,
                     new int[]    { 1,   1},
                     new int[]    { 1,   1},
                     true);
    a.close();
  }

  public void testDedup() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = false;
    add(b, "a b", "ab", keepOrig);
    add(b, "a b", "ab", keepOrig);
    add(b, "a b", "ab", keepOrig);
    Analyzer a = getFlattenAnalyzer(b, true);

    assertAnalyzesTo(a, "a b",
        new String[]{"ab"},
        new int[]{1});
    a.close();
  }

  public void testNoDedup() throws Exception {
    // dedup is false:
    SynonymMap.Builder b = new SynonymMap.Builder(false);
    final boolean keepOrig = false;
    add(b, "a b", "ab", keepOrig);
    add(b, "a b", "ab", keepOrig);
    add(b, "a b", "ab", keepOrig);
    Analyzer a = getFlattenAnalyzer(b, true);

    assertAnalyzesTo(a, "a b",
        new String[]{"ab", "ab", "ab"},
        new int[]{1, 0, 0});
    a.close();
  }

  public void testMatching() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = false;
    add(b, "a b", "ab", keepOrig);
    add(b, "a c", "ac", keepOrig);
    add(b, "a", "aa", keepOrig);
    add(b, "b", "bb", keepOrig);
    add(b, "z x c v", "zxcv", keepOrig);
    add(b, "x c", "xc", keepOrig);

    Analyzer a = getFlattenAnalyzer(b, true);

    checkOneTerm(a, "$", "$");
    checkOneTerm(a, "a", "aa");
    checkOneTerm(a, "b", "bb");

    assertAnalyzesTo(a, "a $",
        new String[]{"aa", "$"},
        new int[]{1, 1});

    assertAnalyzesTo(a, "$ a",
        new String[]{"$", "aa"},
        new int[]{1, 1});

    assertAnalyzesTo(a, "a a",
        new String[]{"aa", "aa"},
        new int[]{1, 1});

    assertAnalyzesTo(a, "z x c v",
        new String[]{"zxcv"},
        new int[]{1});

    assertAnalyzesTo(a, "z x c $",
        new String[]{"z", "xc", "$"},
        new int[]{1, 1, 1});
    a.close();
  }

  public void testBasic1() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    add(b, "a", "foo", true);
    add(b, "a b", "bar fee", true);
    add(b, "b c", "dog collar", true);
    add(b, "c d", "dog harness holder extras", true);
    add(b, "m c e", "dog barks loudly", false);
    add(b, "i j k", "feep", true);

    add(b, "e f", "foo bar", false);
    add(b, "e f", "baz bee", false);

    add(b, "z", "boo", false);
    add(b, "y", "bee", true);
    Analyzer a = getFlattenAnalyzer(b, true);

    assertAnalyzesTo(a, "a b c",
                     new String[] {"bar", "a", "fee", "b", "c"},
                     new int[] {1, 0, 1, 0, 1});

    assertAnalyzesTo(a, "x a b c d",
                     new String[] {"x", "bar", "a", "fee", "b", "dog", "c", "harness", "d", "holder", "extras"},
                     new int[] {1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 1});

    assertAnalyzesTo(a, "a b a",
                     new String[] {"bar", "a", "fee", "b", "foo", "a"},
                     new int[] {1, 0, 1, 0, 1, 0});

    // outputs no longer add to one another:
    assertAnalyzesTo(a, "c d c d",
                     new String[] {"dog", "c", "harness", "d", "holder", "extras", "dog", "c", "harness", "d", "holder", "extras"},
                     new int[] {1, 0, 1, 0, 1, 1, 1, 0, 1, 0, 1, 1});

    // two outputs for same input
    assertAnalyzesTo(a, "e f",
                     new String[] {"foo", "baz", "bar", "bee"},
                     new int[] {1, 0, 1, 0});

    // verify multi-word / single-output offsets:
    assertAnalyzesTo(a, "g i j k g",
                     new String[] {"g", "feep", "i", "j", "k", "g"},
                     new int[] {1, 1, 0, 1, 1, 1});

    // mixed keepOrig true/false:
    assertAnalyzesTo(a, "a m c e x",
                     new String[] {"foo", "a", "dog", "barks", "loudly", "x"},
                     new int[] {1, 0, 1, 1, 1, 1});
    assertAnalyzesTo(a, "c d m c e x",
                     new String[] {"dog", "c", "harness", "d", "holder", "extras", "dog", "barks", "loudly","x"},
                     new int[] {1, 0, 1, 0, 1, 1, 1, 1, 1, 1});
    assertTrue(synFilter.getCaptureCount() > 0);

    // no captureStates when no syns matched
    assertAnalyzesTo(a, "p q r s t",
                     new String[] {"p", "q", "r", "s", "t"},
                     new int[] {1, 1, 1, 1, 1});
    assertEquals(0, synFilter.getCaptureCount());

    // captureStates are necessary for the single-token syn case:
    assertAnalyzesTo(a, "p q z y t",
                     new String[] {"p", "q", "boo", "bee", "y", "t"},
                     new int[] {1, 1, 1, 1, 0, 1});
    assertTrue(synFilter.getCaptureCount() > 0);
  }

  public void testBasic2() throws Exception {
    boolean keepOrig = true;
    do {
      keepOrig = !keepOrig;

      SynonymMap.Builder b = new SynonymMap.Builder(true);
      add(b,"aaa", "aaaa1 aaaa2 aaaa3", keepOrig);
      add(b, "bbb", "bbbb1 bbbb2", keepOrig);
      Analyzer a = getFlattenAnalyzer(b, true);

      if (keepOrig) {
        assertAnalyzesTo(a, "xyzzy bbb pot of gold",
                         new String[] {"xyzzy", "bbbb1", "bbb", "bbbb2", "pot", "of", "gold"},
                         new int[] {1, 1, 0, 1, 1, 1, 1});
        assertAnalyzesTo(a, "xyzzy aaa pot of gold",
                         new String[] {"xyzzy", "aaaa1", "aaa", "aaaa2", "aaaa2", "pot", "of", "gold"},
                         new int[] {1, 1, 0, 1, 1, 1, 1, 1});
      } else {
        assertAnalyzesTo(a, "xyzzy bbb pot of gold",
                         new String[] {"xyzzy", "bbbb1", "bbbb2", "pot", "of", "gold"},
                         new int[] {1, 1, 1, 1, 1, 1});
        assertAnalyzesTo(a, "xyzzy aaa pot of gold",
                         new String[] {"xyzzy", "aaaa1", "aaaa2", "aaaa3", "pot", "of", "gold"},
                         new int[] {1, 1, 1, 1, 1, 1, 1});
      }
    } while (keepOrig);
  }

  /** If we expand synonyms during indexing, it's a bit better than
   *  SynonymFilter is today, but still necessarily has false
   *  positive and negative PhraseQuery matches because we do not  
   *  index posLength, so we lose information. */
  public void testFlattenedGraph() throws Exception {

    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "wtf", "what the fudge", true);

    Analyzer a = getFlattenAnalyzer(b, true);

    assertAnalyzesTo(a, "wtf happened",
                     new String[] {"what", "wtf", "the", "fudge", "happened"},
                     new int[]    {    0,     0,      0,     0,       4},  
                     new int[]    {    3,     3,      3,     3,       12},
                     null,
                     new int[]    {    1,     0,      1,     1,       1},
                     new int[]    {    1,     3,      1,     1,       1},
                     true);

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, a);
    Document doc = new Document();
    doc.add(newTextField("field", "wtf happened", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);

    // Good (this should not match, and doesn't):
    assertEquals(0, s.count(new PhraseQuery("field", "what", "happened")));

    // Bad (this should match, but doesn't):
    assertEquals(0, s.count(new PhraseQuery("field", "wtf", "happened")));

    // Good (this should match, and does):
    assertEquals(1, s.count(new PhraseQuery("field", "what", "the", "fudge", "happened")));

    // Bad (this should not match, but does):
    assertEquals(1, s.count(new PhraseQuery("field", "wtf", "the")));

    IOUtils.close(r, dir);
  }

  // Needs TermAutomatonQuery, which is in sandbox still:
  /*
  public void testAccurateGraphQuery1() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "wtf happened", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);

    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "what the fudge", "wtf", true);

    SynonymMap map = b.build();

    TokenStreamToTermAutomatonQuery ts2q = new TokenStreamToTermAutomatonQuery();

    TokenStream in = new CannedTokenStream(0, 23, new Token[] {
        token("what", 1, 1, 0, 4),
        token("the", 1, 1, 5, 8),
        token("fudge", 1, 1, 9, 14),
        token("happened", 1, 1, 15, 23),
      });

    assertEquals(1, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

    in = new CannedTokenStream(0, 12, new Token[] {
        token("wtf", 1, 1, 0, 3),
        token("happened", 1, 1, 4, 12),
      });

    assertEquals(1, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

    // "what happened" should NOT match:
    in = new CannedTokenStream(0, 13, new Token[] {
        token("what", 1, 1, 0, 4),
        token("happened", 1, 1, 5, 13),
      });
    assertEquals(0, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

    IOUtils.close(r, dir);
  }
  */

  /** If we expand synonyms at search time, the results are correct. */
  // Needs TermAutomatonQuery, which is in sandbox still:
  /*
  public void testAccurateGraphQuery2() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "say wtf happened", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);

    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "what the fudge", "wtf", true);

    SynonymMap map = b.build();

    TokenStream in = new CannedTokenStream(0, 26, new Token[] {
        token("say", 1, 1, 0, 3),
        token("what", 1, 1, 3, 7),
        token("the", 1, 1, 8, 11),
        token("fudge", 1, 1, 12, 17),
        token("happened", 1, 1, 18, 26),
      });

    TokenStreamToTermAutomatonQuery ts2q = new TokenStreamToTermAutomatonQuery();

    assertEquals(1, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

    // "what happened" should NOT match:
    in = new CannedTokenStream(0, 13, new Token[] {
        token("what", 1, 1, 0, 4),
        token("happened", 1, 1, 5, 13),
      });
    assertEquals(0, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

    IOUtils.close(r, dir);
  }
  */

  // Needs TermAutomatonQuery, which is in sandbox still:
  /*
  public void testAccurateGraphQuery3() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(newTextField("field", "say what the fudge happened", Field.Store.NO));
    w.addDocument(doc);
    IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r);

    SynonymMap.Builder b = new SynonymMap.Builder();
    add(b, "wtf", "what the fudge", true);

    SynonymMap map = b.build();

    TokenStream in = new CannedTokenStream(0, 15, new Token[] {
        token("say", 1, 1, 0, 3),
        token("wtf", 1, 1, 3, 6),
        token("happened", 1, 1, 7, 15),
      });

    TokenStreamToTermAutomatonQuery ts2q = new TokenStreamToTermAutomatonQuery();

    assertEquals(1, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

    // "what happened" should NOT match:
    in = new CannedTokenStream(0, 13, new Token[] {
        token("what", 1, 1, 0, 4),
        token("happened", 1, 1, 5, 13),
      });
    assertEquals(0, s.count(ts2q.toQuery("field", new SynonymGraphFilter(in, map, true))));

    IOUtils.close(r, dir);
  }

  private static Token token(String term, int posInc, int posLength, int startOffset, int endOffset) {
    final Token t = new Token(term, startOffset, endOffset);
    t.setPositionIncrement(posInc);
    t.setPositionLength(posLength);
    return t;
  }
  */

  private String randomNonEmptyString() {
    while(true) {
      String s = TestUtil.randomUnicodeString(random()).trim();
      //String s = TestUtil.randomSimpleString(random()).trim();
      if (s.length() != 0 && s.indexOf('\u0000') == -1) {
        return s;
      }
    }
  }

  // Adds MockGraphTokenFilter after SynFilter:
  public void testRandomGraphAfter() throws Exception {
    final int numIters = atLeast(3);
    for (int i = 0; i < numIters; i++) {
      SynonymMap.Builder b = new SynonymMap.Builder(random().nextBoolean());
      final int numEntries = atLeast(10);
      for (int j = 0; j < numEntries; j++) {
        add(b, randomNonEmptyString(), randomNonEmptyString(), random().nextBoolean());
      }
      final SynonymMap map = b.build();
      final boolean ignoreCase = random().nextBoolean();
      final boolean doFlatten = random().nextBoolean();
      
      final Analyzer analyzer = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.SIMPLE, true);
          TokenStream syns = new SynonymGraphFilter(tokenizer, map, ignoreCase);
          TokenStream graph = new MockGraphTokenFilter(random(), syns);
          if (doFlatten) {
            graph = new FlattenGraphFilter(graph);
          }
          return new TokenStreamComponents(tokenizer, graph);
        }
      };

      checkRandomData(random(), analyzer, 100);
      analyzer.close();
    }
  }

  public void testEmptyStringInput() throws IOException {
    final int numIters = atLeast(10);
    for (int i = 0; i < numIters; i++) {
      SynonymMap.Builder b = new SynonymMap.Builder(random().nextBoolean());
      final int numEntries = atLeast(10);
      for (int j = 0; j < numEntries; j++) {
        add(b, randomNonEmptyString(), randomNonEmptyString(), random().nextBoolean());
      }
      final boolean ignoreCase = random().nextBoolean();

      Analyzer analyzer = getAnalyzer(b, ignoreCase);

      checkAnalysisConsistency(random(), analyzer, random().nextBoolean(), "");
      analyzer.close();
    }
  }

  /** simple random test, doesn't verify correctness.
   *  does verify it doesnt throw exceptions, or that the stream doesn't misbehave
   */
  public void testRandom2() throws Exception {
    final int numIters = atLeast(3);
    for (int i = 0; i < numIters; i++) {
      SynonymMap.Builder b = new SynonymMap.Builder(random().nextBoolean());
      final int numEntries = atLeast(10);
      for (int j = 0; j < numEntries; j++) {
        add(b, randomNonEmptyString(), randomNonEmptyString(), random().nextBoolean());
      }
      final boolean ignoreCase = random().nextBoolean();
      final boolean doFlatten = random().nextBoolean();

      Analyzer analyzer;
      if (doFlatten) {
        analyzer = getFlattenAnalyzer(b, ignoreCase);
      } else {
        analyzer = getAnalyzer(b, ignoreCase);
      }

      checkRandomData(random(), analyzer, 100);
      analyzer.close();
    }
  }

  /** simple random test like testRandom2, but for larger docs
   */
  public void testRandomHuge() throws Exception {
    final int numIters = atLeast(3);
    for (int i = 0; i < numIters; i++) {
      SynonymMap.Builder b = new SynonymMap.Builder(random().nextBoolean());
      final int numEntries = atLeast(10);
      if (VERBOSE) {
        System.out.println("TEST: iter=" + i + " numEntries=" + numEntries);
      }
      for (int j = 0; j < numEntries; j++) {
        add(b, randomNonEmptyString(), randomNonEmptyString(), random().nextBoolean());
      }
      final boolean ignoreCase = random().nextBoolean();
      final boolean doFlatten = random().nextBoolean();
      
      Analyzer analyzer;
      if (doFlatten) {
        analyzer = getFlattenAnalyzer(b, ignoreCase);
      } else {
        analyzer = getAnalyzer(b, ignoreCase);
      }

      checkRandomData(random(), analyzer, 100, 1024);
      analyzer.close();
    }
  }

  public void testEmptyTerm() throws IOException {
    final int numIters = atLeast(10);
    for (int i = 0; i < numIters; i++) {
      SynonymMap.Builder b = new SynonymMap.Builder(random().nextBoolean());
      final int numEntries = atLeast(10);
      for (int j = 0; j < numEntries; j++) {
        add(b, randomNonEmptyString(), randomNonEmptyString(), random().nextBoolean());
      }
      final boolean ignoreCase = random().nextBoolean();
      
      final Analyzer analyzer = getAnalyzer(b, ignoreCase);

      checkAnalysisConsistency(random(), analyzer, random().nextBoolean(), "");
      analyzer.close();
    }
  }

  // LUCENE-3375
  public void testVanishingTermsNoFlatten() throws Exception {
    String testFile = 
      "aaa => aaaa1 aaaa2 aaaa3\n" + 
      "bbb => bbbb1 bbbb2\n";
    Analyzer analyzer = solrSynsToAnalyzer(testFile);

    assertAnalyzesTo(analyzer, "xyzzy bbb pot of gold",
                     new String[] { "xyzzy", "bbbb1", "bbbb2", "pot", "of", "gold" });
    
    // xyzzy aaa pot of gold -> xyzzy aaaa1 aaaa2 aaaa3 gold
    assertAnalyzesTo(analyzer, "xyzzy aaa pot of gold",
                     new String[] { "xyzzy", "aaaa1", "aaaa2", "aaaa3", "pot", "of", "gold" });
    analyzer.close();
  }

  // LUCENE-3375
  public void testVanishingTermsWithFlatten() throws Exception {
    String testFile = 
      "aaa => aaaa1 aaaa2 aaaa3\n" + 
      "bbb => bbbb1 bbbb2\n";
      
    Analyzer analyzer = solrSynsToAnalyzer(testFile);
    
    assertAnalyzesTo(analyzer, "xyzzy bbb pot of gold",
                     new String[] { "xyzzy", "bbbb1", "bbbb2", "pot", "of", "gold" });
    
    // xyzzy aaa pot of gold -> xyzzy aaaa1 aaaa2 aaaa3 gold
    assertAnalyzesTo(analyzer, "xyzzy aaa pot of gold",
                     new String[] { "xyzzy", "aaaa1", "aaaa2", "aaaa3", "pot", "of", "gold" });
    analyzer.close();
  }

  public void testBuilderDedup() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = false;
    add(b, "a b", "ab", keepOrig);
    add(b, "a b", "ab", keepOrig);
    add(b, "a b", "ab", keepOrig);
    Analyzer a = getAnalyzer(b, true);

    assertAnalyzesTo(a, "a b",
        new String[] { "ab" },
        new int[] { 1 });
    a.close();
  }

  public void testBuilderNoDedup() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(false);
    final boolean keepOrig = false;
    add(b, "a b", "ab", keepOrig);
    add(b, "a b", "ab", keepOrig);
    add(b, "a b", "ab", keepOrig);
    Analyzer a = getAnalyzer(b, true);

    assertAnalyzesTo(a, "a b",
        new String[] { "ab", "ab", "ab" },
        new int[] { 1, 0, 0 });
    a.close();
  }

  public void testRecursion1() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = false;
    add(b, "zoo", "zoo", keepOrig);
    Analyzer a = getAnalyzer(b, true);
    
    assertAnalyzesTo(a, "zoo zoo $ zoo",
        new String[] { "zoo", "zoo", "$", "zoo" },
        new int[] { 1, 1, 1, 1 });
    a.close();
  }

  public void testRecursion2() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = false;
    add(b, "zoo", "zoo", keepOrig);
    add(b, "zoo", "zoo zoo", keepOrig);
    Analyzer a = getAnalyzer(b, true);

    // verify("zoo zoo $ zoo", "zoo/zoo zoo/zoo/zoo $/zoo zoo/zoo zoo");
    assertAnalyzesTo(a, "zoo zoo $ zoo",
                     new String[] { "zoo", "zoo", "zoo", "zoo", "zoo", "zoo", "$", "zoo", "zoo", "zoo" },
        new int[] { 1, 0, 1, 1, 0, 1, 1, 1, 0, 1 });
    a.close();
  }

  public void testRecursion3() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = true;
    add(b, "zoo zoo", "zoo", keepOrig);
    Analyzer a = getFlattenAnalyzer(b, true);

    assertAnalyzesTo(a, "zoo zoo $ zoo",
       new String[]{"zoo", "zoo", "zoo", "$", "zoo"},
        new int[]{1, 0, 1, 1, 1});
    a.close();
  }

  public void testRecursion4() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = true;
    add(b, "zoo zoo", "zoo", keepOrig);
    add(b, "zoo", "zoo zoo", keepOrig);
    Analyzer a = getFlattenAnalyzer(b, true);
    assertAnalyzesTo(a, "zoo zoo $ zoo",
        new String[]{"zoo", "zoo", "zoo", "$", "zoo", "zoo", "zoo"},
       new int[]{1, 0, 1, 1, 1, 0, 1});
    a.close();
  }

  public void testKeepOrig() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = true;
    add(b, "a b", "ab", keepOrig);
    add(b, "a c", "ac", keepOrig);
    add(b, "a", "aa", keepOrig);
    add(b, "b", "bb", keepOrig);
    add(b, "z x c v", "zxcv", keepOrig);
    add(b, "x c", "xc", keepOrig);
    Analyzer a = getAnalyzer(b, true);
    
    assertAnalyzesTo(a, "$", 
        new String[] { "$" },
        new int[] { 1 });
    assertAnalyzesTo(a, "a", 
        new String[] { "aa", "a" },
        new int[] { 1, 0 });
    assertAnalyzesTo(a, "a", 
        new String[] { "aa", "a" },
        new int[] { 1, 0 });
    assertAnalyzesTo(a, "$ a", 
        new String[] { "$", "aa", "a" },
        new int[] { 1, 1, 0 });
    assertAnalyzesTo(a, "a $", 
        new String[] { "aa", "a", "$" },
        new int[] { 1, 0, 1 });
    assertAnalyzesTo(a, "$ a !", 
        new String[] { "$", "aa", "a", "!" },
        new int[] { 1, 1, 0, 1 });
    assertAnalyzesTo(a, "a a", 
        new String[] { "aa", "a", "aa", "a" },
        new int[] { 1, 0, 1, 0 });
    assertAnalyzesTo(a, "b", 
        new String[] { "bb", "b" },
        new int[] { 1, 0 });
    assertAnalyzesTo(a, "z x c v",
        new String[] { "zxcv", "z", "x", "c", "v" },
        new int[] { 1, 0, 1, 1, 1 });
    assertAnalyzesTo(a, "z x c $",
        new String[] { "z", "xc", "x", "c", "$" },
        new int[] { 1, 1, 0, 1, 1 });
    a.close();
  }

  /**
   * verify type of token and positionLengths on synonyms of different word counts, with non preserving, explicit rules.
   */
  public void testNonPreservingMultiwordSynonyms() throws Exception {
    String testFile =
      "aaa => two words\n" +
      "bbb => one two, very many multiple words\n" +
      "ee ff, gg, h i j k, h i => one\n" +
      "cc dd => usa,united states,u s a,united states of america";

    Analyzer analyzer = solrSynsToAnalyzer(testFile);

    assertAnalyzesTo(analyzer, "aaa",
        new String[]{"two", "words"},
        new int[]{0, 0},
        new int[]{3, 3},
        new String[]{"SYNONYM", "SYNONYM"},
        new int[]{1, 1},
        new int[]{1, 1});

    assertAnalyzesToPositions(analyzer, "amazing aaa",
        new String[]{"amazing", "two", "words"},
        new String[]{"word", "SYNONYM", "SYNONYM"},
        new int[]{1, 1, 1},
        new int[]{1, 1, 1});

    assertAnalyzesTo(analyzer, "p bbb s",
        new String[]{"p", "one", "very", "two", "many", "multiple", "words", "s"},
        new int[]{0, 2, 2, 2, 2, 2, 2, 6},
        new int[]{1, 5, 5, 5, 5, 5, 5, 7},
        new String[]{"word", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "word"},
        new int[]{1, 1, 0, 1, 0, 1, 1, 1},
        new int[]{1, 1, 1, 3, 1, 1, 1, 1});

    assertAnalyzesTo(analyzer, "p ee ff s",
        new String[]{"p", "one", "s"},
        new int[]{0, 2, 8},
        new int[]{1, 7, 9},
        new String[]{"word", "SYNONYM", "word"},
        new int[]{1, 1, 1},
        new int[]{1, 1, 1});

    assertAnalyzesTo(analyzer, "p h i j s",
        new String[]{"p", "one", "j", "s"},
        new int[]{0, 2, 6, 8},
        new int[]{1, 5, 7, 9},
        new String[]{"word", "SYNONYM", "word", "word"},
        new int[]{1, 1, 1, 1},
        new int[]{1, 1, 1, 1});

    analyzer.close();
  }

  private Analyzer getAnalyzer(SynonymMap.Builder b, final boolean ignoreCase) throws IOException {
    final SynonymMap map = b.build();
    return new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
          // Make a local variable so testRandomHuge doesn't share it across threads!
          SynonymGraphFilter synFilter = new SynonymGraphFilter(tokenizer, map, ignoreCase);
          TestSynonymGraphFilter.this.flattenFilter = null;
          TestSynonymGraphFilter.this.synFilter = synFilter;
          return new TokenStreamComponents(tokenizer, synFilter);
        }
      };
  }

  /** Appends FlattenGraphFilter too */
  private Analyzer getFlattenAnalyzer(SynonymMap.Builder b, boolean ignoreCase) throws IOException {
    final SynonymMap map = b.build();
    return new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
          Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
          // Make a local variable so testRandomHuge doesn't share it across threads!
          SynonymGraphFilter synFilter = new SynonymGraphFilter(tokenizer, map, ignoreCase);
          FlattenGraphFilter flattenFilter = new FlattenGraphFilter(synFilter);
          TestSynonymGraphFilter.this.synFilter = synFilter;
          TestSynonymGraphFilter.this.flattenFilter = flattenFilter;
          return new TokenStreamComponents(tokenizer, flattenFilter);
        }
      };
  }

  private void add(SynonymMap.Builder b, String input, String output, boolean keepOrig) {
    if (VERBOSE) {
      //System.out.println("  add input=" + input + " output=" + output + " keepOrig=" + keepOrig);
    }
    CharsRefBuilder inputCharsRef = new CharsRefBuilder();
    SynonymMap.Builder.join(input.split(" +"), inputCharsRef);

    CharsRefBuilder outputCharsRef = new CharsRefBuilder();
    SynonymMap.Builder.join(output.split(" +"), outputCharsRef);

    b.add(inputCharsRef.get(), outputCharsRef.get(), keepOrig);
  }

  private char[] randomBinaryChars(int minLen, int maxLen, double bias, char base) {
    int len = TestUtil.nextInt(random(), minLen, maxLen);
    char[] chars = new char[len];
    for(int i=0;i<len;i++) {
      char ch;
      if (random().nextDouble() < bias) {
        ch = base;
      } else {
        ch = (char) (base+1);
      }
      chars[i] = ch;
    }

    return chars;
  }

  private static String toTokenString(char[] chars) {
    StringBuilder b = new StringBuilder();
    for(char c : chars) {
      if (b.length() > 0) {
        b.append(' ');
      }
      b.append(c);
    }
    return b.toString();
  }

  private static class OneSyn {
    char[] in;
    char[] out;
    boolean keepOrig;

    @Override
    public String toString() {
      return toTokenString(in) + " --> " + toTokenString(out) + " (keepOrig=" + keepOrig + ")";
    }
  }

  public void testRandomSyns() throws Exception {
    int synCount = atLeast(10);
    double bias = random().nextDouble();
    boolean dedup = random().nextBoolean();

    boolean flatten = random().nextBoolean();

    SynonymMap.Builder b = new SynonymMap.Builder(dedup);
    List<OneSyn> syns = new ArrayList<>();
    // Makes random syns from random a / b tokens, mapping to random x / y tokens
    if (VERBOSE) {
      System.out.println("TEST: make " + synCount + " syns");
      System.out.println("  bias for a over b=" + bias);
      System.out.println("  dedup=" + dedup);
      System.out.println("  flatten=" + flatten);
    }

    int maxSynLength = 0;

    for(int i=0;i<synCount;i++) {
      OneSyn syn = new OneSyn();
      syn.in = randomBinaryChars(1, 5, bias, 'a');
      syn.out = randomBinaryChars(1, 5, 0.5, 'x');
      syn.keepOrig = random().nextBoolean();
      syns.add(syn);

      maxSynLength = Math.max(maxSynLength, syn.in.length);

      if (VERBOSE) {
        System.out.println("  " + syn);
      }
      add(b, toTokenString(syn.in), toTokenString(syn.out), syn.keepOrig);
    }

    // Compute max allowed lookahead for flatten filter:
    int maxFlattenLookahead = 0;
    if (flatten) {
      for(int i=0;i<synCount;i++) {
        OneSyn syn1 = syns.get(i);
        int count = syn1.out.length;
        boolean keepOrig = syn1.keepOrig;
        for(int j=0;j<synCount;j++) {
          OneSyn syn2 = syns.get(i);
          keepOrig |= syn2.keepOrig;
          if (syn1.in.equals(syn2.in)) {
            count += syn2.out.length;
          }
        }

        if (keepOrig) {
          count += syn1.in.length;
        }

        maxFlattenLookahead = Math.max(maxFlattenLookahead, count);
      }
    }

    // Only used w/ VERBOSE:
    Analyzer aNoFlattened;
    if (VERBOSE) {
      aNoFlattened = getAnalyzer(b, true);
    } else {
      aNoFlattened = null;
    }

    Analyzer a;
    if (flatten) {
      a = getFlattenAnalyzer(b, true);
    } else {
      a = getAnalyzer(b, true);
    }

    int iters = atLeast(20);
    for(int iter=0;iter<iters;iter++) {

      String doc = toTokenString(randomBinaryChars(50, 100, bias, 'a'));
      //String doc = toTokenString(randomBinaryChars(10, 50, bias, 'a'));

      if (VERBOSE) {
        System.out.println("TEST: iter="+  iter + " doc=" + doc);
      }
      Automaton expected = slowSynFilter(doc, syns, flatten);
      if (VERBOSE) {
        System.out.println("  expected:\n" + expected.toDot());
        if (flatten) {
          Automaton unflattened = toAutomaton(aNoFlattened.tokenStream("field", new StringReader(doc)));
          System.out.println("  actual unflattened:\n" + unflattened.toDot());
        }
      }
      Automaton actual = toAutomaton(a.tokenStream("field", new StringReader(doc)));
      if (VERBOSE) {
        System.out.println("  actual:\n" + actual.toDot());
      }

      assertTrue("maxLookaheadUsed=" + synFilter.getMaxLookaheadUsed() + " maxSynLength=" + maxSynLength,
                 synFilter.getMaxLookaheadUsed() <= maxSynLength);
      if (flatten) {
        assertTrue("flatten maxLookaheadUsed=" + flattenFilter.getMaxLookaheadUsed() + " maxFlattenLookahead=" + maxFlattenLookahead,
                   flattenFilter.getMaxLookaheadUsed() <= maxFlattenLookahead);
      }

      checkAnalysisConsistency(random(), a, random().nextBoolean(), doc);
      // We can easily have a non-deterministic automaton at this point, e.g. if
      // more than one syn matched at given point, or if the syn mapped to an
      // output token that also happens to be in the input:
      try {
        actual = Operations.determinize(actual, 50000);
      } catch (TooComplexToDeterminizeException tctde) {
        // Unfortunately the syns can easily create difficult-to-determinize graphs:
        assertTrue(approxEquals(actual, expected));
        continue;
      }

      try {
        expected = Operations.determinize(expected, 50000);
      } catch (TooComplexToDeterminizeException tctde) {
        // Unfortunately the syns can easily create difficult-to-determinize graphs:
        assertTrue(approxEquals(actual, expected));
        continue;
      }

      assertTrue(approxEquals(actual, expected));
      assertTrue(Operations.sameLanguage(actual, expected));
    }

    a.close();
  }

  /** Only used when true equality is too costly to check! */
  private boolean approxEquals(Automaton actual, Automaton expected) {
    // Don't collapse these into one line else the thread stack won't say which direction failed!:
    boolean b1 = approxSubsetOf(actual, expected);
    boolean b2 = approxSubsetOf(expected, actual);
    return b1 && b2;
  }

  private boolean approxSubsetOf(Automaton a1, Automaton a2) {
    AutomatonTestUtil.RandomAcceptedStrings ras = new AutomatonTestUtil.RandomAcceptedStrings(a1);
    for(int i=0;i<2000;i++) {
      int[] ints = ras.getRandomAcceptedString(random());
      IntsRef path = new IntsRef(ints, 0, ints.length);
      if (accepts(a2, path) == false) {
        throw new RuntimeException("a2 does not accept " + path);
      }
    }

    // Presumed true
    return true;
  }

  /** Like {@link Operations#run} except the incoming automaton is allowed to be non-deterministic. */
  private static boolean accepts(Automaton a, IntsRef path) {
    Set<Integer> states = new HashSet<>();
    states.add(0);
    Transition t = new Transition();
    for(int i=0;i<path.length;i++) {
      int digit = path.ints[path.offset+i];
      Set<Integer> nextStates = new HashSet<>();
      for(int state : states) {
        int count = a.initTransition(state, t);
        for(int j=0;j<count;j++) {
          a.getNextTransition(t);
          if (digit >= t.min && digit <= t.max) {
            nextStates.add(t.dest);
          }
        }
      }
      states = nextStates;
      if (states.isEmpty()) {
        return false;
      }
    }

    for(int state : states) {
      if (a.isAccept(state)) {
        return true;
      }
    }

    return false;
  }

  /** Stupid, slow brute-force, yet hopefully bug-free, synonym filter. */
  private Automaton slowSynFilter(String doc, List<OneSyn> syns, boolean flatten) {
    String[] tokens = doc.split(" +");
    if (VERBOSE) {
      System.out.println("  doc has " + tokens.length + " tokens");
    }
    int i=0;
    Automaton.Builder a = new Automaton.Builder();
    int lastState = a.createState();
    while (i<tokens.length) {
      // Consider all possible syn matches starting at this point:
      assert tokens[i].length() == 1;
      if (VERBOSE) {
        System.out.println("    i=" + i);
      }

      List<OneSyn> matches = new ArrayList<>();
      for(OneSyn syn : syns) {
        if (i + syn.in.length <= tokens.length) {
          boolean match = true;
          for(int j=0;j<syn.in.length;j++) {
            if (tokens[i+j].charAt(0) != syn.in[j]) {
              match = false;
              break;
            }
          }

          if (match) {
            if (matches.isEmpty() == false) {
              if (syn.in.length < matches.get(0).in.length) {
                // Greedy matching: we already found longer syns matching here
                continue;
              } else if (syn.in.length > matches.get(0).in.length) {
                // Greedy matching: all previous matches were shorter, so we drop them
                matches.clear();
              } else {
                // Keep the current matches: we allow multiple synonyms matching the same input string
              }
            }

            matches.add(syn);
          }
        }
      }

      int nextState = a.createState();

      if (matches.isEmpty() == false) {
        // We have match(es) starting at this token
        if (VERBOSE) {
          System.out.println("  matches @ i=" + i + ": " + matches);
        }
        // We keepOrig if any of the matches said to:
        boolean keepOrig = false;
        for(OneSyn syn : matches) {
          keepOrig |= syn.keepOrig;
        }

        List<Integer> flatStates;
        if (flatten) {
          flatStates = new ArrayList<>();
        } else {
          flatStates = null;
        }

        if (keepOrig) {
          // Add path for the original tokens
          addSidePath(a, lastState, nextState, matches.get(0).in, flatStates);
        }

        for(OneSyn syn : matches) {
          addSidePath(a, lastState, nextState, syn.out, flatStates);
        }

        i += matches.get(0).in.length;
      } else {
        a.addTransition(lastState, nextState, tokens[i].charAt(0));
        i++;
      }

      lastState = nextState;
    }

    a.setAccept(lastState, true);

    return topoSort(a.finish());
  }

  /** Just creates a side path from startState to endState with the provided tokens. */
  private static void addSidePath(Automaton.Builder a, int startState, int endState, char[] tokens, List<Integer> flatStates) {
    int lastState = startState;
    for(int i=0;i<tokens.length;i++) {
      int nextState;
      if (i == tokens.length-1) {
        nextState = endState;
      } else if (flatStates == null || i >= flatStates.size()) {
        nextState = a.createState();
        if (flatStates != null) {
          assert i == flatStates.size();
          flatStates.add(nextState);
        }
      } else {
        nextState = flatStates.get(i);
      }
      a.addTransition(lastState, nextState, tokens[i]);

      lastState = nextState;
    }
  }

  private Automaton toAutomaton(TokenStream ts) throws IOException {
    PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);
    PositionLengthAttribute posLenAtt = ts.addAttribute(PositionLengthAttribute.class);
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    ts.reset();
    Automaton a = new Automaton();
    int srcNode = -1;
    int destNode = -1;
    int state = a.createState();
    while (ts.incrementToken()) {
      assert termAtt.length() == 1;
      char c = termAtt.charAt(0);
      int posInc = posIncAtt.getPositionIncrement();
      if (posInc != 0) {
        srcNode += posInc;
        while (state < srcNode) {
          state = a.createState();
        }
      }
      destNode = srcNode + posLenAtt.getPositionLength();
      while (state < destNode) {
        state = a.createState();
      }
      a.addTransition(srcNode, destNode, c);
    }
    ts.end();
    ts.close();
    a.finishState();
    a.setAccept(destNode, true);
    return a;
  }

  /*
  private String toDot(TokenStream ts) throws IOException {
    PositionIncrementAttribute posIncAtt = ts.addAttribute(PositionIncrementAttribute.class);
    PositionLengthAttribute posLenAtt = ts.addAttribute(PositionLengthAttribute.class);
    CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
    TypeAttribute typeAtt = ts.addAttribute(TypeAttribute.class);
    ts.reset();
    int srcNode = -1;
    int destNode = -1;

    StringBuilder b = new StringBuilder();
    b.append("digraph Automaton {\n");
    b.append("  rankdir = LR\n");
    b.append("  node [width=0.2, height=0.2, fontsize=8]\n");
    b.append("  initial [shape=plaintext,label=\"\"]\n");
    b.append("  initial -> 0\n");

    while (ts.incrementToken()) {
      int posInc = posIncAtt.getPositionIncrement();
      if (posInc != 0) {
        srcNode += posInc;
        b.append("  ");
        b.append(srcNode);
        b.append(" [shape=circle,label=\"" + srcNode + "\"]\n");
      }
      destNode = srcNode + posLenAtt.getPositionLength();
      b.append("  ");
      b.append(srcNode);
      b.append(" -> ");
      b.append(destNode);
      b.append(" [label=\"");
      b.append(termAtt);
      b.append("\"");
      if (typeAtt.type().equals("word") == false) {
        b.append(" color=red");
      }
      b.append("]\n");
    }
    ts.end();
    ts.close();

    b.append('}');
    return b.toString();
  }
  */

  /** Renumbers nodes according to their topo sort */
  private Automaton topoSort(Automaton in) {
    int[] newToOld = Operations.topoSortStates(in);
    int[] oldToNew = new int[newToOld.length];

    Automaton.Builder a = new Automaton.Builder();
    //System.out.println("remap:");
    for(int i=0;i<newToOld.length;i++) {
      a.createState();
      oldToNew[newToOld[i]] = i;
      //System.out.println("  " + newToOld[i] + " -> " + i);
      if (in.isAccept(newToOld[i])) {
        a.setAccept(i, true);
        //System.out.println("    **");
      }
    }

    Transition t = new Transition();
    for(int i=0;i<newToOld.length;i++) {
      int count = in.initTransition(newToOld[i], t);
      for(int j=0;j<count;j++) {
        in.getNextTransition(t);
        a.addTransition(i, oldToNew[t.dest], t.min, t.max);
      }
    }

    return a.finish();
  }

  /**
   * verify type of token and positionLengths on synonyms of different word counts.
   */
  public void testPositionLengthAndType() throws Exception {
    String testFile =
        "spider man, spiderman\n" +
        "usa,united states,u s a,united states of america";
    Analyzer analyzer = new MockAnalyzer(random());
    SolrSynonymParser parser = new SolrSynonymParser(true, true, analyzer);

    parser.parse(new StringReader(testFile));
    analyzer.close();

    SynonymMap map = parser.build();
    analyzer = getFlattenAnalyzer(parser, true);

    BytesRef value = Util.get(map.fst, Util.toUTF32(new CharsRef("usa"), new IntsRefBuilder()));
    ByteArrayDataInput bytesReader = new ByteArrayDataInput(value.bytes, value.offset, value.length);
    final int code = bytesReader.readVInt();
    final int count = code >>> 1;

    final int[] synonymsIdxs = new int[count];
    for (int i = 0; i < count; i++) {
      synonymsIdxs[i] = bytesReader.readVInt();
    }

    BytesRef scratchBytes = new BytesRef();
    map.words.get(synonymsIdxs[2], scratchBytes);

    int synonymLength = 1;
    for (int i = scratchBytes.offset; i < scratchBytes.offset + scratchBytes.length; i++) {
      if (scratchBytes.bytes[i] == SynonymMap.WORD_SEPARATOR) {
        synonymLength++;
      }
    }

    assertEquals(count, 3);
    assertEquals(synonymLength, 4);

    assertAnalyzesTo(analyzer, "spider man",
                     new String[]{"spiderman", "spider", "man"},
                     new int[]{0, 0, 7},
                     new int[]{10, 6, 10},
                     new String[]{"SYNONYM", "word", "word"},
                     new int[]{1, 0, 1},
                     new int[]{2, 1, 1});

    assertAnalyzesToPositions(analyzer, "amazing spider man",
                              new String[]{"amazing", "spiderman", "spider", "man"},
                              new String[]{"word", "SYNONYM", "word", "word"},
                              new int[]{1, 1, 0, 1},
                              new int[]{1, 2, 1, 1});

    // System.out.println(toDot(getAnalyzer(parser, true).tokenStream("field", new StringReader("the usa is wealthy"))));

    assertAnalyzesTo(analyzer, "the united states of america is wealthy",
                     new String[]{"the", "usa", "united", "u", "united", "states", "s", "states", "a", "of", "america", "is", "wealthy"},
                     new int[]      {0,     4,        4,   4,        4,       11,  11,       11,   18,  18,        21,   29,        32},
                     new int[]      {3,    28,       10,  10,       10,       28,  17,       17,   28,  20,        28,   31,        39},
                     new String[]{"word", "SYNONYM", "SYNONYM", "SYNONYM", "word", "SYNONYM", "SYNONYM", "word", "SYNONYM", "word", "word", "word", "word"},
                     new int[]      {1,     1,        0,   0,        0,        1,   0,        0,    1,   0,         1,    1,         1},
                     new int[]      {1,     4,        1,   1,        1,        3,   1,        1,    2,   1,         1,    1,         1});

    assertAnalyzesToPositions(analyzer, "spiderman",
                              new String[]{"spider", "spiderman", "man"},
                              new String[]{"SYNONYM", "word", "SYNONYM"},
                              new int[]{1, 0, 1},
                              new int[]{1, 2, 1});

    assertAnalyzesTo(analyzer, "spiderman enemies",
                     new String[]{"spider", "spiderman", "man", "enemies"},
                     new int[]{0, 0, 0, 10},
                     new int[]{9, 9, 9, 17},
                     new String[]{"SYNONYM", "word", "SYNONYM", "word"},
                     new int[]{1, 0, 1, 1},
                     new int[]{1, 2, 1, 1});

    assertAnalyzesTo(analyzer, "the usa is wealthy",
                     new String[]{"the", "united", "u", "united", "usa", "states", "s", "states", "a", "of", "america", "is", "wealthy"},
                     new int[]      {0,        4,   4,        4,     4,        4,   4,        4,   4,    4,         4,    8,        11},
                     new int[]      {3,        7,   7,        7,     7,        7,   7,        7,   7,    7,         7,   10,        18},
                     new String[]{"word", "SYNONYM", "SYNONYM", "SYNONYM", "word", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "SYNONYM", "word", "word"},
                     new int[]      {1,        1,   0,        0,     0,        1,   0,        0,   1,    0,         1,    1,         1},
                     new int[]      {1,        1,   1,        1,     4,        3,   1,        1,   2,    1,         1,    1,         1});
    
    assertGraphStrings(analyzer, "the usa is wealthy", new String[] {
        "the usa is wealthy",
        "the united states is wealthy",
        "the u s a is wealthy",
        "the united states of america is wealthy",
        // Wrong. Here only due to "sausagization" of the multi word synonyms.
        "the u states is wealthy",
        "the u states a is wealthy",
        "the u s of america is wealthy",
        "the u states of america is wealthy",
        "the united s a is wealthy",
        "the united states a is wealthy",
        "the united s of america is wealthy"});

    assertAnalyzesTo(analyzer, "the united states is wealthy",
                     new String[]{"the", "usa", "u", "united", "united", "s", "states", "states", "a", "of", "america", "is", "wealthy"},
                     new int[]      {0,     4,   4,        4,        4,  11,       11,       11,  11,   11,        11,   18,        21},
                     new int[]      {3,    17,  10,       10,       10,  17,       17,       17,  17,   17,        17,   20,        28},
                     new String[]{"word", "SYNONYM", "SYNONYM", "SYNONYM", "word", "SYNONYM", "SYNONYM", "word", "SYNONYM", "SYNONYM", "SYNONYM", "word", "word"},
                     new int[]      {1,     1,   0,        0,        0,   1,        0,        0,   1,    0,         1,    1,         1},
                     new int[]      {1,     4,   1,        1,        1,   1,        1,        3,   2,    1,         1,    1,         1},
                     false);

    assertAnalyzesTo(analyzer, "the united states of balance",
                     new String[]{"the", "usa", "u", "united", "united", "s", "states", "states", "a", "of", "america", "of", "balance"},
                     new int[]      {0,     4,   4,        4,        4,  11,       11,       11,  11,   11,        11,   18,        21},
                     new int[]      {3,    17,  10,       10,       10,  17,       17,       17,  17,   17,        17,   20,        28},
                     new String[]{"word", "SYNONYM", "SYNONYM", "SYNONYM", "word", "SYNONYM", "SYNONYM", "word", "SYNONYM", "SYNONYM", "SYNONYM", "word", "word"},
                     new int[]      {1,     1,   0,        0,        0,   1,        0,        0,   1,    0,         1,    1,         1},
                     new int[]      {1,     4,   1,        1,        1,   1,        1,        3,   2,    1,         1,    1,         1});

    analyzer.close();
  }

  public void testMultiwordOffsets() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = true;
    add(b, "national hockey league", "nhl", keepOrig);
    Analyzer a = getFlattenAnalyzer(b, true);

    assertAnalyzesTo(a, "national hockey league",
        new String[]{"nhl", "national", "hockey", "league"},
        new int[]{0, 0, 9, 16},
        new int[]{22, 8, 15, 22},
        new int[]{1, 0, 1, 1});
    a.close();
  }

  public void testIncludeOrig() throws Exception {
    SynonymMap.Builder b = new SynonymMap.Builder(true);
    final boolean keepOrig = true;
    add(b, "a b", "ab", keepOrig);
    add(b, "a c", "ac", keepOrig);
    add(b, "a", "aa", keepOrig);
    add(b, "b", "bb", keepOrig);
    add(b, "z x c v", "zxcv", keepOrig);
    add(b, "x c", "xc", keepOrig);

    Analyzer a = getFlattenAnalyzer(b, true);

    assertAnalyzesTo(a, "$",
        new String[]{"$"},
        new int[]{1});
    assertAnalyzesTo(a, "a",
        new String[]{"aa", "a"},
        new int[]{1, 0});
    assertAnalyzesTo(a, "a",
        new String[]{"aa", "a"},
        new int[]{1, 0});
    assertAnalyzesTo(a, "$ a",
        new String[]{"$", "aa", "a"},
        new int[]{1, 1, 0});
    assertAnalyzesTo(a, "a $",
        new String[]{"aa", "a", "$"},
        new int[]{1, 0, 1});
    assertAnalyzesTo(a, "$ a !",
        new String[]{"$", "aa", "a", "!"},
        new int[]{1, 1, 0, 1});
    assertAnalyzesTo(a, "a a",
        new String[]{"aa", "a", "aa", "a"},
        new int[]{1, 0, 1, 0});
    assertAnalyzesTo(a, "b",
        new String[]{"bb", "b"},
        new int[]{1, 0});
    assertAnalyzesTo(a, "z x c v",
        new String[]{"zxcv", "z", "x", "c", "v"},
        new int[]{1, 0, 1, 1, 1});
    assertAnalyzesTo(a, "z x c $",
        new String[]{"z", "xc", "x", "c", "$"},
        new int[]{1, 1, 0, 1, 1});
    a.close();
  }

  public void testUpperCase() throws IOException {
    assertMapping("word", "synonym");
    assertMapping("word".toUpperCase(Locale.ROOT), "synonym");
  }

  private void assertMapping(String inputString, String outputString) throws IOException {
    SynonymMap.Builder builder = new SynonymMap.Builder(false);
    // the rules must be lowercased up front, but the incoming tokens will be case insensitive:
    CharsRef input = SynonymMap.Builder.join(inputString.toLowerCase(Locale.ROOT).split(" "), new CharsRefBuilder());
    CharsRef output = SynonymMap.Builder.join(outputString.split(" "), new CharsRefBuilder());
    builder.add(input, output, true);
    Analyzer analyzer = new CustomAnalyzer(builder.build());
    TokenStream tokenStream = analyzer.tokenStream("field", inputString);
    assertTokenStreamContents(tokenStream, new String[]{
        outputString, inputString
      });
  }

  static class CustomAnalyzer extends Analyzer {
    private SynonymMap synonymMap;

    CustomAnalyzer(SynonymMap synonymMap) {
      this.synonymMap = synonymMap;
    }

    @Override
    protected TokenStreamComponents createComponents(String s) {
      Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
      TokenStream tokenStream = new SynonymGraphFilter(tokenizer, synonymMap, true); // Ignore case True
      return new TokenStreamComponents(tokenizer, tokenStream);
    }
  }
}
