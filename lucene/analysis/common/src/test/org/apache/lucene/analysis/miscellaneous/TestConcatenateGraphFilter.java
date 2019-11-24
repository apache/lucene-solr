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
package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.junit.Test;

public class TestConcatenateGraphFilter extends BaseTokenStreamTestCase {

  private static final char SEP_LABEL = (char) ConcatenateGraphFilter.SEP_LABEL;
  
  @Test
  public void testBasic() throws Exception {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword";
    tokenStream.setReader(new StringReader(input));
    ConcatenateGraphFilter stream = new ConcatenateGraphFilter(tokenStream);
    assertTokenStreamContents(stream, new String[] {input}, null, null, new int[] { 1 });
  }

  @Test
  public void testWithNoPreserveSep() throws Exception {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword another keyword";
    tokenStream.setReader(new StringReader(input));
    ConcatenateGraphFilter stream = new ConcatenateGraphFilter(tokenStream, null, false, 100);
    assertTokenStreamContents(stream, new String[] {"mykeywordanotherkeyword"}, null, null, new int[] { 1 });
  }

  @Test
  public void testWithMultipleTokens() throws Exception {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword another keyword";
    tokenStream.setReader(new StringReader(input));
    ConcatenateGraphFilter stream = new ConcatenateGraphFilter(tokenStream);
    CharsRefBuilder builder = new CharsRefBuilder();
    builder.append("mykeyword");
    builder.append(SEP_LABEL);
    builder.append("another");
    builder.append(SEP_LABEL);
    builder.append("keyword");
    assertTokenStreamContents(stream, new String[]{builder.toCharsRef().toString()}, null, null, new int[]{1});
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testWithSynonym() throws Exception {
    SynonymMap.Builder builder = new SynonymMap.Builder(true);
    builder.add(new CharsRef("mykeyword"), new CharsRef("mysynonym"), true);
    Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    tokenizer.setReader(new StringReader("mykeyword"));
    org.apache.lucene.analysis.synonym.SynonymFilter filter = new org.apache.lucene.analysis.synonym.SynonymFilter(tokenizer, builder.build(), true);
    ConcatenateGraphFilter stream = new ConcatenateGraphFilter(filter);
    assertTokenStreamContents(stream, new String[] {"mykeyword", "mysynonym"}, null, null, new int[] { 1, 0 });
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testWithSynonyms() throws Exception {
    SynonymMap.Builder builder = new SynonymMap.Builder(true);
    builder.add(new CharsRef("mykeyword"), new CharsRef("mysynonym"), true);
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword another keyword";
    tokenStream.setReader(new StringReader(input));
    org.apache.lucene.analysis.synonym.SynonymFilter filter = new org.apache.lucene.analysis.synonym.SynonymFilter(tokenStream, builder.build(), true);
    ConcatenateGraphFilter stream = new ConcatenateGraphFilter(filter, SEP_LABEL, false, 100);
    String[] expectedOutputs = new String[2];
    CharsRefBuilder expectedOutput = new CharsRefBuilder();
    expectedOutput.append("mykeyword");
    expectedOutput.append(SEP_LABEL);
    expectedOutput.append("another");
    expectedOutput.append(SEP_LABEL);
    expectedOutput.append("keyword");
    expectedOutputs[0] = expectedOutput.toCharsRef().toString();
    expectedOutput.clear();
    expectedOutput.append("mysynonym");
    expectedOutput.append(SEP_LABEL);
    expectedOutput.append("another");
    expectedOutput.append(SEP_LABEL);
    expectedOutput.append("keyword");
    expectedOutputs[1] = expectedOutput.toCharsRef().toString();
    assertTokenStreamContents(stream, expectedOutputs, null, null, new int[]{1, 0});
  }

  @Test
  public void testWithStopword() throws Exception {
    for (boolean preservePosInc : new boolean[]{true, false}) {
      Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
      String input = "a mykeyword a keyword"; //LUCENE-8344 add "a"
      tokenStream.setReader(new StringReader(input));
      TokenFilter tokenFilter = new StopFilter(tokenStream, StopFilter.makeStopSet("a"));
      ConcatenateGraphFilter concatStream = new ConcatenateGraphFilter(tokenFilter, SEP_LABEL, preservePosInc, 10);
      CharsRefBuilder builder = new CharsRefBuilder();
      if (preservePosInc) {
        builder.append(SEP_LABEL);
      }
      builder.append("mykeyword");
      builder.append(SEP_LABEL);
      if (preservePosInc) {
        builder.append(SEP_LABEL);
      }
      builder.append("keyword");
//      if (preservePosInc) { LUCENE-8344 uncomment
//        builder.append(SEP_LABEL);
//      }
      assertTokenStreamContents(concatStream, new String[]{builder.toCharsRef().toString()});
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testValidNumberOfExpansions() throws IOException {
    SynonymMap.Builder builder = new SynonymMap.Builder(true);
    for (int i = 0; i < 256; i++) {
      builder.add(new CharsRef("" + (i+1)), new CharsRef("" + (1000 + (i+1))), true);
    }
    StringBuilder valueBuilder = new StringBuilder();
    for (int i = 0 ; i < 8 ; i++) {
      valueBuilder.append(i+1);
      valueBuilder.append(" ");
    }
    MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    tokenizer.setReader(new StringReader(valueBuilder.toString()));
    org.apache.lucene.analysis.synonym.SynonymFilter filter = new org.apache.lucene.analysis.synonym.SynonymFilter(tokenizer, builder.build(), true);

    int count;
    try (ConcatenateGraphFilter stream = new ConcatenateGraphFilter(filter)) {
      stream.reset();
      ConcatenateGraphFilter.BytesRefBuilderTermAttribute attr = stream.addAttribute(ConcatenateGraphFilter.BytesRefBuilderTermAttribute.class);
      count = 0;
      while (stream.incrementToken()) {
        count++;
        assertNotNull(attr.getBytesRef());
        assertTrue(attr.getBytesRef().length > 0);
      }
    }
    assertEquals(count, 256);
  }

  public void testEmpty() throws IOException {
    Tokenizer tokenizer = whitespaceMockTokenizer("");
    ConcatenateGraphFilter filter = new ConcatenateGraphFilter(tokenizer);
    assertTokenStreamContents(filter, new String[0]);
  }

  @Test
  public void testSeparator() throws IOException {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.SIMPLE, true);
    String input = "...mykeyword.another.keyword.";
    tokenStream.setReader(new StringReader(input));
    ConcatenateGraphFilter stream = new ConcatenateGraphFilter(tokenStream, ' ', false, 100); //not \u001F
    assertTokenStreamContents(stream, new String[] {"mykeyword another keyword"}, null, null, new int[] { 1 });
  }

  @Test
  public void testSeparatorWithStopWords() throws IOException {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    String input = "A B C D E F J H";
    tokenStream.setReader(new StringReader(input));
    TokenStream tokenFilter = new StopFilter(tokenStream, StopFilter.makeStopSet("A", "D", "E", "J"));
    ConcatenateGraphFilter stream = new ConcatenateGraphFilter(tokenFilter, '-', false, 100);

    assertTokenStreamContents(stream, new String[] {"B-C-F-H"}, null, null, new int[] { 1 });
  }

  @Test
  public void testSeparatorWithStopWordsAndPreservePositionIncrements() throws IOException {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    String input = "A B C D E F J H";
    tokenStream.setReader(new StringReader(input));
    TokenStream tokenFilter = new StopFilter(tokenStream, StopFilter.makeStopSet("A", "D", "E", "J"));
    ConcatenateGraphFilter stream = new ConcatenateGraphFilter(tokenFilter, '-', true, 100);

    assertTokenStreamContents(stream, new String[] {"-B-C---F--H"}, null, null, new int[] { 1 });
  }

  @Test
  public void testSeparatorWithSynonyms() throws IOException {
    SynonymMap.Builder builder = new SynonymMap.Builder(true);
    builder.add(new CharsRef("mykeyword"), new CharsRef("mysynonym"), true);
    builder.add(new CharsRef("mykeyword"), new CharsRef("three words synonym"), true);
    Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = " mykeyword another keyword   ";
    tokenizer.setReader(new StringReader(input));
    SynonymGraphFilter filter = new SynonymGraphFilter(tokenizer, builder.build(), true);
    ConcatenateGraphFilter stream = new ConcatenateGraphFilter(filter, '-', false, 100);
    assertTokenStreamContents(stream, new String[] {
        "mykeyword-another-keyword",
        "mysynonym-another-keyword",
        "three words synonym-another-keyword"
    }, null, null, new int[] { 1, 0 ,0});
  }

}
