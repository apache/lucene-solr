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
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.junit.Test;

public class TestConcatenateGraphFilter extends BaseTokenStreamTestCase {

  @Test
  public void testBasic() throws Exception {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword";
    BytesRef payload = new BytesRef("payload");
    tokenStream.setReader(new StringReader(input));
    ConcatenateGraphFilter concatStream = new ConcatenateGraphFilter(tokenStream);
    concatStream.setPayload(payload);
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(concatStream);
    assertTokenStreamContents(stream, new String[] {input}, null, null, new String[] {payload.utf8ToString()}, new int[] { 1 }, null, null);
  }

  @Test
  public void testWithNoPreserveSep() throws Exception {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword another keyword";
    BytesRef payload = new BytesRef("payload");
    tokenStream.setReader(new StringReader(input));
    ConcatenateGraphFilter concatStream = new ConcatenateGraphFilter(tokenStream, false, false, 100);
    concatStream.setPayload(payload);
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(concatStream);
    assertTokenStreamContents(stream, new String[] {"mykeywordanotherkeyword"}, null, null, new String[] {payload.utf8ToString()}, new int[] { 1 }, null, null);
  }

  @Test
  public void testWithMultipleTokens() throws Exception {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword another keyword";
    tokenStream.setReader(new StringReader(input));
    BytesRef payload = new BytesRef("payload");
    ConcatenateGraphFilter concatStream = new ConcatenateGraphFilter(tokenStream);
    concatStream.setPayload(payload);
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(concatStream);
    CharsRefBuilder builder = new CharsRefBuilder();
    builder.append("mykeyword");
    builder.append((ConcatenateGraphFilter.SEP_CHAR));
    builder.append("another");
    builder.append((ConcatenateGraphFilter.SEP_CHAR));
    builder.append("keyword");
    assertTokenStreamContents(stream, new String[]{builder.toCharsRef().toString()}, null, null, new String[]{payload.utf8ToString()}, new int[]{1}, null, null);
  }

  @Test
  public void testWithSynonym() throws Exception {
    SynonymMap.Builder builder = new SynonymMap.Builder(true);
    builder.add(new CharsRef("mykeyword"), new CharsRef("mysynonym"), true);
    Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    tokenizer.setReader(new StringReader("mykeyword"));
    SynonymFilter filter = new SynonymFilter(tokenizer, builder.build(), true);
    ConcatenateGraphFilter concatStream = new ConcatenateGraphFilter(filter);
    BytesRef payload = new BytesRef("payload");
    concatStream.setPayload(payload);
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(concatStream);
    assertTokenStreamContents(stream, new String[] {"mykeyword", "mysynonym"}, null, null, new String[] {payload.utf8ToString(), payload.utf8ToString()}, new int[] { 1, 0 }, null, null);
  }

  @Test
  public void testWithSynonyms() throws Exception {
    SynonymMap.Builder builder = new SynonymMap.Builder(true);
    builder.add(new CharsRef("mykeyword"), new CharsRef("mysynonym"), true);
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword another keyword";
    tokenStream.setReader(new StringReader(input));
    SynonymFilter filter = new SynonymFilter(tokenStream, builder.build(), true);
    BytesRef payload = new BytesRef("payload");
    ConcatenateGraphFilter concatStream = new ConcatenateGraphFilter(filter, true, false, 100);
    concatStream.setPayload(payload);
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(concatStream);
    String[] expectedOutputs = new String[2];
    CharsRefBuilder expectedOutput = new CharsRefBuilder();
    expectedOutput.append("mykeyword");
    expectedOutput.append(ConcatenateGraphFilter.SEP_CHAR);
    expectedOutput.append("another");
    expectedOutput.append(ConcatenateGraphFilter.SEP_CHAR);
    expectedOutput.append("keyword");
    expectedOutputs[0] = expectedOutput.toCharsRef().toString();
    expectedOutput.clear();
    expectedOutput.append("mysynonym");
    expectedOutput.append(ConcatenateGraphFilter.SEP_CHAR);
    expectedOutput.append("another");
    expectedOutput.append(ConcatenateGraphFilter.SEP_CHAR);
    expectedOutput.append("keyword");
    expectedOutputs[1] = expectedOutput.toCharsRef().toString();
    assertTokenStreamContents(stream, expectedOutputs, null, null, new String[]{payload.utf8ToString(), payload.utf8ToString()}, new int[]{1, 0}, null, null);
  }

  @Test
  public void testWithStopword() throws Exception {
    for (boolean preservePosInc : new boolean[]{true, false}) {
      Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
      String input = "a mykeyword a keyword a";
      tokenStream.setReader(new StringReader(input));
      TokenFilter tokenFilter = new StopFilter(tokenStream, StopFilter.makeStopSet("a"));
      ConcatenateGraphFilter concatStream = new ConcatenateGraphFilter(tokenFilter, true, preservePosInc, 10);
      CharsRefBuilder builder = new CharsRefBuilder();
      if (preservePosInc) {
        builder.append((ConcatenateGraphFilter.SEP_CHAR));
      }
      builder.append("mykeyword");
      builder.append((ConcatenateGraphFilter.SEP_CHAR));
      if (preservePosInc) {
        builder.append((ConcatenateGraphFilter.SEP_CHAR));
      }
      builder.append("keyword");
      if (preservePosInc) {
        builder.append((ConcatenateGraphFilter.SEP_CHAR));
      }
      assertTokenStreamContents(concatStream, new String[]{builder.toCharsRef().toString()});
    }
  }

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
    SynonymFilter filter = new SynonymFilter(tokenizer, builder.build(), true);

    ConcatenateGraphFilter concatStream = new ConcatenateGraphFilter(filter);
    concatStream.setPayload(new BytesRef());
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(concatStream);
    stream.reset();
    ConcatenateGraphFilter.BytesRefBuilderTermAttribute attr = stream.addAttribute(ConcatenateGraphFilter.BytesRefBuilderTermAttribute.class);
    int count = 0;
    while(stream.incrementToken()) {
      count++;
      assertNotNull(attr.getBytesRef());
      assertTrue(attr.getBytesRef().length > 0);
    }
    stream.close();
    assertEquals(count, 256);
  }

  public void testEmpty() throws IOException {
    Tokenizer tokenizer = whitespaceMockTokenizer("");
    ConcatenateGraphFilter filter = new ConcatenateGraphFilter(tokenizer);
    assertTokenStreamContents(filter, new String[0]);
  }

  public final static class PayloadAttrToTypeAttrFilter extends TokenFilter {
    private PayloadAttribute payload = addAttribute(PayloadAttribute.class);
    private TypeAttribute type = addAttribute(TypeAttribute.class);

    protected PayloadAttrToTypeAttrFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        // we move them over so we can assert them more easily in the tests
        type.setType(payload.getPayload().utf8ToString());
        return true;
      }
      return false;
    }
  }
}
