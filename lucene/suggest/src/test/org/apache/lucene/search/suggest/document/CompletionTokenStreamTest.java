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
package org.apache.lucene.search.suggest.document;

import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.junit.Test;

public class CompletionTokenStreamTest extends BaseTokenStreamTestCase {

  @Test
  public void testBasic() throws Exception {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword";
    BytesRef payload = new BytesRef("payload");
    tokenStream.setReader(new StringReader(input));
    CompletionTokenStream completionTokenStream = new CompletionTokenStream(tokenStream);
    completionTokenStream.setPayload(payload);
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(completionTokenStream);
    assertTokenStreamContents(stream, new String[] {input}, null, null, new String[] {payload.utf8ToString()}, new int[] { 1 }, null, null);
  }

  @Test
  public void testWithNoPreserveSep() throws Exception {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword another keyword";
    BytesRef payload = new BytesRef("payload");
    tokenStream.setReader(new StringReader(input));
    CompletionTokenStream completionTokenStream = new CompletionTokenStream(tokenStream, false, false, 100);
    completionTokenStream.setPayload(payload);
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(completionTokenStream);
    assertTokenStreamContents(stream, new String[] {"mykeywordanotherkeyword"}, null, null, new String[] {payload.utf8ToString()}, new int[] { 1 }, null, null);
  }

  @Test
  public void testWithMultipleTokens() throws Exception {
    Tokenizer tokenStream = new MockTokenizer(MockTokenizer.WHITESPACE, true);
    String input = "mykeyword another keyword";
    tokenStream.setReader(new StringReader(input));
    BytesRef payload = new BytesRef("payload");
    CompletionTokenStream completionTokenStream = new CompletionTokenStream(tokenStream);
    completionTokenStream.setPayload(payload);
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(completionTokenStream);
    CharsRefBuilder builder = new CharsRefBuilder();
    builder.append("mykeyword");
    builder.append(((char) CompletionAnalyzer.SEP_LABEL));
    builder.append("another");
    builder.append(((char) CompletionAnalyzer.SEP_LABEL));
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
    CompletionTokenStream completionTokenStream = new CompletionTokenStream(filter);
    BytesRef payload = new BytesRef("payload");
    completionTokenStream.setPayload(payload);
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(completionTokenStream);
    assertTokenStreamContents(stream, new String[] {"mykeyword", "mysynonym"}, null, null, new String[] {payload.utf8ToString(), payload.utf8ToString()}, new int[] { 1, 1 }, null, null);
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
    CompletionTokenStream completionTokenStream = new CompletionTokenStream(filter, true, false, 100);
    completionTokenStream.setPayload(payload);
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(completionTokenStream);
    String[] expectedOutputs = new String[2];
    CharsRefBuilder expectedOutput = new CharsRefBuilder();
    expectedOutput.append("mykeyword");
    expectedOutput.append(((char) CompletionAnalyzer.SEP_LABEL));
    expectedOutput.append("another");
    expectedOutput.append(((char) CompletionAnalyzer.SEP_LABEL));
    expectedOutput.append("keyword");
    expectedOutputs[0] = expectedOutput.toCharsRef().toString();
    expectedOutput.clear();
    expectedOutput.append("mysynonym");
    expectedOutput.append(((char) CompletionAnalyzer.SEP_LABEL));
    expectedOutput.append("another");
    expectedOutput.append(((char) CompletionAnalyzer.SEP_LABEL));
    expectedOutput.append("keyword");
    expectedOutputs[1] = expectedOutput.toCharsRef().toString();
    assertTokenStreamContents(stream, expectedOutputs, null, null, new String[]{payload.utf8ToString(), payload.utf8ToString()}, new int[]{1, 1}, null, null);
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

    CompletionTokenStream completionTokenStream = new CompletionTokenStream(filter);
    completionTokenStream.setPayload(new BytesRef());
    PayloadAttrToTypeAttrFilter stream = new PayloadAttrToTypeAttrFilter(completionTokenStream);
    stream.reset();
    CompletionTokenStream.BytesRefBuilderTermAttribute attr = stream.addAttribute(CompletionTokenStream.BytesRefBuilderTermAttribute.class);
    PositionIncrementAttribute posAttr = stream.addAttribute(PositionIncrementAttribute.class);
    int maxPos = 0;
    int count = 0;
    while(stream.incrementToken()) {
      count++;
      assertNotNull(attr.getBytesRef());
      assertTrue(attr.getBytesRef().length > 0);
      maxPos += posAttr.getPositionIncrement();
    }
    stream.close();
    assertEquals(count, 256);
    assertEquals(count, maxPos);
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
