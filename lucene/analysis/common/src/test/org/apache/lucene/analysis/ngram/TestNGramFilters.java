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
package org.apache.lucene.analysis.ngram;


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * Simple tests to ensure the NGram filter factories are working.
 */
public class TestNGramFilters extends BaseTokenStreamFactoryTestCase {
  /**
   * Test NGramTokenizerFactory
   */
  public void testNGramTokenizer() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("NGram").create();
    ((Tokenizer)stream).setReader(reader);
    assertTokenStreamContents(stream,
        new String[]{"t", "te", "e", "es", "s", "st", "t"});
  }

  /**
   * Test NGramTokenizerFactory with min and max gram options
   */
  public void testNGramTokenizer2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("NGram",
        "minGramSize", "2",
        "maxGramSize", "3").create();
    ((Tokenizer)stream).setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] { "te", "tes", "es", "est", "st" });
  }

  /**
   * Test the NGramFilterFactory with old defaults
   */
  public void testNGramFilter() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("NGram",
        "minGramSize", "1",
        "maxGramSize", "2").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "t", "te", "e", "es", "s", "st", "t" });
  }

  /**
   * Test the NGramFilterFactory with min and max gram options
   */
  public void testNGramFilter2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("NGram",
        "minGramSize", "2",
        "maxGramSize", "3").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "te", "tes", "es", "est", "st" });
  }

  /**
   * Test the NGramFilterFactory with preserve option
   */
  public void testNGramFilter3() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("NGram",
        "minGramSize", "2",
        "maxGramSize", "3",
        "preserveOriginal", "true").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "te", "tes", "es", "est", "st", "test" });
  }

  /**
   * Test NGramFilterFactory on tokens with payloads
   */
  public void testNGramFilterPayload() throws Exception {
    Reader reader = new StringReader("test|0.1");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("DelimitedPayload", "encoder", "float").create(stream);
    stream = tokenFilterFactory("NGram", "minGramSize", "1", "maxGramSize", "2").create(stream);

    stream.reset();
    while (stream.incrementToken()) {
      PayloadAttribute payAttr = stream.getAttribute(PayloadAttribute.class);
      assertNotNull(payAttr);
      BytesRef payData = payAttr.getPayload();
      assertNotNull(payData);
      float payFloat = PayloadHelper.decodeFloat(payData.bytes);
      assertEquals(0.1f, payFloat, 0.0f);
    }
    stream.end();
    stream.close();
  }

  /**
   * Test EdgeNGramTokenizerFactory
   */
  public void testEdgeNGramTokenizer() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("EdgeNGram").create();
    ((Tokenizer)stream).setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] { "t" });
  }

  /**
   * Test EdgeNGramTokenizerFactory with min and max gram size
   */
  public void testEdgeNGramTokenizer2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = tokenizerFactory("EdgeNGram",
        "minGramSize", "1",
        "maxGramSize", "2").create();
    ((Tokenizer)stream).setReader(reader);
    assertTokenStreamContents(stream, 
        new String[] { "t", "te" });
  }

  /**
   * Test EdgeNGramFilterFactory with old defaults
   */
  public void testEdgeNGramFilter() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("EdgeNGram", "minGramSize", "1",
        "maxGramSize", "1").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "t" });
  }

  /**
   * Test EdgeNGramFilterFactory with min and max gram size
   */
  public void testEdgeNGramFilter2() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("EdgeNGram",
        "minGramSize", "1",
        "maxGramSize", "2").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "t", "te" });
  }

  /**
   * Test EdgeNGramFilterFactory with preserve option
   */
  public void testEdgeNGramFilter3() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("EdgeNGram",
        "minGramSize", "1",
        "maxGramSize", "2",
        "preserveOriginal", "true").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "t", "te", "test" });
  }

  /**
   * Test EdgeNGramFilterFactory on tokens with payloads
   */
  public void testEdgeNGramFilterPayload() throws Exception {
    Reader reader = new StringReader("test|0.1");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("DelimitedPayload", "encoder", "float").create(stream);
    stream = tokenFilterFactory("EdgeNGram", "minGramSize", "1", "maxGramSize", "2").create(stream);

    stream.reset();
    while (stream.incrementToken()) {
      PayloadAttribute payAttr = stream.getAttribute(PayloadAttribute.class);
      assertNotNull(payAttr);
      BytesRef payData = payAttr.getPayload();
      assertNotNull(payData);
      float payFloat = PayloadHelper.decodeFloat(payData.bytes);
      assertEquals(0.1f, payFloat, 0.0f);
    }
    stream.end();
    stream.close();
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = null;
    expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenizerFactory("NGram", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenizerFactory("EdgeNGram", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("NGram", "minGramSize", "2", "maxGramSize", "5", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("EdgeNGram", "minGramSize", "2", "maxGramSize", "5", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
