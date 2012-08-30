package org.apache.lucene.analysis.core;

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
import java.io.Reader;
import java.io.StringReader;
import java.util.Random;

import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public class TestAnalyzers extends BaseTokenStreamTestCase {

  public void testSimple() throws Exception {
    Analyzer a = new SimpleAnalyzer(TEST_VERSION_CURRENT);
    assertAnalyzesTo(a, "foo bar FOO BAR", 
                     new String[] { "foo", "bar", "foo", "bar" });
    assertAnalyzesTo(a, "foo      bar .  FOO <> BAR", 
                     new String[] { "foo", "bar", "foo", "bar" });
    assertAnalyzesTo(a, "foo.bar.FOO.BAR", 
                     new String[] { "foo", "bar", "foo", "bar" });
    assertAnalyzesTo(a, "U.S.A.", 
                     new String[] { "u", "s", "a" });
    assertAnalyzesTo(a, "C++", 
                     new String[] { "c" });
    assertAnalyzesTo(a, "B2B", 
                     new String[] { "b", "b" });
    assertAnalyzesTo(a, "2B", 
                     new String[] { "b" });
    assertAnalyzesTo(a, "\"QUOTED\" word", 
                     new String[] { "quoted", "word" });
  }

  public void testNull() throws Exception {
    Analyzer a = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    assertAnalyzesTo(a, "foo bar FOO BAR", 
                     new String[] { "foo", "bar", "FOO", "BAR" });
    assertAnalyzesTo(a, "foo      bar .  FOO <> BAR", 
                     new String[] { "foo", "bar", ".", "FOO", "<>", "BAR" });
    assertAnalyzesTo(a, "foo.bar.FOO.BAR", 
                     new String[] { "foo.bar.FOO.BAR" });
    assertAnalyzesTo(a, "U.S.A.", 
                     new String[] { "U.S.A." });
    assertAnalyzesTo(a, "C++", 
                     new String[] { "C++" });
    assertAnalyzesTo(a, "B2B", 
                     new String[] { "B2B" });
    assertAnalyzesTo(a, "2B", 
                     new String[] { "2B" });
    assertAnalyzesTo(a, "\"QUOTED\" word", 
                     new String[] { "\"QUOTED\"", "word" });
  }

  public void testStop() throws Exception {
    Analyzer a = new StopAnalyzer(TEST_VERSION_CURRENT);
    assertAnalyzesTo(a, "foo bar FOO BAR", 
                     new String[] { "foo", "bar", "foo", "bar" });
    assertAnalyzesTo(a, "foo a bar such FOO THESE BAR", 
                     new String[] { "foo", "bar", "foo", "bar" });
  }

  void verifyPayload(TokenStream ts) throws IOException {
    PayloadAttribute payloadAtt = ts.getAttribute(PayloadAttribute.class);
    ts.reset();
    for(byte b=1;;b++) {
      boolean hasNext = ts.incrementToken();
      if (!hasNext) break;
      // System.out.println("id="+System.identityHashCode(nextToken) + " " + t);
      // System.out.println("payload=" + (int)nextToken.getPayload().toByteArray()[0]);
      assertEquals(b, payloadAtt.getPayload().bytes[0]);
    }
  }

  // Make sure old style next() calls result in a new copy of payloads
  public void testPayloadCopy() throws IOException {
    String s = "how now brown cow";
    TokenStream ts;
    ts = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(s));
    ts = new PayloadSetter(ts);
    verifyPayload(ts);

    ts = new WhitespaceTokenizer(TEST_VERSION_CURRENT, new StringReader(s));
    ts = new PayloadSetter(ts);
    verifyPayload(ts);
  }

  // LUCENE-1150: Just a compile time test, to ensure the
  // StandardAnalyzer constants remain publicly accessible
  @SuppressWarnings("unused")
  public void _testStandardConstants() {
    int x = StandardTokenizer.ALPHANUM;
    x = StandardTokenizer.APOSTROPHE;
    x = StandardTokenizer.ACRONYM;
    x = StandardTokenizer.COMPANY;
    x = StandardTokenizer.EMAIL;
    x = StandardTokenizer.HOST;
    x = StandardTokenizer.NUM;
    x = StandardTokenizer.CJ;
    String[] y = StandardTokenizer.TOKEN_TYPES;
  }

  private static class LowerCaseWhitespaceAnalyzer extends Analyzer {

    @Override
    public TokenStreamComponents createComponents(String fieldName, Reader reader) {
      Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, reader);
      return new TokenStreamComponents(tokenizer, new LowerCaseFilter(TEST_VERSION_CURRENT, tokenizer));
    }
    
  }
  
  /**
   * Test that LowercaseFilter handles entire unicode range correctly
   */
  public void testLowerCaseFilter() throws IOException {
    Analyzer a = new LowerCaseWhitespaceAnalyzer();
    // BMP
    assertAnalyzesTo(a, "AbaCaDabA", new String[] { "abacadaba" });
    // supplementary
    assertAnalyzesTo(a, "\ud801\udc16\ud801\udc16\ud801\udc16\ud801\udc16",
        new String[] {"\ud801\udc3e\ud801\udc3e\ud801\udc3e\ud801\udc3e"});
    assertAnalyzesTo(a, "AbaCa\ud801\udc16DabA", 
        new String[] { "abaca\ud801\udc3edaba" });
    // unpaired lead surrogate
    assertAnalyzesTo(a, "AbaC\uD801AdaBa", 
        new String [] { "abac\uD801adaba" });
    // unpaired trail surrogate
    assertAnalyzesTo(a, "AbaC\uDC16AdaBa", 
        new String [] { "abac\uDC16adaba" });
  }
  
  /**
   * Test that LowercaseFilter handles the lowercasing correctly if the term
   * buffer has a trailing surrogate character leftover and the current term in
   * the buffer ends with a corresponding leading surrogate.
   */
  public void testLowerCaseFilterLowSurrogateLeftover() throws IOException {
    // test if the limit of the termbuffer is correctly used with supplementary
    // chars
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT, 
        new StringReader("BogustermBogusterm\udc16"));
    LowerCaseFilter filter = new LowerCaseFilter(TEST_VERSION_CURRENT,
        tokenizer);
    assertTokenStreamContents(filter, new String[] {"bogustermbogusterm\udc16"});
    filter.reset();
    String highSurEndingUpper = "BogustermBoguster\ud801";
    String highSurEndingLower = "bogustermboguster\ud801";
    tokenizer.setReader(new StringReader(highSurEndingUpper));
    assertTokenStreamContents(filter, new String[] {highSurEndingLower});
    assertTrue(filter.hasAttribute(CharTermAttribute.class));
    char[] termBuffer = filter.getAttribute(CharTermAttribute.class).buffer();
    int length = highSurEndingLower.length();
    assertEquals('\ud801', termBuffer[length - 1]);
    assertEquals('\udc3e', termBuffer[length]);
    
  }
  
  public void testLowerCaseTokenizer() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    LowerCaseTokenizer tokenizer = new LowerCaseTokenizer(TEST_VERSION_CURRENT,
        reader);
    assertTokenStreamContents(tokenizer, new String[] { "tokenizer",
        "\ud801\udc44test" });
  }

  /** @deprecated (3.1) */
  @Deprecated
  public void testLowerCaseTokenizerBWCompat() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    LowerCaseTokenizer tokenizer = new LowerCaseTokenizer(Version.LUCENE_30,
        reader);
    assertTokenStreamContents(tokenizer, new String[] { "tokenizer", "test" });
  }

  public void testWhitespaceTokenizer() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT,
        reader);
    assertTokenStreamContents(tokenizer, new String[] { "Tokenizer",
        "\ud801\udc1ctest" });
  }

  /** @deprecated (3.1) */
  @Deprecated
  public void testWhitespaceTokenizerBWCompat() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer(Version.LUCENE_30,
        reader);
    assertTokenStreamContents(tokenizer, new String[] { "Tokenizer",
        "\ud801\udc1ctest" });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random(), new WhitespaceAnalyzer(TEST_VERSION_CURRENT), 1000*RANDOM_MULTIPLIER);
    checkRandomData(random(), new SimpleAnalyzer(TEST_VERSION_CURRENT), 1000*RANDOM_MULTIPLIER);
    checkRandomData(random(), new StopAnalyzer(TEST_VERSION_CURRENT), 1000*RANDOM_MULTIPLIER);
  }
  
  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Random random = random();
    checkRandomData(random, new WhitespaceAnalyzer(TEST_VERSION_CURRENT), 100*RANDOM_MULTIPLIER, 8192);
    checkRandomData(random, new SimpleAnalyzer(TEST_VERSION_CURRENT), 100*RANDOM_MULTIPLIER, 8192);
    checkRandomData(random, new StopAnalyzer(TEST_VERSION_CURRENT), 100*RANDOM_MULTIPLIER, 8192);
  } 
}

final class PayloadSetter extends TokenFilter {
  PayloadAttribute payloadAtt;
  public  PayloadSetter(TokenStream input) {
    super(input);
    payloadAtt = addAttribute(PayloadAttribute.class);
  }

  byte[] data = new byte[1];
  BytesRef p = new BytesRef(data,0,1);

  @Override
  public boolean incrementToken() throws IOException {
    boolean hasNext = input.incrementToken();
    if (!hasNext) return false;
    payloadAtt.setPayload(p);  // reuse the payload / byte[]
    data[0]++;
    return true;
  }
}
