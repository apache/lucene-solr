package org.apache.lucene.analysis;

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

import java.io.IOException;
import java.io.StringReader;
import java.io.Reader;

import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Payload;

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
    for(byte b=1;;b++) {
      boolean hasNext = ts.incrementToken();
      if (!hasNext) break;
      // System.out.println("id="+System.identityHashCode(nextToken) + " " + t);
      // System.out.println("payload=" + (int)nextToken.getPayload().toByteArray()[0]);
      assertEquals(b, payloadAtt.getPayload().toByteArray()[0]);
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
    public TokenStream tokenStream(String fieldName, Reader reader) {
      return new LowerCaseFilter(TEST_VERSION_CURRENT,
          new WhitespaceTokenizer(TEST_VERSION_CURRENT, reader));
    }
    
  }
  
  /**
   * @deprecated remove this when lucene 3.0 "broken unicode 4" support
   * is no longer needed.
   */
  @Deprecated
  private static class LowerCaseWhitespaceAnalyzerBWComp extends Analyzer {

    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
      return new LowerCaseFilter(new WhitespaceTokenizer(reader));
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
    tokenizer.reset(new StringReader(highSurEndingUpper));
    assertTokenStreamContents(filter, new String[] {highSurEndingLower});
    assertTrue(filter.hasAttribute(CharTermAttribute.class));
    char[] termBuffer = filter.getAttribute(CharTermAttribute.class).buffer();
    int length = highSurEndingLower.length();
    assertEquals('\ud801', termBuffer[length - 1]);
    assertEquals('\udc3e', termBuffer[length]);
    
  }
  
  public void testLimitTokenCountAnalyzer() throws IOException {
    Analyzer a = new LimitTokenCountAnalyzer(new WhitespaceAnalyzer(TEST_VERSION_CURRENT), 2);
    // dont use assertAnalyzesTo here, as the end offset is not the end of the string!
    assertTokenStreamContents(a.tokenStream("dummy", new StringReader("1  2     3  4  5")), new String[] { "1", "2" }, new int[] { 0, 3 }, new int[] { 1, 4 }, 4);
    assertTokenStreamContents(a.reusableTokenStream("dummy", new StringReader("1 2 3 4 5")), new String[] { "1", "2" }, new int[] { 0, 2 }, new int[] { 1, 3 }, 3);
    
    a = new LimitTokenCountAnalyzer(new StandardAnalyzer(TEST_VERSION_CURRENT), 2);
    // dont use assertAnalyzesTo here, as the end offset is not the end of the string!
    assertTokenStreamContents(a.tokenStream("dummy", new StringReader("1 2 3 4 5")), new String[] { "1", "2" }, new int[] { 0, 2 }, new int[] { 1, 3 }, 3);
    assertTokenStreamContents(a.reusableTokenStream("dummy", new StringReader("1 2 3 4 5")), new String[] { "1", "2" }, new int[] { 0, 2 }, new int[] { 1, 3 }, 3);
  }
  
  /**
   * Test that LowercaseFilter only works on BMP for back compat,
   * depending upon version
   * @deprecated remove this test when lucene 3.0 "broken unicode 4" support
   * is no longer needed.
   */
  @Deprecated
  public void testLowerCaseFilterBWComp() throws IOException {
    Analyzer a = new LowerCaseWhitespaceAnalyzerBWComp();
    // BMP
    assertAnalyzesTo(a, "AbaCaDabA", new String[] { "abacadaba" });
    // supplementary, no-op
    assertAnalyzesTo(a, "\ud801\udc16\ud801\udc16\ud801\udc16\ud801\udc16",
        new String[] {"\ud801\udc16\ud801\udc16\ud801\udc16\ud801\udc16"});
    assertAnalyzesTo(a, "AbaCa\ud801\udc16DabA",
        new String[] { "abaca\ud801\udc16daba" });
    // unpaired lead surrogate
    assertAnalyzesTo(a, "AbaC\uD801AdaBa", 
        new String [] { "abac\uD801adaba" });
    // unpaired trail surrogate
    assertAnalyzesTo(a, "AbaC\uDC16AdaBa", 
        new String [] { "abac\uDC16adaba" });
  }

  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    checkRandomData(random, new WhitespaceAnalyzer(TEST_VERSION_CURRENT), 10000*RANDOM_MULTIPLIER);
    checkRandomData(random, new SimpleAnalyzer(TEST_VERSION_CURRENT), 10000*RANDOM_MULTIPLIER);
    checkRandomData(random, new StopAnalyzer(TEST_VERSION_CURRENT), 10000*RANDOM_MULTIPLIER);
  } 
}

final class PayloadSetter extends TokenFilter {
  PayloadAttribute payloadAtt;
  public  PayloadSetter(TokenStream input) {
    super(input);
    payloadAtt = addAttribute(PayloadAttribute.class);
  }

  byte[] data = new byte[1];
  Payload p = new Payload(data,0,1);

  @Override
  public boolean incrementToken() throws IOException {
    boolean hasNext = input.incrementToken();
    if (!hasNext) return false;
    payloadAtt.setPayload(p);  // reuse the payload / byte[]
    data[0]++;
    return true;
  }
}