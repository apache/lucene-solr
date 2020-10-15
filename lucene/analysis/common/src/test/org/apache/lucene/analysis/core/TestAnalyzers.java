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
package org.apache.lucene.analysis.core;


import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class TestAnalyzers extends BaseTokenStreamTestCase {

  public void testSimple() throws Exception {
    Analyzer a = new SimpleAnalyzer();
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
    assertEquals(new BytesRef("\"\\à3[]()! cz@"), a.normalize("dummy", "\"\\À3[]()! Cz@"));
    a.close();
  }

  public void testNull() throws Exception {
    Analyzer a = new WhitespaceAnalyzer();
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
    assertEquals(new BytesRef("\"\\À3[]()! Cz@"), a.normalize("dummy", "\"\\À3[]()! Cz@"));
    a.close();
  }

  public void testStop() throws Exception {
    Analyzer a = new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
    assertAnalyzesTo(a, "foo bar FOO BAR", 
                     new String[] { "foo", "bar", "foo", "bar" });
    assertAnalyzesTo(a, "foo a bar such FOO THESE BAR", 
                     new String[] { "foo", "bar", "foo", "bar" });
    assertEquals(new BytesRef("\"\\à3[]()! cz@"), a.normalize("dummy", "\"\\À3[]()! Cz@"));
    assertEquals(new BytesRef("the"), a.normalize("dummy", "the"));
    a.close();
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
    ts = new WhitespaceTokenizer();
    ((Tokenizer)ts).setReader(new StringReader(s));
    ts = new PayloadSetter(ts);
    verifyPayload(ts);

    ts = new WhitespaceTokenizer();
    ((Tokenizer)ts).setReader(new StringReader(s));
    ts = new PayloadSetter(ts);
    verifyPayload(ts);
  }

  // LUCENE-1150: Just a compile time test, to ensure the
  // StandardAnalyzer constants remain publicly accessible
  @SuppressWarnings("unused")
  public void _testStandardConstants() {
    int x = StandardTokenizer.ALPHANUM;
    x = StandardTokenizer.NUM;
    x = StandardTokenizer.SOUTHEAST_ASIAN;
    x = StandardTokenizer.IDEOGRAPHIC;
    x = StandardTokenizer.HIRAGANA;
    x = StandardTokenizer.KATAKANA;
    x = StandardTokenizer.HANGUL;
    String[] y = StandardTokenizer.TOKEN_TYPES;
  }

  private static class LowerCaseWhitespaceAnalyzer extends Analyzer {

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = random().nextBoolean() ? new WhitespaceTokenizer() : new UnicodeWhitespaceTokenizer();
      return new TokenStreamComponents(tokenizer, new LowerCaseFilter(tokenizer));
    }
    
  }
  
  private static class UpperCaseWhitespaceAnalyzer extends Analyzer {

    @Override
    public TokenStreamComponents createComponents(String fieldName) {
      Tokenizer tokenizer = random().nextBoolean() ? new WhitespaceTokenizer() : new UnicodeWhitespaceTokenizer();
      return new TokenStreamComponents(tokenizer, new UpperCaseFilter(tokenizer));
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
    a.close();
  }

  /**
   * Test that LowercaseFilter handles entire unicode range correctly
   */
  public void testUpperCaseFilter() throws IOException {
    Analyzer a = new UpperCaseWhitespaceAnalyzer();
    // BMP
    assertAnalyzesTo(a, "AbaCaDabA", new String[] { "ABACADABA" });
    // supplementary
    assertAnalyzesTo(a, "\ud801\udc3e\ud801\udc3e\ud801\udc3e\ud801\udc3e",
          new String[] {"\ud801\udc16\ud801\udc16\ud801\udc16\ud801\udc16"});
    assertAnalyzesTo(a, "AbaCa\ud801\udc3eDabA", 
         new String[] { "ABACA\ud801\udc16DABA" });
    // unpaired lead surrogate
    assertAnalyzesTo(a, "AbaC\uD801AdaBa", 
        new String [] { "ABAC\uD801ADABA" });
    // unpaired trail surrogate
    assertAnalyzesTo(a, "AbaC\uDC16AdaBa", 
        new String [] { "ABAC\uDC16ADABA" });
    a.close();
  }
  
  /**
   * Test that LowercaseFilter handles the lowercasing correctly if the term
   * buffer has a trailing surrogate character leftover and the current term in
   * the buffer ends with a corresponding leading surrogate.
   */
  public void testLowerCaseFilterLowSurrogateLeftover() throws IOException {
    // test if the limit of the termbuffer is correctly used with supplementary
    // chars
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
    tokenizer.setReader(new StringReader("BogustermBogusterm\udc16"));
    LowerCaseFilter filter = new LowerCaseFilter(tokenizer);
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
  }

  public void testWhitespaceTokenizer() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer, new String[] { "Tokenizer",
        "\ud801\udc1ctest" });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer analyzers[] = new Analyzer[] { new WhitespaceAnalyzer(), new SimpleAnalyzer(),
        new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET), new UnicodeWhitespaceAnalyzer() };
    for (Analyzer analyzer : analyzers) {
      checkRandomData(random(), analyzer, 100*RANDOM_MULTIPLIER);
    }
    IOUtils.close(analyzers);
  }
  
  /** blast some random large strings through the analyzer */
  public void testRandomHugeStrings() throws Exception {
    Analyzer analyzers[] = new Analyzer[] { new WhitespaceAnalyzer(), new SimpleAnalyzer(),
        new StopAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET), new UnicodeWhitespaceAnalyzer() };
    for (Analyzer analyzer : analyzers) {
      checkRandomData(random(), analyzer, 10*RANDOM_MULTIPLIER, 8192);
    }
    IOUtils.close(analyzers);
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
