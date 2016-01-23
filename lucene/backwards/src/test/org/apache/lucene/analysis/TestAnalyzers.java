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
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.index.Payload;
import org.apache.lucene.util.Version;

public class TestAnalyzers extends BaseTokenStreamTestCase {

   public TestAnalyzers(String name) {
      super(name);
   }

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
  }

  public void testStop() throws Exception {
    Analyzer a = new StopAnalyzer(Version.LUCENE_CURRENT);
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
    ts = new WhitespaceTokenizer(new StringReader(s));
    ts = new PayloadSetter(ts);
    verifyPayload(ts);

    ts = new WhitespaceTokenizer(new StringReader(s));
    ts = new PayloadSetter(ts);
    verifyPayload(ts);
  }

  // LUCENE-1150: Just a compile time test, to ensure the
  // StandardAnalyzer constants remain publicly accessible
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

  private static class MyStandardAnalyzer extends StandardAnalyzer {
    public MyStandardAnalyzer() {
      super(org.apache.lucene.util.Version.LUCENE_CURRENT);
    }
  
    @Override
    public TokenStream tokenStream(String field, Reader reader) {
      return new WhitespaceAnalyzer().tokenStream(field, reader);
    }
  }

  public void testSubclassOverridingOnlyTokenStream() throws Throwable {
    Analyzer a = new MyStandardAnalyzer();
    TokenStream ts = a.reusableTokenStream("field", new StringReader("the"));
    // StandardAnalyzer will discard "the" (it's a
    // stopword), by my subclass will not:
    assertTrue(ts.incrementToken());
    assertFalse(ts.incrementToken());
  }
}

class PayloadSetter extends TokenFilter {
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