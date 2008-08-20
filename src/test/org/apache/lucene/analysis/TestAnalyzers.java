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
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.index.Payload;
import org.apache.lucene.util.LuceneTestCase;

public class TestAnalyzers extends LuceneTestCase {

   public TestAnalyzers(String name) {
      super(name);
   }

  public void assertAnalyzesTo(Analyzer a, 
                               String input, 
                               String[] output) throws Exception {
    TokenStream ts = a.tokenStream("dummy", new StringReader(input));
    final Token reusableToken  = new Token();
    for (int i=0; i<output.length; i++) {
      Token nextToken = ts.next(reusableToken);
      assertNotNull(nextToken);
      assertEquals(nextToken.term(), output[i]);
    }
    assertNull(ts.next(reusableToken));
    ts.close();
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
    Analyzer a = new StopAnalyzer();
    assertAnalyzesTo(a, "foo bar FOO BAR", 
                     new String[] { "foo", "bar", "foo", "bar" });
    assertAnalyzesTo(a, "foo a bar such FOO THESE BAR", 
                     new String[] { "foo", "bar", "foo", "bar" });
  }

  void verifyPayload(TokenStream ts) throws IOException {
    final Token reusableToken = new Token();
    for(byte b=1;;b++) {
      reusableToken.clear();
      Token nextToken = ts.next(reusableToken);
      if (nextToken==null) break;
      // System.out.println("id="+System.identityHashCode(nextToken) + " " + t);
      // System.out.println("payload=" + (int)nextToken.getPayload().toByteArray()[0]);
      assertEquals(b, nextToken.getPayload().toByteArray()[0]);
    }
  }

  // Make sure old style next() calls result in a new copy of payloads
  public void testPayloadCopy() throws IOException {
    String s = "how now brown cow";
    TokenStream ts;
    ts = new WhitespaceTokenizer(new StringReader(s));
    ts = new BuffTokenFilter(ts);
    ts = new PayloadSetter(ts);
    verifyPayload(ts);

    ts = new WhitespaceTokenizer(new StringReader(s));
    ts = new PayloadSetter(ts);
    ts = new BuffTokenFilter(ts);
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
}

class BuffTokenFilter extends TokenFilter {
  List lst;

  public BuffTokenFilter(TokenStream input) {
    super(input);
  }

  public Token next(final Token reusableToken) throws IOException {
    if (lst == null) {
      lst = new LinkedList();
      for(Token nextToken = input.next(reusableToken); nextToken != null; nextToken = input.next(reusableToken)) {
        lst.add(nextToken.clone());
      }
    }
    return lst.size()==0 ? null : (Token)lst.remove(0);
  }
}

class PayloadSetter extends TokenFilter {
  public  PayloadSetter(TokenStream input) {
    super(input);
  }

  byte[] data = new byte[1];
  Payload p = new Payload(data,0,1);

  public Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    Token nextToken = input.next(reusableToken);
    if (nextToken==null) return null;
    nextToken.setPayload(p);  // reuse the payload / byte[]
    data[0]++;
    return nextToken;
  }
}