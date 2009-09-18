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

package org.apache.solr.analysis;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.io.IOException;
import java.io.StringReader;

/**
 * Test that BufferedTokenStream behaves as advertised in subclasses.
 */
public class TestBufferedTokenStream extends BaseTokenTestCase {

  /** Example of a class implementing the rule "A" "B" => "Q" "B" */
  public static class AB_Q_Stream extends BufferedTokenStream {
    public AB_Q_Stream(TokenStream input) {super(input);}
    protected Token process(Token t) throws IOException {
      if ("A".equals(new String(t.termBuffer(), 0, t.termLength()))) {
        Token t2 = read();
        if (t2!=null && "B".equals(new String(t2.termBuffer(), 0, t2.termLength()))) t.setTermText("Q");
        if (t2!=null) pushBack(t2);
      }
      return t;
    }
  }

  /** Example of a class implementing "A" "B" => "A" "A" "B" */
  public static class AB_AAB_Stream extends BufferedTokenStream {
    public AB_AAB_Stream(TokenStream input) {super(input);}
    protected Token process(Token t) throws IOException {
      if ("A".equals(new String(t.termBuffer(), 0, t.termLength())) && 
          "B".equals(new String(peek(1).termBuffer(), 0, peek(1).termLength())))
        write(t);
      return t;
    }
  }
    
  public void testABQ() throws Exception {
    final String input = "How now A B brown A cow B like A B thing?";
    final String expected = "How now Q B brown A cow B like Q B thing?";
    TokenStream ts = new AB_Q_Stream
      (new WhitespaceTokenizer(new StringReader(input)));
    final String actual = tsToString(ts);
    //System.out.println(actual);
    assertEquals(expected, actual);
  }
  
  public void testABAAB() throws Exception {
    final String input = "How now A B brown A cow B like A B thing?";
    final String expected = "How now A A B brown A cow B like A A B thing?";
    TokenStream ts = new AB_AAB_Stream
      (new WhitespaceTokenizer(new StringReader(input)));
    final String actual = tsToString(ts);
    //System.out.println(actual);
    assertEquals(expected, actual);
  }
  
  public void testReset() throws Exception {
    final String input = "How now A B brown A cow B like A B thing?";
    Tokenizer tokenizer = new WhitespaceTokenizer(new StringReader(input));
    TokenStream ts = new AB_AAB_Stream(tokenizer);
    TermAttribute term = (TermAttribute) ts.addAttribute(TermAttribute.class);
    assertTrue(ts.incrementToken());
    assertEquals("How", term.term());
    assertTrue(ts.incrementToken());
    assertEquals("now", term.term());
    assertTrue(ts.incrementToken());
    assertEquals("A", term.term());
    // reset back to input, 
    // if reset() does not work correctly then previous buffered tokens will remain 
    tokenizer.reset(new StringReader(input));
    ts.reset();
    assertTrue(ts.incrementToken());
    assertEquals("How", term.term());
  }
}
