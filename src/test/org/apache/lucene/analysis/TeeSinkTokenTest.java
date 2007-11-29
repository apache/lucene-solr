package org.apache.lucene.analysis;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import junit.framework.TestCase;

import java.io.StringReader;
import java.io.IOException;

/**
 * tests for the TeeTokenFilter and SinkTokenizer
 */
public class TeeSinkTokenTest extends TestCase {
  protected StringBuffer buffer1;
  protected StringBuffer buffer2;
  protected String[] tokens1;
  protected String[] tokens2;


  public TeeSinkTokenTest(String s) {
    super(s);
  }

  protected void setUp() {
    tokens1 = new String[]{"The", "quick", "Burgundy", "Fox", "jumped", "over", "the", "lazy", "Red", "Dogs"};
    tokens2 = new String[]{"The", "Lazy", "Dogs", "should", "stay", "on", "the", "porch"};
    buffer1 = new StringBuffer();

    for (int i = 0; i < tokens1.length; i++) {
      buffer1.append(tokens1[i]).append(' ');
    }
    buffer2 = new StringBuffer();
    for (int i = 0; i < tokens2.length; i++) {
      buffer2.append(tokens2[i]).append(' ');

    }
  }

  protected void tearDown() {

  }

  public void test() throws IOException {

    SinkTokenizer sink1 = new SinkTokenizer(null){
      public void add(Token t) {
        if (t != null && t.termText().equalsIgnoreCase("The")){
          super.add(t);
        }
      }
    };
    TokenStream source = new TeeTokenFilter(new WhitespaceTokenizer(new StringReader(buffer1.toString())), sink1);
    Token token = null;
    int i = 0;
    while ((token = source.next()) != null){
      assertTrue(token.termText() + " is not equal to " + tokens1[i], token.termText().equals(tokens1[i]) == true);
      i++;
    }
    assertTrue(i + " does not equal: " + tokens1.length, i == tokens1.length);
    assertTrue("sink1 Size: " + sink1.getTokens().size() + " is not: " + 2, sink1.getTokens().size() == 2);
    i = 0;
    while ((token = sink1.next()) != null){
      assertTrue(token.termText() + " is not equal to " + "The", token.termText().equalsIgnoreCase("The") == true);
      i++;
    }
    assertTrue(i + " does not equal: " + sink1.getTokens().size(), i == sink1.getTokens().size());
  }

  public void testMultipleSources() throws Exception {
    SinkTokenizer theDetector = new SinkTokenizer(null){
      public void add(Token t) {
        if (t != null && t.termText().equalsIgnoreCase("The")){
          super.add(t);
        }
      }
    };
    SinkTokenizer dogDetector = new SinkTokenizer(null){
      public void add(Token t) {
        if (t != null && t.termText().equalsIgnoreCase("Dogs")){
          super.add(t);
        }
      }
    };
    TokenStream source1 = new CachingTokenFilter(new TeeTokenFilter(new TeeTokenFilter(new WhitespaceTokenizer(new StringReader(buffer1.toString())), theDetector), dogDetector));
    TokenStream source2 = new TeeTokenFilter(new TeeTokenFilter(new WhitespaceTokenizer(new StringReader(buffer2.toString())), theDetector), dogDetector);
    Token token = null;
    int i = 0;
    while ((token = source1.next()) != null){
      assertTrue(token.termText() + " is not equal to " + tokens1[i], token.termText().equals(tokens1[i]) == true);
      i++;
    }
    assertTrue(i + " does not equal: " + tokens1.length, i == tokens1.length);
    assertTrue("theDetector Size: " + theDetector.getTokens().size() + " is not: " + 2, theDetector.getTokens().size() == 2);
    assertTrue("dogDetector Size: " + dogDetector.getTokens().size() + " is not: " + 1, dogDetector.getTokens().size() == 1);
    i = 0;
    while ((token = source2.next()) != null){
      assertTrue(token.termText() + " is not equal to " + tokens2[i], token.termText().equals(tokens2[i]) == true);
      i++;
    }
    assertTrue(i + " does not equal: " + tokens2.length, i == tokens2.length);
    assertTrue("theDetector Size: " + theDetector.getTokens().size() + " is not: " + 4, theDetector.getTokens().size() == 4);
    assertTrue("dogDetector Size: " + dogDetector.getTokens().size() + " is not: " + 2, dogDetector.getTokens().size() == 2);
    i = 0;
    while ((token = theDetector.next()) != null){
      assertTrue(token.termText() + " is not equal to " + "The", token.termText().equalsIgnoreCase("The") == true);
      i++;
    }
    assertTrue(i + " does not equal: " + theDetector.getTokens().size(), i == theDetector.getTokens().size());
    i = 0;
    while ((token = dogDetector.next()) != null){
      assertTrue(token.termText() + " is not equal to " + "Dogs", token.termText().equalsIgnoreCase("Dogs") == true);
      i++;
    }
    assertTrue(i + " does not equal: " + dogDetector.getTokens().size(), i == dogDetector.getTokens().size());
    source1.reset();
    TokenStream lowerCasing = new LowerCaseFilter(source1);
    i = 0;
    while ((token = lowerCasing.next()) != null){
      assertTrue(token.termText() + " is not equal to " + tokens1[i].toLowerCase(), token.termText().equals(tokens1[i].toLowerCase()) == true);
      i++;
    }
    assertTrue(i + " does not equal: " + tokens1.length, i == tokens1.length);
  }
}