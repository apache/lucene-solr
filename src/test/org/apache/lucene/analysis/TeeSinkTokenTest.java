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

import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.English;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * tests for the TeeTokenFilter and SinkTokenizer
 */
public class TeeSinkTokenTest extends LuceneTestCase {
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

    SinkTokenizer sink1 = new SinkTokenizer(null) {
      public void add(Token t) {
        if (t != null && t.term().equalsIgnoreCase("The")) {
          super.add(t);
        }
      }
    };
    TokenStream source = new TeeTokenFilter(new WhitespaceTokenizer(new StringReader(buffer1.toString())), sink1);
    int i = 0;
    final Token reusableToken = new Token();
    for (Token nextToken = source.next(reusableToken); nextToken != null; nextToken = source.next(reusableToken)) {
      assertTrue(nextToken.term() + " is not equal to " + tokens1[i], nextToken.term().equals(tokens1[i]) == true);
      i++;
    }
    assertTrue(i + " does not equal: " + tokens1.length, i == tokens1.length);
    assertTrue("sink1 Size: " + sink1.getTokens().size() + " is not: " + 2, sink1.getTokens().size() == 2);
    i = 0;
    for (Token token = sink1.next(reusableToken); token != null; token = sink1.next(reusableToken)) {
      assertTrue(token.term() + " is not equal to " + "The", token.term().equalsIgnoreCase("The") == true);
      i++;
    }
    assertTrue(i + " does not equal: " + sink1.getTokens().size(), i == sink1.getTokens().size());
  }

  public void testMultipleSources() throws Exception {
    SinkTokenizer theDetector = new SinkTokenizer(null) {
      public void add(Token t) {
        if (t != null && t.term().equalsIgnoreCase("The")) {
          super.add(t);
        }
      }
    };
    SinkTokenizer dogDetector = new SinkTokenizer(null) {
      public void add(Token t) {
        if (t != null && t.term().equalsIgnoreCase("Dogs")) {
          super.add(t);
        }
      }
    };
    TokenStream source1 = new CachingTokenFilter(new TeeTokenFilter(new TeeTokenFilter(new WhitespaceTokenizer(new StringReader(buffer1.toString())), theDetector), dogDetector));
    TokenStream source2 = new TeeTokenFilter(new TeeTokenFilter(new WhitespaceTokenizer(new StringReader(buffer2.toString())), theDetector), dogDetector);
    int i = 0;
    final Token reusableToken = new Token();
    for (Token nextToken = source1.next(reusableToken); nextToken != null; nextToken = source1.next(reusableToken)) {
      assertTrue(nextToken.term() + " is not equal to " + tokens1[i], nextToken.term().equals(tokens1[i]) == true);
      i++;
    }
    assertTrue(i + " does not equal: " + tokens1.length, i == tokens1.length);
    assertTrue("theDetector Size: " + theDetector.getTokens().size() + " is not: " + 2, theDetector.getTokens().size() == 2);
    assertTrue("dogDetector Size: " + dogDetector.getTokens().size() + " is not: " + 1, dogDetector.getTokens().size() == 1);
    i = 0;
    for (Token nextToken = source2.next(reusableToken); nextToken != null; nextToken = source2.next(reusableToken)) {
      assertTrue(nextToken.term() + " is not equal to " + tokens2[i], nextToken.term().equals(tokens2[i]) == true);
      i++;
    }
    assertTrue(i + " does not equal: " + tokens2.length, i == tokens2.length);
    assertTrue("theDetector Size: " + theDetector.getTokens().size() + " is not: " + 4, theDetector.getTokens().size() == 4);
    assertTrue("dogDetector Size: " + dogDetector.getTokens().size() + " is not: " + 2, dogDetector.getTokens().size() == 2);
    i = 0;
    for (Token nextToken = theDetector.next(reusableToken); nextToken != null; nextToken = theDetector.next(reusableToken)) {
      assertTrue(nextToken.term() + " is not equal to " + "The", nextToken.term().equalsIgnoreCase("The") == true);
      i++;
    }
    assertTrue(i + " does not equal: " + theDetector.getTokens().size(), i == theDetector.getTokens().size());
    i = 0;
    for (Token nextToken = dogDetector.next(reusableToken); nextToken != null; nextToken = dogDetector.next(reusableToken)) {
      assertTrue(nextToken.term() + " is not equal to " + "Dogs", nextToken.term().equalsIgnoreCase("Dogs") == true);
      i++;
    }
    assertTrue(i + " does not equal: " + dogDetector.getTokens().size(), i == dogDetector.getTokens().size());
    source1.reset();
    TokenStream lowerCasing = new LowerCaseFilter(source1);
    i = 0;
    for (Token nextToken = lowerCasing.next(reusableToken); nextToken != null; nextToken = lowerCasing.next(reusableToken)) {
      assertTrue(nextToken.term() + " is not equal to " + tokens1[i].toLowerCase(), nextToken.term().equals(tokens1[i].toLowerCase()) == true);
      i++;
    }
    assertTrue(i + " does not equal: " + tokens1.length, i == tokens1.length);
  }

  /**
   * Not an explicit test, just useful to print out some info on performance
   *
   * @throws Exception
   */
  public void testPerformance() throws Exception {
    int[] tokCount = {100, 500, 1000, 2000, 5000, 10000};
    int[] modCounts = {1, 2, 5, 10, 20, 50, 100, 200, 500};
    for (int k = 0; k < tokCount.length; k++) {
      StringBuffer buffer = new StringBuffer();
      System.out.println("-----Tokens: " + tokCount[k] + "-----");
      for (int i = 0; i < tokCount[k]; i++) {
        buffer.append(English.intToEnglish(i).toUpperCase()).append(' ');
      }
      //make sure we produce the same tokens
      ModuloSinkTokenizer sink = new ModuloSinkTokenizer(tokCount[k], 100);
      final Token reusableToken = new Token();
      TokenStream stream = new TeeTokenFilter(new StandardFilter(new StandardTokenizer(new StringReader(buffer.toString()))), sink);
      while (stream.next(reusableToken) != null) {
      }
      stream = new ModuloTokenFilter(new StandardFilter(new StandardTokenizer(new StringReader(buffer.toString()))), 100);
      List tmp = new ArrayList();
      for (Token nextToken = stream.next(reusableToken); nextToken != null; nextToken = stream.next(reusableToken)) {
        tmp.add(nextToken.clone());
      }
      List sinkList = sink.getTokens();
      assertTrue("tmp Size: " + tmp.size() + " is not: " + sinkList.size(), tmp.size() == sinkList.size());
      for (int i = 0; i < tmp.size(); i++) {
        Token tfTok = (Token) tmp.get(i);
        Token sinkTok = (Token) sinkList.get(i);
        assertTrue(tfTok.term() + " is not equal to " + sinkTok.term() + " at token: " + i, tfTok.term().equals(sinkTok.term()) == true);
      }
      //simulate two fields, each being analyzed once, for 20 documents

      for (int j = 0; j < modCounts.length; j++) {
        int tfPos = 0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 20; i++) {
          stream = new StandardFilter(new StandardTokenizer(new StringReader(buffer.toString())));
          for (Token nextToken = stream.next(reusableToken); nextToken != null; nextToken = stream.next(reusableToken)) {
            tfPos += nextToken.getPositionIncrement();
          }
          stream = new ModuloTokenFilter(new StandardFilter(new StandardTokenizer(new StringReader(buffer.toString()))), modCounts[j]);
          for (Token nextToken = stream.next(reusableToken); nextToken != null; nextToken = stream.next(reusableToken)) {
            tfPos += nextToken.getPositionIncrement();
          }
        }
        long finish = System.currentTimeMillis();
        System.out.println("ModCount: " + modCounts[j] + " Two fields took " + (finish - start) + " ms");
        int sinkPos = 0;
        //simulate one field with one sink
        start = System.currentTimeMillis();
        for (int i = 0; i < 20; i++) {
          sink = new ModuloSinkTokenizer(tokCount[k], modCounts[j]);
          stream = new TeeTokenFilter(new StandardFilter(new StandardTokenizer(new StringReader(buffer.toString()))), sink);
          for (Token nextToken = stream.next(reusableToken); nextToken != null; nextToken = stream.next(reusableToken)) {
            sinkPos += nextToken.getPositionIncrement();
          }
          //System.out.println("Modulo--------");
          stream = sink;
          for (Token nextToken = stream.next(reusableToken); nextToken != null; nextToken = stream.next(reusableToken)) {
            sinkPos += nextToken.getPositionIncrement();
          }
        }
        finish = System.currentTimeMillis();
        System.out.println("ModCount: " + modCounts[j] + " Tee fields took " + (finish - start) + " ms");
        assertTrue(sinkPos + " does not equal: " + tfPos, sinkPos == tfPos);

      }
      System.out.println("- End Tokens: " + tokCount[k] + "-----");
    }

  }


  class ModuloTokenFilter extends TokenFilter {

    int modCount;

    ModuloTokenFilter(TokenStream input, int mc) {
      super(input);
      modCount = mc;
    }

    int count = 0;

    //return every 100 tokens
    public Token next(final Token reusableToken) throws IOException {
      Token nextToken = null;
      for (nextToken = input.next(reusableToken);
           nextToken != null && count % modCount != 0;
           nextToken = input.next(reusableToken)) {
        count++;
      }
      count++;
      return nextToken;
    }
  }

  class ModuloSinkTokenizer extends SinkTokenizer {
    int count = 0;
    int modCount;


    ModuloSinkTokenizer(int numToks, int mc) {
      modCount = mc;
      lst = new ArrayList(numToks % mc);
    }

    public void add(Token t) {
      if (t != null && count % modCount == 0) {
        super.add(t);
      }
      count++;
    }
  }
}

