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

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.solr.analysis.TestBufferedTokenStream.AB_AAB_Stream;

/**
 * Tests CommonGramsQueryFilter
 */
public class CommonGramsFilterTest extends TestCase {
  private static final String[] commonWords = { "s", "a", "b", "c", "d", "the",
      "of" };
  
  public void testReset() throws Exception {
    final String input = "How the s a brown s cow d like A B thing?";
    WhitespaceTokenizer wt = new WhitespaceTokenizer(new StringReader(input));
    CommonGramsFilter cgf = new CommonGramsFilter(wt, commonWords);
    
    TermAttribute term = (TermAttribute) cgf.addAttribute(TermAttribute.class);
    assertTrue(cgf.incrementToken());
    assertEquals("How", term.term());
    assertTrue(cgf.incrementToken());
    assertEquals("How_the", term.term());
    assertTrue(cgf.incrementToken());
    assertEquals("the", term.term());
    assertTrue(cgf.incrementToken());
    assertEquals("the_s", term.term());
    
    wt.reset(new StringReader(input));
    cgf.reset();
    assertTrue(cgf.incrementToken());
    assertEquals("How", term.term());
  }
  
  public void testCommonGramsQueryFilter() throws Exception {
    Set<Map.Entry<String, String>> input2expectedSet = initQueryMap().entrySet();
    for (Iterator<Entry<String, String>> i = input2expectedSet.iterator(); i
        .hasNext();) {
      Map.Entry<String, String> me = i.next();
      String input = me.getKey();
      String expected = me.getValue();
      String message = "message: input value is: " + input;
      assertEquals(message, expected, testFilter(input, "query"));
    }
  }
  
  public void testQueryReset() throws Exception {
    final String input = "How the s a brown s cow d like A B thing?";
    WhitespaceTokenizer wt = new WhitespaceTokenizer(new StringReader(input));
    CommonGramsFilter cgf = new CommonGramsFilter(wt, commonWords);
    CommonGramsQueryFilter nsf = new CommonGramsQueryFilter(cgf);
    
    TermAttribute term = (TermAttribute) wt.addAttribute(TermAttribute.class);
    assertTrue(nsf.incrementToken());
    assertEquals("How_the", term.term());
    assertTrue(nsf.incrementToken());
    assertEquals("the_s", term.term());
    
    wt.reset(new StringReader(input));
    nsf.reset();
    assertTrue(nsf.incrementToken());
    assertEquals("How_the", term.term());
  }
  
  public void testCommonGramsFilter() throws Exception {
    Set<Map.Entry<String, String>> input2expectedSet = initMap().entrySet();
    for (Iterator<Entry<String, String>> i = input2expectedSet.iterator(); i
        .hasNext();) {
      Map.Entry<String, String> me = i.next();
      String input = me.getKey();
      String expected = me.getValue();
      String message = "message: input value is: " + input;
      assertEquals(message, expected, testFilter(input, "common"));
    }
  }
  
  /**
   * This is for testing CommonGramsQueryFilter which outputs a set of tokens
   * optimized for querying with only one token at each position, either a
   * unigram or a bigram It also will not return a token for the final position
   * if the final word is already in the preceding bigram Example:(three
   * tokens/positions in)
   * "foo bar the"=>"foo:1|bar:2,bar-the:2|the:3=> "foo" "bar-the" (2 tokens
   * out)
   * 
   * @return Map<String,String>
   */
  private static Map<String, String> initQueryMap() {
    Map<String, String> input2expected = new LinkedHashMap<String, String>();

    // Stop words used below are "of" "the" and "s"
    
    // two word queries
    input2expected.put("brown fox", "/brown/fox");
    input2expected.put("the fox", "/the_fox");
    input2expected.put("fox of", "/fox_of");
    input2expected.put("of the", "/of_the");
    
    // one word queries
    input2expected.put("the", "/the");
    input2expected.put("foo", "/foo");

    // 3 word combinations s=stopword/common word n=not a stop word
    input2expected.put("n n n", "/n/n/n");
    input2expected.put("quick brown fox", "/quick/brown/fox");

    input2expected.put("n n s", "/n/n_s");
    input2expected.put("quick brown the", "/quick/brown_the");

    input2expected.put("n s n", "/n_s/s_n");
    input2expected.put("quick the brown", "/quick_the/the_brown");

    input2expected.put("n s s", "/n_s/s_s");
    input2expected.put("fox of the", "/fox_of/of_the");

    input2expected.put("s n n", "/s_n/n/n");
    input2expected.put("the quick brown", "/the_quick/quick/brown");

    input2expected.put("s n s", "/s_n/n_s");
    input2expected.put("the fox of", "/the_fox/fox_of");

    input2expected.put("s s n", "/s_s/s_n");
    input2expected.put("of the fox", "/of_the/the_fox");

    input2expected.put("s s s", "/s_s/s_s");
    input2expected.put("of the of", "/of_the/the_of");

    return input2expected;
  }
  
  private static Map<String, String> initMap() {
    Map<String, String> input2expected = new HashMap<String, String>();

    // Stop words used below are "of" "the" and "s"
    // one word queries
    input2expected.put("the", "/the");
    input2expected.put("foo", "/foo");

    // two word queries
    input2expected.put("brown fox", "/brown/fox");
    input2expected.put("the fox", "/the,the_fox/fox");
    input2expected.put("fox of", "/fox,fox_of/of");
    input2expected.put("of the", "/of,of_the/the");

    // 3 word combinations s=stopword/common word n=not a stop word
    input2expected.put("n n n", "/n/n/n");
    input2expected.put("quick brown fox", "/quick/brown/fox");

    input2expected.put("n n s", "/n/n,n_s/s");
    input2expected.put("quick brown the", "/quick/brown,brown_the/the");

    input2expected.put("n s n", "/n,n_s/s,s_n/n");
    input2expected.put("quick the fox", "/quick,quick_the/the,the_fox/fox");

    input2expected.put("n s s", "/n,n_s/s,s_s/s");
    input2expected.put("fox of the", "/fox,fox_of/of,of_the/the");

    input2expected.put("s n n", "/s,s_n/n/n");
    input2expected.put("the quick brown", "/the,the_quick/quick/brown");

    input2expected.put("s n s", "/s,s_n/n,n_s/s");
    input2expected.put("the fox of", "/the,the_fox/fox,fox_of/of");

    input2expected.put("s s n", "/s,s_s/s,s_n/n");
    input2expected.put("of the fox", "/of,of_the/the,the_fox/fox");

    input2expected.put("s s s", "/s,s_s/s,s_s/s");
    input2expected.put("of the of", "/of,of_the/the,the_of/of");

    return input2expected;
  }
  
  /*
   * Helper methodsCopied and from CDL XTF BigramsStopFilter.java and slightly
   * modified to use with CommonGrams http://xtf.wiki.sourceforge.net/
   */
  /**
   * Very simple tokenizer that breaks up a string into a series of Lucene
   * {@link Token Token}s.
   */
  static class StringTokenStream extends TokenStream {
    private String str;

    private int prevEnd = 0;

    private StringTokenizer tok;

    private int count = 0;

    public StringTokenStream(String str, String delim) {
      this.str = str;
      tok = new StringTokenizer(str, delim);
    }

    public Token next() {
      if (!tok.hasMoreTokens())
        return null;
      count++;
      String term = tok.nextToken();
      Token t = new Token(term, str.indexOf(term, prevEnd), str.indexOf(term,
          prevEnd)
          + term.length(), "word");
      prevEnd = t.endOffset();
      return t;
    }
  }
  
  public static String testFilter(String in, String type) throws IOException {
    TokenStream nsf;
    StringTokenStream ts = new StringTokenStream(in, " .");
    if (type.equals("query")) {
      CommonGramsFilter cgf = new CommonGramsFilter(ts, commonWords);
      nsf = new CommonGramsQueryFilter(cgf);
    } else {
      nsf = new CommonGramsFilter(ts, commonWords);
    }

    StringBuffer outBuf = new StringBuffer();
    while (true) {
      Token t = nsf.next();
      if (t == null)
        break;
      for (int i = 0; i < t.getPositionIncrement(); i++)
        outBuf.append('/');
      if (t.getPositionIncrement() == 0)
        outBuf.append(',');
      outBuf.append(t.term());
    }

    String out = outBuf.toString();
    out = out.replaceAll(" ", "");
    return out;
  }
}
