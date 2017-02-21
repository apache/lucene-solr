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
package org.apache.lucene.analysis.pattern;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.charfilter.MappingCharFilter;
import org.apache.lucene.analysis.charfilter.NormalizeCharMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.automaton.Automaton;

public class TestSimplePatternTokenizer extends BaseTokenStreamTestCase {

  public void testGreedy() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("(foo)+");
    t.setReader(new StringReader("bar foofoo baz"));
    assertTokenStreamContents(t,
                              new String[] {"foofoo"},
                              new int[] {4},
                              new int[] {10});
  }

  public void testBigLookahead() throws Exception {
    StringBuilder b = new StringBuilder();
    for(int i=0;i<100;i++) {
      b.append('a');
    }
    b.append('b');
    Tokenizer t = new SimplePatternTokenizer(b.toString());

    b = new StringBuilder();
    for(int i=0;i<200;i++) {
      b.append('a');
    }
    t.setReader(new StringReader(b.toString()));
    t.reset();
    assertFalse(t.incrementToken());
  }

  public void testOneToken() throws Exception {
    Tokenizer t = new SimplePatternTokenizer(".*");
    CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);
    String s;
    while (true) {
      s = TestUtil.randomUnicodeString(random());
      if (s.length() > 0) {
        break;
      }
    }
    t.setReader(new StringReader(s));
    t.reset();
    assertTrue(t.incrementToken());
    assertEquals(s, termAtt.toString());
  }

  public void testEmptyStringPatternNoMatch() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("a*");
    t.setReader(new StringReader("bbb"));
    t.reset();
    assertFalse(t.incrementToken());
  }

  public void testEmptyStringPatternOneMatch() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("a*");
    CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);
    t.setReader(new StringReader("bbab"));
    t.reset();
    assertTrue(t.incrementToken());
    assertEquals("a", termAtt.toString());
    assertFalse(t.incrementToken());
  }

  public void testEndOffset() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("a+");
    CharTermAttribute termAtt = t.getAttribute(CharTermAttribute.class);
    OffsetAttribute offsetAtt = t.getAttribute(OffsetAttribute.class);
    t.setReader(new StringReader("aaabbb"));
    t.reset();
    assertTrue(t.incrementToken());
    assertEquals("aaa", termAtt.toString());
    assertFalse(t.incrementToken());
    t.end();
    assertEquals(6, offsetAtt.endOffset());
  }

  public void testFixedToken() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("aaaa");

    t.setReader(new StringReader("aaaaaaaaaaaaaaa"));
    assertTokenStreamContents(t,
                              new String[] {"aaaa", "aaaa", "aaaa"},
                              new int[] {0, 4, 8},
                              new int[] {4, 8, 12});
  }

  public void testBasic() throws Exception  {
    String qpattern = "\\'([^\\']+)\\'"; // get stuff between "'"
    String[][] tests = {
      // pattern        input                    output
      { ":",           "boo:and:foo",           ": :" },
      { qpattern,      "aaa 'bbb' 'ccc'",       "'bbb' 'ccc'" },
    };
    
    for(String[] test : tests) {     
      TokenStream stream = new SimplePatternTokenizer(test[0]);
      ((Tokenizer)stream).setReader(new StringReader(test[1]));
      String out = tsToString(stream);

      assertEquals("pattern: "+test[0]+" with input: "+test[1], test[2], out);
    } 
  }

  public void testNotDeterminized() throws Exception {
    Automaton a = new Automaton();
    int start = a.createState();
    int mid1 = a.createState();
    int mid2 = a.createState();
    int end = a.createState();
    a.setAccept(end, true);
    a.addTransition(start, mid1, 'a', 'z');
    a.addTransition(start, mid2, 'a', 'z');
    a.addTransition(mid1, end, 'b');
    a.addTransition(mid2, end, 'b');
    expectThrows(IllegalArgumentException.class, () -> {new SimplePatternTokenizer(a);});
  }

  public void testOffsetCorrection() throws Exception {
    final String INPUT = "G&uuml;nther G&uuml;nther is here";

    // create MappingCharFilter
    List<String> mappingRules = new ArrayList<>();
    mappingRules.add( "\"&uuml;\" => \"ü\"" );
    NormalizeCharMap.Builder builder = new NormalizeCharMap.Builder();
    builder.add("&uuml;", "ü");
    NormalizeCharMap normMap = builder.build();
    CharFilter charStream = new MappingCharFilter( normMap, new StringReader(INPUT));

    // create SimplePatternTokenizer
    Tokenizer stream = new SimplePatternTokenizer("Günther");
    stream.setReader(charStream);
    assertTokenStreamContents(stream,
        new String[] { "Günther", "Günther" },
        new int[] { 0, 13 },
        new int[] { 12, 25 },
        INPUT.length());
  }
  
  /** 
   * TODO: rewrite tests not to use string comparison.
   */
  private static String tsToString(TokenStream in) throws IOException {
    StringBuilder out = new StringBuilder();
    CharTermAttribute termAtt = in.addAttribute(CharTermAttribute.class);
    // extra safety to enforce, that the state is not preserved and also
    // assign bogus values
    in.clearAttributes();
    termAtt.setEmpty().append("bogusTerm");
    in.reset();
    while (in.incrementToken()) {
      if (out.length() > 0) {
        out.append(' ');
      }
      out.append(termAtt.toString());
      in.clearAttributes();
      termAtt.setEmpty().append("bogusTerm");
    }

    in.close();
    return out.toString();
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new SimplePatternTokenizer("a");
        return new TokenStreamComponents(tokenizer);
      }    
    };
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    a.close();
    
    Analyzer b = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new SimplePatternTokenizer("a");
        return new TokenStreamComponents(tokenizer);
      }    
    };
    checkRandomData(random(), b, 1000*RANDOM_MULTIPLIER);
    b.close();
  }

  public void testEndLookahead() throws Exception {
    Tokenizer t = new SimplePatternTokenizer("(ab)+");
    t.setReader(new StringReader("aba"));
    assertTokenStreamContents(t,
        new String[] { "ab" },
        new int[] { 0 },
        new int[] { 2 },
        3);
  }
}
