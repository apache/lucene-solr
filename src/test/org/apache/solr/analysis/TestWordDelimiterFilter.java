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

import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashSet;

/**
 * New WordDelimiterFilter tests... most of the tests are in ConvertedLegacyTest
 */
public class TestWordDelimiterFilter extends AbstractSolrTestCase {
  public String getSchemaFile() { return "solr/conf/schema.xml"; }
  public String getSolrConfigFile() { return "solr/conf/solrconfig.xml"; }


  public void posTst(String v1, String v2, String s1, String s2) {
    assertU(adoc("id",  "42",
                 "subword", v1,
                 "subword", v2));
    assertU(commit());

    // there is a positionIncrementGap of 100 between field values, so
    // we test if that was maintained.
    assertQ("position increment lost",
            req("+id:42 +subword:\"" + s1 + ' ' + s2 + "\"~90")
            ,"//result[@numFound=0]"
    );
    assertQ("position increment lost",
            req("+id:42 +subword:\"" + s1 + ' ' + s2 + "\"~110")
            ,"//result[@numFound=1]"
    );
  }


  public void testRetainPositionIncrement() {
    posTst("foo","bar","foo","bar");
    posTst("-foo-","-bar-","foo","bar");
    posTst("foo","bar","-foo-","-bar-");

    posTst("123","456","123","456");
    posTst("/123/","/456/","123","456");

    posTst("/123/abc","qwe/456/","abc","qwe");

    posTst("zoo-foo","bar-baz","foo","bar");
    posTst("zoo-foo-123","456-bar-baz","foo","bar");
  }

  public void testNoGenerationEdgeCase() {
    assertU(adoc("id", "222", "numberpartfail", "123.123.123.123"));
  }

  public void testIgnoreCaseChange() {

    assertU(adoc("id",  "43",
                 "wdf_nocase", "HellO WilliAM",
                 "subword", "GoodBye JonEs"));
    assertU(commit());
    
    assertQ("no case change",
            req("wdf_nocase:(hell o am)")
            ,"//result[@numFound=0]"
    );
    assertQ("case change",
            req("subword:(good jon)")
            ,"//result[@numFound=1]"
    );
  }


  public void testPreserveOrignalTrue() {

    assertU(adoc("id",  "144",
                 "wdf_preserve", "404-123"));
    assertU(commit());
    
    assertQ("preserving original word",
            req("wdf_preserve:404")
            ,"//result[@numFound=1]"
    );
    
    assertQ("preserving original word",
        req("wdf_preserve:123")
        ,"//result[@numFound=1]"
    );

    assertQ("preserving original word",
        req("wdf_preserve:404-123*")
        ,"//result[@numFound=1]"
    );

  }

  /***
  public void testPerformance() throws IOException {
    String s = "now is the time-for all good men to come to-the aid of their country.";
    Token tok = new Token();
    long start = System.currentTimeMillis();
    int ret=0;
    for (int i=0; i<1000000; i++) {
      StringReader r = new StringReader(s);
      TokenStream ts = new WhitespaceTokenizer(r);
      ts = new WordDelimiterFilter(ts, 1,1,1,1,0);

      while (ts.next(tok) != null) ret++;
    }

    System.out.println("ret="+ret+" time="+(System.currentTimeMillis()-start));
  }
  ***/


  public void testOffsets() throws IOException {

    // test that subwords and catenated subwords have
    // the correct offsets.
    WordDelimiterFilter wdf = new WordDelimiterFilter(
            new TokenStream() {
              Token t;
              public Token next() throws IOException {
                if (t!=null) return null;
                t = new Token("foo-bar", 5, 12);  // actual
                return t;
              }
            },
    1,1,0,0,1,1,0);

    int i=0;
    for(Token t; (t=wdf.next())!=null;) {
      String termText = new String(t.termBuffer(), 0, t.termLength());
      if (termText.equals("foo")) {
        assertEquals(5, t.startOffset());
        assertEquals(8, t.endOffset());
        i++;
      }
      if (termText.equals("bar")) {
        assertEquals(9, t.startOffset());
        assertEquals(12, t.endOffset());
        i++;
      }
      if (termText.equals("foobar")) {
        assertEquals(5, t.startOffset());
        assertEquals(12, t.endOffset());
        i++;
      }
    }
    assertEquals(3,i); // make sure all 3 tokens were generated

    // test that if splitting or catenating a synonym, that the offsets
    // are not altered (they would be incorrect).
    wdf = new WordDelimiterFilter(
            new TokenStream() {
              Token t;
              public Token next() throws IOException {
                if (t!=null) return null;
                t = new Token("foo-bar", 5, 6);  // a synonym
                return t;
              }
            },
    1,1,0,0,1,1,0);
    for(Token t; (t=wdf.next())!=null;) {
      assertEquals(5, t.startOffset());
      assertEquals(6, t.endOffset());
    }
  }
  
  public void testOffsetChange() throws Exception
  {
    WordDelimiterFilter wdf = new WordDelimiterFilter(
      new TokenStream() {
        Token t;
        public Token next() {
         if (t != null) return null;
         t = new Token("übelkeit)", 7, 16);
         return t;
        }
      },
      1,1,0,0,1,1,0
    );
    
    Token t = wdf.next();
    
    assertNotNull(t);
    assertEquals("übelkeit", t.term());
    assertEquals(7, t.startOffset());
    assertEquals(15, t.endOffset());
  }
  
  public void testOffsetChange2() throws Exception
  {
    WordDelimiterFilter wdf = new WordDelimiterFilter(
      new TokenStream() {
        Token t;
        public Token next() {
         if (t != null) return null;
         t = new Token("(übelkeit", 7, 17);
         return t;
        }
      },
      1,1,0,0,1,1,0
    );
    
    Token t = wdf.next();
    
    assertNotNull(t);
    assertEquals("übelkeit", t.term());
    assertEquals(8, t.startOffset());
    assertEquals(17, t.endOffset());
  }
  
  public void testOffsetChange3() throws Exception
  {
    WordDelimiterFilter wdf = new WordDelimiterFilter(
      new TokenStream() {
        Token t;
        public Token next() {
         if (t != null) return null;
         t = new Token("(übelkeit", 7, 16);
         return t;
        }
      },
      1,1,0,0,1,1,0
    );
    
    Token t = wdf.next();
    
    assertNotNull(t);
    assertEquals("übelkeit", t.term());
    assertEquals(8, t.startOffset());
    assertEquals(16, t.endOffset());
  }
  
  public void testOffsetChange4() throws Exception
  {
    WordDelimiterFilter wdf = new WordDelimiterFilter(
      new TokenStream() {
        private Token t;
        public Token next() {
         if (t != null) return null;
         t = new Token("(foo,bar)", 7, 16);
         return t;
        }
      },
      1,1,0,0,1,1,0
    );
    
    Token t = wdf.next();
    
    assertNotNull(t);
    assertEquals("foo", t.term());
    assertEquals(8, t.startOffset());
    assertEquals(11, t.endOffset());
    
    t = wdf.next();
    
    assertNotNull(t);
    assertEquals("bar", t.term());
    assertEquals(12, t.startOffset());
    assertEquals(15, t.endOffset());
  }

  public void testAlphaNumericWords(){
     assertU(adoc("id",  "68","numericsubword","Java/J2SE"));
     assertU(commit());

     assertQ("j2se found",
            req("numericsubword:(J2SE)")
            ,"//result[@numFound=1]"
    );
      assertQ("no j2 or se",
            req("numericsubword:(J2 OR SE)")
            ,"//result[@numFound=0]"
    );
  }

  public void testProtectedWords(){
    assertU(adoc("id", "70","protectedsubword","c# c++ .net Java/J2SE"));
    assertU(commit());

    assertQ("java found",
            req("protectedsubword:(java)")
            ,"//result[@numFound=1]"
    );

    assertQ(".net found",
            req("protectedsubword:(.net)")
            ,"//result[@numFound=1]"
    );

    assertQ("c# found",
            req("protectedsubword:(c#)")
            ,"//result[@numFound=1]"
    );

    assertQ("c++ found",
            req("protectedsubword:(c++)")
            ,"//result[@numFound=1]"
    );

    assertQ("c found?",
            req("protectedsubword:c")
            ,"//result[@numFound=0]"
    );
    assertQ("net found?",
            req("protectedsubword:net")
            ,"//result[@numFound=0]"
    );
  }


  public void doSplit(final String input, String... output) throws Exception {
    WordDelimiterFilter wdf = new WordDelimiterFilter(new TokenStream() {
      boolean done=false;
      @Override
      public Token next() throws IOException {
        if (done) return null;
        done = true;
        return new Token(input,0,input.length());
      }
    }
            ,1,1,0,0,0
    );

    for(String expected : output) {
      Token t = wdf.next();
      assertEquals(expected, t.term());
    }

    assertEquals(null, wdf.next());
  }

  public void testSplits() throws Exception {
    doSplit("basic-split","basic","split");
    doSplit("camelCase","camel","Case");

    // non-space marking symbol shouldn't cause split
    // this is an example in Thai    
    doSplit("\u0e1a\u0e49\u0e32\u0e19","\u0e1a\u0e49\u0e32\u0e19");


  }
  
  public void doSplitPossessive(int stemPossessive, final String input, final String... output) throws Exception {
    WordDelimiterFilter wdf = new WordDelimiterFilter(new TokenStream() {
      boolean done=false;
      @Override
      public Token next() throws IOException {
        if (done) return null;
        done = true;
        return new Token(input,0,input.length());
      }
    }
            ,1,1,0,0,0,1,0,1,stemPossessive,null
    );

    for(String expected : output) {
      Token t = wdf.next();
      assertEquals(expected, t.term());
    }

    assertEquals(null, wdf.next());
  }
  
  /*
   * Test option that allows disabling the special "'s" stemming, instead treating the single quote like other delimiters. 
   */
  public void testPossessives() throws Exception {
    doSplitPossessive(1, "ra's", "ra");
    doSplitPossessive(0, "ra's", "ra", "s");
  }
  
  /*
   * Set a large position increment gap of 10 if the token is "largegap" or "/"
   */
  private final class LargePosIncTokenFilter extends TokenFilter {
    private TermAttribute termAtt;
    private PositionIncrementAttribute posIncAtt;
    
    protected LargePosIncTokenFilter(TokenStream input) {
      super(input);
      termAtt = (TermAttribute) addAttribute(TermAttribute.class);
      posIncAtt = (PositionIncrementAttribute) addAttribute(PositionIncrementAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        if (termAtt.term().equals("largegap") || termAtt.term().equals("/"))
          posIncAtt.setPositionIncrement(10);
        return true;
      } else {
        return false;
      }
    }  
  }
  
  public void testPositionIncrements() throws Exception {
    final CharArraySet protWords = new CharArraySet(new HashSet<String>(Arrays.asList("NUTCH")), false);
    
    /* analyzer that uses whitespace + wdf */
    Analyzer a = new Analyzer() {
      public TokenStream tokenStream(String field, Reader reader) {
        return new WordDelimiterFilter(
            new WhitespaceTokenizer(reader),
            1, 1, 0, 0, 1, 1, 0, 1, 1, protWords);
      }
    };

    /* in this case, works as expected. */
    assertAnalyzesTo(a, "LUCENE / SOLR", new String[] { "LUCENE", "SOLR" },
        new int[] { 0, 9 },
        new int[] { 6, 13 },
        new int[] { 1, 1 });
    
    /* only in this case, posInc of 2 ?! */
    assertAnalyzesTo(a, "LUCENE / solR", new String[] { "LUCENE", "sol", "R", "solR" },
        new int[] { 0, 9, 12, 9 },
        new int[] { 6, 12, 13, 13 },
        new int[] { 1, 1, 1, 0 });
    
    assertAnalyzesTo(a, "LUCENE / NUTCH SOLR", new String[] { "LUCENE", "NUTCH", "SOLR" },
        new int[] { 0, 9, 15 },
        new int[] { 6, 14, 19 },
        new int[] { 1, 1, 1 });
    
    /* analyzer that will consume tokens with large position increments */
    Analyzer a2 = new Analyzer() {
      public TokenStream tokenStream(String field, Reader reader) {
        return new WordDelimiterFilter(
            new LargePosIncTokenFilter(
            new WhitespaceTokenizer(reader)),
            1, 1, 0, 0, 1, 1, 0, 1, 1, protWords);
      }
    };
    
    /* increment of "largegap" is preserved */
    assertAnalyzesTo(a2, "LUCENE largegap SOLR", new String[] { "LUCENE", "largegap", "SOLR" },
        new int[] { 0, 7, 16 },
        new int[] { 6, 15, 20 },
        new int[] { 1, 10, 1 });
    
    /* the "/" had a position increment of 10, where did it go?!?!! */
    assertAnalyzesTo(a2, "LUCENE / SOLR", new String[] { "LUCENE", "SOLR" },
        new int[] { 0, 9 },
        new int[] { 6, 13 },
        new int[] { 1, 11 });
    
    /* in this case, the increment of 10 from the "/" is carried over */
    assertAnalyzesTo(a2, "LUCENE / solR", new String[] { "LUCENE", "sol", "R", "solR" },
        new int[] { 0, 9, 12, 9 },
        new int[] { 6, 12, 13, 13 },
        new int[] { 1, 11, 1, 0 });
    
    assertAnalyzesTo(a2, "LUCENE / NUTCH SOLR", new String[] { "LUCENE", "NUTCH", "SOLR" },
        new int[] { 0, 9, 15 },
        new int[] { 6, 14, 19 },
        new int[] { 1, 11, 1 });
  }

  private void assertAnalyzesTo(Analyzer a, String input, String[] output,
      int startOffsets[], int endOffsets[], int posIncs[]) throws Exception {

    TokenStream ts = a.tokenStream("dummy", new StringReader(input));
    TermAttribute termAtt = (TermAttribute) ts
        .getAttribute(TermAttribute.class);
    OffsetAttribute offsetAtt = (OffsetAttribute) ts
        .getAttribute(OffsetAttribute.class);
    PositionIncrementAttribute posIncAtt = (PositionIncrementAttribute) ts
        .getAttribute(PositionIncrementAttribute.class);
    for (int i = 0; i < output.length; i++) {
      assertTrue(ts.incrementToken());
      assertEquals(output[i], termAtt.term());
      assertEquals(startOffsets[i], offsetAtt.startOffset());
      assertEquals(endOffsets[i], offsetAtt.endOffset());
      assertEquals(posIncs[i], posIncAtt.getPositionIncrement());
    }
    assertFalse(ts.incrementToken());
    ts.close();
  }
}
