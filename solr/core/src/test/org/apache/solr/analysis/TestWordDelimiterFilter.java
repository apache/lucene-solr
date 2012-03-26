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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.SingleTokenTokenStream;
import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.cz.CzechStemFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analysis.BaseTokenTestCase;
import org.apache.solr.analysis.WordDelimiterFilterFactory;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.analysis.BaseTokenTestCase.*;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.apache.solr.analysis.WordDelimiterFilter.*;
import static org.apache.solr.analysis.WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE;

/**
 * New WordDelimiterFilter tests... most of the tests are in ConvertedLegacyTest
 */
public class TestWordDelimiterFilter extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

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
    clearIndex();
  }

  @Test
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

  @Test
  public void testNoGenerationEdgeCase() {
    assertU(adoc("id", "222", "numberpartfail", "123.123.123.123"));
    clearIndex();
  }

  @Test
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
    clearIndex();
  }

  @Test
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
    clearIndex();
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

  @Test
  public void testOffsets() throws IOException {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    // test that subwords and catenated subwords have
    // the correct offsets.
    WordDelimiterFilter wdf = new WordDelimiterFilter(new SingleTokenTokenStream(new Token("foo-bar", 5, 12)), DEFAULT_WORD_DELIM_TABLE, flags, null);

    assertTokenStreamContents(wdf, 
        new String[] { "foo", "bar", "foobar" },
        new int[] { 5, 9, 5 }, 
        new int[] { 8, 12, 12 });

    wdf = new WordDelimiterFilter(new SingleTokenTokenStream(new Token("foo-bar", 5, 6)), DEFAULT_WORD_DELIM_TABLE, flags, null);
    
    assertTokenStreamContents(wdf,
        new String[] { "foo", "bar", "foobar" },
        new int[] { 5, 5, 5 },
        new int[] { 6, 6, 6 });
  }
  
  @Test
  public void testOffsetChange() throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    WordDelimiterFilter wdf = new WordDelimiterFilter(new SingleTokenTokenStream(new Token("übelkeit)", 7, 16)), DEFAULT_WORD_DELIM_TABLE, flags, null);
    
    assertTokenStreamContents(wdf,
        new String[] { "übelkeit" },
        new int[] { 7 },
        new int[] { 15 });
  }
  
  @Test
  public void testOffsetChange2() throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    WordDelimiterFilter wdf = new WordDelimiterFilter(new SingleTokenTokenStream(new Token("(übelkeit", 7, 17)), DEFAULT_WORD_DELIM_TABLE, flags, null);
    
    assertTokenStreamContents(wdf,
        new String[] { "übelkeit" },
        new int[] { 8 },
        new int[] { 17 });
  }
  
  @Test
  public void testOffsetChange3() throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    WordDelimiterFilter wdf = new WordDelimiterFilter(new SingleTokenTokenStream(new Token("(übelkeit", 7, 16)), DEFAULT_WORD_DELIM_TABLE, flags, null);
    
    assertTokenStreamContents(wdf,
        new String[] { "übelkeit" },
        new int[] { 8 },
        new int[] { 16 });
  }
  
  @Test
  public void testOffsetChange4() throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    WordDelimiterFilter wdf = new WordDelimiterFilter(new SingleTokenTokenStream(new Token("(foo,bar)", 7, 16)), DEFAULT_WORD_DELIM_TABLE, flags, null);
    
    assertTokenStreamContents(wdf,
        new String[] { "foo", "bar", "foobar"},
        new int[] { 8, 12, 8 },
        new int[] { 11, 15, 15 });
  }

  @Test
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
    clearIndex();
  }

  @Test
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
    clearIndex();
  }
  
  @Test
  public void testCustomTypes() throws Exception {
    String testText = "I borrowed $5,400.00 at 25% interest-rate";
    WordDelimiterFilterFactory factoryDefault = new WordDelimiterFilterFactory();
    ResourceLoader loader = new SolrResourceLoader(null, null);
    Map<String,String> args = new HashMap<String,String>();
    args.put("generateWordParts", "1");
    args.put("generateNumberParts", "1");
    args.put("catenateWords", "1");
    args.put("catenateNumbers", "1");
    args.put("catenateAll", "0");
    args.put("splitOnCaseChange", "1");
    
    /* default behavior */
    factoryDefault.init(args);
    factoryDefault.inform(loader);
    
    TokenStream ts = factoryDefault.create(
        new WhitespaceTokenizer(BaseTokenTestCase.DEFAULT_VERSION, new StringReader(testText)));
    BaseTokenStreamTestCase.assertTokenStreamContents(ts, 
        new String[] { "I", "borrowed", "5", "400", "00", "540000", "at", "25", "interest", "rate", "interestrate" });

    ts = factoryDefault.create(
        new WhitespaceTokenizer(BaseTokenTestCase.DEFAULT_VERSION, new StringReader("foo\u200Dbar")));
    BaseTokenStreamTestCase.assertTokenStreamContents(ts, 
        new String[] { "foo", "bar", "foobar" });

    
    /* custom behavior */
    WordDelimiterFilterFactory factoryCustom = new WordDelimiterFilterFactory();
    // use a custom type mapping
    args.put("types", "wdftypes.txt");
    factoryCustom.init(args);
    factoryCustom.inform(loader);
    
    ts = factoryCustom.create(
        new WhitespaceTokenizer(BaseTokenTestCase.DEFAULT_VERSION, new StringReader(testText)));
    BaseTokenStreamTestCase.assertTokenStreamContents(ts, 
        new String[] { "I", "borrowed", "$5,400.00", "at", "25%", "interest", "rate", "interestrate" });
    
    /* test custom behavior with a char > 0x7F, because we had to make a larger byte[] */
    ts = factoryCustom.create(
        new WhitespaceTokenizer(BaseTokenTestCase.DEFAULT_VERSION, new StringReader("foo\u200Dbar")));
    BaseTokenStreamTestCase.assertTokenStreamContents(ts, 
        new String[] { "foo\u200Dbar" });
  }

  public void doSplit(final String input, String... output) throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    WordDelimiterFilter wdf = new WordDelimiterFilter(new MockTokenizer(
                new StringReader(input), MockTokenizer.KEYWORD, false), WordDelimiterIterator.DEFAULT_WORD_DELIM_TABLE, flags, null);
    
    assertTokenStreamContents(wdf, output);
  }

  @Test
  public void testSplits() throws Exception {
    doSplit("basic-split","basic","split");
    doSplit("camelCase","camel","Case");

    // non-space marking symbol shouldn't cause split
    // this is an example in Thai    
    doSplit("\u0e1a\u0e49\u0e32\u0e19","\u0e1a\u0e49\u0e32\u0e19");
    // possessive followed by delimiter
    doSplit("test's'", "test");

    // some russian upper and lowercase
    doSplit("Роберт", "Роберт");
    // now cause a split (russian camelCase)
    doSplit("РобЕрт", "Роб", "Ерт");

    // a composed titlecase character, don't split
    doSplit("aǅungla", "aǅungla");
    
    // a modifier letter, don't split
    doSplit("ســـــــــــــــــلام", "ســـــــــــــــــلام");
    
    // enclosing mark, don't split
    doSplit("test⃝", "test⃝");
    
    // combining spacing mark (the virama), don't split
    doSplit("हिन्दी", "हिन्दी");
    
    // don't split non-ascii digits
    doSplit("١٢٣٤", "١٢٣٤");
    
    // don't split supplementaries into unpaired surrogates
    doSplit("𠀀𠀀", "𠀀𠀀");
  }
  
  public void doSplitPossessive(int stemPossessive, final String input, final String... output) throws Exception {
    int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS;
    flags |= (stemPossessive == 1) ? STEM_ENGLISH_POSSESSIVE : 0;
    WordDelimiterFilter wdf = new WordDelimiterFilter(new MockTokenizer(
        new StringReader(input), MockTokenizer.KEYWORD, false), flags, null);

    assertTokenStreamContents(wdf, output);
  }
  
  /*
   * Test option that allows disabling the special "'s" stemming, instead treating the single quote like other delimiters. 
   */
  @Test
  public void testPossessives() throws Exception {
    doSplitPossessive(1, "ra's", "ra");
    doSplitPossessive(0, "ra's", "ra", "s");
  }
  
  /*
   * Set a large position increment gap of 10 if the token is "largegap" or "/"
   */
  private final class LargePosIncTokenFilter extends TokenFilter {
    private CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    
    protected LargePosIncTokenFilter(TokenStream input) {
      super(input);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        if (termAtt.toString().equals("largegap") || termAtt.toString().equals("/"))
          posIncAtt.setPositionIncrement(10);
        return true;
      } else {
        return false;
      }
    }  
  }
  
  @Test
  public void testPositionIncrements() throws Exception {
    final int flags = GENERATE_WORD_PARTS | GENERATE_NUMBER_PARTS | CATENATE_ALL | SPLIT_ON_CASE_CHANGE | SPLIT_ON_NUMERICS | STEM_ENGLISH_POSSESSIVE;
    final CharArraySet protWords = new CharArraySet(TEST_VERSION_CURRENT, new HashSet<String>(Arrays.asList("NUTCH")), false);
    
    /* analyzer that uses whitespace + wdf */
    Analyzer a = new Analyzer() {
      @Override
      public TokenStream tokenStream(String field, Reader reader) {
        return new WordDelimiterFilter(
            new MockTokenizer(reader, MockTokenizer.WHITESPACE, false),
            flags, protWords);
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
      @Override
      public TokenStream tokenStream(String field, Reader reader) {
        return new WordDelimiterFilter(
            new LargePosIncTokenFilter(
            new MockTokenizer(reader, MockTokenizer.WHITESPACE, false)),
            flags, protWords);
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

    Analyzer a3 = new Analyzer() {
      @Override
      public TokenStream tokenStream(String field, Reader reader) {
        StopFilter filter = new StopFilter(TEST_VERSION_CURRENT,
            new MockTokenizer(reader, MockTokenizer.WHITESPACE, false), StandardAnalyzer.STOP_WORDS_SET);
        filter.setEnablePositionIncrements(true);
        return new WordDelimiterFilter(filter, flags, protWords);
      }
    };

    assertAnalyzesTo(a3, "lucene.solr", 
        new String[] { "lucene", "solr", "lucenesolr" },
        new int[] { 0, 7, 0 },
        new int[] { 6, 11, 11 },
        new int[] { 1, 1, 0 });

    /* the stopword should add a gap here */
    assertAnalyzesTo(a3, "the lucene.solr", 
        new String[] { "lucene", "solr", "lucenesolr" }, 
        new int[] { 4, 11, 4 }, 
        new int[] { 10, 15, 15 },
        new int[] { 2, 1, 0 });
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomStrings() throws Exception {
    int numIterations = atLeast(5);
    for (int i = 0; i < numIterations; i++) {
      final int flags = random.nextInt(512);
      final CharArraySet protectedWords;
      if (random.nextBoolean()) {
        protectedWords = new CharArraySet(TEST_VERSION_CURRENT, new HashSet<String>(Arrays.asList("a", "b", "cd")), false);
      } else {
        protectedWords = null;
      }
      
      Analyzer a = new ReusableAnalyzerBase() {
        
        @Override
        protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
          Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
          return new TokenStreamComponents(tokenizer, new WordDelimiterFilter(tokenizer, flags, protectedWords));
        }
      };
      checkRandomData(random, a, 10000*RANDOM_MULTIPLIER);
    }
  }
  
  public void testEmptyTerm() throws IOException {
    for (int i = 0; i < 512; i++) {
      final int flags = i;
      final CharArraySet protectedWords;
      if (random.nextBoolean()) {
        protectedWords = new CharArraySet(TEST_VERSION_CURRENT, new HashSet<String>(Arrays.asList("a", "b", "cd")), false);
      } else {
        protectedWords = null;
      }
    
      Analyzer a = new ReusableAnalyzerBase() { 
        @Override
        protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
          Tokenizer tokenizer = new KeywordTokenizer(reader);
          return new TokenStreamComponents(tokenizer, new WordDelimiterFilter(tokenizer, flags, protectedWords));
        }
      };
      // depending upon options, this thing may or may not preserve the empty term
      checkAnalysisConsistency(random, a, random.nextBoolean(), "");
    }
  }
}
