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

package org.apache.solr.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.util.*;
import org.apache.solr.common.params.HighlightParams;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Tests some basic functionality of Solr while demonstrating good
 * Best Practices for using AbstractSolrTestCase
 */
public class HighlighterTest extends SolrTestCaseJ4 {

  private static String LONG_TEXT = "a long days night this should be a piece of text which is is is is is is is is is is is is is is is is is is is " +
          "is is is is is isis is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is " +
          "is is is is is is is is is is is is is " +
          "is is is is is is is is is is is is is is is is is is is is sufficiently lengthly to produce multiple fragments which are not concatenated " +
          "at all--we want two disjoint long fragments.";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }
 
  @After
  @Override 
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    clearIndex();
    super.tearDown();
  }
  
  @Test
  public void testConfig()
  {
    SolrHighlighter highlighter = HighlightComponent.getHighlighter(h.getCore());

    // Make sure we loaded the one formatter
    SolrFormatter fmt1 = highlighter.formatters.get( null );
    SolrFormatter fmt2 = highlighter.formatters.get( "" );
    assertSame( fmt1, fmt2 );
    assertTrue( fmt1 instanceof HtmlFormatter );
    
    
    // Make sure we loaded the one formatter
    SolrFragmenter gap = highlighter.fragmenters.get( "gap" );
    SolrFragmenter regex = highlighter.fragmenters.get( "regex" );
    SolrFragmenter frag = highlighter.fragmenters.get( null );
    assertSame( gap, frag );
    assertTrue( gap instanceof GapFragmenter );
    assertTrue( regex instanceof RegexFragmenter );
  }

  @Test
  public void testMergeContiguous() throws Exception {
    HashMap<String,String> args = new HashMap<String,String>();
    args.put(HighlightParams.HIGHLIGHT, "true");
    args.put("df", "t_text");
    args.put(HighlightParams.FIELDS, "");
    args.put(HighlightParams.SNIPPETS, String.valueOf(4));
    args.put(HighlightParams.FRAGSIZE, String.valueOf(40));
    args.put(HighlightParams.MERGE_CONTIGUOUS_FRAGMENTS, "true");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    String input = "this is some long text.  It has the word long in many places.  In fact, it has long on some different fragments.  " +
            "Let us see what happens to long in this case.";
    String gold = "this is some <em>long</em> text.  It has the word <em>long</em> in many places.  In fact, it has <em>long</em> on some different fragments.  " +
            "Let us see what happens to <em>long</em> in this case.";
    assertU(adoc("t_text", input, "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Merge Contiguous",
            sumLRF.makeRequest("t_text:long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='t_text']/str[.='" + gold + "']"
            );
    args.put("f.t_text." + HighlightParams.MERGE_CONTIGUOUS_FRAGMENTS, "true");
    assertU(adoc("t_text", input, "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Merge Contiguous",
            sumLRF.makeRequest("t_text:long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='t_text']/str[.='" + gold + "']"
            );

    args.put(HighlightParams.MERGE_CONTIGUOUS_FRAGMENTS, "false");
    args.put("f.t_text." + HighlightParams.MERGE_CONTIGUOUS_FRAGMENTS, "false");
    sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    assertQ("Merge Contiguous",
            sumLRF.makeRequest("t_text:long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='t_text']/str[.='this is some <em>long</em> text.  It has']",
            "//lst[@name='1']/arr[@name='t_text']/str[.=' the word <em>long</em> in many places.  In fact, it has']",
            "//lst[@name='1']/arr[@name='t_text']/str[.=' <em>long</em> on some different fragments.  Let us']",
            "//lst[@name='1']/arr[@name='t_text']/str[.=' see what happens to <em>long</em> in this case.']"
            );
  }

  @Test
  public void testTermVecHighlight() {

    // do summarization using term vectors
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_text");
    args.put("hl.snippets", "2");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard",0,200,args);
    
    assertU(adoc("tv_text", LONG_TEXT, 
                 "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("tv_text:long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='tv_text']/str[.='a <em>long</em> days night this should be a piece of text which']",
            "//arr[@name='tv_text']/str[.=' <em>long</em> fragments.']"
            );
  }
  
  @Test
  public void testTermOffsetsTokenStream() throws Exception {
    String[] multivalued = { "a b c d", "e f g", "h", "i j k l m n" };
    Analyzer a1 = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    TokenStream tokenStream = a1.tokenStream("", new StringReader("a b c d e f g h i j k l m n"));
    tokenStream.reset();

    TermOffsetsTokenStream tots = new TermOffsetsTokenStream(
        tokenStream);
    for( String v : multivalued ){
      TokenStream ts1 = tots.getMultiValuedTokenStream( v.length() );
      Analyzer a2 = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
      TokenStream ts2 = a2.tokenStream("", new StringReader(v));
      ts2.reset();

      while (ts1.incrementToken()) {
        assertTrue(ts2.incrementToken());
        assertEquals(ts1, ts2);
      }
      assertFalse(ts2.incrementToken());
    }
  }

  @Test
  public void testTermVecMultiValuedHighlight() throws Exception {

    // do summarization using term vectors on multivalued field
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_mv_text");
    args.put("hl.snippets", "2");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard",0,200,args);
    
    assertU(adoc("tv_mv_text", LONG_TEXT, 
                 "tv_mv_text", LONG_TEXT, 
                 "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("tv_mv_text:long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='tv_mv_text']/str[.='a <em>long</em> days night this should be a piece of text which']",
            "//arr[@name='tv_mv_text']/str[.=' <em>long</em> fragments.']"
            );
  }

  // Variant of testTermVecMultiValuedHighlight to make sure that
  // more than just the first value of a multi-valued field is
  // considered for highlighting.
  @Test
  public void testTermVecMultiValuedHighlight2() throws Exception {

    // do summarization using term vectors on multivalued field
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_mv_text");
    args.put("hl.snippets", "2");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard",0,200,args);

    String shortText = "short";
    assertU(adoc("tv_mv_text", shortText,
                 "tv_mv_text", LONG_TEXT,
                 "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("tv_mv_text:long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='tv_mv_text']/str[.='a <em>long</em> days night this should be a piece of text which']",
            "//arr[@name='tv_mv_text']/str[.=' <em>long</em> fragments.']"
            );
  }

  @Test
  public void testDisMaxHighlight() {

    // same test run through dismax handler
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_text");
    args.put("qf", "tv_text");
    args.put("q.alt", "*:*");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "dismax",0,200,args);
    
    assertU(adoc("tv_text", "a long day's night", "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='tv_text']/str"
            );
    
    // try the same thing without a q param
    assertQ("Should not explode...", // q.alt should return everything
        sumLRF.makeRequest( new String[] { null } ), // empty query
        "//result[@numFound='1']"
        );
  }

  @Test
  public void testMultiValueAnalysisHighlight() {

    // do summarization using re-analysis of the field
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "textgap");
    args.put("df", "textgap");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    
    assertU(adoc("textgap", "first entry hasnt queryword", 
                 "textgap", "second entry has queryword long",
                 "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='textgap']/str"
            );

  }
  
  @Test
  public void testMultiValueBestFragmentHighlight() {
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "textgap");
    args.put("df", "textgap");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
        "standard", 0, 200, args);
    
    assertU(adoc("textgap", "first entry has one word foo", 
        "textgap", "second entry has both words foo bar",
        "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Best fragment summarization",
        sumLRF.makeRequest("foo bar"),
        "//lst[@name='highlighting']/lst[@name='1']",
        "//lst[@name='1']/arr[@name='textgap']/str[.=\'second entry has both words <em>foo</em> <em>bar</em>\']"
    );
  }

  @Test
  public void testDefaultFieldHighlight() {

    // do summarization using re-analysis of the field
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("df", "t_text");
    args.put("hl.fl", "");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    
    assertU(adoc("t_text", "a long day's night", "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='t_text']/str"
            );

  }


  @Test
  public void testHighlightDisabled() {

    // ensure highlighting can be explicitly disabled
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "false");
    args.put("hl.fl", "t_text");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    
    assertU(adoc("t_text", "a long day's night", "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("t_text:long"), "not(//lst[@name='highlighting'])");

  }

  @Test
  public void testTwoFieldHighlight() {

    // do summarization using re-analysis of the field
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "t_text tv_text");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    
    assertU(adoc("t_text", "a long day's night", "id", "1",
                 "tv_text", "a long night's day"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("t_text:long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='t_text']/str",
            "//lst[@name='1']/arr[@name='tv_text']/str"
            );
  }
  
  @Test
  public void testFieldMatch()
  {
     assertU(adoc("t_text1", "random words for highlighting tests", "id", "1",
           "t_text2", "more random words for second field"));
     assertU(commit());
     assertU(optimize());
     
     HashMap<String,String> args = new HashMap<String,String>();
     args.put("hl", "true");
     args.put("hl.fl", "t_text1 t_text2");
     
     TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
           "standard", 0, 200, args);
     // default should highlight both random and words in both fields
     assertQ("Test Default",
           sumLRF.makeRequest("t_text1:random OR t_text2:words"),
           "//lst[@name='highlighting']/lst[@name='1']",
           "//lst[@name='1']/arr[@name='t_text1']/str[.='<em>random</em> <em>words</em> for highlighting tests']",
           "//lst[@name='1']/arr[@name='t_text2']/str[.='more <em>random</em> <em>words</em> for second field']"
           );
     
     // requireFieldMatch=true - highlighting should only occur if term matched in that field
     args.put("hl.requireFieldMatch", "true");
     sumLRF = h.getRequestFactory(
           "standard", 0, 200, args);
     assertQ("Test RequireFieldMatch",
           sumLRF.makeRequest("t_text1:random OR t_text2:words"),
           "//lst[@name='highlighting']/lst[@name='1']",
           "//lst[@name='1']/arr[@name='t_text1']/str[.='<em>random</em> words for highlighting tests']",
           "//lst[@name='1']/arr[@name='t_text2']/str[.='more random <em>words</em> for second field']"
           );

     // test case for un-optimized index
     assertU(adoc("t_text1", "random words for highlighting tests", "id", "2",
             "t_text2", "more random words for second field"));
     assertU(delI("1"));
     assertU(commit());
     sumLRF = h.getRequestFactory(
           "standard", 0, 200, args);
     assertQ("Test RequireFieldMatch on un-optimized index",
           sumLRF.makeRequest("t_text1:random OR t_text2:words"),
           "//lst[@name='highlighting']/lst[@name='2']",
           "//lst[@name='2']/arr[@name='t_text1']/str[.='<em>random</em> words for highlighting tests']",
           "//lst[@name='2']/arr[@name='t_text2']/str[.='more random <em>words</em> for second field']"
           );
  }

  @Test
  public void testCustomSimpleFormatterHighlight() {

    // do summarization using a custom formatter
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "t_text");
    args.put("hl.simple.pre","<B>");
    args.put("hl.simple.post","</B>");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    
    assertU(adoc("t_text", "a long days night", "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("t_text:long"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='t_text']/str[.='a <B>long</B> days night']"
            );
    
    // test a per-field override
    args.put("f.t_text.hl.simple.pre","<I>");
    args.put("f.t_text.hl.simple.post","</I>");
    sumLRF = h.getRequestFactory(
          "standard", 0, 200, args);
    assertQ("Basic summarization",
          sumLRF.makeRequest("t_text:long"),
          "//lst[@name='highlighting']/lst[@name='1']",
          "//lst[@name='1']/arr[@name='t_text']/str[.='a <I>long</I> days night']"
          );
    
  }

  @Test
  public void testLongFragment() {

    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_text");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    

    String text = 
      "junit: [mkdir] Created dir: /home/klaas/worio/backend/trunk/build-src/solr-nightly/build/test-results [junit] Running org.apache.solr.BasicFunctionalityTest [junit] Tests run: 7, Failures: 0, Errors: 0, Time elapsed: 5.36 sec [junit] Running org.apache.solr.ConvertedLegacyTest [junit] Tests run: 1, Failures: 0, Errors: 0, Time elapsed: 8.268 sec [junit] Running org.apache.solr.DisMaxRequestHandlerTest [junit] Tests run: 1, Failures: 0, Errors: 0, Time elapsed: 1.56 sec [junit] Running org.apache.solr.HighlighterTest [junit] Tests run: 7, Failures: 0, Errors: 0, Time elapsed: 4.979 sec [junit] Running org.apache.solr.OutputWriterTest [junit] Tests run: 2, Failures: 0, Errors: 0, Time elapsed: 0.797 sec [junit] Running org.apache.solr.SampleTest [junit] Tests run: 2, Failures: 0, Errors: 0, Time elapsed: 1.021 sec [junit] Running org.apache.solr.analysis.TestBufferedTokenStream [junit] Tests run: 2, Failures: 0, Errors: 0, Time elapsed: 0.05 sec [junit] Running org.apache.solr.analysis.TestRemoveDuplicatesTokenFilter [junit] Tests run: 3, Failures: 0, Errors: 0, Time elapsed: 0.054 sec [junit] Running org.apache.solr.analysis.TestSynonymFilter [junit] Tests run: 6, Failures: 0, Errors: 0, Time elapsed: 0.081 sec [junit] Running org.apache.solr.analysis.TestWordDelimiterFilter [junit] Tests run: 1, Failures: 0, Errors: 0, Time elapsed: 1.714 sec [junit] Running org.apache.solr.search.TestDocSet [junit] Tests run: 1, Failures: 0, Errors: 0, Time elapsed: 0.788 sec [junit] Running org.apache.solr.util.SolrPluginUtilsTest [junit] Tests run: 5, Failures: 0, Errors: 0, Time elapsed: 3.519 sec [junit] Running org.apache.solr.util.TestOpenBitSet [junit] Tests run: 2, Failures: 0, Errors: 0, Time elapsed: 0.533 sec";
    assertU(adoc("tv_text", text, "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("tv_text:dir"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='tv_text']/str"
            );
  }

  @Test
  public void testMaxChars() {
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("fl", "id score");
    args.put("hl", "true");
    args.put("hl.snippets", "10");
    args.put("hl.fl", "t_text");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    

    assertU(adoc("t_text", LONG_TEXT, "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("token at start of text",
            sumLRF.makeRequest("t_text:disjoint"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[count(str)=1]"
            );
    args.put("hl.maxAnalyzedChars", "20");
    sumLRF = h.getRequestFactory("standard", 0, 200, args);
    assertQ("token at end of text",
            sumLRF.makeRequest("t_text:disjoint"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1'][not(*)]"
            );
    args.put("hl.maxAnalyzedChars", "-1");
    sumLRF = h.getRequestFactory("standard", 0, 200, args);
    assertQ("token at start of text",
        sumLRF.makeRequest("t_text:disjoint"),
        "//lst[@name='highlighting']/lst[@name='1']",
        "//lst[@name='1']/arr[count(str)=1]"
    );
  }
  
  @Test
  public void testRegexFragmenter() {
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("fl", "id score");
    args.put("hl", "true");
    args.put("hl.snippets", "10");
    args.put("hl.fl", "t_text");
    args.put("hl.fragmenter", "regex");
    args.put("hl.regex.pattern", "[-\\w ,\"']{20,200}");
    args.put("hl.regex.slop", ".9");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    
    String t = "This is an example of a sentence. Another example \"sentence\" with " +
      "special characters\nand a line-break! Miscellaneous character like ^ are " +
      "unknowns and end up being bad example s of sentences? I wonder how " +
      "slashes/other punctuation fare in these examples?";
    assertU(adoc("t_text", t, "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("regex fragmenter",
            sumLRF.makeRequest("t_text:example"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//arr/str[.='This is an <em>example</em> of a sentence']",
            "//arr/str[.='. Another <em>example</em> \"sentence\" with special characters\nand a line-break']",
            "//arr/str[.=' ^ are unknowns and end up being bad <em>example</em> s of sentences']",
            "//arr/str[.='/other punctuation fare in these <em>examples</em>?']"
            );
    // try with some punctuation included
    args.put("hl.regex.pattern", "[-\\w ,^/\\n\"']{20,200}");
    sumLRF = h.getRequestFactory("standard", 0, 200, args);
    assertQ("regex fragmenter 2",
            sumLRF.makeRequest("t_text:example"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//arr/str[.='This is an <em>example</em> of a sentence']",
            "//arr/str[.='. Another <em>example</em> \"sentence\" with special characters\nand a line-break']",
            "//arr/str[.='! Miscellaneous character like ^ are unknowns and end up being bad <em>example</em> s of sentences']",
            "//arr/str[.='? I wonder how slashes/other punctuation fare in these <em>examples</em>?']"
            );
  }
  
  @Test
  public void testVariableFragsize() {
     assertU(adoc("tv_text", "a long days night this should be a piece of text which is is is is is is is is is is is is is is is is is is is is is is is is isis is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is sufficiently lengthly to produce multiple fragments which are not concatenated at all", 
           "id", "1"));
     assertU(commit());
     assertU(optimize());

     // default length
     HashMap<String,String> args = new HashMap<String,String>();
     args.put("hl", "true");
     args.put("hl.fl", "tv_text");
     TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
       "standard", 0, 200, args);
     assertQ("Basic summarization",
           sumLRF.makeRequest("tv_text:long"),
           "//lst[@name='highlighting']/lst[@name='1']",
           "//lst[@name='1']/arr[@name='tv_text']/str[.='a <em>long</em> days night this should be a piece of text which']"
           );
     
     // 25
     args.put("hl.fragsize","25");
     sumLRF = h.getRequestFactory(
           "standard", 0, 200, args);
     assertQ("Basic summarization",
           sumLRF.makeRequest("tv_text:long"),
           "//lst[@name='highlighting']/lst[@name='1']",
           "//lst[@name='1']/arr[@name='tv_text']/str[.='a <em>long</em> days night']"
           );
     
     // 0 - NullFragmenter
     args.put("hl.fragsize","0");
     sumLRF = h.getRequestFactory(
           "standard", 0, 200, args);
     assertQ("Basic summarization",
           sumLRF.makeRequest("tv_text:long"),
           "//lst[@name='highlighting']/lst[@name='1']",
           "//lst[@name='1']/arr[@name='tv_text']/str[.='a <em>long</em> days night this should be a piece of text which is is is is is is is is is is is is is is is is is is is is is is is is isis is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is sufficiently lengthly to produce multiple fragments which are not concatenated at all']"
           );
  }
  
  @Test
  public void testAlternateSummary() {
     //long document
     assertU(adoc("tv_text", "keyword is only here",
                  "t_text", "a piece of text to be substituted",
                  "id", "1",
                  "foo_t","hi"));
     assertU(commit());
     assertU(optimize());

    // do summarization
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fragsize","0");
    args.put("hl.fl", "t_text");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);

    // no alternate
    assertQ("Alternate summarization",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=0]"
            );

    // with an alternate
    args.put("hl.alternateField", "foo_t");
    sumLRF = h.getRequestFactory("standard", 0, 200, args);
    assertQ("Alternate summarization",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='hi']"
            );

    // with an alternate + max length
    args.put("hl.alternateField", "t_text");
    args.put("hl.maxAlternateFieldLength", "15");
    sumLRF = h.getRequestFactory("standard", 0, 200, args);
    assertQ("Alternate summarization",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='a piece of text']"
            );
  }
  
  @Test
  public void testPhraseHighlighter() {
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "t_text");
    args.put("hl.fragsize", "40");
    args.put("hl.snippets", "10");
    args.put("hl.usePhraseHighlighter", "false");

    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);

    // String borrowed from Lucene's HighlighterTest
    String t = "This piece of text refers to Kennedy at the beginning then has a longer piece of text that is very long in the middle and finally ends with another reference to Kennedy";
    
    assertU(adoc("t_text", t, "id", "1"));
    assertU(commit());
    assertU(optimize());
    
    String oldHighlight1 = "//lst[@name='1']/arr[@name='t_text']/str[.='This piece of <em>text</em> <em>refers</em> to Kennedy']";
    String oldHighlight2 = "//lst[@name='1']/arr[@name='t_text']/str[.=' at the beginning then has a longer piece of <em>text</em>']";
    String oldHighlight3 = "//lst[@name='1']/arr[@name='t_text']/str[.=' with another <em>reference</em> to Kennedy']";
    String newHighlight1 = "//lst[@name='1']/arr[@name='t_text']/str[.='This piece of <em>text</em> <em>refers</em> to Kennedy']";
  
    // check if old functionality is still the same
    assertQ("Phrase highlighting - old",
        sumLRF.makeRequest("t_text:\"text refers\""),
        "//lst[@name='highlighting']/lst[@name='1']",
        oldHighlight1, oldHighlight2, oldHighlight3
        );

    assertQ("Phrase highlighting - old",
        sumLRF.makeRequest("t_text:text refers"),
        "//lst[@name='highlighting']/lst[@name='1']",
        oldHighlight1, oldHighlight2, oldHighlight3
        );
    
    // now check if Lucene-794 highlighting works as expected
    args.put("hl.usePhraseHighlighter", "true");

    sumLRF = h.getRequestFactory("standard", 0, 200, args);
    
    // check phrase highlighting
    assertQ("Phrase highlighting - Lucene-794",
        sumLRF.makeRequest("t_text:\"text refers\""),
        "//lst[@name='highlighting']/lst[@name='1']",
        newHighlight1
        );

    // non phrase queries should be highlighted as they were before this fix
    assertQ("Phrase highlighting - Lucene-794",
        sumLRF.makeRequest("t_text:text refers"),
        "//lst[@name='highlighting']/lst[@name='1']",
        oldHighlight1, oldHighlight2, oldHighlight3
        );
  }
  
  @Test
  public void testGetHighlightFields() {
    HashMap<String, String> args = new HashMap<String, String>();
    args.put("fl", "id score");
    args.put("hl", "true");
    args.put("hl.fl", "t*");

    assertU(adoc("id", "0", "title", "test", // static stored
        "text", "test", // static not stored
        "foo_s", "test", // dynamic stored
        "foo_sI", "test", // dynamic not stored
        "weight", "1.0")); // stored but not text
    assertU(commit());
    assertU(optimize());

    TestHarness.LocalRequestFactory lrf = h.getRequestFactory("standard", 0,
        10, args);

    SolrQueryRequest request = lrf.makeRequest("test");
    SolrHighlighter highlighter = HighlightComponent.getHighlighter(h.getCore());
    List<String> highlightFieldNames = Arrays.asList(highlighter
        .getHighlightFields(null, request, new String[] {}));
    assertTrue("Expected to highlight on field \"title\"", highlightFieldNames
        .contains("title"));
    assertFalse("Expected to not highlight on field \"text\"",
        highlightFieldNames.contains("text"));
    assertFalse("Expected to not highlight on field \"weight\"",
        highlightFieldNames.contains("weight"));
    request.close();

    args.put("hl.fl", "foo_*");
    lrf = h.getRequestFactory("standard", 0, 10, args);
    request = lrf.makeRequest("test");
    highlighter = HighlightComponent.getHighlighter(h.getCore());
    highlightFieldNames = Arrays.asList(highlighter.getHighlightFields(null,
        request, new String[] {}));
    assertEquals("Expected one field to highlight on", 1, highlightFieldNames
        .size());
    assertEquals("Expected to highlight on field \"foo_s\"", "foo_s",
        highlightFieldNames.get(0));
    request.close();
  }

  @Test
  public void testDefaultFieldPrefixWildcardHighlight() {

    // do summarization using re-analysis of the field
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("df", "t_text");
    args.put("hl.fl", "");
    args.put("hl.usePhraseHighlighter", "true");
    args.put("hl.highlightMultiTerm", "true");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    
    assertU(adoc("t_text", "a long day's night", "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("lon*"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='t_text']/str"
            );

  }

  @Test
  public void testDefaultFieldNonPrefixWildcardHighlight() {

    // do summarization using re-analysis of the field
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("df", "t_text");
    args.put("hl.fl", "");
    args.put("hl.usePhraseHighlighter", "true");
    args.put("hl.highlightMultiTerm", "true");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard", 0, 200, args);
    
    assertU(adoc("t_text", "a long day's night", "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("l*g"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[@name='t_text']/str"
            );

  }
  
  public void testSubwordWildcardHighlight() {
    assertU(adoc("subword", "lorem PowerShot.com ipsum", "id", "1"));
    assertU(commit());
    assertQ("subword wildcard highlighting", 
            req("q", "subword:pow*", "hl", "true", "hl.fl", "subword"),
            "//lst[@name='highlighting']/lst[@name='1']" +
            "/arr[@name='subword']/str='lorem <em>PowerShot.com</em> ipsum'");
  }

  public void testSubwordWildcardHighlightWithTermOffsets() {
    assertU(adoc("subword_offsets", "lorem PowerShot.com ipsum", "id", "1"));
    assertU(commit());
    assertQ("subword wildcard highlighting", 
            req("q", "subword_offsets:pow*", "hl", "true", "hl.fl", "subword_offsets"),
            "//lst[@name='highlighting']/lst[@name='1']" +
            "/arr[@name='subword_offsets']/str='lorem <em>PowerShot.com</em> ipsum'");
  }
  public void testSubwordWildcardHighlightWithTermOffsets2() {
    assertU(adoc("subword_offsets", "lorem PowerShot ipsum", "id", "1"));
    assertU(commit());
    assertQ("subword wildcard highlighting",
            req("q", "subword_offsets:pow*", "hl", "true", "hl.fl", "subword_offsets"),
            "//lst[@name='highlighting']/lst[@name='1']" +
            "/arr[@name='subword_offsets']/str='lorem <em>PowerShot</em> ipsum'");
 }
}
