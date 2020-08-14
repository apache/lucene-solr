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
package org.apache.solr.highlight;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.queries.payloads.SpanPayloadCheckQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocSet;
import org.apache.solr.util.TestHarness;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

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
    DefaultSolrHighlighter highlighter = (DefaultSolrHighlighter) HighlightComponent.getHighlighter(h.getCore());

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
    assertTrue(gap instanceof GapFragmenter);
    assertTrue(regex instanceof RegexFragmenter);
  }

  @Test
  public void testMethodPostings() {
    String field = "t_text";
    assertU(adoc(field, LONG_TEXT,
        "id", "1"));
    assertU(commit());

    IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
      h.query(req("q", "long", "hl.method", "postings", "df", field, "hl", "true"));
    });
    assertTrue("Should warn no offsets", e.getMessage().contains("indexed without offsets"));
    // note: the default schema.xml has no offsets in postings to test the PostingsHighlighter. Leave that for another
    //  test class.
  }

  @Test
  public void testMergeContiguous() throws Exception {
    HashMap<String,String> args = new HashMap<>();
    args.put(HighlightParams.HIGHLIGHT, "true");
    args.put("df", "t_text");
    args.put(HighlightParams.FIELDS, "");
    args.put(HighlightParams.SNIPPETS, String.valueOf(4));
    args.put(HighlightParams.FRAGSIZE, String.valueOf(40));
    args.put(HighlightParams.MERGE_CONTIGUOUS_FRAGMENTS, "true");
    args.put(HighlightParams.METHOD, "original"); // test works; no complaints
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
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
      "", 0, 200, args);
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
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_text");
    args.put("hl.snippets", "2");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "",0,200,args);
    
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
  public void testTermVectorWithoutOffsetsHighlight() {

    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_no_off_text");

    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory("", 0, 200, args);

    assertU(adoc("tv_no_off_text", "Crackerjack Cameron", "id", "1"));
    assertU(commit());
    assertU(optimize());

    assertQ("Fields with term vectors switched on but no offsets should be correctly highlighted",
            sumLRF.makeRequest("tv_no_off_text:cameron"),
            "//arr[@name='tv_no_off_text']/str[.='Crackerjack <em>Cameron</em>']");

  }
  
  @Test
  public void testOffsetWindowTokenFilter() throws Exception {
    String[] multivalued = { "a b c d", "e f g", "h", "i j k l m n" };
    try (Analyzer a1 = new WhitespaceAnalyzer()) {
      TokenStream tokenStream = a1.tokenStream("", "a b c d e f g h i j k l m n");

      try (OffsetWindowTokenFilter tots = new OffsetWindowTokenFilter(tokenStream)) {
        for (String v : multivalued) {
          TokenStream ts1 = tots.advanceToNextWindowOfLength(v.length());
          ts1.reset();
          try (Analyzer a2 = new WhitespaceAnalyzer()) {
            TokenStream ts2 = a2.tokenStream("", v);
            ts2.reset();

            while (ts1.incrementToken()) {
              assertTrue(ts2.incrementToken());
              assertEquals(ts1, ts2);
            }
            assertFalse(ts2.incrementToken());
          }
        }
      }
    }
  }

  @Test
  public void testTermVecMultiValuedHighlight() throws Exception {

    // do summarization using term vectors on multivalued field
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_mv_text");
    args.put("hl.snippets", "2");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "",0,200,args);
    
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
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_mv_text");
    args.put("hl.snippets", "2");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "",0,200,args);

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
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_text");
    args.put("qf", "tv_text");
    args.put("q.alt", "*:*");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "/dismax",0,200,args);
    
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
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "textgap");
    args.put("df", "textgap");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    
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
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "textgap");
    args.put("df", "textgap");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
        "", 0, 200, args);
    
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
  public void testPreserveMulti() throws Exception {
    assertU(adoc("id","1", "cat", "electronics", "cat", "monitor"));
    assertU(commit());

    assertJQ(req("q", "cat:monitor", "hl", "true", "hl.fl", "cat", "hl.snippets", "2", "f.cat.hl.preserveMulti", "true"),
        "/highlighting/1/cat==['electronics','<em>monitor</em>']"
    );

    // No match still lists all snippets?
    assertJQ(req("q", "id:1 OR cat:duuuude", "hl", "true", "hl.fl", "cat", "hl.snippets", "2", "f.cat.hl.preserveMulti", "true"),
        "/highlighting/1/cat==['electronics','monitor']"
    );
  }

  @Test
  public void testDefaultFieldHighlight() {

    // do summarization using re-analysis of the field
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("df", "t_text");
    args.put("hl.fl", "");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    
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
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "false");
    args.put("hl.fl", "t_text");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    
    assertU(adoc("t_text", "a long day's night", "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
            sumLRF.makeRequest("t_text:long"), "not(//lst[@name='highlighting'])");

  }

  @Test
  public void testTwoFieldHighlight() {

    // do summarization using re-analysis of the field
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "t_text tv_text");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    
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
     
     HashMap<String,String> args = new HashMap<>();
     args.put("hl", "true");
     args.put("hl.fl", "t_text1 t_text2");
     
     TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
           "", 0, 200, args);
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
           "", 0, 200, args);
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
           "", 0, 200, args);
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
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "t_text");
    args.put("hl.simple.pre","<B>");
    args.put("hl.simple.post", "</B>");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    
    assertU(adoc("t_text", "a long days night", "id", "1"));
    assertU(commit());
    assertU(optimize());
    assertQ("Basic summarization",
        sumLRF.makeRequest("t_text:long"),
        "//lst[@name='highlighting']/lst[@name='1']",
        "//lst[@name='1']/arr[@name='t_text']/str[.='a <B>long</B> days night']"
    );
    
    // test a per-field override
    args.put("f.t_text.hl.simple.pre", "<I>");
    args.put("f.t_text.hl.simple.post", "</I>");
    sumLRF = h.getRequestFactory(
          "", 0, 200, args);
    assertQ("Basic summarization",
          sumLRF.makeRequest("t_text:long"),
          "//lst[@name='highlighting']/lst[@name='1']",
          "//lst[@name='1']/arr[@name='t_text']/str[.='a <I>long</I> days night']"
          );
    
  }

  @Test
  public void testLongFragment() {

    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_text");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    

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
    HashMap<String,String> args = new HashMap<>();
    args.put("fl", "id score");
    args.put("hl", "true");
    args.put("hl.snippets", "10");
    final String field = random().nextBoolean() ? "t_text" : "tv_text";
    args.put("hl.fl", field);
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    

    assertU(adoc(field, LONG_TEXT, "id", "1"));
    assertU(commit());

    assertQ("token at start of text",
            sumLRF.makeRequest(field + ":disjoint"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='1']/arr[count(str)=1]"
            );
    args.put("hl.maxAnalyzedChars", "20");
    sumLRF = h.getRequestFactory("", 0, 200, args);
    assertQ("token at end of text",
        sumLRF.makeRequest(field + ":disjoint"),
        "//lst[@name='highlighting']/lst[@name='1']",
        "//lst[@name='1'][not(*)]"
    );
    args.put("hl.maxAnalyzedChars", "-1");
    sumLRF = h.getRequestFactory("", 0, 200, args);
    assertQ("token at start of text",
        sumLRF.makeRequest(field + ":disjoint"),
        "//lst[@name='highlighting']/lst[@name='1']",
        "//lst[@name='1']/arr[count(str)=1]"
    );

  }

  // Test multi-valued together with hl.maxAnalyzedChars
  @Test
  public void testMultiValuedMaxAnalyzedChars() throws Exception {
    String shortText = "some short blah blah blah blah";
    final String field = random().nextBoolean() ? "tv_mv_text" : "textgap"; // term vecs or not
    assertU(adoc(field, shortText,
        field, LONG_TEXT,
        "id", "1"));
    assertU(commit());

    assertQ(req("q", field + ":(short OR long)",
            "indent", "on",
            "hl", "true",
            "hl.fl", field,
            "hl.snippets", "2",
            "hl.maxAnalyzedChars", "8"),
        "//lst[@name='highlighting']/lst[@name='1']/arr[count(*)=1]",
        "//lst[@name='1']/arr/str[1][.='some <em>short</em>']"
        //"//lst[@name='1']/arr/str[2][.='a <em>long</em> days']"
    );
  }

  @Test
  public void testRegexFragmenter() {
    HashMap<String,String> args = new HashMap<>();
    args.put("fl", "id score");
    args.put("hl", "true");
    args.put("hl.snippets", "10");
    args.put("hl.fl", "t_text");
    args.put("hl.fragmenter", "regex");
    args.put("hl.regex.pattern", "[-\\w ,\"']{20,200}");
    args.put("hl.regex.slop", ".9");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    
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
    sumLRF = h.getRequestFactory("", 0, 200, args);
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
     HashMap<String,String> args = new HashMap<>();
     args.put("hl", "true");
     args.put("hl.fl", "tv_text");
     TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
       "", 0, 200, args);
     assertQ("Basic summarization",
           sumLRF.makeRequest("tv_text:long"),
           "//lst[@name='highlighting']/lst[@name='1']",
           "//lst[@name='1']/arr[@name='tv_text']/str[.='a <em>long</em> days night this should be a piece of text which']"
           );
     
     // 25
     args.put("hl.fragsize","25");
     sumLRF = h.getRequestFactory(
           "", 0, 200, args);
     assertQ("Basic summarization",
           sumLRF.makeRequest("tv_text:long"),
           "//lst[@name='highlighting']/lst[@name='1']",
           "//lst[@name='1']/arr[@name='tv_text']/str[.='a <em>long</em> days night']"
           );
     
     // 0 - NullFragmenter
     args.put("hl.fragsize","0");
     sumLRF = h.getRequestFactory(
           "", 0, 200, args);
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
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fragsize","0");
    args.put("hl.fl", "t_text");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);

    // no alternate
    assertQ("Alternate summarization",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1']",
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=0]"
            );

    // with an alternate
    args.put("hl.alternateField", "foo_t");
    sumLRF = h.getRequestFactory("", 0, 200, args);
    assertQ("Alternate summarization",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='hi']"
            );

    // with an alternate + max length
    args.put("hl.alternateField", "t_text");
    args.put("hl.maxAlternateFieldLength", "15");
    sumLRF = h.getRequestFactory("", 0, 200, args);
    assertQ("Alternate summarization",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='a piece of text']"
            );

    // with a non-existing alternate field + max length
    args.put("hl.alternateField", "NonExistingField");
    args.put("hl.maxAlternateFieldLength", "15");
    sumLRF = h.getRequestFactory("", 0, 200, args);
    assertQ("Alternate summarization",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='a piece of text']"
            );
  }

  @Test
  public void testAlternateSummaryWithHighlighting() {
     //long document
     assertU(adoc("tv_text", "keyword is only here, tv_text alternate field",
                  "t_text", "a piece of text to be substituted",
                  "other_t", "keyword",
                  "id", "1",
                  "foo_t","hi"));
     assertU(commit());
     assertU(optimize());

    // Prove that hl.highlightAlternate is default true and respects maxAlternateFieldLength
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fragsize","0");
    args.put("hl.fl", "t_text");
    args.put("hl.simple.pre", "<simplepre>");
    args.put("hl.simple.post", "</simplepost>");
    args.put("hl.alternateField", "tv_text");
    args.put("hl.maxAlternateFieldLength", "39");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    assertQ("Alternate summarization with highlighting",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='<simplepre>keyword</simplepost> is only here, tv_text']"
            );

    // Query on other field than hl or alternate. Still we get the hightlighted snippet from alternate
    assertQ("Alternate summarization with highlighting, query other field",
            sumLRF.makeRequest("other_t:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='<simplepre>keyword</simplepost> is only here, tv_text']"
            );

    // With hl.requireFieldMatch, will not highlight but fall back to plain-text alternate
    args.put("hl.requireFieldMatch", "true");
    sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    assertQ("Alternate summarization with highlighting, requireFieldMatch",
            sumLRF.makeRequest("other_t:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='keyword is only here, tv_text alternate']"
            );
    args.put("hl.requireFieldMatch", "false");


    // Works with field specific params, overriding maxAlternateFieldLength to return everything
    args.remove("hl.alternateField");
    args.put("f.t_text.hl.alternateField", "tv_text");
    args.put("f.t_text.hl.maxAlternateFieldLength", "0");
    sumLRF = h.getRequestFactory("", 0, 200, args);
    assertQ("Alternate summarization with highlighting",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='<simplepre>keyword</simplepost> is only here, tv_text alternate field']"
            );

    // Prove fallback highlighting works also with FVH
    args.put("hl.method", "fastVector");
    args.put("hl.tag.pre", "<fvhpre>");
    args.put("hl.tag.post", "</fvhpost>");
    args.put("f.t_text.hl.maxAlternateFieldLength", "18");
    sumLRF = h.getRequestFactory("", 0, 200, args);
    assertQ("Alternate summarization with highlighting using FVH",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
        "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='<fvhpre>keyword</fvhpost> is only here']"
            );

    // Prove it is possible to turn off highlighting of alternate field
    args.put("hl.highlightAlternate", "false");
    sumLRF = h.getRequestFactory("", 0, 200, args);
    assertQ("Alternate summarization without highlighting",
            sumLRF.makeRequest("tv_text:keyword"),
            "//lst[@name='highlighting']/lst[@name='1' and count(*)=1]",
            "//lst[@name='highlighting']/lst[@name='1']/arr[@name='t_text']/str[.='keyword is only he']"
            );
  }

  @Test
  public void testPhraseHighlighter() {
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("hl.fl", "t_text");
    args.put("hl.fragsize", "40");
    args.put("hl.snippets", "10");
    args.put("hl.usePhraseHighlighter", "false");

    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);

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

    sumLRF = h.getRequestFactory("", 0, 200, args);
    
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
    HashMap<String, String> args = new HashMap<>();
    args.put("fl", "id score");
    args.put("hl", "true");
    args.put("hl.fl", "t*");

    assertU(adoc("id", "0", "title", "test", // static stored
        "text", "test", // static not stored
        "foo_s", "test", // dynamic stored
        "foo_sI", "test", // dynamic not stored
        "bar_s", "test", // dynamic stored
        "bar_sI", "test", // dynamic not stored
        "weight", "1.0")); // stored but not text
    assertU(commit());
    assertU(optimize());

    TestHarness.LocalRequestFactory lrf = h.getRequestFactory("", 0,
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
    lrf = h.getRequestFactory("", 0, 10, args);
    request = lrf.makeRequest("test");
    highlighter = HighlightComponent.getHighlighter(h.getCore());
    highlightFieldNames = Arrays.asList(highlighter.getHighlightFields(null,
        request, new String[] {}));
    assertEquals("Expected one field to highlight on", 1, highlightFieldNames
        .size());
    assertEquals("Expected to highlight on field \"foo_s\"", "foo_s",
        highlightFieldNames.get(0));
    request.close();

    // SOLR-5127
    args.put("hl.fl", (random().nextBoolean() ? "foo_*,bar_*" : "bar_*,foo_*"));
    lrf = h.getRequestFactory("", 0, 10, args);
    // hl.fl ordering need not be preserved in output
    final Set<String> highlightedSetExpected = new HashSet<String>();
    highlightedSetExpected.add("foo_s");
    highlightedSetExpected.add("bar_s");
    try (LocalSolrQueryRequest localRequest = lrf.makeRequest("test")) {
      highlighter = HighlightComponent.getHighlighter(h.getCore());
      final Set<String> highlightedSetActual = new HashSet<String>(
          Arrays.asList(highlighter.getHighlightFields(null,
              localRequest, new String[] {})));
      assertEquals(highlightedSetExpected, highlightedSetActual);
    }
  }

  @Test
  public void testDefaultFieldPrefixWildcardHighlight() {

    // do summarization using re-analysis of the field
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("df", "t_text");
    args.put("hl.fl", "");
    args.put("hl.usePhraseHighlighter", "true");
    args.put("hl.highlightMultiTerm", "true");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    
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
    HashMap<String,String> args = new HashMap<>();
    args.put("hl", "true");
    args.put("df", "t_text");
    args.put("hl.fl", "");
    args.put("hl.usePhraseHighlighter", "true");
    args.put("hl.highlightMultiTerm", "true");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "", 0, 200, args);
    
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
  
  public void testHlQParameter() {
    assertU(adoc("title", "Apache Software Foundation", "t_text", "apache software foundation", "id", "1"));
    assertU(commit());
    assertQ("hl.q parameter overrides q parameter", 
        req("q", "title:Apache", "hl", "true", "hl.fl", "title", "hl.q", "title:Software"),
        "//lst[@name='highlighting']/lst[@name='1']" +
        "/arr[@name='title']/str='Apache <em>Software</em> Foundation'");
    assertQ("hl.q parameter overrides q parameter", 
        req("q", "title:Apache", "hl", "true", "hl.fl", "title", "hl.q", "{!v=$qq}", "qq", "title:Foundation"),
        "//lst[@name='highlighting']/lst[@name='1']" +
        "/arr[@name='title']/str='Apache Software <em>Foundation</em>'");
    assertQ("hl.q parameter uses localparam parser definition correctly",
        req("q", "Apache", "defType", "edismax", "qf", "title t_text", "hl", "true", "hl.fl", "title", "hl.q", "{!edismax}Software", "hl.qparser", "lucene"),
        "//lst[@name='highlighting']/lst[@name='1']" +
            "/arr[@name='title']/str='Apache <em>Software</em> Foundation'");
    assertQ("hl.q parameter uses defType correctly",
        req("q", "Apache", "defType", "edismax", "qf", "title t_text", "hl", "true", "hl.fl", "title", "hl.q", "Software"),
        "//lst[@name='highlighting']/lst[@name='1']" +
        "/arr[@name='title']/str='Apache <em>Software</em> Foundation'");
    assertQ("hl.q parameter uses hl.qparser param correctly",
        req("q", "t_text:Apache", "qf", "title t_text", "hl", "true", "hl.fl", "title", "hl.q", "Software", "hl.qparser", "edismax"),
        "//lst[@name='highlighting']/lst[@name='1']" +
            "/arr[@name='title']/str='Apache <em>Software</em> Foundation'");
  }

  public void testHlQEdismaxParameter() {
    assertU(adoc("title", "Apache Software Foundation", "id", "1"));
    assertU(commit());
    assertQ("hl.q parameter overrides q parameter",
        req("q", "title:Apache", "hl", "true", "hl.fl", "title", "hl.q", "{!edismax qf=title v=Software}"),
        "//lst[@name='highlighting']/lst[@name='1']" +
            "/arr[@name='title']/str='Apache <em>Software</em> Foundation'");
    assertQ("hl.q parameter overrides q parameter",
        req("q", "title:Apache", "hl", "true", "hl.fl", "title", "hl.q", "{!v=$qq}", "qq", "title:Foundation"),
        "//lst[@name='highlighting']/lst[@name='1']" +
            "/arr[@name='title']/str='Apache Software <em>Foundation</em>'");
  }

  @Test
  public void testMaxMvParams() {
    assertU(adoc("title", "Apache Software Foundation", "id", "1000",
        "lower", "gap1 target",
        "lower", "gap2 target",
        "lower", "gap3 nothing",
        "lower", "gap4 nothing",
        "lower", "gap5 target",
        "lower", "gap6 target",
        "lower", "gap7 nothing",
        "lower", "gap8 nothing",
        "lower", "gap9 target",
        "lower", "gap10 target"));

    assertU(commit());

    // First ensure we can count all six
    assertQ("Counting all MV pairs failed",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100"
        ),
        "//lst[@name='highlighting']/lst[@name='1000']/arr[@name='lower' and count(*)=6]"
    );

    // NOTE: These tests seem repeated, but we're testing for off-by-one errors

    // Now we should see exactly 2 by limiting the number of values searched to 4
    assertQ("Off by one by going too far",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_EXAMINE, "4"
        ),
        "//lst[@name='highlighting']/lst[@name='1000']/arr[@name='lower' and count(*)=2]"
    );

    // Does 0 work?
    assertQ("Off by one by going too far",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_EXAMINE, "0"
        ),
        "//lst[@name='highlighting']/lst[@name='1000' and count(child::*) = 0]"
    );

    // Now we should see exactly 2 by limiting the number of values searched to 2
    assertQ("Off by one by not going far enough",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_EXAMINE, "2"
        ),
        "//lst[@name='highlighting']/lst[@name='1000']/arr[@name='lower' and count(*)=2]"
    );

    // Now we should see exactly 1 by limiting the number of values searched to 1
    assertQ("Not counting exactly 1",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_EXAMINE, "1"
        ),
        "//lst[@name='highlighting']/lst[@name='1000']/arr[@name='lower' and count(*)=1]"
    );

    // Now we should see exactly 4 by limiting the number of values found to 4
    assertQ("Matching 4 should exactly match 4",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_MATCH, "4"
        ),
        "//lst[@name='highlighting']/lst[@name='1000']/arr[@name='lower' and count(*)=4]"
    );

    // But if hl.preserveMulti=true then we should see 6 snippets even though 2 didn't match
    assertQ("hl.preserveMulti",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_MATCH, "4",
            HighlightParams.PRESERVE_MULTI, "true"
        ),
        "//lst[@name='highlighting']/lst[@name='1000']/arr[@name='lower' and count(*)=6]"
    );

    // Now we should see exactly 2 by limiting the number of values found to 2
    assertQ("Matching 6 should exactly search them all",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_MATCH, "6"
        ),
        "//lst[@name='highlighting']/lst[@name='1000']/arr[@name='lower' and count(*)=6]"
    );

    // Now we should see exactly 1 by limiting the number of values found to 1
    assertQ("Matching 6 should exactly match them all",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_MATCH, "1"
        ),
        "//lst[@name='highlighting']/lst[@name='1000']/arr[@name='lower' and count(*)=1]"
    );

    // Now we should see exactly 0 by limiting the number of values found to 0
    assertQ("Matching 6 should exactly match them all",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_MATCH, "0"
        ),
        "//lst[@name='highlighting']/lst[@name='1000' and count(child::*) = 0]"
    );


    // Should bail at the first parameter matched.
    assertQ("Matching 6 should exactly match them all",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_MATCH, "2",
            HighlightParams.MAX_MULTIVALUED_TO_EXAMINE, "10"
        ),
        "//lst[@name='highlighting']/lst[@name='1000']/arr[@name='lower' and count(*)=2]"
    );

    // Should bail at the first parameter matched.
    assertQ("Matching 6 should exactly match them all",
        req(
            "q", "id:1000",
            HighlightParams.HIGHLIGHT, "true",
            HighlightParams.FIELDS, "lower",
            HighlightParams.Q, "target",
            HighlightParams.SNIPPETS, "100",
            HighlightParams.MAX_MULTIVALUED_TO_MATCH, "10",
            HighlightParams.MAX_MULTIVALUED_TO_EXAMINE, "2"
        ),
        "//lst[@name='highlighting']/lst[@name='1000']/arr[@name='lower' and count(*)=2]"
    );

  }

  @Test
  public void payloadFilteringSpanQuery() throws IOException {
    clearIndex();

    String FIELD_NAME = "payloadDelimited";
    assertU(adoc("id", "0", FIELD_NAME, "word|7 word|2"));
    assertU(commit());

    //We search at a lower level than typical Solr tests because there's no QParser for payloads

    //Create query matching this payload
    Query query = new SpanPayloadCheckQuery(new SpanTermQuery(new Term(FIELD_NAME, "word")),
        Collections.singletonList(new BytesRef(new byte[]{0, 0, 0, 7})));//bytes for integer 7

    //invoke highlight component... the hard way
    final SearchComponent hlComp = h.getCore().getSearchComponent("highlight");
    SolrQueryRequest req = req("hl", "true", "hl.fl", FIELD_NAME, HighlightParams.USE_PHRASE_HIGHLIGHTER, "true");
    try {
      SolrQueryResponse resp = new SolrQueryResponse();
      ResponseBuilder rb = new ResponseBuilder(req, resp, Collections.singletonList(hlComp));
      rb.setHighlightQuery(query);
      rb.setResults(req.getSearcher().getDocListAndSet(query, (DocSet) null, null, 0, 1));
      //highlight:
      hlComp.prepare(rb);
      hlComp.process(rb);
      //inspect response
      final String[] snippets = (String[]) resp.getValues().findRecursive("highlighting", "0", FIELD_NAME);
      assertEquals("<em>word|7</em> word|2", snippets[0]);
    } finally {
      req.close();
    }
  }
}
