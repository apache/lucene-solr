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

import java.util.HashMap;

import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.TestHarness;

/**
 * Tests some basic functionality of Solr while demonstrating good
 * Best Practices for using AbstractSolrTestCase
 */
public class HighlighterTest extends AbstractSolrTestCase {

  @Override public String getSchemaFile() { return "schema.xml"; }
  @Override public String getSolrConfigFile() { return "solrconfig.xml"; }

  @Override 
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
  }
  
  @Override 
  public void tearDown() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.tearDown();
  }

  public void testTermVecHighlight() {

    // do summarization using term vectors
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_text");
    args.put("hl.snippets", "2");
    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
      "standard",0,200,args);
    
    assertU(adoc("tv_text", "a long days night this should be a piece of text which is is is is is is is is is is is is is is is is is is is is is is is is isis is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is is sufficiently lengthly to produce multiple fragments which are not concatenated at all--we want two disjoint long fragments.", 
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

  public void testDisMaxHighlight() {

    // same test run through dismax handler
    HashMap<String,String> args = new HashMap<String,String>();
    args.put("hl", "true");
    args.put("hl.fl", "tv_text");
    args.put("qf", "tv_text");
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

  }


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
  }

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
}
