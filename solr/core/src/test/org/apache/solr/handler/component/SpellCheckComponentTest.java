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
package org.apache.solr.handler.component;

import java.io.File;
import java.util.*;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.LuceneTestCase.SuppressTempFileChecks;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.spelling.AbstractLuceneSpellChecker;
import org.apache.solr.spelling.SolrSpellChecker;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @since solr 1.3
 */
@Slow
@SuppressTempFileChecks(bugUrl = "https://issues.apache.org/jira/browse/SOLR-1877 Spellcheck IndexReader leak bug?")
public class SpellCheckComponentTest extends SolrTestCaseJ4 {
  static String rh = "/spellCheckCompRH";


  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-spellcheckcomponent.xml","schema.xml");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    assertU(adoc("id", "0", "lowerfilt", "This is a title"));
    assertU((adoc("id", "1", "lowerfilt",
            "The quick reb fox jumped over the lazy brown dogs.")));
    assertU((adoc("id", "2", "lowerfilt", "This is a document")));
    assertU((adoc("id", "3", "lowerfilt", "another document")));
    //bunch of docs that are variants on blue
    assertU((adoc("id", "4", "lowerfilt", "this blue")));
    assertU((adoc("id", "5", "lowerfilt", "this blud")));
    assertU((adoc("id", "6", "lowerfilt", "this boue")));
    assertU((adoc("id", "7", "lowerfilt", "this glue")));
    assertU((adoc("id", "8", "lowerfilt", "this blee")));
    assertU((adoc("id", "9", "lowerfilt", "pixmaa 12345")));
    assertU((commit()));
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    assertU(delQ("*:*"));
    optimize();
    assertU((commit()));

  }
  
  @Test
  public void testMaximumResultsForSuggest() throws Exception {
   assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", SpellingParams.SPELLCHECK_BUILD, "true", "q","lowerfilt:(this OR brwn)",
        SpellingParams.SPELLCHECK_COUNT,"5", SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"false", SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST, "7")
        ,"/spellcheck/suggestions/[0]=='brwn'"
        ,"/spellcheck/suggestions/[1]/numFound==1"
     );

   expectThrows(Exception.class, () -> {
     assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", SpellingParams.SPELLCHECK_BUILD, "true", "q","lowerfilt:(this OR brwn)",
         SpellingParams.SPELLCHECK_COUNT,"5", SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"false", SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST, "6")
         ,"/spellcheck/suggestions/[1]/numFound==1"
     );
   });

    assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", SpellingParams.SPELLCHECK_BUILD, "true", "q","lowerfilt:(this OR brwn)",
        "fq", "id:[0 TO 9]", /*returns 10, less selective */ "fq", "lowerfilt:th*", /* returns 8, most selective */
        SpellingParams.SPELLCHECK_COUNT,"5", SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"false", SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST, ".90")
        ,"/spellcheck/suggestions/[0]=='brwn'"
        ,"/spellcheck/suggestions/[1]/numFound==1"
     );

    expectThrows(Exception.class, () -> {
      assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", SpellingParams.SPELLCHECK_BUILD, "true", "q","lowerfilt:(this OR brwn)",
          "fq", "id:[0 TO 9]", /*returns 10, less selective */ "fq", "lowerfilt:th*", /* returns 8, most selective */
          SpellingParams.SPELLCHECK_COUNT,"5", SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"false", SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST, ".80")
          ,"/spellcheck/suggestions/[1]/numFound==1"
      );
    });
    
    assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", SpellingParams.SPELLCHECK_BUILD, "true", "q","lowerfilt:(this OR brwn)",
        "fq", "id:[0 TO 9]", SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST_FQ, "id:[0 TO 9]", 
        SpellingParams.SPELLCHECK_COUNT,"5", SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"false", SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST, ".70")
        ,"/spellcheck/suggestions/[0]=='brwn'"
        ,"/spellcheck/suggestions/[1]/numFound==1"
     );

    expectThrows(Exception.class, () -> {
      assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", SpellingParams.SPELLCHECK_BUILD, "true", "q","lowerfilt:(this OR brwn)",
          "fq", "id:[0 TO 9]", SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST_FQ, "lowerfilt:th*",
          SpellingParams.SPELLCHECK_COUNT,"5", SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"false", SpellingParams.SPELLCHECK_MAX_RESULTS_FOR_SUGGEST, ".64")
          ,"/spellcheck/suggestions/[1]/numFound==1"
      );
    });
  } 
  
  @Test
  public void testExtendedResultsCount() throws Exception {
    assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", SpellingParams.SPELLCHECK_BUILD, "true", "q","bluo", SpellingParams.SPELLCHECK_COUNT,"5", SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"false")
       ,"/spellcheck/suggestions/[0]=='bluo'"
       ,"/spellcheck/suggestions/[1]/numFound==5"
    );

    assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", "q","bluo", SpellingParams.SPELLCHECK_COUNT,"3", SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"true")
       ,"/spellcheck/suggestions/[1]/suggestion==[{'word':'blud','freq':1}, {'word':'blue','freq':1}, {'word':'blee','freq':1}]"
    );
  }

  @Test
  public void test() throws Exception {
    assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", "q","documemt")
       ,"/spellcheck=={'suggestions':['documemt',{'numFound':1,'startOffset':0,'endOffset':8,'suggestion':['document']}]}"
    );
  }
  
  @Test
  public void testNumericQuery() throws Exception {
    assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", "q","12346")
       ,"/spellcheck=={'suggestions':['12346',{'numFound':1,'startOffset':0,'endOffset':5,'suggestion':['12345']}]}"
    );
  }


  @Test
  public void testPerDictionary() throws Exception {
    assertJQ(req("json.nl","map", "qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", SpellingParams.SPELLCHECK_BUILD, "true", "q","documemt"
        , SpellingParams.SPELLCHECK_DICT, "perDict", SpellingParams.SPELLCHECK_PREFIX + "perDict.foo", "bar", SpellingParams.SPELLCHECK_PREFIX + "perDict.bar", "foo")
       ,"/spellcheck/suggestions/bar=={'numFound':1, 'startOffset':0, 'endOffset':1, 'suggestion':['foo']}"
       ,"/spellcheck/suggestions/foo=={'numFound':1, 'startOffset':2, 'endOffset':3, 'suggestion':['bar']}"        
    );
  }

  @Test
  public void testCollate() throws Exception {
    assertJQ(req("json.nl","map", "qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", SpellingParams.SPELLCHECK_BUILD, "true", "q","documemt", SpellingParams.SPELLCHECK_COLLATE, "true")
       ,"/spellcheck/collations/collation=='document'"
    );
    assertJQ(req("json.nl","map", "qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", "q","documemt lowerfilt:broen^4", SpellingParams.SPELLCHECK_COLLATE, "true")
       ,"/spellcheck/collations/collation=='document lowerfilt:brown^4'"
    );
    assertJQ(req("json.nl","map", "qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", "q","documemtsss broens", SpellingParams.SPELLCHECK_COLLATE, "true")
       ,"/spellcheck/collations/collation=='document brown'"
    );
    assertJQ(req("json.nl","map", "qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", "q","pixma", SpellingParams.SPELLCHECK_COLLATE, "true")
       ,"/spellcheck/collations/collation=='pixmaa'"
    );
  }
  

  @Test
  public void testCollateExtendedResultsWithJsonNl() throws Exception {
    final String q = "documemtsss broens";
    final String jsonNl = "map";
    final boolean collateExtendedResults = random().nextBoolean();
    final List<String> testsList = new ArrayList<String>();
    if (collateExtendedResults) {
      testsList.add("/spellcheck/collations/collation/collationQuery=='document brown'");
      testsList.add("/spellcheck/collations/collation/hits==0");
      switch (jsonNl) {
        case "map":
          testsList.add("/spellcheck/collations/collation/misspellingsAndCorrections/documemtsss=='document'");
          testsList.add("/spellcheck/collations/collation/misspellingsAndCorrections/broens=='brown'");
          break;
        default:
          fail("unexpected json.nl choice: "+jsonNl);
          break;
      }
    } else {
      testsList.add("/spellcheck/collations/collation=='document brown'");
    }
    final String[] testsArray = new String[testsList.size()];
    implTestCollateExtendedResultsWithJsonNl(q, jsonNl, collateExtendedResults, testsList.toArray(testsArray));
  }

  private void implTestCollateExtendedResultsWithJsonNl(String q, String jsonNl, boolean collateExtendedResults, String ... tests) throws Exception {
    final SolrQueryRequest solrQueryRequest = req(
        CommonParams.QT, rh,
        CommonParams.Q, q,
        "json.nl", jsonNl,
        SpellCheckComponent.COMPONENT_NAME, "true",
        SpellingParams.SPELLCHECK_COLLATE_EXTENDED_RESULTS, Boolean.toString(collateExtendedResults),
        SpellingParams.SPELLCHECK_COLLATE, "true");
    assertJQ(solrQueryRequest, tests);
  }

  @Test
  public void testCorrectSpelling() throws Exception {
    // Make sure correct spellings are signaled in the response
    assertJQ(req("json.nl","map", "qt",rh, SpellCheckComponent.COMPONENT_NAME, "true",
        "q","lowerfilt:lazy lowerfilt:brown", SpellingParams.SPELLCHECK_EXTENDED_RESULTS, "true")
       ,"/spellcheck/correctlySpelled==true"
    );
    assertJQ(req("json.nl","map", "qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", "spellcheck.dictionary", "direct_lowerfilt",
        "q","lowerfilt:lazy lowerfilt:brown", SpellingParams.SPELLCHECK_EXTENDED_RESULTS, "true")
       ,"/spellcheck/correctlySpelled==true"
    );
    assertJQ(req("json.nl","map", "qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", "spellcheck.dictionary", "direct_lowerfilt",
        "q","lakkle", SpellingParams.SPELLCHECK_EXTENDED_RESULTS, "true")
       ,"/spellcheck/correctlySpelled==false"
    );
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testRelativeIndexDirLocation() throws Exception {
    SolrCore core = h.getCore();
    File indexDir = new File(core.getDataDir() + File.separator + "spellchecker1");
    assertTrue(indexDir.exists());
    
    indexDir = new File(core.getDataDir() + File.separator + "spellchecker2");
    assertTrue(indexDir.exists());
    
    indexDir = new File(core.getDataDir() + File.separator + "spellchecker3");
    assertTrue(indexDir.exists());
  }

  @Test
  public void testReloadOnStart() throws Exception {
    assertU(adoc("id", "0", "lowerfilt", "This is a title"));
    assertU(commit());
    SolrQueryRequest request = req("qt", "/spellCheckCompRH", "q", "*:*",
        "spellcheck.q", "ttle", "spellcheck", "true", "spellcheck.dictionary",
        "default", "spellcheck.build", "true");
    assertQ(request, "//arr[@name='suggestion'][.='title']");

    NamedList args = new NamedList();
    NamedList spellchecker = new NamedList();
    spellchecker.add(SolrSpellChecker.DICTIONARY_NAME, "default");
    spellchecker.add(AbstractLuceneSpellChecker.FIELD, "lowerfilt");
    spellchecker.add(AbstractLuceneSpellChecker.INDEX_DIR, "spellchecker1");
    args.add("spellchecker", spellchecker);

    // TODO: this is really fragile and error prone - find a higher level way to test this.
    SpellCheckComponent checker = new SpellCheckComponent();
    checker.init(args);
    checker.inform(h.getCore());

    request = req("qt", "/spellCheckCompRH", "q", "*:*", "spellcheck.q", "ttle",
        "spellcheck", "true", "spellcheck.dictionary", "default",
        "spellcheck.reload", "true");
    List<SearchComponent> components = new ArrayList<>();
    for (String name : h.getCore().getSearchComponents().keySet()) {
      components.add(h.getCore().getSearchComponent(name));
    }
    ResponseBuilder rb = new ResponseBuilder(request, new SolrQueryResponse(), components);
    checker.prepare(rb);

    try {
      checker.process(rb);
    } catch (NullPointerException e) {
      fail("NullPointerException due to reload not initializing analyzers");
    }

    rb.req.close();
  }
  
    @SuppressWarnings("unchecked")
    @Test
  public void testRebuildOnCommit() throws Exception {
    SolrQueryRequest req = req("q", "lowerfilt:lucenejavt", "qt", "/spellCheckCompRH", "spellcheck", "true");
    String response = h.query(req);
    assertFalse("No suggestions should be returned", response.contains("lucenejava"));
    
    assertU(adoc("id", "11231", "lowerfilt", "lucenejava"));
    assertU("commit", commit());
    
    assertQ(req, "//arr[@name='suggestion'][.='lucenejava']");
  }
    
    @Test
    public void testThresholdTokenFrequency() throws Exception {

        //"document" is in 2 documents but "another" is only in 1.
        //So with a threshold of 29%, "another" is absent from the dictionary
        //while "document" is present.

        assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", "q","documenq", SpellingParams.SPELLCHECK_DICT, "threshold", SpellingParams.SPELLCHECK_COUNT,"5", SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"true")
            ,"/spellcheck/suggestions/[1]/suggestion==[{'word':'document','freq':2}]"
        );

        assertJQ(req("qt",rh, SpellCheckComponent.COMPONENT_NAME, "true", "q","documenq", SpellingParams.SPELLCHECK_DICT, "threshold_direct", SpellingParams.SPELLCHECK_COUNT,"5", SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"true")
            ,"/spellcheck/suggestions/[1]/suggestion==[{'word':'document','freq':2}]"
        );

        //TODO:  how do we make this into a 1-liner using "assertQ()" ???
        SolrCore core = h.getCore();
        SearchComponent speller = core.getSearchComponent("spellcheck");
        assertTrue("speller is null and it shouldn't be", speller != null);

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add(SpellCheckComponent.COMPONENT_NAME, "true");
        params.add(SpellingParams.SPELLCHECK_COUNT, "10");
        params.add(SpellingParams.SPELLCHECK_DICT, "threshold");
        params.add(SpellingParams.SPELLCHECK_EXTENDED_RESULTS,"true");
        params.add(CommonParams.Q, "anotheq");

        SolrRequestHandler handler = core.getRequestHandler("/spellCheckCompRH");
        SolrQueryResponse rsp = new SolrQueryResponse();
        rsp.addResponseHeader(new SimpleOrderedMap());
        SolrQueryRequest req = new LocalSolrQueryRequest(core, params);
        handler.handleRequest(req, rsp);
        req.close();
        NamedList values = rsp.getValues();
        NamedList spellCheck = (NamedList) values.get("spellcheck");
        NamedList suggestions = (NamedList) spellCheck.get("suggestions");
        assertTrue(suggestions.get("suggestion")==null);
        assertTrue((Boolean) spellCheck.get("correctlySpelled")==false);

        params.remove(SpellingParams.SPELLCHECK_DICT);
        params.add(SpellingParams.SPELLCHECK_DICT, "threshold_direct");
        rsp = new SolrQueryResponse();
        rsp.addResponseHeader(new SimpleOrderedMap());
        req = new LocalSolrQueryRequest(core, params);
        handler.handleRequest(req, rsp);
        req.close();
        values = rsp.getValues();
        spellCheck = (NamedList) values.get("spellcheck");
        suggestions = (NamedList) spellCheck.get("suggestions");
        assertTrue(suggestions.get("suggestion")==null);
        assertTrue((Boolean) spellCheck.get("correctlySpelled")==false);
    }
}
