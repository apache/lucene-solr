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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.spelling.suggest.SuggesterParams;
import org.junit.BeforeClass;
import org.junit.Test;


public class SuggestComponentTest extends SolrTestCaseJ4 {

  private static final String rh = "/suggest";

  private static CoreContainer cc;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-suggestercomponent.xml","schema.xml");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    // id, cat, price, weight
    assertU(adoc("id", "0", "cat", "This is a title", "price", "5", "weight", "10"));
    assertU(adoc("id", "1", "cat", "This is another title", "price", "10", "weight", "10"));
    assertU(adoc("id", "2", "cat", "Yet another", "price", "15", "weight", "10"));
    assertU(adoc("id", "3", "cat", "Yet another title", "price", "20", "weight", "20"));
    assertU(adoc("id", "4", "cat", "suggestions for suggest", "price", "25", "weight", "20"));
    assertU(adoc("id", "5", "cat", "Red fox", "price", "30", "weight", "20"));
    assertU(adoc("id", "6", "cat", "Rad fox", "price", "35", "weight", "30"));
    assertU(adoc("id", "7", "cat", "example data", "price", "40", "weight", "30"));
    assertU(adoc("id", "8", "cat", "example inputdata", "price", "45", "weight", "30"));
    assertU(adoc("id", "9", "cat", "blah in blah", "price", "50", "weight", "40"));
    assertU(adoc("id", "10", "cat", "another blah in blah", "price", "55", "weight", "40"));
    assertU((commit()));
    waitForWarming();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    assertU(delQ("*:*"));
    assertU((commit()));
    waitForWarming();
    // rebuild suggesters with empty index
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
        );
  }
  
  @Test
  public void testDocumentBased() throws Exception {
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, "suggest_fuzzy_doc_dict", 
        SuggesterParams.SUGGEST_BUILD, "true",
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='exampel']/int[@name='numFound'][.='2']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example inputdata']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='45']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='example data']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='40']"
        );
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, "suggest_fuzzy_doc_dict", 
        SuggesterParams.SUGGEST_BUILD, "true",
        SuggesterParams.SUGGEST_Q, "Rad",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='Rad']/int[@name='numFound'][.='2']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='Rad']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='Rad fox']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='Rad']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='35']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='Rad']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='Red fox']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='Rad']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='30']"
        );
  }
  
  @Test
  public void testExpressionBased() throws Exception {
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, "suggest_fuzzy_doc_expr_dict", 
        SuggesterParams.SUGGEST_BUILD, "true",
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_expr_dict']/lst[@name='exampel']/int[@name='numFound'][.='2']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_expr_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example inputdata']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_expr_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='120']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_expr_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='example data']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_expr_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='110']"
        );
  }
  
  @Test
  public void testFileBased() throws Exception {
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, "suggest_fuzzy_file_based", 
        SuggesterParams.SUGGEST_BUILD, "true",
        SuggesterParams.SUGGEST_Q, "chn",
        SuggesterParams.SUGGEST_COUNT, "2"),
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_file_based']/lst[@name='chn']/int[@name='numFound'][.='2']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_file_based']/lst[@name='chn']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='chance']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_file_based']/lst[@name='chn']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='1']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_file_based']/lst[@name='chn']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='change']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_file_based']/lst[@name='chn']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='1']"
        );
  }
  @Test
  public void testMultiSuggester() throws Exception {
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, "suggest_fuzzy_doc_dict",
        SuggesterParams.SUGGEST_DICT, "suggest_fuzzy_doc_expr_dict",
        SuggesterParams.SUGGEST_BUILD, "true",
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='exampel']/int[@name='numFound'][.='2']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example inputdata']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='45']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='example data']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='40']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_expr_dict']/lst[@name='exampel']/int[@name='numFound'][.='2']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_expr_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example inputdata']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_expr_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='120']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_expr_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='example data']",
        "//lst[@name='suggest']/lst[@name='suggest_fuzzy_doc_expr_dict']/lst[@name='exampel']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='110']"
        );
  }
  
  @Test
  public void testBuildAllSuggester() throws Exception {
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_BUILD_ALL, "true",
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//str[@name='command'][.='buildAll']"
        );
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_BUILD_ALL, "true"),
        "//str[@name='command'][.='buildAll']"
        );
  }
  
  @Test
  public void testReloadAllSuggester() throws Exception {
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_RELOAD_ALL, "true",
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//str[@name='command'][.='reloadAll']"
        );
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_RELOAD_ALL, "true"),
        "//str[@name='command'][.='reloadAll']"
        );
  }
  
  @Test
  public void testBadSuggesterName() throws Exception {
    String fakeSuggesterName = "does-not-exist";
    assertQEx("No suggester named " + fakeSuggesterName +" was configured",
        req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, fakeSuggesterName,
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        SolrException.ErrorCode.BAD_REQUEST
        );
    
    assertQEx("'" + SuggesterParams.SUGGEST_DICT + 
        "' parameter not specified and no default suggester configured",
        req("qt", rh, 
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        SolrException.ErrorCode.BAD_REQUEST
        );
  }
  

  @Test
  public void testDefaultBuildOnStartupNotStoredDict() throws Exception {
    
    final String suggester = "suggest_doc_default_startup_no_store";
    
    // validate that this suggester is not storing the lookup
    assertEquals(suggester,
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",7).
            get("str", n -> "name".equals(n.attr("name"))).txt());

    assertNull( h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
        .get("lst",7).
            get("str", n -> "storeDir".equals(n.attr("name"))).txt());
    
    // validate that this suggester only builds manually and has not buildOnStartup parameter

    assertEquals("false",
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",7).
            get("str", n -> "buildOnCommit".equals(n.attr("name"))).txt());

    assertNull(h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
        .get("lst",7).
            get("str", n -> "buildOnStartup".equals(n.attr("name"))).txt());
    
    reloadCore(random().nextBoolean());
    
    // Validate that the suggester was built on new/reload core
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "example",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='example']/int[@name='numFound'][.='2']"
        );
    
    // add one more doc, should be visible after core reload
    assertU(adoc("id", "10", "cat", "example data extra ", "price", "40", "weight", "35"));
    assertU((commit()));
    
    waitForWarming();
    
    // buildOnCommit=false, this doc should not be in the suggester yet
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "example",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='example']/int[@name='numFound'][.='2']"
        );
    
    reloadCore(random().nextBoolean());
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "example",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='example']/int[@name='numFound'][.='3']"
        );
    
  }
  
  @Test
  public void testDefaultBuildOnStartupStoredDict() throws Exception {
    
    final String suggester = "suggest_doc_default_startup";
    
    // validate that this suggester is storing the lookup

    assertEquals(suggester,
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",6)
            .get("str", n -> "name".equals(n.attr("name"))).txt());

    assertEquals(suggester,
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",6)
            .get("str", n -> "storeDir".equals(n.attr("name"))).txt());
    
    // validate that this suggester only builds manually and has not buildOnStartup parameter

    assertEquals("false",
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",6)
            .get("str", n -> "buildOnCommit".equals(n.attr("name"))).txt());

    assertNull( h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
        .get("lst",6)
        .get("str", n -> "buildOnStartup".equals(n.attr("name"))).txt());
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "example",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='example']/int[@name='numFound'][.='0']"
        );
    
    // build the suggester manually
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_BUILD, "true"),
        "//str[@name='command'][.='build']"
        );
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "example",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='example']/int[@name='numFound'][.='2']"
        );
    
    reloadCore(random().nextBoolean());
    
    // Validate that the suggester was loaded on new/reload core
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "example",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='example']/int[@name='numFound'][.='2']"
        );
    
    // add one more doc, this should not be seen after a core reload (not until the suggester is manually rebuilt)
    assertU(adoc("id", "10", "cat", "example data extra ", "price", "40", "weight", "35"));
    assertU((commit()));
    
    waitForWarming();
    // buildOnCommit=false, this doc should not be in the suggester yet
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "example",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='example']/int[@name='numFound'][.='2']"
        );
    
    reloadCore(random().nextBoolean());
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "example",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='example']/int[@name='numFound'][.='2']"
        );
    
    // build the suggester manually
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_BUILD, "true"),
        "//str[@name='command'][.='build']"
        );
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "example",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='example']/int[@name='numFound'][.='3']"
        );
    
  }
  
  @Test
  public void testLoadOnStartup() throws Exception {
    
    final String suggester = "suggest_fuzzy_doc_manal_build";
    
    // validate that this suggester is storing the lookup

    assertEquals(suggester,
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",5).
            get("str", n -> "name".equals(n.attr("name"))).txt());

    assertEquals(suggester,
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",5).
            get("str", n -> "storeDir".equals(n.attr("name"))).txt());
    
    // validate that this suggester only builds manually

    assertEquals("false",
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",5).
            get("str", n -> "buildOnCommit".equals(n.attr("name"))).txt());
    assertEquals("false",
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",5).
            get("str", n -> "buildOnStartup".equals(n.attr("name"))).txt());
    
    // build the suggester manually
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_BUILD, "true"),
        "//str[@name='command'][.='build']"
        );
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='exampel']/int[@name='numFound'][.='2']"
        );
    
    reloadCore(false);
    
    // Validate that the suggester was loaded on core reload
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='exampel']/int[@name='numFound'][.='2']"
        );
    
    reloadCore(true);
    
    // Validate that the suggester was loaded on new core
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggester,
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggester + "']/lst[@name='exampel']/int[@name='numFound'][.='2']"
        );
  }
  
  public void testBuildOnStartupWithCoreReload() throws Exception {
    doTestBuildOnStartup(false);
  }
  
  public void testBuildOnStartupWithNewCores() throws Exception {
    doTestBuildOnStartup(true);
  }
  
  private void doTestBuildOnStartup(boolean createNewCores) throws Exception {
    
    final String suggesterFuzzy = "suggest_fuzzy_doc_dict";
    
    // the test relies on useColdSearcher=false
    assertFalse("Precondition not met for test. useColdSearcher must be false", 
        h.getCore().getSolrConfig().useColdSearcher);
    
    // validate that this suggester is not storing the lookup and buildOnStartup is not set
    assertEquals(suggesterFuzzy,
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",2).
            get("str", n -> "name".equals(n.attr("name"))).txt());

    assertNull(h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
        .get("lst",2)
        .get("str", n -> "storeDir".equals(n.attr("name"))).txt());

    
    // assert that buildOnStartup=false
    assertEquals("false",
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",2)
            .get("str", n -> "buildOnStartup".equals(n.attr("name"))).txt());
    assertEquals("true",
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",2).
            get("str", n -> "buildOnCommit".equals(n.attr("name"))).txt());
    
    // verify that this suggester is built (there was a commit in setUp)
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggesterFuzzy, 
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggesterFuzzy + "']/lst[@name='exampel']/int[@name='numFound'][.='2']"
        );
    
    // reload the core and wait for for the listeners to finish
    reloadCore(createNewCores);
    if (System.getProperty(SYSPROP_NIGHTLY) != null) {
      // wait some time here in nightly to make sure there are no race conditions in suggester build
      Thread.sleep(1000);
    }
    
    // The suggester should be empty
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggesterFuzzy, 
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggesterFuzzy + "']/lst[@name='exampel']/int[@name='numFound'][.='0']"
        );
    
    // build the suggester manually
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggesterFuzzy, 
        SuggesterParams.SUGGEST_BUILD, "true"),
        "//str[@name='command'][.='build']"
        );
    
    // validate the suggester is built again
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggesterFuzzy, 
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggesterFuzzy + "']/lst[@name='exampel']/int[@name='numFound'][.='2']"
        );
    
    final String suggestStartup = "suggest_fuzzy_doc_dict_build_startup";
    
    // repeat the test with "suggest_fuzzy_doc_dict_build_startup", it is exactly the same but with buildOnStartup=true
    assertEquals(suggestStartup,
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",4)
            .get("str", n -> "name".equals(n.attr("name"))).txt());
    assertNull( h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
        .get("lst",4)
        .get("str", n -> "storeDir".equals(n.attr("name"))).txt());
    assertEquals("true",
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",4)
            .get("str", n -> "buildOnStartup".equals(n.attr("name"))).txt());
    assertEquals("false",
        h.getCore().getSolrConfig().get("searchComponent", n -> "suggest".equals(n.attr("name")))
            .get("lst",4)
            .get("str", n -> "buildOnCommit".equals(n.attr("name"))).txt());
    
    // reload the core
    reloadCore(createNewCores);
    // verify that this suggester is built (should build on startup)
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggestStartup, 
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggestStartup + "']/lst[@name='exampel']/int[@name='numFound'][.='2']"
        );
    
    // add one more doc, this should not be seen without rebuilding manually or reloading the core (buildOnCommit=false)
    assertU(adoc("id", "10", "cat", "example data extra ", "price", "40", "weight", "35"));
    assertU((commit()));
    
    waitForWarming();

    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggestStartup, 
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggestStartup + "']/lst[@name='exampel']/int[@name='numFound'][.='2']"
        );
    
    // build the suggester manually
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggestStartup, 
        SuggesterParams.SUGGEST_BUILD, "true"),
        "//str[@name='command'][.='build']"
        );
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, suggestStartup, 
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "5"),
        "//lst[@name='suggest']/lst[@name='" + suggestStartup + "']/lst[@name='exampel']/int[@name='numFound'][.='3']"
        );
  }
  
  private void reloadCore(boolean createNewCore) throws Exception {
    if (createNewCore) {
      CoreContainer cores = h.getCoreContainer();
      SolrCore core = h.getCore();
      String dataDir1 = core.getDataDir();
      CoreDescriptor cd = core.getCoreDescriptor();
      h.close();
      createCore();
      SolrCore createdCore = h.getCore();
      assertEquals(dataDir1, createdCore.getDataDir());
      assertEquals(createdCore, h.getCore());
    } else {
      h.reload();
      // On regular reloading, wait until the new searcher is registered
      waitForWarming();
    }
    
    assertQ(req("qt", "/select",
        "q", "*:*"), 
        "//*[@numFound='11']"
        );
  }

}
