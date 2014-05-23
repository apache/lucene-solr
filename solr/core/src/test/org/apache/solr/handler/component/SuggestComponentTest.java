package org.apache.solr.handler.component;

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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.spelling.suggest.SuggesterParams;
import org.junit.BeforeClass;
import org.junit.Test;


public class SuggestComponentTest extends SolrTestCaseJ4 {
  static String rh = "/suggest";


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
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    assertU(delQ("*:*"));
    optimize();
    assertU((commit()));
  }
  
  @Test
  public void testDocumentBased() throws Exception {
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_DICT, "suggest_fuzzy_doc_dict", 
        SuggesterParams.SUGGEST_BUILD, "true",
        SuggesterParams.SUGGEST_Q, "exampel",
        SuggesterParams.SUGGEST_COUNT, "2"),
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
        SuggesterParams.SUGGEST_COUNT, "2"),
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
        SuggesterParams.SUGGEST_COUNT, "2"),
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
        SuggesterParams.SUGGEST_COUNT, "2"),
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
        SuggesterParams.SUGGEST_COUNT, "2"),
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
        SuggesterParams.SUGGEST_COUNT, "2"),
        "//str[@name='command'][.='reloadAll']"
        );
    
    assertQ(req("qt", rh, 
        SuggesterParams.SUGGEST_RELOAD_ALL, "true"),
        "//str[@name='command'][.='reloadAll']"
        );
  }
  
}
