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
package org.apache.solr.spelling.suggest;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;

public class TestHighFrequencyDictionaryFactory extends SolrTestCaseJ4  {
  
  static final String REQUEST_URI = "/fuzzy_suggest_analyzing_with_high_freq_dict";
  static final String DICT_NAME = "fuzzy_suggest_analyzing_with_high_freq_dict";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-phrasesuggest.xml","schema-phrasesuggest.xml");
    // Suggestions text include : change, charge, chance
    assertU(adoc("id", "9999990",
        "text", "true",
        "stext", "foobar"));
    assertU(adoc("id", "9999991",
        "text", "true",
        "stext", "foobar"));
    assertU(adoc("id", "9999992",
        "text", "true",
        "stext", "change"));
    assertU(adoc("id", "9999993",
        "text", "true",
        "stext", "charge"));    
    assertU(adoc("id", "9999993",
            "text", "true",
            "stext", "chance"));

    assertU(commit());
    assertQ(req("qt", REQUEST_URI, "q", "", SuggesterParams.SUGGEST_DICT, DICT_NAME, SuggesterParams.SUGGEST_BUILD, "true"));
  }
  
  
  public void testDefault() throws Exception {
    
 // tests to demonstrate default maxEdit parameter (value: 1), control for testWithMaxEdit2
    assertQ(req("qt", REQUEST_URI, "q", "chagn", SuggesterParams.SUGGEST_DICT, DICT_NAME, SuggesterParams.SUGGEST_COUNT, "3"),
      "//lst[@name='suggest']/lst[@name='"+ DICT_NAME +"']/lst[@name='chagn']/int[@name='numFound'][.='2']",
      "//lst[@name='suggest']/lst[@name='"+ DICT_NAME +"']/lst[@name='chagn']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='chance']",
      "//lst[@name='suggest']/lst[@name='"+ DICT_NAME +"']/lst[@name='chagn']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='change']"
      );
    
    assertQ(req("qt", REQUEST_URI, "q", "chacn", SuggesterParams.SUGGEST_DICT, DICT_NAME, SuggesterParams.SUGGEST_COUNT, "3"),
      "//lst[@name='suggest']/lst[@name='"+ DICT_NAME +"']/lst[@name='chacn']/int[@name='numFound'][.='2']",
      "//lst[@name='suggest']/lst[@name='"+ DICT_NAME +"']/lst[@name='chacn']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='chance']",
      "//lst[@name='suggest']/lst[@name='"+ DICT_NAME +"']/lst[@name='chacn']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='change']"
      );
    
    assertQ(req("qt", REQUEST_URI, "q", "chagr", SuggesterParams.SUGGEST_DICT, DICT_NAME, SuggesterParams.SUGGEST_COUNT, "3"),
      "//lst[@name='suggest']/lst[@name='"+ DICT_NAME +"']/lst[@name='chagr']/int[@name='numFound'][.='1']",
      "//lst[@name='suggest']/lst[@name='"+ DICT_NAME +"']/lst[@name='chagr']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='charge']"
      );
    
    assertQ(req("qt", REQUEST_URI, "q", "chanr", SuggesterParams.SUGGEST_DICT, DICT_NAME, SuggesterParams.SUGGEST_COUNT, "3"),
    "//lst[@name='suggest']/lst[@name='"+ DICT_NAME +"']/lst[@name='chanr']/int[@name='numFound'][.='3']"
    );
    
    assertQ(req("qt", REQUEST_URI, "q", "cyhnce", SuggesterParams.SUGGEST_DICT, DICT_NAME, SuggesterParams.SUGGEST_COUNT, "3"),
    "//lst[@name='suggest']/lst[@name='"+ DICT_NAME +"']/lst[@name='cyhnce']/int[@name='numFound'][.='0']"
    );
  }
}
