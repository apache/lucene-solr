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
import org.apache.solr.common.params.SpellingParams;
import org.junit.BeforeClass;

public class TestFuzzyAnalyzedSuggestions extends SolrTestCaseJ4  {
  static final String URI_DEFAULT = "/fuzzy_suggest_analyzing";
  static final String URI_MIN_EDIT_2 = "/fuzzy_suggest_analyzing_with_max_edit_2";
  static final String URI_NON_PREFIX_LENGTH_4 = "/fuzzy_suggest_analyzing_with_non_fuzzy_prefix_4";
  static final String URI_MIN_FUZZY_LENGTH = "/fuzzy_suggest_analyzing_with_min_fuzzy_length_2";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-phrasesuggest.xml","schema-phrasesuggest.xml");
    // Suggestions text include : change, charge, chance
    assertQ(req("qt", URI_DEFAULT, "q", "", SpellingParams.SPELLCHECK_BUILD, "true"));
    assertQ(req("qt", URI_MIN_EDIT_2, "q", "", SpellingParams.SPELLCHECK_BUILD, "true"));
    assertQ(req("qt", URI_NON_PREFIX_LENGTH_4, "q", "", SpellingParams.SPELLCHECK_BUILD, "true"));
    assertQ(req("qt", URI_MIN_FUZZY_LENGTH, "q", "", SpellingParams.SPELLCHECK_BUILD, "true"));
  }
  
  public void testDefault() throws Exception {
    
    // tests to demonstrate default maxEdit parameter (value: 1), control for testWithMaxEdit2
    assertQ(req("qt", URI_DEFAULT, "q", "chagn", SpellingParams.SPELLCHECK_COUNT, "3"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagn']/int[@name='numFound'][.='2']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagn']/arr[@name='suggestion']/str[1][.='chance']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagn']/arr[@name='suggestion']/str[2][.='change']"
      );
    
    assertQ(req("qt", URI_DEFAULT, "q", "chacn", SpellingParams.SPELLCHECK_COUNT, "3"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chacn']/int[@name='numFound'][.='2']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chacn']/arr[@name='suggestion']/str[1][.='chance']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chacn']/arr[@name='suggestion']/str[2][.='change']"
      );
    
    assertQ(req("qt", URI_DEFAULT, "q", "chagr", SpellingParams.SPELLCHECK_COUNT, "3"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagr']/int[@name='numFound'][.='1']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagr']/arr[@name='suggestion']/str[1][.='charge']"
      );
    
    // test to demonstrate default nonFuzzyPrefix parameter (value: 1), control for testWithNonFuzzyPrefix4
    assertQ(req("qt", URI_DEFAULT, "q", "chanr", SpellingParams.SPELLCHECK_COUNT, "3"),
    "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chanr']/int[@name='numFound'][.='3']"
    );
    
    // test to demonstrate default minFuzzyPrefix parameter (value: 3), control for testWithMinFuzzyLength2
    assertQ(req("qt", URI_DEFAULT, "q", "cyhnce", SpellingParams.SPELLCHECK_COUNT, "3"),
    "//lst[@name='spellcheck']/lst[@name='suggestions'][not(node())]"
    );
  }
  
  public void testWithMaxEdit2() throws Exception {
    
    assertQ(req("qt", URI_MIN_EDIT_2, "q", "chagn", SpellingParams.SPELLCHECK_COUNT, "3"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagn']/int[@name='numFound'][.='3']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagn']/arr[@name='suggestion']/str[1][.='chance']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagn']/arr[@name='suggestion']/str[2][.='change']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagn']/arr[@name='suggestion']/str[3][.='charge']"
      );
    
    assertQ(req("qt", URI_MIN_EDIT_2, "q", "chagr", SpellingParams.SPELLCHECK_COUNT, "3"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagr']/int[@name='numFound'][.='3']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagr']/arr[@name='suggestion']/str[1][.='chance']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagr']/arr[@name='suggestion']/str[2][.='change']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chagr']/arr[@name='suggestion']/str[3][.='charge']"
      );
    
    assertQ(req("qt", URI_MIN_EDIT_2, "q", "chacn", SpellingParams.SPELLCHECK_COUNT, "3"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chacn']/int[@name='numFound'][.='3']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chacn']/arr[@name='suggestion']/str[1][.='chance']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chacn']/arr[@name='suggestion']/str[2][.='change']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chacn']/arr[@name='suggestion']/str[3][.='charge']"
      );
  }
  
  public void testWithNonFuzzyPrefix4() throws Exception {
    
    // This test should not match charge, as the nonFuzzyPrefix has been set to 4
    assertQ(req("qt", URI_NON_PREFIX_LENGTH_4, "q", "chanr", SpellingParams.SPELLCHECK_COUNT, "3"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chanr']/int[@name='numFound'][.='2']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chanr']/arr[@name='suggestion']/str[1][.='chance']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chanr']/arr[@name='suggestion']/str[2][.='change']"
      );
  }
  
  public void testWithMinFuzzyLength2() throws Exception {
    
    // This test should match chance as the minFuzzyLength parameter has been set to 2
    assertQ(req("qt", URI_MIN_FUZZY_LENGTH, "q", "chynce", SpellingParams.SPELLCHECK_COUNT, "3"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chynce']/int[@name='numFound'][.='1']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='chynce']/arr[@name='suggestion']/str[1][.='chance']"
      );
  }
}
