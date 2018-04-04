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

public class TestAnalyzeInfixSuggestions extends SolrTestCaseJ4  {
  static final String URI_DEFAULT = "/infix_suggest_analyzing";
  static final String URI_SUGGEST_DEFAULT = "/analyzing_infix_suggest";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-phrasesuggest.xml","schema-phrasesuggest.xml");
    assertQ(req("qt", URI_DEFAULT, "q", "", SpellingParams.SPELLCHECK_BUILD, "true"));
    assertQ(req("qt", URI_SUGGEST_DEFAULT, "q", "", SuggesterParams.SUGGEST_BUILD_ALL, "true"));    
  }
  
  public void testSingle() throws Exception {
    
    assertQ(req("qt", URI_DEFAULT, "q", "japan", SpellingParams.SPELLCHECK_COUNT, "1"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/int[@name='numFound'][.='1']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/arr[@name='suggestion']/str[1][.='<b>Japan</b>ese Autocomplete and <b>Japan</b>ese Highlighter broken']"
      );
    
    assertQ(req("qt", URI_DEFAULT, "q", "high", SpellingParams.SPELLCHECK_COUNT, "1"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='high']/int[@name='numFound'][.='1']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='high']/arr[@name='suggestion']/str[1][.='Japanese Autocomplete and Japanese <b>High</b>lighter broken']"
      );
   
    /* equivalent SolrSuggester, SuggestComponent tests */ 
    assertQ(req("qt", URI_SUGGEST_DEFAULT, "q", "japan", SuggesterParams.SUGGEST_COUNT, "1", SuggesterParams.SUGGEST_DICT, "analyzing_infix_suggest_default"),
      "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_default']/lst[@name='japan']/int[@name='numFound'][.='1']",
      "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_default']/lst[@name='japan']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='<b>Japan</b>ese Autocomplete and <b>Japan</b>ese Highlighter broken']"
    );
    
    assertQ(req("qt", URI_SUGGEST_DEFAULT, "q", "high", SuggesterParams.SUGGEST_COUNT, "1", SuggesterParams.SUGGEST_DICT, "analyzing_infix_suggest_default"),
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_default']/lst[@name='high']/int[@name='numFound'][.='1']",
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_default']/lst[@name='high']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='Japanese Autocomplete and Japanese <b>High</b>lighter broken']"
      );
  }
  
  public void testMultiple() throws Exception {
    
    assertQ(req("qt", URI_DEFAULT, "q", "japan", SpellingParams.SPELLCHECK_COUNT, "2"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/int[@name='numFound'][.='2']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/arr[@name='suggestion']/str[1][.='<b>Japan</b>ese Autocomplete and <b>Japan</b>ese Highlighter broken']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/arr[@name='suggestion']/str[2][.='Add <b>Japan</b>ese Kanji number normalization to Kuromoji']"
      );
    assertQ(req("qt", URI_DEFAULT, "q", "japan", SpellingParams.SPELLCHECK_COUNT, "3"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/int[@name='numFound'][.='3']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/arr[@name='suggestion']/str[1][.='<b>Japan</b>ese Autocomplete and <b>Japan</b>ese Highlighter broken']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/arr[@name='suggestion']/str[2][.='Add <b>Japan</b>ese Kanji number normalization to Kuromoji']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/arr[@name='suggestion']/str[3][.='Add decompose compound <b>Japan</b>ese Katakana token capability to Kuromoji']"
      );
    assertQ(req("qt", URI_DEFAULT, "q", "japan", SpellingParams.SPELLCHECK_COUNT, "4"),
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/int[@name='numFound'][.='3']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/arr[@name='suggestion']/str[1][.='<b>Japan</b>ese Autocomplete and <b>Japan</b>ese Highlighter broken']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/arr[@name='suggestion']/str[2][.='Add <b>Japan</b>ese Kanji number normalization to Kuromoji']",
      "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='japan']/arr[@name='suggestion']/str[3][.='Add decompose compound <b>Japan</b>ese Katakana token capability to Kuromoji']"
      );
    
    /* SolrSuggester, SuggestComponent tests: allTermsRequire (true), highlight (true) */ 
    assertQ(req("qt", URI_SUGGEST_DEFAULT, "q", "japan", SuggesterParams.SUGGEST_COUNT, "2", SuggesterParams.SUGGEST_DICT, "analyzing_infix_suggest_default"),
      "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_default']/lst[@name='japan']/int[@name='numFound'][.='2']",
      "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_default']/lst[@name='japan']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='<b>Japan</b>ese Autocomplete and <b>Japan</b>ese Highlighter broken']",
      "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_default']/lst[@name='japan']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='Add <b>Japan</b>ese Kanji number normalization to Kuromoji']"
      );
    
    assertQ(req("qt", URI_SUGGEST_DEFAULT, "q", "japanese ka", SuggesterParams.SUGGEST_COUNT, "2", SuggesterParams.SUGGEST_DICT, "analyzing_infix_suggest_default"),
      "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_default']/lst[@name='japanese ka']/int[@name='numFound'][.='2']",
      "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_default']/lst[@name='japanese ka']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='Add <b>Japanese</b> <b>Ka</b>nji number normalization to Kuromoji']",
      "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_default']/lst[@name='japanese ka']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='Add decompose compound <b>Japanese</b> <b>Ka</b>takana token capability to Kuromoji']"
      );
    
  }
  
  public void testWithoutHighlight() throws Exception {
     assertQ(req("qt", URI_SUGGEST_DEFAULT, "q", "japan", SuggesterParams.SUGGEST_COUNT, "2", SuggesterParams.SUGGEST_DICT, "analyzing_infix_suggest_without_highlight"),
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_without_highlight']/lst[@name='japan']/int[@name='numFound'][.='2']",
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_without_highlight']/lst[@name='japan']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='Japanese Autocomplete and Japanese Highlighter broken']",
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_without_highlight']/lst[@name='japan']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='Add Japanese Kanji number normalization to Kuromoji']"
     );
  }
  
  public void testNotAllTermsRequired() throws Exception {
     assertQ(req("qt", URI_SUGGEST_DEFAULT, "q", "japanese javanese", SuggesterParams.SUGGEST_COUNT, "5", SuggesterParams.SUGGEST_DICT, "analyzing_infix_suggest_not_all_terms_required"),
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_not_all_terms_required']/lst[@name='japanese javanese']/int[@name='numFound'][.='3']",
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_not_all_terms_required']/lst[@name='japanese javanese']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='<b>Japanese</b> Autocomplete and <b>Japanese</b> Highlighter broken']",
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_not_all_terms_required']/lst[@name='japanese javanese']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='Add <b>Japanese</b> Kanji number normalization to Kuromoji']",
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_not_all_terms_required']/lst[@name='japanese javanese']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='Add decompose compound <b>Japanese</b> Katakana token capability to Kuromoji']"
     );
     
     assertQ(req("qt", URI_SUGGEST_DEFAULT, "q", "just number", SuggesterParams.SUGGEST_COUNT, "5", SuggesterParams.SUGGEST_DICT, "analyzing_infix_suggest_not_all_terms_required"),
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_not_all_terms_required']/lst[@name='just number']/int[@name='numFound'][.='2']",
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_not_all_terms_required']/lst[@name='just number']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='Add Japanese Kanji <b>number</b> normalization to Kuromoji']",
       "//lst[@name='suggest']/lst[@name='analyzing_infix_suggest_not_all_terms_required']/lst[@name='just number']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='This is <b>just</b> another entry!']"
     );
     
  }
  
}
