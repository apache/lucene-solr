package org.apache.solr.spelling.suggest;

import java.io.File;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.SpellingParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;

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

public class TestAnalyzeInfixSuggestions extends SolrTestCaseJ4  {
  static final String URI_DEFAULT = "/infix_suggest_analyzing";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-phrasesuggest.xml","schema-phrasesuggest.xml");
    assertQ(req("qt", URI_DEFAULT, "q", "", SpellingParams.SPELLCHECK_BUILD, "true"));
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    File indexPathDir = new File("analyzingInfixSuggesterIndexDir");
    File indexPathDirTmp = new File("analyzingInfixSuggesterIndexDir.tmp");
    if (indexPathDir.exists())
      recurseDelete(indexPathDir);
    if (indexPathDirTmp.exists())
      recurseDelete(indexPathDirTmp);
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
  }
  
}