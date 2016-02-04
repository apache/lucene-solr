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

public class TestPhraseSuggestions extends SolrTestCaseJ4 {
  static final String URI = "/suggest_wfst";
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-phrasesuggest.xml","schema-phrasesuggest.xml");
    assertQ(req("qt", URI, "q", "", SpellingParams.SPELLCHECK_BUILD, "true"));
  }
  
  public void test() {
    assertQ(req("qt", URI, "q", "the f", SpellingParams.SPELLCHECK_COUNT, "4"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='the f']/int[@name='numFound'][.='3']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='the f']/arr[@name='suggestion']/str[1][.='the final phrase']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='the f']/arr[@name='suggestion']/str[2][.='the fifth phrase']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='the f']/arr[@name='suggestion']/str[3][.='the first phrase']"
    );
    
    assertQ(req("qt", URI, "q", "Testing +12", SpellingParams.SPELLCHECK_COUNT, "4"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='testing 12']/int[@name='numFound'][.='1']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='testing 12']/arr[@name='suggestion']/str[1][.='testing 1234']"
    );
  }
}
