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

public class TestAnalyzedSuggestions extends SolrTestCaseJ4 {
  static final String URI = "/suggest_analyzing";
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-phrasesuggest.xml","schema-phrasesuggest.xml");
    assertQ(req("qt", URI, "q", "", SpellingParams.SPELLCHECK_BUILD, "true"));
  }
  
  public void test() {
    assertQ(req("qt", URI, "q", "hokk", SpellingParams.SPELLCHECK_COUNT, "1"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='hokk']/int[@name='numFound'][.='1']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='hokk']/arr[@name='suggestion']/str[1][.='北海道']"
    );
    assertQ(req("qt", URI, "q", "ほっk", SpellingParams.SPELLCHECK_COUNT, "1"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ほっk']/int[@name='numFound'][.='1']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ほっk']/arr[@name='suggestion']/str[1][.='北海道']"
    );
    assertQ(req("qt", URI, "q", "ホッk", SpellingParams.SPELLCHECK_COUNT, "1"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ホッk']/int[@name='numFound'][.='1']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ホッk']/arr[@name='suggestion']/str[1][.='北海道']"
    );
    assertQ(req("qt", URI, "q", "ﾎｯk", SpellingParams.SPELLCHECK_COUNT, "1"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ﾎｯk']/int[@name='numFound'][.='1']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='ﾎｯk']/arr[@name='suggestion']/str[1][.='北海道']"
    );
  }
  
  public void testMultiple() {
    assertQ(req("qt", URI, "q", "h", SpellingParams.SPELLCHECK_COUNT, "2"),
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='h']/int[@name='numFound'][.='2']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='h']/arr[@name='suggestion']/str[1][.='話した']",
        "//lst[@name='spellcheck']/lst[@name='suggestions']/lst[@name='h']/arr[@name='suggestion']/str[2][.='北海道']"
    );
  }
}
