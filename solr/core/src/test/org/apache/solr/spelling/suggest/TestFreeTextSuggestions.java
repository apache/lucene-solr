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

public class TestFreeTextSuggestions extends SolrTestCaseJ4 {
  static final String URI = "/free_text_suggest";
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-phrasesuggest.xml","schema-phrasesuggest.xml");
    assertQ(req("qt", URI, "q", "", SuggesterParams.SUGGEST_BUILD_ALL, "true"));
  }
  
  public void test() {
    assertQ(req("qt", URI, "q", "foo b", SuggesterParams.SUGGEST_COUNT, "1", SuggesterParams.SUGGEST_DICT, "free_text_suggest"),
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='foo b']/int[@name='numFound'][.='1']",
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='foo b']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='foo bar']"
    );
    
    assertQ(req("qt", URI, "q", "foo ", SuggesterParams.SUGGEST_COUNT, "2", SuggesterParams.SUGGEST_DICT, "free_text_suggest"),
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='foo ']/int[@name='numFound'][.='2']",
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='foo ']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='foo bar']",
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='foo ']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='foo bee']"
    );
    
    assertQ(req("qt", URI, "q", "foo", SuggesterParams.SUGGEST_COUNT, "2", SuggesterParams.SUGGEST_DICT, "free_text_suggest"),
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='foo']/int[@name='numFound'][.='1']",
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='foo']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='foo']"
    );
    assertQ(req("qt", URI, "q", "b", SuggesterParams.SUGGEST_COUNT, "5", SuggesterParams.SUGGEST_DICT, "free_text_suggest"),
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='b']/int[@name='numFound'][.='5']",
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='b']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='bar']",
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='b']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='baz']",
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='b']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='bee']",
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='b']/arr[@name='suggestions']/lst[4]/str[@name='term'][.='blah']",
        "//lst[@name='suggest']/lst[@name='free_text_suggest']/lst[@name='b']/arr[@name='suggestions']/lst[5]/str[@name='term'][.='boo']"
    );
  }
  
}
