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

public class TestBlendedInfixSuggestions extends SolrTestCaseJ4 {
  static final String URI = "/blended_infix_suggest";
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-phrasesuggest.xml","schema-phrasesuggest.xml");
    assertQ(req("qt", URI, "q", "", SuggesterParams.SUGGEST_BUILD_ALL, "true"));
  }

  public void testLinearBlenderType() {
    assertQ(req("qt", URI, "q", "the", SuggesterParams.SUGGEST_COUNT, "10", SuggesterParams.SUGGEST_DICT, "blended_infix_suggest_linear"),
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/int[@name='numFound'][.='3']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='top of <b>the</b> lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='14']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='payload'][.='lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='<b>the</b> returned']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='10']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='payload'][.='ret']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='star wars: episode v - <b>the</b> empire strikes back']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[3]/long[@name='weight'][.='7']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='payload'][.='star']"
    );
  }
  
  public void testReciprocalBlenderType() {
    assertQ(req("qt", URI, "q", "the", SuggesterParams.SUGGEST_COUNT, "10", SuggesterParams.SUGGEST_DICT, "blended_infix_suggest_reciprocal"),
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/int[@name='numFound'][.='3']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='<b>the</b> returned']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='10']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='payload'][.='ret']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='top of <b>the</b> lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='6']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='payload'][.='lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='star wars: episode v - <b>the</b> empire strikes back']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[3]/long[@name='weight'][.='2']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='payload'][.='star']"
    );
  }

  public void testExponentialReciprocalBlenderTypeExponent1() { //exponent=1 will give same output as reciprocal
    assertQ(req("qt", URI, "q", "the", SuggesterParams.SUGGEST_COUNT, "10", SuggesterParams.SUGGEST_DICT, "blended_infix_suggest_exponential_reciprocal_1"),
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal_1']/lst[@name='the']/int[@name='numFound'][.='3']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal_1']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='<b>the</b> returned']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal_1']/lst[@name='the']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='10']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal_1']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='payload'][.='ret']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal_1']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='top of <b>the</b> lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal_1']/lst[@name='the']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='6']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal_1']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='payload'][.='lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal_1']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='star wars: episode v - <b>the</b> empire strikes back']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal_1']/lst[@name='the']/arr[@name='suggestions']/lst[3]/long[@name='weight'][.='2']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal_1']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='payload'][.='star']"
    );
  }

  public void testExponentialReciprocalBlenderType() { // default is exponent=2.0
    assertQ(req("qt", URI, "q", "the", SuggesterParams.SUGGEST_COUNT, "10", SuggesterParams.SUGGEST_DICT, "blended_infix_suggest_exponential_reciprocal"),
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal']/lst[@name='the']/int[@name='numFound'][.='3']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='<b>the</b> returned']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='10']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='payload'][.='ret']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='top of <b>the</b> lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='2']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='payload'][.='lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='star wars: episode v - <b>the</b> empire strikes back']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[3]/long[@name='weight'][.='0']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_exponential_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='payload'][.='star']"
    );
  }

  public void testMultiSuggester() {
    assertQ(req("qt", URI, "q", "the", SuggesterParams.SUGGEST_COUNT, "10", SuggesterParams.SUGGEST_DICT, "blended_infix_suggest_linear", SuggesterParams.SUGGEST_DICT, "blended_infix_suggest_reciprocal"),
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/int[@name='numFound'][.='3']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='top of <b>the</b> lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='14']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='payload'][.='lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='<b>the</b> returned']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='10']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='payload'][.='ret']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='star wars: episode v - <b>the</b> empire strikes back']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[3]/long[@name='weight'][.='7']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_linear']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='payload'][.='star']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/int[@name='numFound'][.='3']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='<b>the</b> returned']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[1]/long[@name='weight'][.='10']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[1]/str[@name='payload'][.='ret']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='top of <b>the</b> lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[2]/long[@name='weight'][.='6']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[2]/str[@name='payload'][.='lake']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='star wars: episode v - <b>the</b> empire strikes back']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[3]/long[@name='weight'][.='2']",
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/arr[@name='suggestions']/lst[3]/str[@name='payload'][.='star']"
    );
  }

  public void testSuggestCount() {

    assertQ(req("qt", URI, "q", "the", SuggesterParams.SUGGEST_COUNT, "1", SuggesterParams.SUGGEST_DICT, "blended_infix_suggest_reciprocal"),
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/int[@name='numFound'][.='1']"
    );

    assertQ(req("qt", URI, "q", "the", SuggesterParams.SUGGEST_COUNT, "2", SuggesterParams.SUGGEST_DICT, "blended_infix_suggest_reciprocal"),
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/int[@name='numFound'][.='2']"
    );

    assertQ(req("qt", URI, "q", "the", SuggesterParams.SUGGEST_COUNT, "3", SuggesterParams.SUGGEST_DICT, "blended_infix_suggest_reciprocal"),
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/int[@name='numFound'][.='3']"
    );

    assertQ(req("qt", URI, "q", "the", SuggesterParams.SUGGEST_COUNT, "20", SuggesterParams.SUGGEST_DICT, "blended_infix_suggest_reciprocal"),
        "//lst[@name='suggest']/lst[@name='blended_infix_suggest_reciprocal']/lst[@name='the']/int[@name='numFound'][.='3']"
    );
  }

}
