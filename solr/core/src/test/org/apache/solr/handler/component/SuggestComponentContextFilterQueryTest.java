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
import org.apache.solr.spelling.suggest.SuggesterParams;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.core.Is.is;

public class SuggestComponentContextFilterQueryTest extends SolrTestCaseJ4 {

  static String rh = "/suggest";

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-suggestercomponent-context-filter-query.xml", "schema.xml");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();

    assertU(delQ("*:*"));
    // id, cat, price, weight, contexts
    assertU(adoc("id", "0", "cat", "This is a title", "price", "5", "weight", "10", "my_contexts_t", "ctx1"));
    assertU(adoc("id", "1", "cat", "This is another title", "price", "10", "weight", "10", "my_contexts_t", "ctx1"));
    assertU(adoc("id", "7", "cat", "example with ctx1 at 40", "price", "40", "weight", "30", "my_contexts_t", "ctx1"));
    assertU(adoc("id", "8", "cat", "example with ctx2 and ctx3 at 45", "price", "45", "weight", "30", "my_contexts_t", "CTX2", "my_contexts_t", "CTX3"));
    assertU(adoc("id", "9", "cat", "example with ctx4 at 50 using my_contexts_s", "price", "50", "weight", "40", "my_contexts_s", "ctx4"));
    assertU((commit()));
    waitForWarming();
  }

  @Test
  public void testContextFilterParamIsIgnoredWhenContextIsNotImplemented() throws Exception {
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_lookup_has_no_context_implementation",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "ctx1",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_lookup_has_no_context_implementation']/lst[@name='examp']/int[@name='numFound'][.='3']",
        "//lst[@name='suggest']/lst[@name='suggest_lookup_has_no_context_implementation']/lst[@name='examp']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example with ctx4 at 50 using my_contexts_s']",
        "//lst[@name='suggest']/lst[@name='suggest_lookup_has_no_context_implementation']/lst[@name='examp']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='example with ctx2 and ctx3 at 45']",
        "//lst[@name='suggest']/lst[@name='suggest_lookup_has_no_context_implementation']/lst[@name='examp']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='example with ctx1 at 40']"
    );
  }


  @Test
  public void testContextFilteringIsIgnoredWhenContextIsImplementedButNotConfigured() throws Exception {
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_context_implemented_but_not_configured",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_context_implemented_but_not_configured']/lst[@name='examp']/int[@name='numFound'][.='3']",
        "//lst[@name='suggest']/lst[@name='suggest_context_implemented_but_not_configured']/lst[@name='examp']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example with ctx4 at 50 using my_contexts_s']",
        "//lst[@name='suggest']/lst[@name='suggest_context_implemented_but_not_configured']/lst[@name='examp']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='example with ctx2 and ctx3 at 45']",
        "//lst[@name='suggest']/lst[@name='suggest_context_implemented_but_not_configured']/lst[@name='examp']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='example with ctx1 at 40']"
    );
  }

  @Test
  public void testBuildThrowsIllegalArgumentExceptionWhenContextIsConfiguredButNotImplemented() throws Exception {
    IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> {
      h.query(req("qt", rh, SuggesterParams.SUGGEST_BUILD, "true",
          SuggesterParams.SUGGEST_DICT, "suggest_context_filtering_not_implemented",
          SuggesterParams.SUGGEST_Q, "examp"));
    });
    assertThat(ex.getMessage(), is("this suggester doesn't support contexts"));

    // When not building, no exception is thrown
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "false",
            SuggesterParams.SUGGEST_DICT, "suggest_context_filtering_not_implemented",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_context_filtering_not_implemented']/lst[@name='examp']/int[@name='numFound'][.='0']"
    );
  }


  @Test
  public void testContextFilterIsTrimmed() throws Exception {
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "     ", //trimmed to null... just as if there was no context filter param
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='3']"
    );
  }

  public void testExplicitFieldedQuery() throws Exception {
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "contexts:ctx1",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='1']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example with ctx1 at 40']"
    );
  }

  public void testContextFilterOK() throws Exception {
    //No filtering
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='3']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example with ctx4 at 50 using my_contexts_s']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='example with ctx2 and ctx3 at 45']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[3]/str[@name='term'][.='example with ctx1 at 40']"
    );

    //TermQuery
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "ctx1",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='1']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example with ctx1 at 40']"
    );

    //OR BooleanQuery
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "ctx1 OR CTX2",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='2']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example with ctx2 and ctx3 at 45']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='example with ctx1 at 40']"
    );

    //AND BooleanQuery
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "CTX2 AND CTX3",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='1']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example with ctx2 and ctx3 at 45']");


    //PrefixQuery
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "ctx*",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='1']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example with ctx1 at 40']"
    );

    //RangeQuery
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "[* TO *]",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='2']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example with ctx2 and ctx3 at 45']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[2]/str[@name='term'][.='example with ctx1 at 40']"
    );

    //WildcardQuery
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "c*1",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='1']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='example with ctx1 at 40']");
  }

  @Test
  public void testStringContext(){
    //Here, the context field is a string, so it's case sensitive
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester_string",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "Ctx4",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester_string']/lst[@name='examp']/int[@name='numFound'][.='0']");

    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester_string",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "ctx4",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester_string']/lst[@name='examp']/int[@name='numFound'][.='1']");
  }

  @Test
  public void testContextFilterOnInvalidFieldGivesNoSuggestions() throws Exception {
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "some_invalid_context_field:some_invalid_value",
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='0']");
  }


  @Test
  public void testContextFilterUsesAnalyzer() throws Exception {
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "CTx1", // Will not match due to case
            SuggesterParams.SUGGEST_Q, "examp"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='examp']/int[@name='numFound'][.='0']");
  }

  @Ignore// TODO: SOLR-7964
  @Test
  public void testContextFilterWithHighlight() throws Exception {
    assertQ(req("qt", rh,
            SuggesterParams.SUGGEST_BUILD, "true",
            SuggesterParams.SUGGEST_DICT, "suggest_blended_infix_suggester",
            SuggesterParams.SUGGEST_CONTEXT_FILTER_QUERY, "ctx1",
            SuggesterParams.SUGGEST_HIGHLIGHT, "true",
            SuggesterParams.SUGGEST_Q, "example"),
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='example']/int[@name='numFound'][.='1']",
        "//lst[@name='suggest']/lst[@name='suggest_blended_infix_suggester']/lst[@name='example']/arr[@name='suggestions']/lst[1]/str[@name='term'][.='<b>example</b> data']"
    );
  }

}

