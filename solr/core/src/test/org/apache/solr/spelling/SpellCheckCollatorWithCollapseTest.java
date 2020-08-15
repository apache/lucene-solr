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
package org.apache.solr.spelling;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.handler.component.SpellCheckComponent;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SpellCheckCollatorWithCollapseTest  extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-collapseqparser.xml", "schema11.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }
  
  @Test
  public void test() throws Exception {
    for(int i=0 ; i<200 ; i++) {
      String[] doc = {"id","" + i, "group_i", "" + (i % 10), "a_s", ((i%2)==0 ? "love" : "peace")};
      assertU(adoc(doc));
      if(i%5==0) {
        assertU(commit());
      }
    }
    assertU(commit());

    for (SolrParams params : new SolrParams[]{
        params(CommonParams.FQ, "{!collapse field=group_i}"),
        params(CommonParams.FQ, "${bleh}", "bleh", "{!collapse field=group_i}"), // substitution
        params(CommonParams.FQ, "{!tag=collapser}{!collapse field=group_i}"), // with tag & collapse in localparams
        params(CommonParams.FQ, "{!collapse tag=collapser field=group_i}")
    }) {
      assertQ(
          req(params,
              SpellCheckComponent.COMPONENT_NAME, "true",
          SpellCheckComponent.SPELLCHECK_DICT, "direct",
          SpellingParams.SPELLCHECK_COUNT, "10",
          SpellingParams.SPELLCHECK_COLLATE, "true",
          SpellingParams.SPELLCHECK_MAX_COLLATION_TRIES, "5",
          SpellingParams.SPELLCHECK_MAX_COLLATIONS, "1",
          CommonParams.Q, "a_s:lpve",
          CommonParams.QT, "/spellCheckCompRH_Direct",
          SpellingParams.SPELLCHECK_COLLATE_MAX_COLLECT_DOCS, "5",
          "expand", "true"),
          "//lst[@name='spellcheck']/lst[@name='collations']/str[@name='collation']='a_s:love'"
      );
    }
  }

}
