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

package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class TestCollapseQParserPlugin extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-collapseqparser.xml", "schema11.xml");
  }

  @Override
  @Before
  public void setUp() throws Exception {
    // if you override setUp or tearDown, you better call
    // the super classes version
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testCollapseQueries() throws Exception {
    String[] doc = {"id","1", "term_s", "YYYY", "group_s", "group1", "test_ti", "5", "test_tl", "10", "test_tf", "2000"};
    assertU(adoc(doc));
    String[] doc1 = {"id","2", "term_s","YYYY", "group_s", "group1", "test_ti", "50", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc1));

    String[] doc2 = {"id","3", "term_s", "YYYY", "test_ti", "5000", "test_tl", "100", "test_tf", "200"};
    assertU(adoc(doc2));

    String[] doc3 = {"id","4", "term_s", "YYYY", "test_ti", "500", "test_tl", "1000", "test_tf", "2000"};
    assertU(adoc(doc3));

    assertU(commit());

    //Test collapse by score
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    assertQ(req(params), "*[count(//doc)=1]", "//doc[./int[@name='test_ti']='50']");

    //Test collapse by score with elevation

    params = new ModifiableSolrParams();
    params.add("q", "YYYY");
    params.add("fq", "{!collapse field=group_s nullPolicy=collapse}");
    params.add("defType", "edismax");
    params.add("bf", "field(test_ti)");
    params.add("qf", "term_s");
    params.add("qt", "/elevate");
    assertQ(req(params), "*[count(//doc)=3]", "//doc[./int[1][@name='test_ti']='5']");

    //Test collapse by min int field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s min=test_ti}");
    assertQ(req(params), "*[count(//doc)=1]", "//doc[./int[@name='test_ti']='5']");

    //Test collapse by max int field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s max=test_ti}");
    assertQ(req(params), "*[count(//doc)=1]", "//doc[./int[@name='test_ti']='50']");

    //Test collapse by min long field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s min=test_tl}");
    assertQ(req(params), "*[count(//doc)=1]", "//doc[./int[@name='test_ti']='5']");

    //Test collapse by max long field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s max=test_tl}");
    assertQ(req(params), "*[count(//doc)=1]", "//doc[./int[@name='test_ti']='50']");

    //Test collapse by min float field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s min=test_tf}");
    assertQ(req(params), "*[count(//doc)=1]", "//doc[./int[@name='test_ti']='50']");

    //Test collapse by min float field
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s max=test_tf}");
    assertQ(req(params), "*[count(//doc)=1]", "//doc[./int[@name='test_ti']='5']");

    //Test nullPolicy expand
    params = new ModifiableSolrParams();
    params.add("q", "*:*");
    params.add("fq", "{!collapse field=group_s max=test_tf nullPolicy=expand}");
    assertQ(req(params), "*[count(//doc)=3]");

    //Test nullPolicy collapse
    params = new ModifiableSolrParams();
    params.add("q", "test_ti:(500 5000)");
    params.add("fq", "{!collapse field=group_s max=test_tf nullPolicy=collapse}");
    assertQ(req(params), "*[count(//doc)=1]", "//doc[./int[@name='test_ti']='500']");
  }
}
