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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestTermQParserPlugin extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");

    assertU(adoc("id","1", "author_s1", "Lev Grossman", "t_title", "The Magicians",  "cat_s", "fantasy", "pubyear_i", "2009"));
    assertU(adoc("id", "2", "author_s1", "Robert Jordan", "t_title", "The Eye of the World", "cat_s", "fantasy", "cat_s", "childrens", "pubyear_i", "1990"));
    assertU(adoc("id", "3", "author_s1", "Robert Jordan", "t_title", "The Great Hunt", "cat_s", "fantasy", "cat_s", "childrens", "pubyear_i", "1990"));
    assertU(adoc("id", "4", "author_s1", "N.K. Jemisin", "t_title", "The Fifth Season", "cat_s", "fantasy", "pubyear_i", "2015"));
    assertU(commit());
    assertU(adoc("id", "5", "author_s1", "Ursula K. Le Guin", "t_title", "The Dispossessed", "cat_s", "scifi", "pubyear_i", "1974"));
    assertU(adoc("id", "6", "author_s1", "Ursula K. Le Guin", "t_title", "The Left Hand of Darkness", "cat_s", "scifi", "pubyear_i", "1969"));
    assertU(adoc("id", "7", "author_s1", "Isaac Asimov", "t_title", "Foundation", "cat_s", "scifi", "pubyear_i", "1951"));
    assertU(commit());
  }

  @Test
  public void testTextTermsQuery() {
    // Single term value
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q", "{!term f=t_title}left");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=1]",
        "//result/doc[1]/str[@name='id'][.='6']"
    );
    // Single term value
    params = new ModifiableSolrParams();
    params.add("q", "{!term f=t_title}the");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=0]");
  }
  
  @Test
  public void testMissingField() {
    assertQEx("Expecting bad request", "Missing field to query", req("q", "{!term}childrens"), SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testTermsMethodEquivalency() {
    // Single-valued field
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("q","{!term f=author_s1}Robert Jordan");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=2]",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='3']"
    );

    // Multi-valued field
    params = new ModifiableSolrParams();
    params.add("q", "{!term f=cat_s}childrens");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=2]",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='3']"
    );

    // Numeric field
    params = new ModifiableSolrParams();
    params.add("q", "{!term f=pubyear_i}2009");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");

    // Numeric field
    params = new ModifiableSolrParams();
    params.add("q", "{!term f=pubyear_i}2009");
    params.add("sort", "id asc");
    assertQ(req(params, "indent", "on"), "*[count(//doc)=1]", "//result/doc[1]/str[@name='id'][.='1']");
  }
}
