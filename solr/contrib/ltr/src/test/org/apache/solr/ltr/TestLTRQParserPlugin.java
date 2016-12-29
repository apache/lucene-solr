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
package org.apache.solr.ltr;

import org.apache.solr.client.solrj.SolrQuery;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLTRQParserPlugin extends TestRerankBase {


  @BeforeClass
  public static void before() throws Exception {
    setuptest(true);

    loadFeatures("features-linear.json");
    loadModels("linear-model.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void ltrModelIdMissingTest() throws Exception {
    final String solrQuery = "_query_:{!edismax qf='title' mm=100% v='bloomberg' tie=0.1}";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("fv", "true");
    query.add("rq", "{!ltr reRankDocs=100}");

    final String res = restTestHarness.query("/query" + query.toQueryString());
    assert (res.contains("Must provide model in the request"));
  }

  @Test
  public void ltrModelIdDoesNotExistTest() throws Exception {
    final String solrQuery = "_query_:{!edismax qf='title' mm=100% v='bloomberg' tie=0.1}";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("fv", "true");
    query.add("rq", "{!ltr model=-1 reRankDocs=100}");

    final String res = restTestHarness.query("/query" + query.toQueryString());
    assert (res.contains("cannot find model"));
  }

  @Test
  public void ltrBadRerankDocsTest() throws Exception {
    final String solrQuery = "_query_:{!edismax qf='title' mm=100% v='bloomberg' tie=0.1}";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("fv", "true");
    query.add("rq", "{!ltr model=6029760550880411648 reRankDocs=-1}");

    final String res = restTestHarness.query("/query" + query.toQueryString());
    assert (res.contains("Must rerank at least 1 document"));
  }

  @Test
  public void ltrMoreResultsThanReRankedTest() throws Exception {
    final String solrQuery = "_query_:{!edismax qf='title' mm=100% v='bloomberg' tie=0.1}";
    final SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("fv", "true");

    String nonRerankedScore = "0.09271725";

    // Normal solr order
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/id=='9'",
        "/response/docs/[1]/id=='8'",
        "/response/docs/[2]/id=='7'",
        "/response/docs/[3]/id=='6'",
        "/response/docs/[3]/score=="+nonRerankedScore
    );

    query.add("rq", "{!ltr model=6029760550880411648 reRankDocs=3}");

    // Different order for top 3 reranked, but last one is the same top nonreranked doc
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/id=='7'",
        "/response/docs/[1]/id=='8'",
        "/response/docs/[2]/id=='9'",
        "/response/docs/[3]/id=='6'",
        "/response/docs/[3]/score=="+nonRerankedScore
    );
  }

  @Test
  public void ltrNoResultsTest() throws Exception {
    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg23");
    query.add("fl", "*,[fv]");
    query.add("rows", "3");
    query.add("debugQuery", "on");
    query.add("rq", "{!ltr reRankDocs=3 model=6029760550880411648}");
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==0");
  }

}
