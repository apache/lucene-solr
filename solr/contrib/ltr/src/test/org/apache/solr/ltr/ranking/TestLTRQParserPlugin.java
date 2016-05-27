package org.apache.solr.ltr.ranking;

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

import java.lang.invoke.MethodHandles;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressCodecs({"Lucene3x", "Lucene41", "Lucene40", "Appending"})
public class TestLTRQParserPlugin extends TestRerankBase {

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void before() throws Exception {
    setuptest("solrconfig-ltr.xml", "schema-ltr.xml");
    // store = getModelStore();
    bulkIndex();

    loadFeatures("features-ranksvm.json");
    loadModels("ranksvm-model.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
    // store.clear();
  }

  @Test
  public void ltrModelIdMissingTest() throws Exception {
    String solrQuery = "_query_:{!edismax qf='title' mm=100% v='bloomberg' tie=0.1}";
    // SolrQueryRequest req = req("q", solrQuery, "rows", "4", "fl", "*,score",
    // "fv", "true", "rq", "{!ltr reRankDocs=100}");
    SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("fv", "true");
    query.add("rq", "{!ltr reRankDocs=100}");

    String res = restTestHarness.query("/query" + query.toQueryString());
    assert (res.contains("Must provide model in the request"));

    // h.query("/query", req);

    /*
     * String solrQuery =
     * "_query_:{!edismax qf='title' mm=100% v='bloomberg' tie=0.1}";
     * SolrQueryRequest req = req("q", solrQuery, "rows", "4", "fl", "*,score",
     * "fv", "true", "rq", "{!ltr reRankDocs=100}");
     *
     * h.query("/query", req);
     */
  }

  @Test
  public void ltrModelIdDoesNotExistTest() throws Exception {
    String solrQuery = "_query_:{!edismax qf='title' mm=100% v='bloomberg' tie=0.1}";
    SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("fv", "true");
    query.add("rq", "{!ltr model=-1 reRankDocs=100}");

    String res = restTestHarness.query("/query" + query.toQueryString());
    assert (res.contains("cannot find model"));
    /*
     * String solrQuery =
     * "_query_:{!edismax qf='title' mm=100% v='bloomberg' tie=0.1}";
     * SolrQueryRequest req = req("q", solrQuery, "rows", "4", "fl", "*,score",
     * "fv", "true", "rq", "{!ltr model=-1 reRankDocs=100}");
     *
     * h.query("/query", req);
     */
  }

  @Test
  public void ltrMoreResultsThanReRankedTest() throws Exception {
    String solrQuery = "_query_:{!edismax qf='title' mm=100% v='bloomberg' tie=0.1}";
    SolrQuery query = new SolrQuery();
    query.setQuery(solrQuery);
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("fv", "true");
    query.add("rq", "{!ltr model=6029760550880411648 reRankDocs=3}");

    String res = restTestHarness.query("/query" + query.toQueryString());
    System.out.println(res);
    assert (res.contains("Requesting more documents than being reranked."));
    /*
     * String solrQuery =
     * "_query_:{!edismax qf='title' mm=100% v='bloomberg' tie=0.1}";
     * SolrQueryRequest req = req("q", solrQuery, "rows", "999999", "fl",
     * "*,score", "fv", "true", "rq",
     * "{!ltr model=6029760550880411648 reRankDocs=100}");
     *
     * h.query("/query", req);
     */
  }

  @Test
  public void ltrNoResultsTest() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg23");
    query.add("fl", "*,[fv]");
    query.add("rows", "3");
    query.add("debugQuery", "on");
    query.add(LTRComponent.LTRParams.FV, "true");
    query.add("rq", "{!ltr reRankDocs=3 model=6029760550880411648}");
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==0");
    // assertJQ("/query?" + query.toString(), "/response/numFound/==0");
  }

}
