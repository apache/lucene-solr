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
package org.apache.solr.ltr.feature;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.FeatureLoggerTestUtils;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFilterSolrFeature extends TestRerankBase {
  @Before
  public void before() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity",
        "1"));
    assertU(adoc("id", "2", "title", "w2 2asd asdd didid", "description",
        "w2 2asd asdd didid", "popularity", "2"));
    assertU(adoc("id", "3", "title", "w1", "description", "w1", "popularity",
        "3"));
    assertU(adoc("id", "4", "title", "w1", "description", "w1", "popularity",
        "4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity",
        "5"));
    assertU(adoc("id", "6", "title", "w6 w2", "description", "w1 w2",
        "popularity", "6"));
    assertU(adoc("id", "7", "title", "w1 w2 w3 w4 w5", "description",
        "w6 w2 w3 w4 w5 w8", "popularity", "88888"));
    assertU(adoc("id", "8", "title", "w1 w1 w1 w2 w2 w8", "description",
        "w1 w1 w1 w2 w2", "popularity", "88888"));
    assertU(commit());
  }

  @After
  public void after() throws Exception {
    aftertest();
  }

  @Test
  public void testUserTermScoreWithFQ() throws Exception {
    loadFeature("SomeTermFQ", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=popularity}88888\"]}");
    loadFeature("SomeEfiFQ", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=title}${user_query}\"]}");
    loadModel("Term-modelFQ", LinearModel.class.getName(),
        new String[] {"SomeTermFQ", "SomeEfiFQ"},
        "{\"weights\":{\"SomeTermFQ\":1.6, \"SomeEfiFQ\":2.0}}");
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score");
    query.add("rows", "3");
    query.add("fq", "{!terms f=title}w1");
    query.add("rq",
        "{!ltr model=Term-modelFQ reRankDocs=5 efi.user_query='w5'}");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==5");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==3.6");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==1.6");
  }

  @Test
  public void testBadFeature() throws Exception {
    // Missing q/fq
    final String feature = getFeatureInJson("badFeature", "test",
        SolrFeature.class.getName(), "{\"df\":\"foo\"]}");
    assertJPut(ManagedFeatureStore.REST_END_POINT, feature,
        "/responseHeader/status==500");
  }

  @Test
  public void testFeatureNotEqualWhenNormalizerDifferent() throws Exception {
    loadFeatures("fq_features.json"); // features that use filter query
    loadModels("fq-model.json"); // model that uses filter query features
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score");
    query.add("rows", "4");

    query.add("rq", "{!ltr reRankDocs=4 model=fqmodel efi.user_query=w2}");
    query.add("fl", "fv:[fv]");

    final String docs0fv_csv= FeatureLoggerTestUtils.toFeatureVector(
        "matchedTitle","1.0", "popularity","3.0");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fv=='"+docs0fv_csv+"'");
  }

}
