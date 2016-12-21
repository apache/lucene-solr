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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestExternalFeatures extends TestRerankBase {

  @BeforeClass
  public static void before() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity",
        "1"));
    assertU(adoc("id", "2", "title", "w2", "description", "w2", "popularity",
        "2"));
    assertU(adoc("id", "3", "title", "w3", "description", "w3", "popularity",
        "3"));
    assertU(adoc("id", "4", "title", "w4", "description", "w4", "popularity",
        "4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity",
        "5"));
    assertU(commit());

    loadFeatures("external_features.json");
    loadModels("external_model.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void testEfiInTransformerShouldNotChangeOrderOfRerankedResults() throws Exception {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score");
    query.add("rows", "3");

    // Regular scores
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==1.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==1.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==1.0");

    query.add("fl", "[fv]");
    query.add("rq", "{!ltr reRankDocs=3 model=externalmodel efi.user_query=w3}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==0.999");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==0.0");

    // Adding an efi in the transformer should not affect the rq ranking with a
    // different value for efi of the same parameter
    query.remove("fl");
    query.add("fl", "id,[fv efi.user_query=w2]");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='2'");
  }

  @Test
  public void testFeaturesUseStopwordQueryReturnEmptyFeatureVector() throws Exception {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score,fv:[fv]");
    query.add("rows", "1");
    // Stopword only query passed in
    query.add("rq", "{!ltr reRankDocs=3 model=externalmodel efi.user_query='a'}");

    // Features are query title matches, which remove stopwords, leaving blank query, so no matches
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fv==''");
  }

  @Test
  public void testEfiFeatureExtraction() throws Exception {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "1");

    final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector(
        "confidence","2.3", "originalScore","1.0");

    // Features we're extracting depend on external feature info not passed in
    query.add("fl", "[fv]");
    assertJQ("/query" + query.toQueryString(), "/error/msg=='Exception from createWeight for SolrFeature [name=matchedTitle, params={q={!terms f=title}${user_query}}] SolrFeatureWeight requires efi parameter that was not passed in request.'");

    // Adding efi in features section should make it work
    query.remove("fl");
    query.add("fl", "score,fvalias:[fv store=fstore2 efi.myconf=2.3]");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fvalias=='"+docs0fv_sparse_csv+"'");

    // Adding efi in transformer + rq should still use the transformer's params for feature extraction
    query.remove("fl");
    query.add("fl", "score,fvalias:[fv store=fstore2 efi.myconf=2.3]");
    query.add("rq", "{!ltr reRankDocs=3 model=externalmodel efi.user_query=w3}");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fvalias=='"+docs0fv_sparse_csv+"'");
  }

  @Test
  public void featureExtraction_valueFeatureImplicitlyNotRequired_shouldNotScoreFeature() throws Exception {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "1");

    // Efi is explicitly not required, so we do not score the feature
    query.remove("fl");
    query.add("fl", "fvalias:[fv store=fstore2]");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fvalias=='"+FeatureLoggerTestUtils.toFeatureVector("originalScore","0.0")+"'");
  }

  @Test
  public void featureExtraction_valueFeatureExplicitlyNotRequired_shouldNotScoreFeature() throws Exception {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "1");

    // Efi is explicitly not required, so we do not score the feature
    query.remove("fl");
    query.add("fl", "fvalias:[fv store=fstore3]");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fvalias=='"+FeatureLoggerTestUtils.toFeatureVector("originalScore","0.0")+"'");
  }

  @Test
  public void featureExtraction_valueFeatureRequired_shouldThrowException() throws Exception {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "1");

    // Using nondefault store should still result in error with no efi when it is required (myPop)
    query.remove("fl");
    query.add("fl", "fvalias:[fv store=fstore4]");
    assertJQ("/query" + query.toQueryString(), "/error/msg=='Exception from createWeight for ValueFeature [name=popularity, params={value=${myPop}, required=true}] ValueFeatureWeight requires efi parameter that was not passed in request.'");
  }
}
