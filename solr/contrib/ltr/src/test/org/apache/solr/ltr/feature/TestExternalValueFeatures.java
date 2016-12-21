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

public class TestExternalValueFeatures extends TestRerankBase {

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

    loadFeatures("external_features_for_sparse_processing.json");
    loadModels("multipleadditivetreesmodel_external_binary_features.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void efiFeatureProcessing_oneEfiMissing_shouldNotCalculateMissingFeature() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score,features:[fv]");
    query.add("rows", "3");
    query.add("rq", "{!ltr reRankDocs=3 model=external_model_binary_feature efi.user_device_tablet=1}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/features=='"+FeatureLoggerTestUtils.toFeatureVector("user_device_tablet","1.0")+"'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/score==65.0");
  }

  @Test
  public void efiFeatureProcessing_allEfisMissing_shouldReturnZeroScore() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score,features:[fv]");
    query.add("rows", "3");

    query.add("fl", "[fv]");
    query
        .add("rq", "{!ltr reRankDocs=3 model=external_model_binary_feature}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/features==''");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/score==0.0");
  }

}
