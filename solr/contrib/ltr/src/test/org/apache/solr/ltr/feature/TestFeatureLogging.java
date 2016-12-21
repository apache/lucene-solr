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
import org.apache.solr.ltr.store.FeatureStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFeatureLogging extends TestRerankBase {

  @BeforeClass
  public static void setup() throws Exception {
    setuptest(true);
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void testGeneratedFeatures() throws Exception {
    loadFeature("c1", ValueFeature.class.getCanonicalName(), "test1",
        "{\"value\":1.0}");
    loadFeature("c2", ValueFeature.class.getCanonicalName(), "test1",
        "{\"value\":2.0}");
    loadFeature("c3", ValueFeature.class.getCanonicalName(), "test1",
        "{\"value\":3.0}");
    loadFeature("pop", FieldValueFeature.class.getCanonicalName(), "test1",
        "{\"field\":\"popularity\"}");
    loadFeature("nomatch", SolrFeature.class.getCanonicalName(), "test1",
        "{\"q\":\"{!terms f=title}foobarbat\"}");
    loadFeature("yesmatch", SolrFeature.class.getCanonicalName(), "test1",
        "{\"q\":\"{!terms f=popularity}2\"}");

    loadModel("sum1", LinearModel.class.getCanonicalName(), new String[] {
        "c1", "c2", "c3"}, "test1",
        "{\"weights\":{\"c1\":1.0,\"c2\":1.0,\"c3\":1.0}}");

    final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector(
        "c1","1.0",
        "c2","2.0",
        "c3","3.0",
        "pop","2.0",
        "yesmatch","1.0");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.add("fl", "title,description,id,popularity,[fv]");
    query.add("rows", "3");
    query.add("debugQuery", "on");
    query.add("rq", "{!ltr reRankDocs=3 model=sum1}");

    restTestHarness.query("/query" + query.toQueryString());
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'title':'bloomberg bloomberg ', 'description':'bloomberg','id':'7', 'popularity':2,  '[fv]':'"+docs0fv_sparse_csv+"'}");

    query.remove("fl");
    query.add("fl", "[fv]");
    query.add("rows", "3");
    query.add("rq", "{!ltr reRankDocs=3 model=sum1}");

    restTestHarness.query("/query" + query.toQueryString());
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'"+docs0fv_sparse_csv+"'}");
  }

  @Test
  public void testDefaultStoreFeatureExtraction() throws Exception {
    loadFeature("defaultf1", ValueFeature.class.getCanonicalName(),
        FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "{\"value\":1.0}");
    loadFeature("store8f1", ValueFeature.class.getCanonicalName(),
        "store8",
        "{\"value\":2.0}");
    loadFeature("store9f1", ValueFeature.class.getCanonicalName(),
        "store9",
        "{\"value\":3.0}");
    loadModel("store9m1", LinearModel.class.getCanonicalName(),
      new String[] {"store9f1"},
      "store9",
      "{\"weights\":{\"store9f1\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("id:7");
    query.add("rows", "1");

    // No store specified, use default store for extraction
    query.add("fl", "fv:[fv]");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"+FeatureLoggerTestUtils.toFeatureVector("defaultf1","1.0")+"'}");

    // Store specified, use store for extraction
    query.remove("fl");
    query.add("fl", "fv:[fv store=store8]");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"+FeatureLoggerTestUtils.toFeatureVector("store8f1","2.0")+"'}");

    // Store specified + model specified, use store for extraction
    query.add("rq", "{!ltr reRankDocs=3 model=store9m1}");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"+FeatureLoggerTestUtils.toFeatureVector("store8f1","2.0")+"'}");

    // No store specified + model specified, use model store for extraction
    query.remove("fl");
    query.add("fl", "fv:[fv]");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/=={'fv':'"+FeatureLoggerTestUtils.toFeatureVector("store9f1","3.0")+"'}");
  }


  @Test
  public void testGeneratedGroup() throws Exception {
    loadFeature("c1", ValueFeature.class.getCanonicalName(), "testgroup",
        "{\"value\":1.0}");
    loadFeature("c2", ValueFeature.class.getCanonicalName(), "testgroup",
        "{\"value\":2.0}");
    loadFeature("c3", ValueFeature.class.getCanonicalName(), "testgroup",
        "{\"value\":3.0}");
    loadFeature("pop", FieldValueFeature.class.getCanonicalName(), "testgroup",
        "{\"field\":\"popularity\"}");

    loadModel("sumgroup", LinearModel.class.getCanonicalName(), new String[] {
        "c1", "c2", "c3"}, "testgroup",
        "{\"weights\":{\"c1\":1.0,\"c2\":1.0,\"c3\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.add("fl", "*,[fv]");
    query.add("debugQuery", "on");

    query.remove("fl");
    query.add("fl", "fv:[fv]");
    query.add("rows", "3");
    query.add("group", "true");
    query.add("group.field", "title");

    query.add("rq", "{!ltr reRankDocs=3 model=sumgroup}");

    final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector(
        "c1","1.0",
        "c2","2.0",
        "c3","3.0",
        "pop","5.0");

    restTestHarness.query("/query" + query.toQueryString());
    assertJQ(
        "/query" + query.toQueryString(),
        "/grouped/title/groups/[0]/doclist/docs/[0]/=={'fv':'"+docs0fv_sparse_csv+"'}");
  }

  @Test
  public void testSparseDenseFeatures() throws Exception {
    loadFeature("match", SolrFeature.class.getCanonicalName(), "test4",
        "{\"q\":\"{!terms f=title}different\"}");
    loadFeature("c4", ValueFeature.class.getCanonicalName(), "test4",
        "{\"value\":1.0}");

    loadModel("sum4", LinearModel.class.getCanonicalName(), new String[] {
        "match"}, "test4",
        "{\"weights\":{\"match\":1.0}}");

    final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("match", "1.0", "c4", "1.0");
    final String docs1fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("c4", "1.0");

    final String docs0fv_dense_csv  = FeatureLoggerTestUtils.toFeatureVector("match", "1.0", "c4", "1.0");
    final String docs1fv_dense_csv  = FeatureLoggerTestUtils.toFeatureVector("match", "0.0", "c4", "1.0");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.add("rows", "10");
    query.add("rq", "{!ltr reRankDocs=10 model=sum4}");

    //csv - no feature format check (default to sparse)
    query.remove("fl");
    query.add("fl", "*,score,fv:[fv store=test4]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/fv/=='"+docs0fv_sparse_csv+"'");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[1]/fv/=='"+docs1fv_sparse_csv+"'");

    //csv - sparse feature format check
    query.remove("fl");
    query.add("fl", "*,score,fv:[fv store=test4 format=sparse]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/fv/=='"+docs0fv_sparse_csv+"'");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[1]/fv/=='"+docs1fv_sparse_csv+"'");

    //csv - dense feature format check
    query.remove("fl");
    query.add("fl", "*,score,fv:[fv store=test4 format=dense]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/fv/=='"+docs0fv_dense_csv+"'");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[1]/fv/=='"+docs1fv_dense_csv+"'");
  }

}
