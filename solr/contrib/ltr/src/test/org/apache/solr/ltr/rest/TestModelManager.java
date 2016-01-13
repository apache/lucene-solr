package org.apache.solr.ltr.rest;

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

import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.ranking.LTRComponent;
import org.apache.solr.ltr.ranking.LTRComponent.LTRParams;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceStorage;
import org.apache.solr.rest.RestManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressSSL
public class TestModelManager extends TestRerankBase {

  @BeforeClass
  public static void init() throws Exception {
    setuptest();
  }

  @Before
  public void restart() throws Exception {
    restTestHarness.delete(LTRParams.MSTORE_END_POINT + "/*");
    restTestHarness.delete(LTRParams.FSTORE_END_POINT + "/*");

  }

  @Test
  public void test() throws Exception {
    SolrResourceLoader loader = new SolrResourceLoader(tmpSolrHome.toPath());

    RestManager.Registry registry = loader.getManagedResourceRegistry();
    assertNotNull(
        "Expected a non-null RestManager.Registry from the SolrResourceLoader!",
        registry);

    String resourceId = "/schema/fstore1";
    registry.registerManagedResource(resourceId, ManagedFeatureStore.class,
        new LTRComponent());

    String resourceId2 = "/schema/mstore1";
    registry.registerManagedResource(resourceId2, ManagedModelStore.class,
        new LTRComponent());

    NamedList<String> initArgs = new NamedList<>();

    RestManager restManager = new RestManager();
    restManager.init(loader, initArgs,
        new ManagedResourceStorage.InMemoryStorageIO());

    ManagedResource res = restManager.getManagedResource(resourceId);
    assertTrue(res instanceof ManagedFeatureStore);
    assertEquals(res.getResourceId(), resourceId);

  }

  @Test
  public void testRestManagerEndpoints() throws Exception {
    // relies on these ManagedResources being activated in the
    // schema-rest.xml used by this test
    assertJQ("/schema/managed", "/responseHeader/status==0");
    System.out.println(restTestHarness.query("/schema/managed"));

    System.out.println("after: \n" + restTestHarness.query("/schema/managed"));

    // Add features
    String feature = "{\"name\": \"test1\", \"type\": \"org.apache.solr.ltr.feature.impl.ValueFeature\", \"params\": {\"value\": 1} }";
    assertJPut(TestRerankBase.FEATURE_ENDPOINT, feature,
        "/responseHeader/status==0");

    feature = "{\"name\": \"test2\", \"type\": \"org.apache.solr.ltr.feature.impl.ValueFeature\", \"params\": {\"value\": 1} }";
    assertJPut(TestRerankBase.FEATURE_ENDPOINT, feature,
        "/responseHeader/status==0");

    feature = "{\"name\": \"test3\", \"type\": \"org.apache.solr.ltr.feature.impl.ValueFeature\", \"params\": {\"value\": 1} }";
    assertJPut(TestRerankBase.FEATURE_ENDPOINT, feature,
        "/responseHeader/status==0");

    feature = "{\"name\": \"test33\", \"store\": \"TEST\", \"type\": \"org.apache.solr.ltr.feature.impl.ValueFeature\", \"params\": {\"value\": 1} }";
    assertJPut(TestRerankBase.FEATURE_ENDPOINT, feature,
        "/responseHeader/status==0");

    String multipleFeatures = "[{\"name\": \"test4\", \"type\": \"org.apache.solr.ltr.feature.impl.ValueFeature\", \"params\": {\"value\": 1} }"
        + ",{\"name\": \"test5\", \"type\": \"org.apache.solr.ltr.feature.impl.ValueFeature\", \"params\": {\"value\": 1} } ]";
    assertJPut(TestRerankBase.FEATURE_ENDPOINT, multipleFeatures,
        "/responseHeader/status==0");

    // Add bad feature (wrong params)_
    String badfeature = "{\"name\": \"fvalue\", \"type\": \"org.apache.solr.ltr.feature.impl.FieldValueFeature\", \"params\": {\"value\": 1} }";
    assertJPut(TestRerankBase.FEATURE_ENDPOINT, badfeature,
        "/responseHeader/status==400");

    // Add models
    String model = "{ \"name\":\"testmodel1\", \"type\":\"org.apache.solr.ltr.ranking.RankSVMModel\", \"features\":[] }";
    // fails since it does not have features
    assertJPut(TestRerankBase.MODEL_ENDPOINT, model,
        "/responseHeader/status==400");
    // fails since it does not have weights
    model = "{ \"name\":\"testmodel2\", \"type\":\"org.apache.solr.ltr.ranking.RankSVMModel\", \"features\":[{\"name\":\"test1\"}, {\"name\":\"test2\"}] }";
    assertJPut(TestRerankBase.MODEL_ENDPOINT, model,
        "/responseHeader/status==400");
    // success
    model = "{ \"name\":\"testmodel3\", \"type\":\"org.apache.solr.ltr.ranking.RankSVMModel\", \"features\":[{\"name\":\"test1\"}, {\"name\":\"test2\"}],\"params\":{\"weights\":{\"test1\":1.5,\"test2\":2.0}}}";
    assertJPut(TestRerankBase.MODEL_ENDPOINT, model,
        "/responseHeader/status==0");
    // success
    String multipleModels = "[{ \"name\":\"testmodel4\", \"type\":\"org.apache.solr.ltr.ranking.RankSVMModel\", \"features\":[{\"name\":\"test1\"}, {\"name\":\"test2\"}],\"params\":{\"weights\":{\"test1\":1.5,\"test2\":2.0}} }\n"
        + ",{ \"name\":\"testmodel5\", \"type\":\"org.apache.solr.ltr.ranking.RankSVMModel\", \"features\":[{\"name\":\"test1\"}, {\"name\":\"test2\"}],\"params\":{\"weights\":{\"test1\":1.5,\"test2\":2.0}} } ]";
    assertJPut(TestRerankBase.MODEL_ENDPOINT, multipleModels,
        "/responseHeader/status==0");
    String qryResult = JQ(LTRParams.MSTORE_END_POINT);

    assert (qryResult.contains("\"name\":\"testmodel3\"")
        && qryResult.contains("\"name\":\"testmodel4\"") && qryResult
          .contains("\"name\":\"testmodel5\""));
    /*
     * assertJQ(LTRParams.MSTORE_END_POINT, "/models/[0]/name=='testmodel3'");
     * assertJQ(LTRParams.MSTORE_END_POINT, "/models/[1]/name=='testmodel4'");
     * assertJQ(LTRParams.MSTORE_END_POINT, "/models/[2]/name=='testmodel5'");
     */
    assertJQ(LTRParams.FSTORE_END_POINT, "/featureStores==['TEST','_DEFAULT_']");
    assertJQ(LTRParams.FSTORE_END_POINT + "/_DEFAULT_",
        "/features/[0]/name=='test1'");
    assertJQ(LTRParams.FSTORE_END_POINT + "/TEST",
        "/features/[0]/name=='test33'");
  }

  @Test
  public void testEndpointsFromFile() throws Exception {
    loadFeatures("features-ranksvm.json");
    loadModels("ranksvm-model.json");

    assertJQ(LTRParams.MSTORE_END_POINT,
        "/models/[0]/name=='6029760550880411648'");
    assertJQ(LTRParams.FSTORE_END_POINT + "/_DEFAULT_",
        "/features/[1]/name=='description'");
  }

  @Test
  public void testLoadInvalidFeature() throws Exception {
    // relies on these ManagedResources being activated in the
    // schema-rest.xml used by this test
    assertJQ("/schema/managed", "/responseHeader/status==0");
    String newEndpoint = LTRParams.FSTORE_END_POINT;
    String feature = "{\"name\": \"^&test1\", \"type\": \"org.apache.solr.ltr.feature.impl.ValueFeature\", \"params\": {\"value\": 1} }";
    assertJPut(newEndpoint, feature, "/responseHeader/status==400");

  }

}
