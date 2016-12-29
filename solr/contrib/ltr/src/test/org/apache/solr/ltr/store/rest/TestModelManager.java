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
package org.apache.solr.ltr.store.rest;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.FieldValueFeature;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.ltr.search.LTRQParserPlugin;
import org.apache.solr.rest.ManagedResource;
import org.apache.solr.rest.ManagedResourceStorage;
import org.apache.solr.rest.RestManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestModelManager extends TestRerankBase {

  @BeforeClass
  public static void init() throws Exception {
    setuptest(true);
  }

  @Before
  public void restart() throws Exception {
    restTestHarness.delete(ManagedFeatureStore.REST_END_POINT + "/*");
    restTestHarness.delete(ManagedModelStore.REST_END_POINT + "/*");

  }

  @Test
  public void test() throws Exception {
    final SolrResourceLoader loader = new SolrResourceLoader(
        tmpSolrHome.toPath());

    final RestManager.Registry registry = loader.getManagedResourceRegistry();
    assertNotNull(
        "Expected a non-null RestManager.Registry from the SolrResourceLoader!",
        registry);

    final String resourceId = "/schema/fstore1";
    registry.registerManagedResource(resourceId, ManagedFeatureStore.class,
        new LTRQParserPlugin());

    final String resourceId2 = "/schema/mstore1";
    registry.registerManagedResource(resourceId2, ManagedModelStore.class,
        new LTRQParserPlugin());

    final NamedList<String> initArgs = new NamedList<>();

    final RestManager restManager = new RestManager();
    restManager.init(loader, initArgs,
        new ManagedResourceStorage.InMemoryStorageIO());

    final ManagedResource res = restManager.getManagedResource(resourceId);
    assertTrue(res instanceof ManagedFeatureStore);
    assertEquals(res.getResourceId(), resourceId);

  }

  @Test
  public void testRestManagerEndpoints() throws Exception {
    // relies on these ManagedResources being activated in the
    // schema-rest.xml used by this test
    assertJQ("/schema/managed", "/responseHeader/status==0");

    final String valueFeatureClassName = ValueFeature.class.getCanonicalName();

    // Add features
    String feature = "{\"name\": \"test1\", \"class\": \""+valueFeatureClassName+"\", \"params\": {\"value\": 1} }";
    assertJPut(ManagedFeatureStore.REST_END_POINT, feature,
        "/responseHeader/status==0");

    feature = "{\"name\": \"test2\", \"class\": \""+valueFeatureClassName+"\", \"params\": {\"value\": 1} }";
    assertJPut(ManagedFeatureStore.REST_END_POINT, feature,
        "/responseHeader/status==0");

    feature = "{\"name\": \"test3\", \"class\": \""+valueFeatureClassName+"\", \"params\": {\"value\": 1} }";
    assertJPut(ManagedFeatureStore.REST_END_POINT, feature,
        "/responseHeader/status==0");

    feature = "{\"name\": \"test33\", \"store\": \"TEST\", \"class\": \""+valueFeatureClassName+"\", \"params\": {\"value\": 1} }";
    assertJPut(ManagedFeatureStore.REST_END_POINT, feature,
        "/responseHeader/status==0");

    final String multipleFeatures = "[{\"name\": \"test4\", \"class\": \""+valueFeatureClassName+"\", \"params\": {\"value\": 1} }"
        + ",{\"name\": \"test5\", \"class\": \""+valueFeatureClassName+"\", \"params\": {\"value\": 1} } ]";
    assertJPut(ManagedFeatureStore.REST_END_POINT, multipleFeatures,
        "/responseHeader/status==0");

    final String fieldValueFeatureClassName = FieldValueFeature.class.getCanonicalName();

    // Add bad feature (wrong params)_
    final String badfeature = "{\"name\": \"fvalue\", \"class\": \""+fieldValueFeatureClassName+"\", \"params\": {\"value\": 1} }";
    assertJPut(ManagedFeatureStore.REST_END_POINT, badfeature,
        "/error/msg/=='No setter corrresponding to \\'value\\' in "+fieldValueFeatureClassName+"'");

    final String linearModelClassName = LinearModel.class.getCanonicalName();

    // Add models
    String model = "{ \"name\":\"testmodel1\", \"class\":\""+linearModelClassName+"\", \"features\":[] }";
    // fails since it does not have features
    assertJPut(ManagedModelStore.REST_END_POINT, model,
        "/responseHeader/status==400");
    // fails since it does not have weights
    model = "{ \"name\":\"testmodel2\", \"class\":\""+linearModelClassName+"\", \"features\":[{\"name\":\"test1\"}, {\"name\":\"test2\"}] }";
    assertJPut(ManagedModelStore.REST_END_POINT, model,
        "/responseHeader/status==400");
    // success
    model = "{ \"name\":\"testmodel3\", \"class\":\""+linearModelClassName+"\", \"features\":[{\"name\":\"test1\"}, {\"name\":\"test2\"}],\"params\":{\"weights\":{\"test1\":1.5,\"test2\":2.0}}}";
    assertJPut(ManagedModelStore.REST_END_POINT, model,
        "/responseHeader/status==0");
    // success
    final String multipleModels = "[{ \"name\":\"testmodel4\", \"class\":\""+linearModelClassName+"\", \"features\":[{\"name\":\"test1\"}, {\"name\":\"test2\"}],\"params\":{\"weights\":{\"test1\":1.5,\"test2\":2.0}} }\n"
        + ",{ \"name\":\"testmodel5\", \"class\":\""+linearModelClassName+"\", \"features\":[{\"name\":\"test1\"}, {\"name\":\"test2\"}],\"params\":{\"weights\":{\"test1\":1.5,\"test2\":2.0}} } ]";
    assertJPut(ManagedModelStore.REST_END_POINT, multipleModels,
        "/responseHeader/status==0");
    final String qryResult = JQ(ManagedModelStore.REST_END_POINT);

    assert (qryResult.contains("\"name\":\"testmodel3\"")
        && qryResult.contains("\"name\":\"testmodel4\"") && qryResult
          .contains("\"name\":\"testmodel5\""));
    /*
     * assertJQ(LTRParams.MSTORE_END_POINT, "/models/[0]/name=='testmodel3'");
     * assertJQ(LTRParams.MSTORE_END_POINT, "/models/[1]/name=='testmodel4'");
     * assertJQ(LTRParams.MSTORE_END_POINT, "/models/[2]/name=='testmodel5'");
     */
    assertJQ(ManagedFeatureStore.REST_END_POINT,
        "/featureStores==['TEST','_DEFAULT_']");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/_DEFAULT_",
        "/features/[0]/name=='test1'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/TEST",
        "/features/[0]/name=='test33'");
  }

  @Test
  public void testEndpointsFromFile() throws Exception {
    loadFeatures("features-linear.json");
    loadModels("linear-model.json");

    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/[0]/name=='6029760550880411648'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/_DEFAULT_",
        "/features/[1]/name=='description'");
  }

}
