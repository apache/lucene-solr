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

package org.apache.solr.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.TestSolrConfigHandler;
import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.junit.BeforeClass;
import org.junit.Test;

public class V2ApiIntegrationTest extends SolrCloudTestCase {
  private List<RestTestHarness> restTestHarnesses = new ArrayList<>();

  private static String COLL_NAME = "collection1";

  private void setupHarnesses() {
    for (final JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      RestTestHarness harness = new RestTestHarness(new ServerProvider(jettySolrRunner));
      restTestHarnesses.add(harness);
    }
  }
  static class ServerProvider implements RESTfulServerProvider {

    final JettySolrRunner jettySolrRunner;
    String baseurl;

    ServerProvider(JettySolrRunner jettySolrRunner) {
      this.jettySolrRunner = jettySolrRunner;
      baseurl = jettySolrRunner.getBaseUrl().toString() + "/" + COLL_NAME;
    }

    @Override
    public String getBaseURL() {
      return baseurl;
    }

  }

  @BeforeClass
  public static void createCluster() throws Exception {
    System.setProperty("managed.schema.mutable", "true");
    configureCluster(2)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
        .configure();
    CollectionAdminRequest.createCollection(COLL_NAME, "conf1", 1, 2)
        .process(cluster.getSolrClient());
  }

  @Test
  public void test() throws Exception {
    try {
      setupHarnesses();
      testApis();

    } finally {
      for (RestTestHarness r : restTestHarnesses) {
        r.close();
      }
    }
  }

  private void testApis() throws Exception {
    RestTestHarness restHarness = restTestHarnesses.get(0);
    ServerProvider serverProvider = (ServerProvider) restHarness.getServerProvider();
    serverProvider.baseurl = serverProvider.jettySolrRunner.getBaseUrl()+"/____v2/c/"+ COLL_NAME;
    Map result = TestSolrConfigHandler.getRespMap("/get/_introspect", restHarness);
    assertEquals("/c/collection1/get", Utils.getObjectByPath(result, true, "/spec[0]/url/paths[0]"));
    serverProvider.baseurl = serverProvider.jettySolrRunner.getBaseUrl()+"/____v2/collections/"+ COLL_NAME;
    result = TestSolrConfigHandler.getRespMap("/get/_introspect", restHarness);
    assertEquals("/collections/collection1/get", Utils.getObjectByPath(result, true, "/spec[0]/url/paths[0]"));


  }
}
