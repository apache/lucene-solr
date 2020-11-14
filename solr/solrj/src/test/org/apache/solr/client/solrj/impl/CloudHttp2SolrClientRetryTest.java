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

package org.apache.solr.client.solrj.impl;

import java.util.Collections;
import java.util.Optional;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.TestInjection;
import org.junit.BeforeClass;
import org.junit.Test;

public class CloudHttp2SolrClientRetryTest extends SolrCloudTestCase {
  private static final int NODE_COUNT = 1;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(NODE_COUNT)
        .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();
  }

  @Test
  public void testRetry() throws Exception {
    String collectionName = "testRetry";
    try (CloudHttp2SolrClient solrClient = new CloudHttp2SolrClient.Builder(Collections.singletonList(cluster.getZkServer().getZkAddress()), Optional.empty()).build()) {
      CollectionAdminRequest.createCollection(collectionName, 1, 1)
          .process(solrClient);

      solrClient.add(collectionName, new SolrInputDocument("id", "1"));

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.QT, "/admin/metrics");
      String updateRequestCountKey = "solr.core.testRetry.shard1.replica_n1:UPDATE./update.requestTimes:count";
      params.set("key", updateRequestCountKey);
      params.set("indent", "true");

      QueryResponse response = solrClient.query(collectionName, params, SolrRequest.METHOD.GET);
      NamedList<Object> namedList = response.getResponse();
      System.out.println(namedList);
      @SuppressWarnings({"rawtypes"})
      NamedList metrics = (NamedList) namedList.get("metrics");
      assertEquals(1L, metrics.get(updateRequestCountKey));

      TestInjection.failUpdateRequests = "true:100";
      try {
        expectThrows(BaseCloudSolrClient.RouteException.class,
            "Expected an exception on the client when failure is injected during updates", () -> {
              solrClient.add(collectionName, new SolrInputDocument("id", "2"));
            });
      } finally {
        TestInjection.reset();
      }

      response = solrClient.query(collectionName, params, SolrRequest.METHOD.GET);
      namedList = response.getResponse();
      System.out.println(namedList);
      metrics = (NamedList) namedList.get("metrics");
      assertEquals(2L, metrics.get(updateRequestCountKey));
    }
  }
}
