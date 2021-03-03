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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.TestInjection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class CloudSolrClientRetryTest extends SolrCloudTestCase {
  private static final int NODE_COUNT = 1;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.enableMetrics", "true");
    configureCluster(NODE_COUNT)
        .addConfig("conf", SolrTestUtil.getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void afterCloudSolrClientRetryTest() throws Exception {
    shutdownCluster();
  }


  @Test
  public void testRetry() throws Exception {
    String collectionName = "testRetry";
    CloudHttp2SolrClient solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collectionName, 1, 1)
        .process(solrClient);

    solrClient.add(collectionName, new SolrInputDocument("id", "1"));

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/admin/metrics");
    String updateRequestCountKey = "solr.core.testRetry.s1.testRetry_s1_r_n1:UPDATE./update.requestTimes:count";
    params.set("key", updateRequestCountKey);
    params.set("indent", "true");

    QueryResponse response = solrClient.query(collectionName, params, SolrRequest.METHOD.GET);
    NamedList<Object> namedList = response.getResponse();
    System.out.println(namedList);
    NamedList metrics = (NamedList) namedList.get("metrics");

    // this does not help
    if (metrics.get(updateRequestCountKey) == null) {
      Thread.sleep(500);
      response = solrClient.query(collectionName, params, SolrRequest.METHOD.GET);
      namedList = response.getResponse();
      System.out.println(namedList);
      metrics = (NamedList) namedList.get("metrics");
    }

    assertEquals(1L, metrics.get(updateRequestCountKey));

    TestInjection.failUpdateRequests = "true:100";
    try {
      LuceneTestCase.expectThrows(BaseCloudSolrClient.RouteException.class,
          "Expected an exception on the client when failure is injected during updates", () -> {
            UpdateRequest req = new UpdateRequest();
            req.add(new SolrInputDocument("id", "2"));
            req.setCommitWithin(-1);
            req.setParam("maxErrors", "0"); // MRM TODO:: did the default change for single doc adds?
            req.process(solrClient, collectionName);
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
