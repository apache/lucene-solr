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

package org.apache.solr.cloud;

import java.io.IOException;
import java.util.Set;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.HealthCheckRequest;
import org.apache.solr.client.solrj.response.HealthCheckResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.params.CommonParams.HEALTH_CHECK_HANDLER_PATH;

public class HealthCheckHandlerTest extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void testHealthCheckHandler() throws IOException, SolrServerException, InterruptedException, KeeperException {
    SolrRequest req = new GenericSolrRequest(SolrRequest.METHOD.GET, HEALTH_CHECK_HANDLER_PATH, new ModifiableSolrParams());
    try (HttpSolrClient httpSolrClient = getHttpSolrClient(cluster.getJettySolrRunner(0).getBaseUrl().toString())) {
      SolrResponse response = req.process(cluster.getSolrClient());
      assertEquals(CommonParams.OK, response.getResponse().get(CommonParams.STATUS));

      JettySolrRunner jetty = cluster.getJettySolrRunner(0);
      cluster.expireZkSession(jetty);
      Set<String> live_nodes = cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes();

      int counter = 0;
      while (live_nodes.size() == 1 && counter++ < 100) {
        Thread.sleep(100);
        live_nodes = cluster.getSolrClient().getZkStateReader().getClusterState().getLiveNodes();
      }

      try {
        req.process(httpSolrClient);
      } catch (HttpSolrClient.RemoteSolrException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("Host Unavailable"));
        assertEquals(SolrException.ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
      }
    }
  }

  @Test
  public void testHealthCheckHandlerSolrJ() throws IOException, SolrServerException {
    HealthCheckRequest req = new HealthCheckRequest();
    try (HttpSolrClient httpSolrClient = getHttpSolrClient(cluster.getJettySolrRunner(0).getBaseUrl().toString())) {
      HealthCheckResponse rsp = req.process(httpSolrClient);
      assertEquals(CommonParams.OK, rsp.getNodeStatus());
    }
  }

  @Test (expected = AssertionError.class)
  public void testHealthCheckHandlerWithCloudClient() throws IOException, SolrServerException {
    HealthCheckRequest req = new HealthCheckRequest();
    req.process(cluster.getSolrClient());
  }

}
