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

import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests SSL (if test framework selects it) with MiniSolrCloudCluster.
 * {@link TestMiniSolrCloudCluster} does not inherit from {@link SolrTestCaseJ4}
 * so does not support SSL.
 */
public class TestMiniSolrCloudClusterSSL extends SolrTestCaseJ4 {

  private static MiniSolrCloudCluster miniCluster;
  private static final int NUM_SERVERS = 5;

  @BeforeClass
  public static void startup() throws Exception {
    JettyConfig config = JettyConfig.builder().withSSLConfig(sslConfig).build();
    miniCluster = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), config);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    if (miniCluster != null) {
      miniCluster.shutdown();
    }
    miniCluster = null;
  }

  @Test
  public void testMiniSolrCloudClusterSSL() throws Exception {
    // test send request to each server
    sendRequestToEachServer();

    // shut down a server
    JettySolrRunner stoppedServer = miniCluster.stopJettySolrRunner(0);
    assertTrue(stoppedServer.isStopped());
    assertEquals(NUM_SERVERS - 1, miniCluster.getJettySolrRunners().size());

    // create a new server
    JettySolrRunner startedServer = miniCluster.startJettySolrRunner();
    assertTrue(startedServer.isRunning());
    assertEquals(NUM_SERVERS, miniCluster.getJettySolrRunners().size());

    // test send request to each server
    sendRequestToEachServer();
  }

  private void sendRequestToEachServer() throws Exception {
    List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
    for (JettySolrRunner jetty : jettys) {
      try (HttpSolrClient client = new HttpSolrClient(jetty.getBaseUrl().toString())) {
        CoreAdminRequest req = new CoreAdminRequest();
        req.setAction( CoreAdminAction.STATUS );
        client.request(req);
      }
    }
  }
}
