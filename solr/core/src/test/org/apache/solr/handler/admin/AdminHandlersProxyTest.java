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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdminHandlersProxyTest extends SolrCloudTestCase {
  private CloseableHttpClient httpClient;
  private CloudSolrClient solrClient;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    solrClient = getCloudSolrClient(cluster);
    solrClient.connect(1000, TimeUnit.MILLISECONDS);
    httpClient = (CloseableHttpClient) solrClient.getHttpClient();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    IOUtils.close(solrClient, httpClient);
  }

  @Test
  public void proxySystemInfoHandlerAllNodes() throws IOException, SolrServerException {
    MapSolrParams params = new MapSolrParams(Collections.singletonMap("nodes", "all"));
    GenericSolrRequest req = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/info/system", params);
    SimpleSolrResponse rsp = req.process(solrClient, null);
    NamedList<Object> nl = rsp.getResponse();
    assertEquals(3, nl.size());
    assertTrue(nl.getName(1).endsWith("_solr"));
    assertTrue(nl.getName(2).endsWith("_solr"));
    assertEquals("solrcloud", ((NamedList)nl.get(nl.getName(1))).get("mode"));
    assertEquals(nl.getName(2), ((NamedList)nl.get(nl.getName(2))).get("node"));
  }

  @Test
  public void proxyMetricsHandlerAllNodes() throws IOException, SolrServerException {
    MapSolrParams params = new MapSolrParams(Collections.singletonMap("nodes", "all"));
    GenericSolrRequest req = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/metrics", params);
    SimpleSolrResponse rsp = req.process(solrClient, null);
    NamedList<Object> nl = rsp.getResponse();
    assertEquals(3, nl.size());
    assertTrue(nl.getName(1).endsWith("_solr"));
    assertTrue(nl.getName(2).endsWith("_solr"));
    assertNotNull(((NamedList)nl.get(nl.getName(1))).get("metrics"));
  }

  @Test(expected = SolrException.class)
  public void proxySystemInfoHandlerNonExistingNode() throws IOException, SolrServerException {
    MapSolrParams params = new MapSolrParams(Collections.singletonMap("nodes", "example.com:1234_solr"));
    GenericSolrRequest req = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/info/system", params);
    SimpleSolrResponse rsp = req.process(solrClient, null);
  }
  
  @Test
  public void proxySystemInfoHandlerOneNode() {
    Set<String> nodes = solrClient.getClusterStateProvider().getLiveNodes();
    assertEquals(2, nodes.size());
    nodes.forEach(node -> {
      MapSolrParams params = new MapSolrParams(Collections.singletonMap("nodes", node));
      GenericSolrRequest req = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/info/system", params);
      SimpleSolrResponse rsp = null;
      try {
        rsp = req.process(solrClient, null);
      } catch (Exception e) {
        fail("Exception while proxying request to node " + node);
      }
      NamedList<Object> nl = rsp.getResponse();
      assertEquals(2, nl.size());
      assertEquals("solrcloud", ((NamedList)nl.get(nl.getName(1))).get("mode"));
      assertEquals(nl.getName(1), ((NamedList)nl.get(nl.getName(1))).get("node"));
    });
  }
}