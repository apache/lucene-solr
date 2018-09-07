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
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZookeeperStatusHandlerTest extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  /*
    Test the monitoring endpoint, used in the Cloud => ZkStatus Admin UI screen
    NOTE: We do not currently test with multiple zookeepers, but the only difference is that there are multiple "details" objects and mode is "ensemble"... 
   */
  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 6-Sep-2018
  public void monitorZookeeper() throws IOException, SolrServerException, InterruptedException, ExecutionException, TimeoutException {
    URL baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    HttpSolrClient solr = new HttpSolrClient.Builder(baseUrl.toString()).build();
    GenericSolrRequest mntrReq = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/zookeeper/status", new ModifiableSolrParams());
    mntrReq.setResponseParser(new DelegationTokenResponse.JsonMapResponseParser());
    NamedList<Object> nl = solr.httpUriRequest(mntrReq).future.get(1000, TimeUnit.MILLISECONDS);

    assertEquals("zkStatus", nl.getName(1));
    Map<String,Object> zkStatus = (Map<String,Object>) nl.get("zkStatus");
    assertEquals("green", zkStatus.get("status"));
    assertEquals("standalone", zkStatus.get("mode"));
    assertEquals(1L, zkStatus.get("ensembleSize"));
    List<Object> detailsList = (List<Object>)zkStatus.get("details");
    assertEquals(1, detailsList.size());
    Map<String,Object> details = (Map<String,Object>) detailsList.get(0);
    assertEquals(true, details.get("ok"));
    assertTrue(Integer.parseInt((String) details.get("zk_znode_count")) > 50);
    solr.close();
  }
}