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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.DelegationTokenResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ZookeeperStatusHandlerFailureTest extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    // Kill the ZK
    cluster.getZkServer().shutdown();
  }

  /*
   Test the monitoring endpoint, when no Zookeeper is answering. There should still be a response
  */
  @Test
  public void monitorZookeeperAfterZkShutdown() throws IOException, SolrServerException, InterruptedException, ExecutionException, TimeoutException {
    URL baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    HttpSolrClient solr = new HttpSolrClient.Builder(baseUrl.toString()).build();
    GenericSolrRequest mntrReq = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/zookeeper/status", new ModifiableSolrParams());
    mntrReq.setResponseParser(new DelegationTokenResponse.JsonMapResponseParser());
    NamedList<Object> nl = solr.httpUriRequest(mntrReq).future.get(10000, TimeUnit.MILLISECONDS);

    assertEquals("zkStatus", nl.getName(1));
    @SuppressWarnings({"unchecked"})
    Map<String,Object> zkStatus = (Map<String,Object>) nl.get("zkStatus");
    assertEquals("red", zkStatus.get("status"));
    assertEquals("standalone", zkStatus.get("mode"));
    assertEquals(1L, zkStatus.get("ensembleSize"));
    @SuppressWarnings({"unchecked"})
    List<Object> detailsList = (List<Object>)zkStatus.get("details");
    assertEquals(1, detailsList.size());
    @SuppressWarnings({"unchecked"})
    Map<String,Object> details = (Map<String,Object>) detailsList.get(0);
    assertEquals(false, details.get("ok"));
    solr.close();
  }
}