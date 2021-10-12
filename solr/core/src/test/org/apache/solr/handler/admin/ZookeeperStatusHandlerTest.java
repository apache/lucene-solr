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
import java.util.Arrays;
import java.util.Collections;
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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkDynamicConfig;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentMatchers;
import org.noggit.JSONUtil;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
  public void monitorZookeeper() throws IOException, SolrServerException, InterruptedException, ExecutionException, TimeoutException {
    URL baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    HttpSolrClient solr = new HttpSolrClient.Builder(baseUrl.toString()).build();
    GenericSolrRequest mntrReq = new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/zookeeper/status", new ModifiableSolrParams());
    mntrReq.setResponseParser(new DelegationTokenResponse.JsonMapResponseParser());
    NamedList<Object> nl = solr.httpUriRequest(mntrReq).future.get(10000, TimeUnit.MILLISECONDS);

    assertEquals("zkStatus", nl.getName(1));
    @SuppressWarnings({"unchecked"})
    Map<String,Object> zkStatus = (Map<String,Object>) nl.get("zkStatus");
    assertEquals("green", zkStatus.get("status"));
    assertEquals("standalone", zkStatus.get("mode"));
    assertEquals(1L, zkStatus.get("ensembleSize"));
    @SuppressWarnings({"unchecked"})
    List<Object> detailsList = (List<Object>)zkStatus.get("details");
    assertEquals(1, detailsList.size());
    @SuppressWarnings({"unchecked"})
    Map<String,Object> details = (Map<String,Object>) detailsList.get(0);
    assertEquals(true, details.get("ok"));
    assertTrue(Integer.parseInt((String) details.get("zk_znode_count")) > 50);
    solr.close();
  }

  @Test
  public void testEnsembleStatusMock() {
    assumeWorkingMockito();
    ZookeeperStatusHandler zkStatusHandler = mock(ZookeeperStatusHandler.class);
    when(zkStatusHandler.getZkRawResponse("zoo1:2181", "ruok")).thenReturn(Arrays.asList("imok"));
    when(zkStatusHandler.getZkRawResponse("zoo1:2181", "mntr")).thenReturn(
        Arrays.asList("zk_version\t3.5.5-390fe37ea45dee01bf87dc1c042b5e3dcce88653, built on 05/03/2019 12:07 GMT",
        "zk_avg_latency\t1"));
    when(zkStatusHandler.getZkRawResponse("zoo1:2181", "conf")).thenReturn(
        Arrays.asList("clientPort=2181",
        "secureClientPort=-1",
        "thisIsUnexpected",
        "membership: "));

    when(zkStatusHandler.getZkRawResponse("zoo2:2181", "ruok")).thenReturn(Arrays.asList(""));

    when(zkStatusHandler.getZkRawResponse("zoo3:2181", "ruok")).thenReturn(Arrays.asList("imok"));
    when(zkStatusHandler.getZkRawResponse("zoo3:2181", "mntr")).thenReturn(
        Arrays.asList("mntr is not executed because it is not in the whitelist.")); // Actual response from ZK if not whitelisted
    when(zkStatusHandler.getZkRawResponse("zoo3:2181", "conf")).thenReturn(
        Arrays.asList("clientPort=2181"));

    when(zkStatusHandler.getZkStatus(anyString(), any())).thenCallRealMethod();
    when(zkStatusHandler.monitorZookeeper(anyString())).thenCallRealMethod();
    when(zkStatusHandler.validateZkRawResponse(ArgumentMatchers.any(), any(), any())).thenAnswer(Answers.CALLS_REAL_METHODS);

    ZkDynamicConfig zkDynamicConfig = ZkDynamicConfig.parseLines(
        "server.1=zoo1:2780:2783:participant;0.0.0.0:2181\n" +
            "server.2=zoo2:2781:2784:participant;0.0.0.0:2181\n" +
            "server.3=zoo3:2782:2785:participant;0.0.0.0:2181\n" +
            "version=400000003");
    Map<String, Object> mockStatus = zkStatusHandler.getZkStatus("zoo4:2181,zoo5:2181,zoo6:2181", zkDynamicConfig);
    String expected = "{\n" +
        "  \"dynamicReconfig\":true,\n" +
        "  \"ensembleSize\":3,\n" +
        "  \"details\":[\n" +
        "    {\n" +
        "      \"role\":\"participant\",\n" +
        "      \"zk_version\":\"3.5.5-390fe37ea45dee01bf87dc1c042b5e3dcce88653, built on 05/03/2019 12:07 GMT\",\n" +
        "      \"zk_avg_latency\":\"1\",\n" +
        "      \"host\":\"zoo1:2181\",\n" +
        "      \"clientPort\":\"2181\",\n" +
        "      \"secureClientPort\":\"-1\",\n" +
        "      \"ok\":true},\n" +
        "    {\n" +
        "      \"host\":\"zoo2:2181\",\n" +
        "      \"ok\":false},\n" +
        "    {\n" +
        "      \"host\":\"zoo3:2181\",\n" +
        "      \"ok\":false}],\n" +
        "  \"zkHost\":\"zoo4:2181,zoo5:2181,zoo6:2181\",\n" +
        "  \"errors\":[\n" +
        "    \"Your ZK connection string (3 hosts) is different from the dynamic ensemble config (3 hosts). Solr does not currently support dynamic reconfiguration and will only be able to connect to the zk hosts in your connection string.\",\n" +
        "    \"Unexpected line in 'conf' response from Zookeeper zoo1:2181: thisIsUnexpected\",\n" +
        "    \"Empty response from Zookeeper zoo2:2181\",\n" +
        "    \"Could not execute mntr towards ZK host zoo3:2181. Add this line to the 'zoo.cfg' configuration file on each zookeeper node: '4lw.commands.whitelist=mntr,conf,ruok'. See also chapter 'Setting Up an External ZooKeeper Ensemble' in the Solr Reference Guide.\"],\n" +
        "  \"status\":\"yellow\"}";
    assertEquals(expected, JSONUtil.toJSON(mockStatus));
  }

  @Test(expected = SolrException.class)
  public void validateNotWhitelisted() {
    try (ZookeeperStatusHandler zsh = new ZookeeperStatusHandler(null)) {
     zsh.validateZkRawResponse(Collections.singletonList("mntr is not executed because it is not in the whitelist."),
          "zoo1:2181", "mntr");
    }  catch (IOException e) {
      fail("Error closing ZookeeperStatusHandler");
    }
  }

  @Test(expected = SolrException.class)
  public void validateEmptyResponse() {
    try (ZookeeperStatusHandler zsh = new ZookeeperStatusHandler(null)) {
      zsh.validateZkRawResponse(Collections.emptyList(), "zoo1:2181", "mntr");
    } catch (IOException e) {
      fail("Error closing ZookeeperStatusHandler");
    }
  }

  @Test(expected = SolrException.class)
  public void validateNotServingRequestsResponse() {
    try (ZookeeperStatusHandler zsh = new ZookeeperStatusHandler(null)) {
      zsh.validateZkRawResponse(Collections.singletonList("This ZooKeeper instance is not currently serving requests"),
          "zoo1:2181", "mntr");
    }  catch (IOException e) {
      fail("Error closing ZookeeperStatusHandler");
    }
  }

  @Test
  public void testMntrBugZk36Solr14463() {
    assumeWorkingMockito();
    ZookeeperStatusHandler zkStatusHandler = mock(ZookeeperStatusHandler.class);
    when(zkStatusHandler.getZkRawResponse("zoo1:2181", "ruok")).thenReturn(Arrays.asList("imok"));
    when(zkStatusHandler.getZkRawResponse("zoo1:2181", "mntr")).thenReturn(
        Arrays.asList("zk_version\t3.5.5-390fe37ea45dee01bf87dc1c042b5e3dcce88653, built on 05/03/2019 12:07 GMT",
            "zk_avg_latency\t1",
            "zk_server_state\tleader",
            "zk_synced_followers\t2"));
    when(zkStatusHandler.getZkRawResponse("zoo1:2181", "conf")).thenReturn(
        Arrays.asList("clientPort=2181"));
    when(zkStatusHandler.getZkStatus(anyString(), any())).thenCallRealMethod();
    when(zkStatusHandler.monitorZookeeper(anyString())).thenCallRealMethod();
    when(zkStatusHandler.validateZkRawResponse(ArgumentMatchers.any(), any(), any())).thenAnswer(Answers.CALLS_REAL_METHODS);

    Map<String, Object> mockStatus = zkStatusHandler.getZkStatus("zoo1:2181", ZkDynamicConfig.fromZkConnectString("zoo1:2181"));
    String expected = "{\n" +
        "  \"mode\":\"ensemble\",\n" +
        "  \"dynamicReconfig\":true,\n" +
        "  \"ensembleSize\":1,\n" +
        "  \"details\":[{\n" +
        "      \"zk_synced_followers\":\"2\",\n" +
        "      \"zk_version\":\"3.5.5-390fe37ea45dee01bf87dc1c042b5e3dcce88653, built on 05/03/2019 12:07 GMT\",\n" +
        "      \"zk_avg_latency\":\"1\",\n" +
        "      \"host\":\"zoo1:2181\",\n" +
        "      \"clientPort\":\"2181\",\n" +
        "      \"ok\":true,\n" +
        "      \"zk_server_state\":\"leader\"}],\n" +
        "  \"zkHost\":\"zoo1:2181\",\n" +
        "  \"errors\":[\"Leader reports 2 followers, but we only found 0. Please check zkHost configuration\"],\n" +
        "  \"status\":\"red\"}";
    assertEquals(expected, JSONUtil.toJSON(mockStatus));
  }

  @Test
  public void testZkWithPrometheusSolr14752() {
    assumeWorkingMockito();
    ZookeeperStatusHandler zkStatusHandler = mock(ZookeeperStatusHandler.class);
    when(zkStatusHandler.getZkRawResponse("zoo1:2181", "ruok")).thenReturn(Arrays.asList("imok"));
    when(zkStatusHandler.getZkRawResponse("zoo1:2181", "mntr")).thenReturn(
        Arrays.asList("zk_version\t3.6.1--104dcb3e3fb464b30c5186d229e00af9f332524b, built on 04/21/2020 15:01 GMT",
            "zk_avg_latency\t0.24",
            "zk_server_state\tleader",
            "zk_synced_followers\t0.0"));
    when(zkStatusHandler.getZkRawResponse("zoo1:2181", "conf")).thenReturn(
        Arrays.asList("clientPort=2181"));
    when(zkStatusHandler.getZkStatus(anyString(), any())).thenCallRealMethod();
    when(zkStatusHandler.monitorZookeeper(anyString())).thenCallRealMethod();
    when(zkStatusHandler.validateZkRawResponse(ArgumentMatchers.any(), any(), any())).thenAnswer(Answers.CALLS_REAL_METHODS);

    // Verifying that parsing the status strings with floating point no longer triggers a NumberFormatException, although floats are still displayed in UI
    Map<String, Object> mockStatus = zkStatusHandler.getZkStatus("Zoo1:2181", ZkDynamicConfig.fromZkConnectString("zoo1:2181"));
    String expected = "{\n" +
            "  \"mode\":\"ensemble\",\n" +
            "  \"dynamicReconfig\":true,\n" +
            "  \"ensembleSize\":1,\n" +
            "  \"details\":[{\n" +
            "      \"zk_synced_followers\":\"0.0\",\n" +
            "      \"zk_version\":\"3.6.1--104dcb3e3fb464b30c5186d229e00af9f332524b, built on 04/21/2020 15:01 GMT\",\n" +
            "      \"zk_avg_latency\":\"0.24\",\n" +
            "      \"host\":\"zoo1:2181\",\n" +
            "      \"clientPort\":\"2181\",\n" +
            "      \"ok\":true,\n" +
            "      \"zk_server_state\":\"leader\"}],\n" +
            "  \"zkHost\":\"Zoo1:2181\",\n" +
            "  \"status\":\"green\"}";
    assertEquals(expected, JSONUtil.toJSON(mockStatus));
  }
}