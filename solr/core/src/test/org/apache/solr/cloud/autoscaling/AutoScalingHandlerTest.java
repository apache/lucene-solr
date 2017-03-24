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

package org.apache.solr.cloud.autoscaling;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for AutoScalingHandler
 */
public class AutoScalingHandlerTest extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void beforeTest() throws Exception {
    // clear any persisted auto scaling configuration
    zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);
  }

  @Test
  public void testSuspendTrigger() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    // todo nocommit -- add testing for the v2 path
    // String path = random().nextBoolean() ? "/admin/autoscaling" : "/v2/cluster/autoscaling";
    String path = "/admin/autoscaling";
    String setTriggerCommand = "{\n" +
        "\t\"set-trigger\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger\",\n" +
        "\t\t\"event\" : \"nodeLost\",\n" +
        "\t\t\"waitFor\" : \"10m\",\n" +
        "\t\t\"enabled\" : \"true\"\n" +
        "\t}\n" +
        "}";
    SolrRequest req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setTriggerCommand = "{\n" +
        "\t\"set-trigger\" : {\n" +
        "\t\t\"name\" : \"node_added_trigger\",\n" +
        "\t\t\"event\" : \"nodeAdded\",\n" +
        "\t\t\"waitFor\" : \"10m\",\n" +
        "\t\t\"enabled\" : \"true\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String suspendTriggerCommand = "{\n" +
        "\t\"suspend-trigger\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    byte[] data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    ZkNodeProps loaded = ZkNodeProps.load(data);
    Map<String, Object> triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(2, triggers.size());
    assertTrue(triggers.containsKey("node_lost_trigger"));
    assertTrue(triggers.containsKey("node_added_trigger"));
    Map<String, Object> nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(4, nodeLostTrigger.size());
    assertEquals("false", nodeLostTrigger.get("enabled").toString());
    Map<String, Object> nodeAddedTrigger = (Map<String, Object>) triggers.get("node_added_trigger");
    assertEquals(4, nodeAddedTrigger.size());
    assertEquals("true", nodeAddedTrigger.get("enabled").toString());

    suspendTriggerCommand = "{\n" +
        "\t\"suspend-trigger\" : {\n" +
        "\t\t\"name\" : \"#EACH\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(2, triggers.size());
    nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(4, nodeLostTrigger.size());
    assertEquals("false", nodeLostTrigger.get("enabled").toString());
    nodeAddedTrigger = (Map<String, Object>) triggers.get("node_added_trigger");
    assertEquals(4, nodeAddedTrigger.size());
    assertEquals("false", nodeAddedTrigger.get("enabled").toString());

    String resumeTriggerCommand = "{\n" +
        "\t\"resume-trigger\" : {\n" +
        "\t\t\"name\" : \"node_added_trigger\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(2, triggers.size());
    nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(4, nodeLostTrigger.size());
    assertEquals("false", nodeLostTrigger.get("enabled").toString());
    nodeAddedTrigger = (Map<String, Object>) triggers.get("node_added_trigger");
    assertEquals(4, nodeAddedTrigger.size());
    assertEquals("true", nodeAddedTrigger.get("enabled").toString());

    resumeTriggerCommand = "{\n" +
        "\t\"resume-trigger\" : {\n" +
        "\t\t\"name\" : \"#EACH\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(2, triggers.size());
    nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(4, nodeLostTrigger.size());
    assertEquals("true", nodeLostTrigger.get("enabled").toString());
    nodeAddedTrigger = (Map<String, Object>) triggers.get("node_added_trigger");
    assertEquals(4, nodeAddedTrigger.size());
    assertEquals("true", nodeAddedTrigger.get("enabled").toString());

    suspendTriggerCommand = "{\n" +
        "\t\"suspend-trigger\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger\",\n" +
        "\t\t\"timeout\" : \"1h\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(2, triggers.size());
    nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(5, nodeLostTrigger.size());
    assertEquals("false", nodeLostTrigger.get("enabled").toString());
    assertTrue(nodeLostTrigger.containsKey("resumeAt"));
  }

  @Test
  public void test() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    // todo nocommit -- add testing for the v2 path
    // String path = random().nextBoolean() ? "/admin/autoscaling" : "/v2/cluster/autoscaling";
    String path = "/admin/autoscaling";
    String setTriggerCommand = "{\n" +
        "\t\"set-trigger\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger\",\n" +
        "\t\t\"event\" : \"nodeLost\",\n" +
        "\t\t\"waitFor\" : \"10m\",\n" +
        "\t\t\"enabled\" : \"true\",\n" +
        "\t\t\"actions\" : [\n" +
        "\t\t\t{\n" +
        "\t\t\t\t\"name\" : \"compute_plan\",\n" +
        "\t\t\t\t\"class\" : \"solr.ComputePlanAction\"\n" +
        "\t\t\t},\n" +
        "\t\t\t{\n" +
        "\t\t\t\t\"name\" : \"log_plan\",\n" +
        "\t\t\t\t\"class\" : \"solr.LogPlanAction\",\n" +
        "\t\t\t\t\"collection\" : \".system\"\n" +
        "\t\t\t}\n" +
        "\t\t]\n" +
        "\t}\n" +
        "}";
    SolrRequest req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);

    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    byte[] data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    ZkNodeProps loaded = ZkNodeProps.load(data);
    Map<String, Object> triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(1, triggers.size());
    assertTrue(triggers.containsKey("node_lost_trigger"));
    Map<String, Object> nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(4, nodeLostTrigger.size());
    List<Map<String, String>> actions = (List<Map<String, String>>) nodeLostTrigger.get("actions");
    assertNotNull(actions);
    assertEquals(2, actions.size());
    assertEquals("600", nodeLostTrigger.get("waitFor").toString());

    setTriggerCommand = "{\n" +
        "\t\"set-trigger\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger\",\n" +
        "\t\t\"event\" : \"nodeLost\",\n" +
        "\t\t\"waitFor\" : \"20m\",\n" +
        "\t\t\"enabled\" : \"false\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(1, triggers.size());
    assertTrue(triggers.containsKey("node_lost_trigger"));
    nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(4, nodeLostTrigger.size());
    assertEquals("1200", nodeLostTrigger.get("waitFor").toString());
    assertEquals("false", nodeLostTrigger.get("enabled").toString());
    actions = (List<Map<String, String>>) nodeLostTrigger.get("actions");
    assertNotNull(actions);
    assertEquals(3, actions.size());

    String setListenerCommand = "{\n" +
        "\t\"set-listener\" : \n" +
        "\t\t{\n" +
        "\t\t\t\"name\" : \"xyz\",\n" +
        "\t\t\t\"trigger\" : \"node_lost_trigger\",\n" +
        "\t\t\t\"stage\" : [\"STARTED\",\"ABORTED\",\"SUCCEEDED\"],\n" +
        "\t\t\t\"beforeAction\" : \"execute_plan\",\n" +
        "\t\t\t\"class\" : \"org.apache.solr.cloud.autoscaling.AutoScaling$HttpCallbackListener\",\n" +
        "\t\t\t\"url\" : \"http://xyz.com/on_node_lost?node={$LOST_NODE_NAME}\"\n" +
        "\t\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    Map<String, Object> listeners = (Map<String, Object>) loaded.get("listeners");
    assertNotNull(listeners);
    assertEquals(1, listeners.size());
    assertTrue(listeners.containsKey("xyz"));
    Map<String, Object> xyzListener = (Map<String, Object>) listeners.get("xyz");
    assertEquals(5, xyzListener.size());
    assertEquals("org.apache.solr.cloud.autoscaling.AutoScaling$HttpCallbackListener", xyzListener.get("class").toString());

    String removeTriggerCommand = "{\n" +
        "\t\"remove-trigger\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, removeTriggerCommand);
    try {
      response = solrClient.request(req);
      fail("Trying to remove trigger which has listeners registered should have failed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
    }

    String removeListenerCommand = "{\n" +
        "\t\"remove-listener\" : {\n" +
        "\t\t\"name\" : \"xyz\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, removeListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    listeners = (Map<String, Object>) loaded.get("listeners");
    assertNotNull(listeners);
    assertEquals(0, listeners.size());

    removeTriggerCommand = "{\n" +
        "\t\"remove-trigger\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, removeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(0, triggers.size());

    setListenerCommand = "{\n" +
        "\t\"set-listener\" : \n" +
        "\t\t{\n" +
        "\t\t\t\"name\" : \"xyz\",\n" +
        "\t\t\t\"trigger\" : \"node_lost_trigger\",\n" +
        "\t\t\t\"stage\" : [\"STARTED\",\"ABORTED\",\"SUCCEEDED\"],\n" +
        "\t\t\t\"beforeAction\" : \"execute_plan\",\n" +
        "\t\t\t\"class\" : \"org.apache.solr.cloud.autoscaling.AutoScaling$HttpCallbackListener\",\n" +
        "\t\t\t\"url\" : \"http://xyz.com/on_node_lost?node={$LOST_NODE_NAME}\"\n" +
        "\t\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, removeListenerCommand);
    try {
      response = solrClient.request(req);
      fail("Adding a listener on a non-existent trigger should have failed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
    }
  }

  static class AutoScalingRequest extends SolrRequest {
    protected final String message;

    public AutoScalingRequest(METHOD m, String path, String message) {
      super(m, path);
      this.message = message;
    }

    @Override
    public SolrParams getParams() {
      return null;
    }

    @Override
    public Collection<ContentStream> getContentStreams() throws IOException {
      return Collections.singletonList(new ContentStreamBase.StringStream(message));
    }

    @Override
    protected SolrResponse createResponse(SolrClient client) {
      return null;
    }
  }
}