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
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CommonParams;
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
  private String path;

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
    // todo nocommit -- add testing for the v2 path
    // String path = random().nextBoolean() ? "/admin/autoscaling" : "/v2/cluster/autoscaling";
    this.path = "/admin/autoscaling";
  }

  @Test
  public void testSuspendTrigger() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String suspendEachCommand = "{\n" +
        "\t\"suspend-trigger\" : {\n" +
        "\t\t\"name\" : \"" + Policy.EACH + "\"\n" +
        "\t}\n" +
        "}";
    String resumeEachCommand = "{\n" +
        "\t\"resume-trigger\" : {\n" +
        "\t\t\"name\" : \"" + Policy.EACH + "\"\n" +
        "\t}\n" +
        "}";
    // these should be no-ops because there are no triggers, and it should succeed
    SolrRequest req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, suspendEachCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    assertEquals(response.get("changed").toString(), "[]");
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, resumeEachCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    assertEquals(response.get("changed").toString(), "[]");

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '10m'," +
        "'enabled' : true}}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '10m'," +
        "'enabled' : true" +
        "}" +
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
    assertEquals(response.get("changed").toString(), "[node_lost_trigger]");

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

    suspendTriggerCommand = "{" +
        "'suspend-trigger' : {" +
        "'name' : '" + Policy.EACH + "'" +
        "}" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    List<String> changed = (List<String>)response.get("changed");
    assertEquals(1, changed.size());
    assertTrue(changed.contains("node_added_trigger"));
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

    String resumeTriggerCommand = "{" +
        "'resume-trigger' : {" +
        "'name' : 'node_added_trigger'" +
        "}" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    changed = (List<String>)response.get("changed");
    assertEquals(1, changed.size());
    assertTrue(changed.contains("node_added_trigger"));
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

    resumeTriggerCommand = "{" +
        "'resume-trigger' : {" +
        "'name' : '" + Policy.EACH + "'" +
        "}" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    changed = (List<String>)response.get("changed");
    assertEquals(1, changed.size());
    assertTrue(changed.contains("node_lost_trigger"));
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

    suspendTriggerCommand = "{" +
        "'suspend-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'timeout' : '1h'" +
        "}" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    changed = (List<String>)response.get("changed");
    assertEquals(1, changed.size());
    assertTrue(changed.contains("node_lost_trigger"));
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
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '10m'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{" +
        "'name' : 'compute_plan'," +
        "'class' : 'solr.ComputePlanAction'" +
        "}," +
        "{" +
        "'name' : 'log_plan'," +
        "'class' : 'solr.LogPlanAction'," +
        "'collection' : '.system'" +
        "}]}}";
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

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '20m'," +
        "'enabled' : false" +
        "}}";
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

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'xyz'," +
        "'trigger' : 'node_lost_trigger'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED']," +
        "'beforeAction' : 'execute_plan'," +
        "'class' : 'org.apache.solr.cloud.autoscaling.AutoScaling$HttpCallbackListener'," +
        "'url' : 'http://xyz.com/on_node_lost?node={$LOST_NODE_NAME}'" +
        "}" +
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

    String removeTriggerCommand = "{" +
        "'remove-trigger' : {" +
        "'name' : 'node_lost_trigger'" +
        "}" +
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

    removeTriggerCommand = "{" +
        "'remove-trigger' : {" +
        "'name' : 'node_lost_trigger'" +
        "}" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, removeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(0, triggers.size());

    setListenerCommand = "{" +
        "'set-listener' : {" +
        "'name' : 'xyz'," +
        "'trigger' : 'node_lost_trigger'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED']," +
        "'beforeAction' : 'execute_plan'," +
        "'class' : 'org.apache.solr.cloud.autoscaling.AutoScaling$HttpCallbackListener'," +
        "'url' : 'http://xyz.com/on_node_lost?node={$LOST_NODE_NAME}'" +
        "}" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setListenerCommand);
    try {
      response = solrClient.request(req);
      fail("Adding a listener on a non-existent trigger should have failed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
    }
  }

  @Test
  public void testPolicyAndPreferences() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    // add multiple policies
    String setPolicyCommand =  "{'set-policy': {" +
        "    'xyz':[" +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'!overseer', 'replica':0}" +
        "    ]," +
        "    'policy1':[" +
        "      {'cores':'<2', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}" +
        "    ]" +
        "}}";
    SolrRequest req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setPolicyCommand);
    NamedList<Object> response = null;
    try {
      response = solrClient.request(req);
      fail("Adding a policy with 'cores' attribute should not have succeeded.");
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
      assertTrue(e.getMessage().contains("cores is only allowed in 'cluster-policy'"));
    } catch (Exception e) {
      throw e;
    }

    setPolicyCommand =  "{'set-policy': {" +
        "    'xyz':[" +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'!overseer', 'replica':0}" +
        "    ]," +
        "    'policy1':[" +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}" +
        "    ]" +
        "}}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    byte[] data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    ZkNodeProps loaded = ZkNodeProps.load(data);
    Map<String, Object> policies = (Map<String, Object>) loaded.get("policies");
    assertNotNull(policies);
    assertNotNull(policies.get("xyz"));
    assertNotNull(policies.get("policy1"));

    // update default policy
    setPolicyCommand = "{'set-policy': {" +
        "    'xyz':[" +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}" +
        "    ]" +
        "}}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    policies = (Map<String, Object>) loaded.get("policies");
    List conditions = (List) policies.get("xyz");
    assertEquals(1, conditions.size());

    // remove policy
    String removePolicyCommand = "{remove-policy : policy1}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, removePolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    policies = (Map<String, Object>) loaded.get("policies");
    assertNull(policies.get("policy1"));

    // set preferences
    String setPreferencesCommand = "{" +
        " 'set-cluster-preferences': [" +
        "        {'minimize': 'cores', 'precision': 3}," +
        "        {'maximize': 'freedisk','precision': 100}," +
        "        {'minimize': 'sysLoadAvg','precision': 10}]" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setPreferencesCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    List preferences = (List) loaded.get("cluster-preferences");
    assertEquals(3, preferences.size());

    // set preferences
    setPreferencesCommand = "{" +
        " 'set-cluster-preferences': [" +
        "        {'minimize': 'sysLoadAvg','precision': 10}]" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setPreferencesCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    preferences = (List) loaded.get("cluster-preferences");
    assertEquals(1, preferences.size());

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'!overseer', 'replica':0}" +
        "    ]" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    List clusterPolicy = (List) loaded.get("cluster-policy");
    assertNotNull(clusterPolicy);
    assertEquals(3, clusterPolicy.size());
  }

  @Test
  public void testReadApi() throws Exception  {
    CloudSolrClient solrClient = cluster.getSolrClient();
    // first trigger
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_added_trigger1'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '0s'," +
        "'enabled' : true" +
        "}}";
    SolrRequest req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setPreferencesCommand = "{" +
        " 'set-cluster-preferences': [" +
        "        {'minimize': 'cores', 'precision': 3}," +
        "        {'maximize': 'freedisk','precision': 100}," +
        "        {'minimize': 'sysLoadAvg','precision': 10}," +
        "        {'minimize': 'heapUsage','precision': 10}]" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setPreferencesCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setPolicyCommand =  "{'set-policy': {" +
        "    'xyz':[" +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]," +
        "    'policy1':[" +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}" +
        "    ]" +
        "}}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, setPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    SolrQuery query = new SolrQuery().setParam(CommonParams.QT, path);
    QueryResponse queryResponse = solrClient.query(query);
    response = queryResponse.getResponse();

    Map triggers = (Map) response.get("triggers");
    assertNotNull(triggers);
    assertEquals(1, triggers.size());
    assertTrue(triggers.containsKey("node_added_trigger1"));
    Map node_added_trigger1 = (Map) triggers.get("node_added_trigger1");
    assertEquals(4, node_added_trigger1.size());
    assertEquals(0L, node_added_trigger1.get("waitFor"));
    assertEquals(true, node_added_trigger1.get("enabled"));
    assertEquals(3, ((List)node_added_trigger1.get("actions")).size());

    List<Map> clusterPrefs = (List<Map>) response.get("cluster-preferences");
    assertNotNull(clusterPrefs);
    assertEquals(4, clusterPrefs.size());

    List<Map> clusterPolicy = (List<Map>) response.get("cluster-policy");
    assertNotNull(clusterPolicy);
    assertEquals(3, clusterPolicy.size());

    Map policies = (Map) response.get("policies");
    assertNotNull(policies);
    assertEquals(2, policies.size());
    assertNotNull(policies.get("xyz"));
    assertNotNull(policies.get("policy1"));

    query = new SolrQuery().setParam(CommonParams.QT, path + "/diagnostics");
    queryResponse = solrClient.query(query);
    response = queryResponse.getResponse();

    Map<String, Object> diagnostics = (Map<String, Object>) response.get("diagnostics");
    List sortedNodes = (List) diagnostics.get("sortedNodes");
    assertNotNull(sortedNodes);

    assertEquals(2, sortedNodes.size());
    String[] sortedNodeNames = new String[2];
    for (int i = 0; i < 2; i++) {
      Map node = (Map) sortedNodes.get(i);
      assertNotNull(node);
      assertEquals(5, node.size());
      assertNotNull(sortedNodeNames[i] = (String) node.get("node"));
      assertNotNull(node.get("cores"));
      assertEquals(0, node.get("cores"));
      assertNotNull(node.get("freedisk"));
      assertNotNull(node.get("sysLoadAvg"));
      assertNotNull(node.get("heapUsage"));
    }

    List<Map<String, Object>> violations = (List<Map<String, Object>>) diagnostics.get("violations");
    assertNotNull(violations);
    assertEquals(0, violations.size());

    violations = (List<Map<String, Object>>) diagnostics.get("violations");
    assertNotNull(violations);
    assertEquals(0, violations.size());

    // lets create a collection which violates the rule replicas < 2
    CollectionAdminRequest.Create create = CollectionAdminRequest.Create.createCollection("readApiTestViolations", 1, 6);
    create.setMaxShardsPerNode(10);
    CollectionAdminResponse adminResponse = create.process(solrClient);
    assertTrue(adminResponse.isSuccess());

    // get the diagnostics output again
    queryResponse = solrClient.query(query);
    response = queryResponse.getResponse();
    diagnostics = (Map<String, Object>) response.get("diagnostics");
    sortedNodes = (List) diagnostics.get("sortedNodes");
    assertNotNull(sortedNodes);

    violations = (List<Map<String, Object>>) diagnostics.get("violations");
    assertNotNull(violations);
    assertEquals(2, violations.size());
    for (Map<String, Object> violation : violations) {
      assertEquals("readApiTestViolations", violation.get("collection"));
      assertEquals("shard1", violation.get("shard"));
      assertEquals(Utils.makeMap("replica", "3", "delta", -1), violation.get("violation"));
      assertNotNull(violation.get("clause"));
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