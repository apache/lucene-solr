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
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;
import static org.apache.solr.common.util.Utils.getObjectByPath;

/**
 * Test for AutoScalingHandler
 */
public class AutoScalingHandlerTest extends SolrCloudTestCase {
  final static String CONFIGSET_NAME = "conf";
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig(CONFIGSET_NAME, configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void beforeTest() throws Exception {
    // clear any persisted auto scaling configuration
    zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);
  }

  @Test
  public void testAutoAddReplicas() throws Exception {
    String collection = "test-collection";
    CollectionAdminRequest
        .createCollection(collection, CONFIGSET_NAME,1, 1)
        .setAutoAddReplicas(true)
        .process(cluster.getSolrClient());
    byte[] data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    ZkNodeProps loaded = ZkNodeProps.load(data);
    Map triggers = (Map) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals("auto add replicas trigger were not created", triggers.size(), 1);
    Map<String, Object> autoAddReplicasTrigger = (Map<String, Object>) triggers.get(".auto_add_replicas");
    assertNotNull(autoAddReplicasTrigger);
    List<Map<String, Object>> actions = (List<Map<String, Object>>) autoAddReplicasTrigger.get("actions");
    assertNotNull(actions);
    assertEquals(3, actions.size());
    assertEquals("auto_add_replicas_plan", actions.get(0).get("name").toString());
    assertEquals("solr.AutoAddReplicasPlanAction", actions.get(0).get("class").toString());

    // lets turn of autoAddReplicas, currently it doesn't change anything in autoscaling config
    // because AutoAddReplicasPlanAction will just ignore any collections with autoAddReplicas turned off
    new CollectionAdminRequest.AsyncCollectionAdminRequest(CollectionParams.CollectionAction.MODIFYCOLLECTION) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
        params.set("collection", collection);
        params.set("autoAddReplicas", false);
        return params;
      }
    }.process(cluster.getSolrClient());

    // lets delete all autoscaling config
    zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);

    // now enable autoAddReplicas and assert that the trigger was re-created correctly
    new CollectionAdminRequest.AsyncCollectionAdminRequest(CollectionParams.CollectionAction.MODIFYCOLLECTION) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
        params.set("collection", collection);
        params.set("autoAddReplicas", true);
        return params;
      }
    }.process(cluster.getSolrClient());
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals("auto add replicas trigger did not created", triggers.size(), 1);
    autoAddReplicasTrigger = (Map<String, Object>) triggers.get(".auto_add_replicas");
    assertNotNull(autoAddReplicasTrigger);
    actions = (List<Map<String, Object>>) autoAddReplicasTrigger.get("actions");
    assertNotNull(actions);
    assertEquals(3, actions.size());
    assertEquals("auto_add_replicas_plan", actions.get(0).get("name").toString());
    assertEquals("solr.AutoAddReplicasPlanAction", actions.get(0).get("class").toString());

    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());
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
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendEachCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    assertEquals(response.get("changed").toString(), "[]");
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, resumeEachCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    assertEquals(response.get("changed").toString(), "[]");

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '10m'," +
        "'enabled' : true}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String suspendTriggerCommand = "{\n" +
        "\t\"suspend-trigger\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger\"\n" +
        "\t}\n" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, resumeTriggerCommand);
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, resumeTriggerCommand);
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, suspendTriggerCommand);
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
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);

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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
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
        "'class' : 'org.apache.solr.cloud.autoscaling.HttpTriggerListener'," +
        "'url' : 'http://xyz.com/on_node_lost?node={$LOST_NODE_NAME}'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
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
    assertEquals("org.apache.solr.cloud.autoscaling.HttpTriggerListener", xyzListener.get("class").toString());

    String removeTriggerCommand = "{" +
        "'remove-trigger' : {" +
        "'name' : 'node_lost_trigger'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, removeTriggerCommand);
    try {
      response = solrClient.request(req);
      String errorMsg = (String) ((NamedList)response.get("error")).get("msg");
      assertTrue(errorMsg.contains("Cannot remove trigger: node_lost_trigger because it has active listeners: [xyz]"));
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
      assertTrue(e.getMessage().contains("Cannot remove trigger: node_lost_trigger because it has active listeners: [xyz]"));
    }

    String removeListenerCommand = "{\n" +
        "\t\"remove-listener\" : {\n" +
        "\t\t\"name\" : \"xyz\"\n" +
        "\t}\n" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, removeListenerCommand);
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, removeTriggerCommand);
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
        "'class' : 'org.apache.solr.cloud.autoscaling.AutoScaling$HttpTriggerListener'," +
        "'url' : 'http://xyz.com/on_node_lost?node={$LOST_NODE_NAME}'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    try {
      response = solrClient.request(req);
      String errorMsg = (String) ((NamedList)response.get("error")).get("msg");
      assertTrue(errorMsg.contains("A trigger with the name node_lost_trigger does not exist"));
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
      assertTrue(e.getMessage().contains("A trigger with the name node_lost_trigger does not exist"));
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
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setPolicyCommand);
    NamedList<Object> response = null;
    try {
      solrClient.request(req);
      fail("Adding a policy with 'cores' attribute should not have succeeded.");
    } catch (HttpSolrClient.RemoteSolrException e)  {
      // expected
      assertTrue(e.getMessage().contains("cores is only allowed in 'cluster-policy'"));
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setPolicyCommand);
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    policies = (Map<String, Object>) loaded.get("policies");
    List conditions = (List) policies.get("xyz");
    assertEquals(1, conditions.size());

    // remove policy
    String removePolicyCommand = "{remove-policy : policy1}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, removePolicyCommand);
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setPreferencesCommand);
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setPreferencesCommand);
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
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPolicyCommand);
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
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setPreferencesCommand = "{" +
        " 'set-cluster-preferences': [" +
        "        {'minimize': 'cores', 'precision': 3}," +
        "        {'maximize': 'freedisk','precision': 100}," +
        "        {'minimize': 'sysLoadAvg','precision': 10}," +
        "        {'minimize': 'heapUsage','precision': 10}]" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setPreferencesCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setPolicyCommand =  "{'set-policy': {" +
        "    'xyz':[" +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]," +
        "    'policy1':[" +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}" +
        "    ]," +
        "    'policy2':[" +
        "      {'replica':'<7', 'shard': '#EACH', 'node': '#ANY'}" +
        "    ]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    req = createAutoScalingRequest(SolrRequest.METHOD.GET, null);
    response = solrClient.request(req);

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
    assertEquals(3, policies.size());
    assertNotNull(policies.get("xyz"));
    assertNotNull(policies.get("policy1"));

    req = createAutoScalingRequest(SolrRequest.METHOD.GET, "/diagnostics", null);
    response = solrClient.request(req);

    Map<String, Object> diagnostics = (Map<String, Object>) response.get("diagnostics");
    List sortedNodes = (List) diagnostics.get("sortedNodes");
    assertNotNull(sortedNodes);

    assertEquals(2, sortedNodes.size());
    for (int i = 0; i < 2; i++) {
      Map node = (Map) sortedNodes.get(i);
      assertNotNull(node);
      assertEquals(5, node.size());
      assertNotNull(node.get("node"));
      assertNotNull(node.get("cores"));
      assertEquals(0L, node.get("cores"));
      assertNotNull(node.get("freedisk"));
      assertTrue(node.get("freedisk") instanceof Double);
      assertNotNull(node.get("sysLoadAvg"));
      assertTrue(node.get("sysLoadAvg") instanceof Double);
      assertNotNull(node.get("heapUsage"));
      assertTrue(node.get("heapUsage") instanceof Double);
    }

    List<Map<String, Object>> violations = (List<Map<String, Object>>) diagnostics.get("violations");
    assertNotNull(violations);
    assertEquals(0, violations.size());

    violations = (List<Map<String, Object>>) diagnostics.get("violations");
    assertNotNull(violations);
    assertEquals(0, violations.size());

    // temporarily increase replica limit in cluster policy so that we can create a collection with 6 replicas
    setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<4', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // lets create a collection which violates the rule replicas < 2
    CollectionAdminResponse adminResponse = CollectionAdminRequest.Create
        .createCollection("readApiTestViolations", CONFIGSET_NAME, 1, 6)
        .setMaxShardsPerNode(10)
        .process(solrClient);
    assertTrue(adminResponse.isSuccess());

    // reset to original cluster policy which allows only 1 replica per shard per node
    setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // get the diagnostics output again
    req = createAutoScalingRequest(SolrRequest.METHOD.GET, "/diagnostics", null);
    response = solrClient.request(req);
    diagnostics = (Map<String, Object>) response.get("diagnostics");
    sortedNodes = (List) diagnostics.get("sortedNodes");
    assertNotNull(sortedNodes);

    violations = (List<Map<String, Object>>) diagnostics.get("violations");
    assertNotNull(violations);
    assertEquals(2, violations.size());
    for (Map<String, Object> violation : violations) {
      assertEquals("readApiTestViolations", violation.get("collection"));
      assertEquals("shard1", violation.get("shard"));
      assertEquals(-1l, getObjectByPath(violation, true, "violation/delta"));
      assertEquals(3l, getObjectByPath(violation, true, "violation/replica/NRT"));
      assertNotNull(violation.get("clause"));
    }
  }


  public static SolrRequest createAutoScalingRequest(SolrRequest.METHOD m, String message) {
    return createAutoScalingRequest(m, null, message);
  }

  static SolrRequest createAutoScalingRequest(SolrRequest.METHOD m, String subPath, String message) {
    boolean useV1 = random().nextBoolean();
    String path = useV1 ? "/admin/autoscaling" : "/cluster/autoscaling";
    path += subPath != null ? subPath : "";
    return useV1
        ? new AutoScalingRequest(m, path, message)
        : new V2Request.Builder(path).withMethod(m).withPayload(message).build();
  }

  static class AutoScalingRequest extends SolrRequest {
    protected final String message;

    AutoScalingRequest(METHOD m, String path, String message) {
      super(m, path);
      this.message = message;
    }

    @Override
    public SolrParams getParams() {
      return null;
    }

    @Override
    public Collection<ContentStream> getContentStreams() throws IOException {
      return message != null ? Collections.singletonList(new ContentStreamBase.StringStream(message)) : null;
    }

    @Override
    protected SolrResponse createResponse(SolrClient client) {
      return null;
    }
  }
}
