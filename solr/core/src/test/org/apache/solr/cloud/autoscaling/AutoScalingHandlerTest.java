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
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.cloud.autoscaling.Suggestion.Type.repair;
import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;
import static org.apache.solr.common.util.Utils.getObjectByPath;

/**
 * Test for AutoScalingHandler
 */
public class AutoScalingHandlerTest extends SolrCloudTestCase {
  final static String CONFIGSET_NAME = "conf";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(2)
        .addConfig(CONFIGSET_NAME, configset("cloud-minimal"))
        .configure();
    testAutoAddReplicas();
  }

  private static void testAutoAddReplicas() throws Exception {
    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeOut.hasTimedOut()) {
      byte[] data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
      ZkNodeProps loaded = ZkNodeProps.load(data);
      @SuppressWarnings({"rawtypes"})
      Map triggers = (Map) loaded.get("triggers");
      if (triggers != null && triggers.containsKey(".auto_add_replicas")) {
        @SuppressWarnings({"unchecked"})
        Map<String, Object> autoAddReplicasTrigger = (Map<String, Object>) triggers.get(".auto_add_replicas");
        assertNotNull(autoAddReplicasTrigger);
        @SuppressWarnings({"unchecked"})
        List<Map<String, Object>> actions = (List<Map<String, Object>>) autoAddReplicasTrigger.get("actions");
        assertNotNull(actions);
        assertEquals(2, actions.size());
        assertEquals("auto_add_replicas_plan", actions.get(0).get("name").toString());
        assertEquals("solr.AutoAddReplicasPlanAction", actions.get(0).get("class").toString());
        break;
      } else {
        Thread.sleep(300);
      }
    }
    if (timeOut.hasTimedOut()) {
      fail("Timeout waiting for .auto_add_replicas being created");
    }
  }

  @Before
  public void beforeTest() throws Exception {
    // clear any persisted auto scaling configuration
    zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);
  }

  public void testSuggestionsWithPayload() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String COLLNAME = "testSuggestionsWithPayload.COLL";
    CollectionAdminResponse adminResponse = CollectionAdminRequest.createCollection(COLLNAME, CONFIGSET_NAME, 1, 2)
        .setMaxShardsPerNode(4)
        .process(solrClient);
    cluster.waitForActiveCollection(COLLNAME, 1, 2);
    DocCollection collection = solrClient.getClusterStateProvider().getCollection(COLLNAME);
    Replica aReplica = collection.getReplicas().get(0);

    String configPayload = "{\n" +
        "  'cluster-policy': [{'replica': 0, 'node': '_NODE'}]\n" +
        "}";
    configPayload = configPayload.replaceAll("_NODE", aReplica.getNodeName());
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, "/suggestions", configPayload);
    NamedList<Object> response = solrClient.request(req);
    assertFalse(((Collection) response.get("suggestions")).isEmpty());
    String replicaName = response._getStr("suggestions[0]/operation/command/move-replica/replica", null);
    boolean[] passed = new boolean[]{false};
    collection.forEachReplica((s, replica) -> {
      if (replica.getName().equals(replicaName) && replica.getNodeName().equals(aReplica.getNodeName())) {
        passed[0] = true;
      }
    });
    assertTrue(passed[0]);

    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, "/suggestions", configPayload, new MapSolrParams(Collections.singletonMap("type", repair.name())));
    response = solrClient.request(req);
    assertTrue(((Collection) response.get("suggestions")).isEmpty());

    CollectionAdminRequest.deleteCollection(COLLNAME)
        .process(cluster.getSolrClient());
  }
  public void testDiagnosticsWithPayload() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String COLLNAME = "testDiagnosticsWithPayload.COLL";
    CollectionAdminResponse adminResponse = CollectionAdminRequest.createCollection(COLLNAME, CONFIGSET_NAME, 1, 2)
        .setMaxShardsPerNode(4)
        .process(solrClient);
    cluster.waitForActiveCollection(COLLNAME, 1, 2);
    DocCollection collection = solrClient.getClusterStateProvider().getCollection(COLLNAME);
    Replica aReplica = collection.getReplicas().get(0);

    String configPayload = "{\n" +
        "  'cluster-policy': [{'replica': 0, 'node': '_NODE'}]\n" +
        "}";
    configPayload = configPayload.replaceAll("_NODE", aReplica.getNodeName());
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, "/diagnostics", configPayload);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response._getStr("diagnostics/violations[0]/node",null),response._getStr("diagnostics/violations[0]/node",null));
    CollectionAdminRequest.deleteCollection(COLLNAME)
        .process(cluster.getSolrClient());
  }

  @Test
  @SuppressWarnings({"unchecked"})
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
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, suspendEachCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    assertEquals(response.get("changed").toString(), "[]");
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeEachCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    assertEquals(response.get("changed").toString(), "[]");

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '10m'," +
        "'enabled' : true}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String suspendTriggerCommand = "{\n" +
        "\t\"suspend-trigger\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger\"\n" +
        "\t}\n" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    assertEquals(response.get("changed").toString(), "[node_lost_trigger]");

    Stat stat = new Stat();
    byte[] data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, stat, true);
    ZkNodeProps loaded = ZkNodeProps.load(data);
    Map<String, Object> triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(2, countNotImplicitTriggers(triggers));
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    List<String> changed = (List<String>) response.get("changed");
    assertEquals(1, changed.size());
    assertTrue(changed.contains("node_added_trigger"));
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(2, countNotImplicitTriggers(triggers));
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    changed = (List<String>) response.get("changed");
    assertEquals(1, changed.size());
    assertTrue(changed.contains("node_added_trigger"));
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(2, countNotImplicitTriggers(triggers));
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, resumeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    changed = (List<String>) response.get("changed");
    assertEquals(1, changed.size());
    assertTrue(changed.contains("node_lost_trigger"));
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(2, countNotImplicitTriggers(triggers));
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, suspendTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    changed = (List<String>) response.get("changed");
    assertEquals(1, changed.size());
    assertTrue(changed.contains("node_lost_trigger"));
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(2, countNotImplicitTriggers(triggers));
    nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(5, nodeLostTrigger.size());
    assertEquals("false", nodeLostTrigger.get("enabled").toString());
    assertTrue(nodeLostTrigger.containsKey("resumeAt"));
  }

  @Test
  @SuppressWarnings({"unchecked"})
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
        "}]}}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);

    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    byte[] data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    ZkNodeProps loaded = ZkNodeProps.load(data);
    Map<String, Object> triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(1, countNotImplicitTriggers(triggers));
    assertTrue(triggers.containsKey("node_lost_trigger"));
    Map<String, Object> nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(4, nodeLostTrigger.size());
    List<Map<String, String>> actions = (List<Map<String, String>>) nodeLostTrigger.get("actions");
    assertNotNull(actions);
    assertEquals(1, actions.size());
    assertEquals("600", nodeLostTrigger.get("waitFor").toString());

    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '20m'," +
        "'enabled' : false" +
        "}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(1, countNotImplicitTriggers(triggers));
    assertTrue(triggers.containsKey("node_lost_trigger"));
    nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(4, nodeLostTrigger.size());
    assertEquals("1200", nodeLostTrigger.get("waitFor").toString());
    assertEquals("false", nodeLostTrigger.get("enabled").toString());
    actions = (List<Map<String, String>>) nodeLostTrigger.get("actions");
    assertNotNull(actions);
    assertEquals(2, actions.size());

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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    Map<String, Object> listeners = (Map<String, Object>) loaded.get("listeners");
    assertNotNull(listeners);
    assertEquals(2, listeners.size());
    assertTrue(listeners.containsKey("xyz"));
    Map<String, Object> xyzListener = (Map<String, Object>) listeners.get("xyz");
    assertEquals(6, xyzListener.size());
    assertEquals("org.apache.solr.cloud.autoscaling.HttpTriggerListener", xyzListener.get("class").toString());

    String removeTriggerCommand = "{" +
        "'remove-trigger' : {" +
        "'name' : 'node_lost_trigger'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, removeTriggerCommand);
    try {
      solrClient.request(req);
      fail("expected exception");
    } catch (HttpSolrClient.RemoteExecutionException e) {
      // expected
      assertTrue(String.valueOf(getObjectByPath(e.getMetaData(),
          false, "error/details[0]/errorMessages[0]")).contains("Cannot remove trigger: node_lost_trigger because it has active listeners: ["));
    }

    String removeListenerCommand = "{\n" +
        "\t\"remove-listener\" : {\n" +
        "\t\t\"name\" : \"xyz\"\n" +
        "\t}\n" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, removeListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    listeners = (Map<String, Object>) loaded.get("listeners");
    assertNotNull(listeners);
    assertEquals(1, listeners.size());

    removeTriggerCommand = "{" +
        "'remove-trigger' : {" +
        "'name' : 'node_lost_trigger'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, removeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(0, countNotImplicitTriggers(triggers));

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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    try {
      solrClient.request(req);
      fail("should have thrown Exception");
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
      assertTrue(String.valueOf(getObjectByPath(((HttpSolrClient.RemoteExecutionException) e).getMetaData(),
          false, "error/details[0]/errorMessages[0]")).contains("A trigger with the name node_lost_trigger does not exist"));
    }
  }

  @Test
  public void testErrorHandling() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    try {
      @SuppressWarnings({"rawtypes"})
      SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
      solrClient.request(req);
      fail("expect exception");
    } catch (HttpSolrClient.RemoteExecutionException e) {
      String message = String.valueOf(getObjectByPath(e.getMetaData(), true, "error/details[0]/errorMessages[0]"));
      assertTrue(message.contains("replica is required in"));
    }

  }

  @Test
  public void testValidation() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();

    // unknown trigger properties
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '10m'," +
        "'enabled' : true," +
        "'foo': 'bar'," +
        "'actions' : [" +
        "{" +
        "'name' : 'compute_plan'," +
        "'class' : 'solr.ComputePlanAction'" +
        "}]}}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);

    try {
      solrClient.request(req);
      fail("should have thrown Exception");
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
      assertTrue(String.valueOf(getObjectByPath(((HttpSolrClient.RemoteExecutionException) e).getMetaData(),
          false, "error/details[0]/errorMessages[0]")).contains("foo=unknown property"));
    }

    // invalid trigger properties
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'search_rate_trigger'," +
        "'event' : 'searchRate'," +
        "'waitFor' : '10m'," +
        "'enabled' : true," +
        "'aboveRate': 'foo'," +
        "'actions' : [" +
        "{" +
        "'name' : 'compute_plan'," +
        "'class' : 'solr.ComputePlanAction'" +
        "}]}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);

    try {
      solrClient.request(req);
      fail("should have thrown Exception");
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
      assertTrue(String.valueOf(getObjectByPath(((HttpSolrClient.RemoteExecutionException) e).getMetaData(),
          false, "error/details[0]/errorMessages[0]")).contains("aboveRate=Invalid configuration value: 'foo'"));
    }

    // unknown trigger action properties
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '10m'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{" +
        "'name' : 'compute_plan'," +
        "'foo' : 'bar'," +
        "'class' : 'solr.ComputePlanAction'" +
        "}]}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);

    try {
      solrClient.request(req);
      fail("should have thrown Exception");
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
      assertTrue(String.valueOf(getObjectByPath(((HttpSolrClient.RemoteExecutionException) e).getMetaData(),
          false, "error/details[0]/errorMessages[0]")).contains("foo=unknown property"));
    }

    // unknown trigger listener properties
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '10m'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{" +
        "'name' : 'compute_plan'," +
        "'class' : 'solr.ComputePlanAction'" +
        "}]}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);

    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'xyz'," +
        "'trigger' : 'node_lost_trigger'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED']," +
        "'foo' : 'bar'," +
        "'beforeAction' : 'execute_plan'," +
        "'class' : 'org.apache.solr.cloud.autoscaling.HttpTriggerListener'," +
        "'url' : 'http://xyz.com/on_node_lost?node={$LOST_NODE_NAME}'" +
        "}" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setListenerCommand);
    try {
      solrClient.request(req);
      fail("should have thrown Exception");
    } catch (HttpSolrClient.RemoteSolrException e) {
      // expected
      assertTrue(String.valueOf(getObjectByPath(((HttpSolrClient.RemoteExecutionException) e).getMetaData(),
          false, "error/details[0]/errorMessages[0]")).contains("foo=unknown property"));
    }
  }

  @Test
  @SuppressWarnings({"unchecked"})
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
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setPolicyCommand);
    NamedList<Object> response = null;
    try {
      solrClient.request(req);
      fail("Adding a policy with 'cores' attribute should not have succeeded.");
    } catch (HttpSolrClient.RemoteExecutionException e)  {
      String message = e.getMetaData()._getStr("error/details[0]/errorMessages[0]",null);

      // expected
      assertTrue(message.contains("cores is only allowed in 'cluster-policy'"));
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setPolicyCommand);
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    policies = (Map<String, Object>) loaded.get("policies");
    @SuppressWarnings({"rawtypes"})
    List conditions = (List) policies.get("xyz");
    assertEquals(1, conditions.size());

    // remove policy
    String removePolicyCommand = "{remove-policy : policy1}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, removePolicyCommand);
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setPreferencesCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    @SuppressWarnings({"rawtypes"})
    List preferences = (List) loaded.get("cluster-preferences");
    assertEquals(3, preferences.size());

    // set preferences
    setPreferencesCommand = "{" +
        " 'set-cluster-preferences': [" +
        "        {'minimize': 'sysLoadAvg','precision': 10}]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setPreferencesCommand);
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
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    @SuppressWarnings({"rawtypes"})
    List clusterPolicy = (List) loaded.get("cluster-policy");
    assertNotNull(clusterPolicy);
    assertEquals(3, clusterPolicy.size());

    setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'replica':0, put : on-each-node, nodeset:{'nodeRole':'overseer'} }" +
        "    ]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    clusterPolicy = (List) loaded.get("cluster-policy");
    assertNotNull(clusterPolicy);
    assertEquals(3, clusterPolicy.size());
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 17-Aug-2018
  @SuppressWarnings({"unchecked", "rawtypes"})
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
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<3', 'shard': '#EACH', 'node': '#ANY'}]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setPreferencesCommand = "{" +
        " 'set-cluster-preferences': [" +
        "        {'minimize': 'cores', 'precision': 3}," +
        "        {'maximize': 'freedisk','precision': 100}," +
        "        {'minimize': 'sysLoadAvg','precision': 10}," +
        "        {'minimize': 'heapUsage','precision': 10}]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setPreferencesCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String setPolicyCommand =  "{'set-policy': {" +
        "    'xyz':[{'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}]," +
        "    'policy1':[{'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}]," +
        "    'policy2':[{'replica':'<7', 'shard': '#EACH', 'node': '#ANY'}]}}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    req = AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    response = solrClient.request(req);

    Map triggers = (Map) response.get("triggers");
    assertNotNull(triggers);
    assertEquals(1, countNotImplicitTriggers(triggers));
    assertTrue(triggers.containsKey("node_added_trigger1"));
    Map node_added_trigger1 = (Map) triggers.get("node_added_trigger1");
    assertEquals(4, node_added_trigger1.size());
    assertEquals(0L, node_added_trigger1.get("waitFor"));
    assertEquals(true, node_added_trigger1.get("enabled"));
    assertEquals(2, ((List)node_added_trigger1.get("actions")).size());

    List<Map> clusterPrefs = (List<Map>) response.get("cluster-preferences");
    assertNotNull(clusterPrefs);
    assertEquals(4, clusterPrefs.size());

    List<Map> clusterPolicy = (List<Map>) response.get("cluster-policy");
    assertNotNull(clusterPolicy);
    assertEquals(2, clusterPolicy.size());

    Map policies = (Map) response.get("policies");
    assertNotNull(policies);
    assertEquals(3, policies.size());
    assertNotNull(policies.get("xyz"));
    assertNotNull(policies.get("policy1"));

    req = AutoScalingRequest.create(SolrRequest.METHOD.GET, "/diagnostics", null);
    response = solrClient.request(req);

    Map<String, Object> diagnostics = (Map<String, Object>) response.get("diagnostics");
    List sortedNodes = (List) response._get("diagnostics/sortedNodes", null);
    assertNotNull(sortedNodes);

    assertEquals(2, sortedNodes.size());
    for (int i = 0; i < 2; i++) {
      Map node = (Map) sortedNodes.get(i);
      assertNotNull(node);
      assertNotNull(node.get("node"));
      assertNotNull(node.get("cores"));
      assertEquals(0d, node.get("cores"));
      assertNotNull(node.get("freedisk"));
      assertNotNull(node.get("replicas"));
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
    String tempClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<4', 'shard': '#EACH', 'node': '#ANY'}"+
        "    ]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, tempClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // lets create a collection which violates the rule replicas < 2
    CollectionAdminRequest.Create create = CollectionAdminRequest.Create.createCollection("readApiTestViolations", CONFIGSET_NAME, 1, 6)
        .setMaxShardsPerNode(3);
    CollectionAdminResponse adminResponse = create.process(solrClient);
    cluster.waitForActiveCollection("readApiTestViolations", 1, 6);
    assertTrue(adminResponse.isSuccess());

    // reset the original cluster policy
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // get the diagnostics output again
    req = AutoScalingRequest.create(SolrRequest.METHOD.GET, "/diagnostics", null);
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
      assertEquals(1.0d, getObjectByPath(violation, true, "violation/delta"));
      assertEquals(3l, getObjectByPath(violation, true, "violation/replica/NRT"));
      assertNotNull(violation.get("clause"));
    }
    if (log.isInfoEnabled()) {
      log.info("Before starting new jetty ,{}", cluster.getJettySolrRunners()
          .stream()
          .map(jettySolrRunner -> jettySolrRunner.getNodeName()).collect(Collectors.toList()));
    }
    JettySolrRunner runner1 = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    if (log.isInfoEnabled()) {
      log.info("started new jetty {}", runner1.getNodeName());
    }

    response = waitForResponse(namedList -> {
          List l = (List) namedList._get("diagnostics/liveNodes",null);
          if (l != null && l.contains(runner1.getNodeName())) return true;
          return false;
        },
        AutoScalingRequest.create(SolrRequest.METHOD.GET, "/diagnostics", null),
        200,
        20,
        runner1.getNodeName() + " could not come up ");

    req = AutoScalingRequest.create(SolrRequest.METHOD.GET, "/suggestions", null);
    response = solrClient.request(req);
    List l = (List) response.get("suggestions");
    assertNotNull(l);
    assertEquals(2, l.size());
    for (int i = 0; i < l.size(); i++) {
      Object suggestion = l.get(i);
      assertEquals("violation", getObjectByPath(suggestion, true, "type"));
      assertEquals("POST", getObjectByPath(suggestion, true, "operation/method"));
      assertEquals("/c/readApiTestViolations", getObjectByPath(suggestion, true, "operation/path"));
      String node = (String) getObjectByPath(suggestion, true, "operation/command/move-replica/targetNode");
      assertNotNull(node);
      assertEquals(runner1.getNodeName(), node);
    }
    CollectionAdminRequest.deleteCollection("readApiTestViolations")
        .process(cluster.getSolrClient());
  }

  @Test
  public void testConcurrentUpdates() throws Exception {
    int COUNT = 50;
    CloudSolrClient solrClient = cluster.getSolrClient();
    CountDownLatch updateLatch = new CountDownLatch(COUNT * 2);
    Runnable r = () -> {
      for (int i = 0; i < COUNT; i++) {
        String setTriggerCommand = "{" +
            "'set-trigger' : {" +
            "'name' : 'node_added_trigger1'," +
            "'event' : 'nodeAdded'," +
            "'waitFor' : '0s'," +
            "'enabled' : true" +
            "}}";
        @SuppressWarnings({"rawtypes"})
        SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
        NamedList<Object> response = null;
        try {
          response = solrClient.request(req);
          assertEquals(response.get("result").toString(), "success");
        } catch (Exception e) {
          fail(e.toString());
        } finally {
          updateLatch.countDown();
        }
      }
    };
    Thread t1 = new Thread(r);
    Thread t2 = new Thread(r);
    t1.start();
    t2.start();
    boolean await = updateLatch.await(60, TimeUnit.SECONDS);
    assertTrue("not all updates executed in time, remaining=" + updateLatch.getCount(), await);
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    NamedList<Object> response = solrClient.request(req);

    @SuppressWarnings({"rawtypes"})
    Map triggers = (Map) response.get("triggers");
    assertNotNull(triggers);
    assertEquals(1, countNotImplicitTriggers(triggers));
    assertTrue(triggers.containsKey("node_added_trigger1"));
    @SuppressWarnings({"rawtypes"})
    Map node_added_trigger1 = (Map) triggers.get("node_added_trigger1");
    assertEquals(4, node_added_trigger1.size());
    assertEquals(0L, node_added_trigger1.get("waitFor"));
    assertEquals(true, node_added_trigger1.get("enabled"));
    assertEquals(2, ((List)node_added_trigger1.get("actions")).size());

  }

  private int countNotImplicitTriggers(@SuppressWarnings({"rawtypes"})Map triggers) {
    if (triggers == null) return 0;
    int count = 0;
    for (Object trigger : triggers.keySet()) {
      if (!trigger.toString().startsWith(".")) count++;
    }
    return count;
  }

  @Test
  public void testDeleteUsedPolicy() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    // add multiple policies
    String setPolicyCommand = "{'set-policy': {" +
        "    'nodelete':[" +
        "      {'nodeRole':'overseer', 'replica':0}]}}";
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, setPolicyCommand));
    CollectionAdminRequest.createCollection("COLL1", "conf", 1, 1)
        .setPolicy("nodelete")
        .process(cluster.getSolrClient());
    String removePolicyCommand = "{remove-policy : nodelete}";
    AutoScalingRequest.create(SolrRequest.METHOD.POST, removePolicyCommand);
    try {
      solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, removePolicyCommand));
      fail("should have failed");
    } catch (HttpSolrClient.RemoteExecutionException e) {
      assertTrue(String.valueOf(getObjectByPath(e.getMetaData(), true, "error/details[0]/errorMessages[0]"))
          .contains("is being used by collection"));
    } catch (Exception e) {
      fail("Only RemoteExecutionException expected");
    }
    solrClient.request(CollectionAdminRequest.deleteCollection("COLL1"));
  }

  @Test
  public void testSetProperties() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setPropertiesCommand = "{\n" +
        "\t\"set-properties\" : {\n" +
        "\t\t\"pqr\" : \"abc\"\n" +
        "\t}\n" +
        "}";
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, setPropertiesCommand));
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    NamedList<Object> response = solrClient.request(req);
    @SuppressWarnings({"rawtypes"})
    Map properties = (Map) response.get("properties");
    assertNotNull(properties);
    assertEquals(1, properties.size());
    assertEquals("abc", properties.get("pqr"));

    setPropertiesCommand = "{\n" +
        "\t\"set-properties\" : {\n" +
        "\t\t\"xyz\" : 123\n" +
        "\t}\n" +
        "}";
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, setPropertiesCommand));
    req = AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    response = solrClient.request(req);
    properties = (Map) response.get("properties");
    assertNotNull(properties);
    assertEquals(2, properties.size());
    assertEquals("abc", properties.get("pqr"));
    assertEquals(123L, properties.get("xyz"));

    setPropertiesCommand = "{\n" +
        "\t\"set-properties\" : {\n" +
        "\t\t\"xyz\" : 456\n" +
        "\t}\n" +
        "}";
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, setPropertiesCommand));
    req = AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    response = solrClient.request(req);
    properties = (Map) response.get("properties");
    assertNotNull(properties);
    assertEquals(2, properties.size());
    assertEquals("abc", properties.get("pqr"));
    assertEquals(456L, properties.get("xyz"));

    setPropertiesCommand = "{\n" +
        "\t\"set-properties\" : {\n" +
        "\t\t\"xyz\" : null\n" +
        "\t}\n" +
        "}";
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, setPropertiesCommand));
    req = AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    response = solrClient.request(req);
    properties = (Map) response.get("properties");
    assertNotNull(properties);
    assertEquals(1, properties.size());
    assertEquals("abc", properties.get("pqr"));

    setPropertiesCommand = "{\n" +
        "\t\"set-properties\" : {\n" +
        "\t\t\"" + AutoScalingParams.TRIGGER_SCHEDULE_DELAY_SECONDS + "\" : 5\n" +
        "\t\t\"" + AutoScalingParams.TRIGGER_COOLDOWN_PERIOD_SECONDS + "\" : 10\n" +
        "\t\t\"" + AutoScalingParams.TRIGGER_CORE_POOL_SIZE + "\" : 10\n" +
        "\t\t\"" + AutoScalingParams.ACTION_THROTTLE_PERIOD_SECONDS + "\" : 5\n" +
        "\t}\n" +
        "}";
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, setPropertiesCommand));
    req = AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    response = solrClient.request(req);
    properties = (Map) response.get("properties");
    assertNotNull(properties);
    assertEquals(5, properties.size());
    assertEquals("abc", properties.get("pqr"));
    assertEquals(5L, properties.get(AutoScalingParams.TRIGGER_SCHEDULE_DELAY_SECONDS));
    assertEquals(10L, properties.get(AutoScalingParams.TRIGGER_COOLDOWN_PERIOD_SECONDS));
    assertEquals(10L, properties.get(AutoScalingParams.TRIGGER_CORE_POOL_SIZE));
    assertEquals(5L, properties.get(AutoScalingParams.ACTION_THROTTLE_PERIOD_SECONDS));
  }

  public void testUpdatePolicy() throws IOException, SolrServerException {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setPropertiesCommand = "{'set-cluster-policy': [" +
        "{'cores': '<4','node': '#ANY'}]}";
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, setPropertiesCommand));
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    NamedList<Object> response = solrClient.request(req);
    assertEquals("<4", response._get("cluster-policy[0]/cores", null));
    assertEquals("#ANY", response._get("cluster-policy[0]/node", null));
    setPropertiesCommand = "{'set-cluster-policy': [" +
        "{'cores': '<3','node': '#ANY'}]}";
    solrClient.request(AutoScalingRequest.create(SolrRequest.METHOD.POST, setPropertiesCommand));
    req = AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    response = solrClient.request(req);
    assertEquals("<3", response._get("cluster-policy[0]/cores", null));
    assertEquals("#ANY", response._get("cluster-policy[0]/node", null));

  }
}
