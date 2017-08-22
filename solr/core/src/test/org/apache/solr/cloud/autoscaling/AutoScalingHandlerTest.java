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
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
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
    } catch (HttpSolrClient.RemoteExecutionException e)  {
      String message = String.valueOf(Utils.getObjectByPath(e.getMetaData(), true, "error/details[0]/errorMessages[0]"));

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
      SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPolicyCommand);
      solrClient.request(req);
      fail("expect exception");
    } catch (HttpSolrClient.RemoteExecutionException e) {
      String message = String.valueOf(Utils.getObjectByPath(e.getMetaData(), true, "error/details[0]/errorMessages[0]"));
      assertTrue(message.contains("replica is required in"));
    }
  }

  @Test
  public void testReadApi() throws Exception  {
    CloudSolrClient solrClient = cluster.getSolrClient();

    String setClusterPolicyCommand = "{" +
        " 'set-cluster-policy': [" +
        "      {'cores':'<10', 'node':'#ANY'}," +
        "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
        "      {'nodeRole':'overseer', 'replica':0}" +
        "    ]" +
        "}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    NamedList<Object> response = solrClient.request(req);
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
        "    ]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    req = createAutoScalingRequest(SolrRequest.METHOD.GET, null);
    response = solrClient.request(req);

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

    String setEmptyClusterPolicyCommand = "{" +
        " 'set-cluster-policy': []" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setEmptyClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    req = createAutoScalingRequest(SolrRequest.METHOD.POST, "{set-cluster-policy : []}");
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // lets create a collection which violates the rule replicas < 2
    try {
      CollectionAdminRequest.Create create = CollectionAdminRequest.Create.createCollection("readApiTestViolations", CONFIGSET_NAME, 1, 6);
      create.setMaxShardsPerNode(10);
      create.process(solrClient);
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("'maxShardsPerNode>0' is not supported when autoScaling policies are used"));

    }


    // lets create a collection which violates the rule replicas < 2
    CollectionAdminRequest.Create create = CollectionAdminRequest.Create.createCollection("readApiTestViolations", CONFIGSET_NAME, 1, 6);
    CollectionAdminResponse adminResponse = create.process(solrClient);
    assertTrue(adminResponse.isSuccess());

    // reset the original cluster policy
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPolicyCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

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
      assertEquals(Utils.makeMap("replica", "3", "delta", -1), violation.get("violation"));
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
