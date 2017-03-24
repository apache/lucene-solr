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
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
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

  @Test
  public void test() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    // todo nocommit -- add testing for the v2 path
    // String path = random().nextBoolean() ? "/admin/autoscaling" : "/v2/cluster/autoscaling";
    String path = "/admin/autoscaling";
    String addTriggerCommand = "{\n" +
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
            "\t\t\t\t\"name\" : \"execute_plan\",\n" +
            "\t\t\t\t\"class\" : \"solr.ExecutePlanAction\"\n" +
            "\t\t\t},\n" +
            "\t\t\t{\n" +
            "\t\t\t\t\"name\" : \"log_plan\",\n" +
            "\t\t\t\t\"class\" : \"solr.LogPlanAction\",\n" +
            "\t\t\t\t\"collection\" : \".system\"\n" +
            "\t\t\t}\n" +
            "\t\t]\n" +
        "\t}\n" +
        "}";
    SolrRequest req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, addTriggerCommand);

    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    byte[] data = zkClient().getData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    ZkNodeProps loaded = ZkNodeProps.load(data);
    Map<String, Object> triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(1, triggers.size());
    assertTrue(triggers.containsKey("node_lost_trigger"));
    Map<String, Object> nodeLostTrigger = (Map<String, Object>) triggers.get("node_lost_trigger");
    assertEquals(4, nodeLostTrigger.size());
    assertEquals("600", nodeLostTrigger.get("waitFor").toString());

    String removeTriggerCommand = "{\n" +
        "\t\"remove-trigger\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger\"\n" +
        "\t}\n" +
        "}";
    req = new AutoScalingRequest(SolrRequest.METHOD.POST, path, removeTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    data = zkClient().getData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, null, null, true);
    loaded = ZkNodeProps.load(data);
    triggers = (Map<String, Object>) loaded.get("triggers");
    assertNotNull(triggers);
    assertEquals(0, triggers.size());
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
