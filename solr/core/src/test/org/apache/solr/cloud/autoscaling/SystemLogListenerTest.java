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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

/**
 * Test for {@link SystemLogListener}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class SystemLogListenerTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final AtomicBoolean fired = new AtomicBoolean(false);
  private static final int NODE_COUNT = 3;
  private static CountDownLatch triggerFiredLatch = new CountDownLatch(1);
  private static final AtomicReference<Map> actionContextPropsRef = new AtomicReference<>();
  private static final AtomicReference<TriggerEvent> eventRef = new AtomicReference<>();

  public static class AssertingTriggerAction extends TriggerActionBase {
    @Override
    public void process(TriggerEvent event, ActionContext context) {
      if (fired.compareAndSet(false, true)) {
        eventRef.set(event);
        actionContextPropsRef.set(context.getProperties());
        triggerFiredLatch.countDown();
      }
    }
  }

  public static class ErrorTriggerAction extends TriggerActionBase {
    @Override
    public void process(TriggerEvent event, ActionContext context) {
      throw new RuntimeException("failure from ErrorTriggerAction");
    }
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 3)
        .process(cluster.getSolrClient());
  }

  @Test
  public void test() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'execute_plan','class':'solr.ExecutePlanAction'}," +
        "{'name':'test','class':'" + AssertingTriggerAction.class.getName() + "'}," +
        "{'name':'error','class':'" + ErrorTriggerAction.class.getName() + "'}]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // remove default listener
    String removeListenerCommand = "{\n" +
        "\t\"remove-listener\" : {\n" +
        "\t\t\"name\" : \"node_lost_trigger.system\"\n" +
        "\t}\n" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, removeListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection("test",
        "conf",3, 2);
    create.setMaxShardsPerNode(3);
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        "test", clusterShape(3, 2));

    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'foo'," +
        "'trigger' : 'node_lost_trigger'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED', 'FAILED']," +
        "'beforeAction' : ['compute_plan','execute_plan','test','error']," +
        "'afterAction' : ['compute_plan','execute_plan','test','error']," +
        "'class' : '" + SystemLogListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // stop non-overseer node
    NamedList<Object> overSeerStatus = cluster.getSolrClient().request(CollectionAdminRequest.getOverseerStatus());
    String overseerLeader = (String) overSeerStatus.get("leader");
    int nonOverseerLeaderIndex = 0;
    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner jetty = cluster.getJettySolrRunner(i);
      if (!jetty.getNodeName().equals(overseerLeader)) {
        nonOverseerLeaderIndex = i;
      }
    }
    log.info("Stopping node " + cluster.getJettySolrRunner(nonOverseerLeaderIndex).getNodeName());
    cluster.stopJettySolrRunner(nonOverseerLeaderIndex);
    cluster.waitForAllNodes(30);
    assertTrue("Trigger was not fired ", triggerFiredLatch.await(30, TimeUnit.SECONDS));
    assertTrue(fired.get());
    Map context = actionContextPropsRef.get();
    assertNotNull(context);

    // make sure the event docs are replicated and committed
    Thread.sleep(5000);
    cluster.getSolrClient().commit(CollectionAdminParams.SYSTEM_COLL, true, true);

    ModifiableSolrParams query = new ModifiableSolrParams();
    query.add(CommonParams.Q, "type:" + SystemLogListener.DOC_TYPE);
    query.add(CommonParams.SORT, "id asc");
    QueryResponse resp = cluster.getSolrClient().query(CollectionAdminParams.SYSTEM_COLL, query);
    SolrDocumentList docs = resp.getResults();
    assertNotNull(docs);
    assertEquals("wrong number of events added to .system", 9, docs.size());
    docs.forEach(doc -> assertCommonFields(doc));

    // STARTED
    SolrDocument doc = docs.get(0);
    assertEquals("STARTED", doc.getFieldValue("stage_s"));

    // BEFORE_ACTION compute_plan
    doc = docs.get(1);
    assertEquals("BEFORE_ACTION", doc.getFieldValue("stage_s"));
    assertEquals("compute_plan", doc.getFieldValue("action_s"));

    // AFTER_ACTION compute_plan
    doc = docs.get(2);
    assertEquals("AFTER_ACTION", doc.getFieldValue("stage_s"));
    assertEquals("compute_plan", doc.getFieldValue("action_s"));
    Collection<Object> vals = doc.getFieldValues("operations.params_ts");
    assertEquals(3, vals.size());
    for (Object val : vals) {
      assertTrue(val.toString(), String.valueOf(val).contains("action=MOVEREPLICA"));
    }

    // BEFORE_ACTION execute_plan
    doc = docs.get(3);
    assertEquals("BEFORE_ACTION", doc.getFieldValue("stage_s"));
    assertEquals("execute_plan", doc.getFieldValue("action_s"));
    vals = doc.getFieldValues("operations.params_ts");
    assertEquals(3, vals.size());

    // AFTER_ACTION execute_plan
    doc = docs.get(4);
    assertEquals("AFTER_ACTION", doc.getFieldValue("stage_s"));
    assertEquals("execute_plan", doc.getFieldValue("action_s"));
    vals = doc.getFieldValues("operations.params_ts");
    assertNotNull(vals);
    assertEquals(3, vals.size());
    vals = doc.getFieldValues("responses_ts");
    assertNotNull(vals);
    assertEquals(3, vals.size());
    vals.forEach(s -> assertTrue(s.toString(), s.toString().startsWith("success MOVEREPLICA action completed successfully")));

    // BEFORE_ACTION test
    doc = docs.get(5);
    assertEquals("BEFORE_ACTION", doc.getFieldValue("stage_s"));
    assertEquals("test", doc.getFieldValue("action_s"));

    // AFTER_ACTION test
    doc = docs.get(6);
    assertEquals("AFTER_ACTION", doc.getFieldValue("stage_s"));
    assertEquals("test", doc.getFieldValue("action_s"));

    // BEFORE_ACTION error
    doc = docs.get(7);
    assertEquals("BEFORE_ACTION", doc.getFieldValue("stage_s"));
    assertEquals("error", doc.getFieldValue("action_s"));

    // FAILED error
    doc = docs.get(8);
    assertEquals("FAILED", doc.getFieldValue("stage_s"));
    assertEquals("error", doc.getFieldValue("action_s"));
    assertEquals("failure from ErrorTriggerAction", doc.getFieldValue("error.message_t"));
    assertTrue(doc.getFieldValue("error.details_t").toString().contains("RuntimeException"));
  }

  private void assertCommonFields(SolrDocument doc) {
    assertEquals(SystemLogListener.class.getSimpleName(), doc.getFieldValue(SystemLogListener.SOURCE_FIELD));
    assertEquals(SystemLogListener.DOC_TYPE, doc.getFieldValue(CommonParams.TYPE));
    assertEquals("node_lost_trigger", doc.getFieldValue("event.source_s"));
    assertNotNull(doc.getFieldValue("event.time_l"));
    assertNotNull(doc.getFieldValue("timestamp"));
    assertNotNull(doc.getFieldValue("event.property.nodeNames_ss"));
    assertNotNull(doc.getFieldValue("event_str"));
    assertEquals("NODELOST", doc.getFieldValue("event.type_s"));
  }
}
