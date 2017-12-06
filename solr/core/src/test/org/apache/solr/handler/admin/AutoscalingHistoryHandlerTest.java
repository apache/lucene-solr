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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.SystemLogListener;
import org.apache.solr.cloud.autoscaling.TriggerActionBase;
import org.apache.solr.cloud.autoscaling.TriggerEvent;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.LogLevel;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG;org.apache.solr.cloud.Overseer=DEBUG;org.apache.solr.cloud.overseer=DEBUG;org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper=DEBUG")
public class AutoscalingHistoryHandlerTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static CountDownLatch actionFiredLatch;
  private static CloudSolrClient solrClient;
  private static String PREFIX = AutoscalingHistoryHandlerTest.class.getSimpleName();

  private static CountDownLatch getActionFiredLatch() {
    return actionFiredLatch;
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
    solrClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(PREFIX + "_collection", null, 1, 3)
        .process(solrClient);
    CollectionAdminRequest.createCollection(CollectionAdminParams.SYSTEM_COLL, null, 1, 3)
        .process(solrClient);
  }

  public static class TesterAction extends TriggerActionBase {

    @Override
    public void process(TriggerEvent event, ActionContext context) {
      getActionFiredLatch().countDown();
    }
  }

  @Before
  public void setupTest() throws Exception {
    actionFiredLatch = new CountDownLatch(1);

    // first trigger
    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : '" + PREFIX + "_node_added_trigger'," +
        "'event' : 'nodeAdded'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'compute_plan','class':'solr.ComputePlanAction'}," +
        "{'name':'execute_plan','class':'solr.ExecutePlanAction'}," +
        "{'name':'test','class':'" + TesterAction.class.getName() + "'}" +
        "]" +
        "}}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // second trigger
    setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : '" + PREFIX + "_node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '0s'," +
        "'enabled' : true," +
        "'actions' : [" +
        "{'name':'compute_plan','class':'solr.ComputePlanAction'}," +
        "{'name':'execute_plan','class':'solr.ExecutePlanAction'}," +
        "{'name':'test','class':'" + TesterAction.class.getName() + "'}" +
        "]" +
        "}}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    // remove default listeners
    String removeListenerCommand = "{\n" +
        "\t\"remove-listener\" : {\n" +
        "\t\t\"name\" : \"" + PREFIX + "_node_lost_trigger.system\"\n" +
        "\t}\n" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, removeListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    removeListenerCommand = "{\n" +
        "\t\"remove-listener\" : {\n" +
        "\t\t\"name\" : \"" + PREFIX + "_node_added_trigger.system\"\n" +
        "\t}\n" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, removeListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    // set up our own listeners
    String setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'node_added'," +
        "'trigger' : '" + PREFIX + "_node_added_trigger'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED', 'FAILED']," +
        "'beforeAction' : ['compute_plan','execute_plan','test']," +
        "'afterAction' : ['compute_plan','execute_plan','test']," +
        "'class' : '" + SystemLogListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");
    setListenerCommand = "{" +
        "'set-listener' : " +
        "{" +
        "'name' : 'node_lost'," +
        "'trigger' : '" + PREFIX + "_node_lost_trigger'," +
        "'stage' : ['STARTED','ABORTED','SUCCEEDED', 'FAILED']," +
        "'beforeAction' : ['compute_plan','execute_plan','test']," +
        "'afterAction' : ['compute_plan','execute_plan','test']," +
        "'class' : '" + SystemLogListener.class.getName() + "'" +
        "}" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setListenerCommand);
    response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

  }

  private void resetLatch() {
    actionFiredLatch = new CountDownLatch(1);
  }

  @Test
  public void testHistory() throws Exception {
    waitForState("Timed out wait for collection be active", PREFIX + "_collection",
        clusterShape(1, 3));
    waitForState("Timed out wait for collection be active", CollectionAdminParams.SYSTEM_COLL,
        clusterShape(1, 3));

    log.info("### Start add node...");
    JettySolrRunner jetty = cluster.startJettySolrRunner();
    String nodeAddedName = jetty.getNodeName();
    log.info("### Added node " + nodeAddedName);
    boolean await = actionFiredLatch.await(60, TimeUnit.SECONDS);
    assertTrue("action did not execute", await);
    // commit on the history collection
    Thread.sleep(2000);
    log.info("### Commit .system");
    solrClient.commit(CollectionAdminParams.SYSTEM_COLL);
    Thread.sleep(2000);
    // verify that new docs exist
    ModifiableSolrParams query = params(CommonParams.Q, "type:" + SystemLogListener.DOC_TYPE,
      CommonParams.FQ, "event.source_s:" + PREFIX + "_node_added_trigger");
    QueryResponse resp = solrClient.query(CollectionAdminParams.SYSTEM_COLL, query);
    SolrDocumentList docs = resp.getResults();
    assertNotNull(docs);

    query = params(CommonParams.QT, CommonParams.AUTOSCALING_HISTORY_PATH,
      AutoscalingHistoryHandler.TRIGGER_PARAM, PREFIX + "_node_added_trigger");
    QueryResponse rsp = solrClient.query(query);
    docs = rsp.getResults();
    if (docs.size() != 8) {
      log.info("Cluster state: " + solrClient.getZkStateReader().getClusterState());
      query = params(CommonParams.QT, CommonParams.AUTOSCALING_HISTORY_PATH);
      log.info("Wrong response: ", rsp);
      log.info("Full response: " + solrClient.query(query));
    }
    assertEquals(8, docs.size());

    query = params(CommonParams.QT, CommonParams.AUTOSCALING_HISTORY_PATH,
        AutoscalingHistoryHandler.STAGE_PARAM, "STARTED");
    docs = solrClient.query(query).getResults();
    assertEquals(1, docs.size());
    assertEquals("NODEADDED", docs.get(0).getFieldValue("event.type_s"));

    query = params(CommonParams.QT, CommonParams.AUTOSCALING_HISTORY_PATH,
        AutoscalingHistoryHandler.NODE_PARAM, nodeAddedName);
    docs = solrClient.query(query).getResults();
    assertEquals(8, docs.size());
    for (SolrDocument doc : docs) {
      assertTrue(doc.getFieldValues("event.property.nodeNames_ss").contains(nodeAddedName));
    }

    query = params(CommonParams.QT, CommonParams.AUTOSCALING_HISTORY_PATH,
        AutoscalingHistoryHandler.ACTION_PARAM, "test");
    docs = solrClient.query(query).getResults();
    assertEquals(2, docs.size());
    assertEquals("BEFORE_ACTION", docs.get(0).getFieldValue("stage_s"));
    assertEquals("AFTER_ACTION", docs.get(1).getFieldValue("stage_s"));

    query = params(CommonParams.QT, CommonParams.AUTOSCALING_HISTORY_PATH,
        AutoscalingHistoryHandler.ACTION_PARAM, "test");
    docs = solrClient.query(query).getResults();
    assertEquals(2, docs.size());
    assertEquals("BEFORE_ACTION", docs.get(0).getFieldValue("stage_s"));
    assertEquals("AFTER_ACTION", docs.get(1).getFieldValue("stage_s"));

    query = params(CommonParams.QT, CommonParams.AUTOSCALING_HISTORY_PATH,
        AutoscalingHistoryHandler.COLLECTION_PARAM, CollectionAdminParams.SYSTEM_COLL);
    docs = solrClient.query(query).getResults();
    assertEquals(5, docs.size());
    assertEquals("AFTER_ACTION", docs.get(0).getFieldValue("stage_s"));
    assertEquals("compute_plan", docs.get(0).getFieldValue("action_s"));

    // reset latch
    resetLatch();

    // kill a node
    String node0Name = cluster.getJettySolrRunner(0).getNodeName();
    log.info("### Stopping node " + node0Name);
    cluster.stopJettySolrRunner(0);
    log.info("### Stopped node " + node0Name);
    await = actionFiredLatch.await(60, TimeUnit.SECONDS);
    assertTrue("action did not execute", await);

    // wait for recovery
    waitForRecovery(PREFIX + "_collection");
    Thread.sleep(5000);
    // commit on the history collection
    log.info("### Commit .system");
    solrClient.commit(CollectionAdminParams.SYSTEM_COLL);
    query = params(CommonParams.QT, CommonParams.AUTOSCALING_HISTORY_PATH,
        AutoscalingHistoryHandler.TRIGGER_PARAM, PREFIX + "_node_lost_trigger");
    docs = solrClient.query(query).getResults();
    assertEquals(8, docs.size());

    query = params(CommonParams.QT, CommonParams.AUTOSCALING_HISTORY_PATH,
        AutoscalingHistoryHandler.TRIGGER_PARAM, PREFIX + "_node_lost_trigger",
        AutoscalingHistoryHandler.COLLECTION_PARAM, PREFIX + "_collection");
    docs = solrClient.query(query).getResults();
    assertEquals(5, docs.size());
  }

  private void waitForRecovery(String collection) throws Exception {
    log.info("Waiting for recovery of " + collection);
    boolean recovered = false;
    boolean allActive = true;
    boolean hasLeaders = true;
    DocCollection collState = null;
    for (int i = 0; i < 300; i++) {
      ClusterState state = solrClient.getZkStateReader().getClusterState();
      collState = getCollectionState(collection);
      log.debug("###### " + collState);
      Collection<Replica> replicas = collState.getReplicas();
      allActive = true;
      hasLeaders = true;
      if (replicas != null && !replicas.isEmpty()) {
        for (Replica r : replicas) {
          if (state.getLiveNodes().contains(r.getNodeName())) {
            if (!r.isActive(state.getLiveNodes())) {
              log.info("Not active: " + r);
              allActive = false;
            }
          } else {
            log.info("Replica no longer on a live node, ignoring: " + r);
          }
        }
      } else {
        allActive = false;
      }
      for (Slice slice : collState.getSlices()) {
        if (slice.getLeader() == null) {
          hasLeaders = false;
        }
      }
      if (allActive && hasLeaders) {
        recovered = true;
        break;
      } else {
        log.info("--- waiting, allActive=" + allActive + ", hasLeaders=" + hasLeaders);
        Thread.sleep(1000);
      }
    }
    assertTrue("replica never fully recovered: allActive=" + allActive + ", hasLeaders=" + hasLeaders + ", collState=" + collState, recovered);

  }

}
