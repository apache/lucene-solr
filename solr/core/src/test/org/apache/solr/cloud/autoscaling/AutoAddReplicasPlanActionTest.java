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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.cloud.autoscaling.AutoScalingHandlerTest.createAutoScalingRequest;

public class AutoAddReplicasPlanActionTest extends SolrCloudTestCase{

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Test
  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028")
  public void testSimple() throws Exception {
    JettySolrRunner jetty1 = cluster.getJettySolrRunner(0);
    JettySolrRunner jetty2 = cluster.getJettySolrRunner(1);
    JettySolrRunner jetty3 = cluster.getJettySolrRunner(2);

    String collection1 = "testSimple1";
    String collection2 = "testSimple2";
    CollectionAdminRequest.createCollection(collection1, "conf", 2, 2)
        .setCreateNodeSet(jetty1.getNodeName()+","+jetty2.getNodeName())
        .setAutoAddReplicas(true)
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection(collection2, "conf", 1, 2)
        .setCreateNodeSet(jetty2.getNodeName()+","+jetty3.getNodeName())
        .setAutoAddReplicas(false)
        .setMaxShardsPerNode(1)
        .process(cluster.getSolrClient());
    // the number of cores in jetty1 (5) will be larger than jetty3 (1)
    CollectionAdminRequest.createCollection("testSimple3", "conf", 3, 1)
        .setCreateNodeSet(jetty1.getNodeName())
        .setAutoAddReplicas(false)
        .setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());

    // we remove the implicit created trigger, so the replicas won't be moved
    String removeTriggerCommand = "{" +
        "'remove-trigger' : {" +
        "'name' : '.auto_add_replicas'," +
        "'removeListeners': true" +
        "}" +
        "}";
    SolrRequest req = createAutoScalingRequest(SolrRequest.METHOD.POST, removeTriggerCommand);
    NamedList response = cluster.getSolrClient().request(req);
    assertEquals(response.get("result").toString(), "success");

    JettySolrRunner lostJetty = random().nextBoolean()? jetty1 : jetty2;
    String lostNodeName = lostJetty.getNodeName();
    List<CloudDescriptor> cloudDescriptors = lostJetty.getCoreContainer().getCores().stream()
        .map(solrCore -> solrCore.getCoreDescriptor().getCloudDescriptor())
        .collect(Collectors.toList());
    lostJetty.stop();
    waitForNodeLeave(lostNodeName);

    List<SolrRequest> operations = getOperations(jetty3, lostNodeName);
    assertOperations(collection1, operations, lostNodeName, cloudDescriptors,  null);

    lostJetty.start();
    ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), 30000);
    String setClusterPreferencesCommand = "{" +
        "'set-cluster-preferences': [" +
        "{'minimize': 'cores','precision': 0}]" +
        "}";
    req = createAutoScalingRequest(SolrRequest.METHOD.POST, setClusterPreferencesCommand);
    response = cluster.getSolrClient().request(req);
    assertEquals(response.get("result").toString(), "success");

    lostJetty = random().nextBoolean()? jetty1 : jetty2;
    lostNodeName = lostJetty.getNodeName();
    cloudDescriptors = lostJetty.getCoreContainer().getCores().stream()
        .map(solrCore -> solrCore.getCoreDescriptor().getCloudDescriptor())
        .collect(Collectors.toList());
    lostJetty.stop();
    waitForNodeLeave(lostNodeName);

    operations = getOperations(jetty3, lostNodeName);
    assertOperations(collection1, operations, lostNodeName, cloudDescriptors, jetty3);

    lostJetty.start();
    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), 30000));

    new CollectionAdminRequest.AsyncCollectionAdminRequest(CollectionParams.CollectionAction.MODIFYCOLLECTION) {
      @Override
      public SolrParams getParams() {
        ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
        params.set("collection", collection1);
        params.set("autoAddReplicas", false);
        return params;
      }
    }.process(cluster.getSolrClient());
    lostJetty = jetty1;
    lostNodeName = lostJetty.getNodeName();
    lostJetty.stop();
    waitForNodeLeave(lostNodeName);
    operations = getOperations(jetty3, lostNodeName);
    assertNull(operations);
  }

  private void waitForNodeLeave(String lostNodeName) throws InterruptedException {
    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    TimeOut timeOut = new TimeOut(10, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (reader.getClusterState().getLiveNodes().contains(lostNodeName)) {
      Thread.sleep(100);
      if (timeOut.hasTimedOut()) fail("Wait for " + lostNodeName + " to leave failed!");
    }
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to create unique id")
  private List<SolrRequest> getOperations(JettySolrRunner actionJetty, String lostNodeName) throws Exception {
    try (AutoAddReplicasPlanAction action = new AutoAddReplicasPlanAction()) {
      TriggerEvent lostNode = new NodeLostTrigger.NodeLostEvent(TriggerEventType.NODELOST, ".auto_add_replicas", Collections.singletonList(System.currentTimeMillis()), Collections.singletonList(lostNodeName), CollectionParams.CollectionAction.MOVEREPLICA.toLower());
      ActionContext context = new ActionContext(actionJetty.getCoreContainer().getZkController().getSolrCloudManager(), null, new HashMap<>());
      action.process(lostNode, context);
      List<SolrRequest> operations = (List) context.getProperty("operations");
      return operations;
    }
  }

  private void assertOperations(String collection, List<SolrRequest> operations, String lostNodeName,
                                List<CloudDescriptor> cloudDescriptors, JettySolrRunner destJetty) {
    assertEquals("Replicas of " + collection + " is not fully moved, operations="+operations,
        cloudDescriptors.stream().filter(cd -> cd.getCollectionName().equals(collection)).count(), operations.size());
    for (SolrRequest solrRequest : operations) {
      assertTrue(solrRequest instanceof CollectionAdminRequest.MoveReplica);
      SolrParams params = solrRequest.getParams();

      assertEquals(params.get("collection"), collection);

      String replica = params.get("replica");
      boolean found = false;
      Iterator<CloudDescriptor> it = cloudDescriptors.iterator();
      while (it.hasNext()) {
        CloudDescriptor cd = it.next();
        if (cd.getCollectionName().equals(collection) && cd.getCoreNodeName().equals(replica)) {
          found = true;
          it.remove();
          break;
        }
      }
      assertTrue("Can not find "+replica+ " in node " + lostNodeName, found);

      String targetNode = params.get("targetNode");
      assertFalse("Target node match the lost one " + lostNodeName, lostNodeName.equals(targetNode));
      if (destJetty != null) {
        assertEquals("Target node is not as expectation", destJetty.getNodeName(), targetNode);
      }
    }

    for (CloudDescriptor cd : cloudDescriptors) {
      if (cd.getCollectionName().equals(collection)) {
        fail("Exist replica which is not moved " + cd);
      }
    }
  }
}
