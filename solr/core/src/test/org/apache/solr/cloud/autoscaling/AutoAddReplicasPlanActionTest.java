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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList; 
import org.apache.solr.common.util.SuppressForbidden;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AutoAddReplicasPlanActionTest extends SolrCloudTestCase{
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    System.setProperty("solr.httpclient.retries", "4");
    System.setProperty("solr.retries.on.forward", "1");
    System.setProperty("solr.retries.to.followers", "1"); 

  }
  
  @Before
  public void beforeTest() throws Exception {
    configureCluster(3)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();

    new V2Request.Builder("/cluster")
        .withMethod(SolrRequest.METHOD.POST)
        .withPayload("{set-obj-property:{defaults : {cluster: {useLegacyReplicaAssignment:true}}}}")
        .build()
        .process(cluster.getSolrClient());
  }
  
  @After 
  public void afterTest() throws Exception {
    shutdownCluster();
  }

  @Test
  //Commented out 11-Dec-2018 @AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/SOLR-13028")
  public void testSimple() throws Exception {
    JettySolrRunner jetty1 = cluster.getJettySolrRunner(0);
    JettySolrRunner jetty2 = cluster.getJettySolrRunner(1);
    JettySolrRunner jetty3 = cluster.getJettySolrRunner(2);

    String collection1 = "testSimple1";
    String collection2 = "testSimple2";
    String collection3 = "testSimple3";
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
    // the number of cores in jetty1 (6) will be larger than jetty3 (1)
    CollectionAdminRequest.createCollection(collection3, "conf", 3, 1)
        .setCreateNodeSet(jetty1.getNodeName())
        .setAutoAddReplicas(false)
        .setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(collection1, 2, 4);
    cluster.waitForActiveCollection(collection2, 1, 2);
    cluster.waitForActiveCollection(collection3, 3, 3);
    
    // we remove the implicit created trigger, so the replicas won't be moved
    String removeTriggerCommand = "{" +
        "'remove-trigger' : {" +
        "'name' : '.auto_add_replicas'," +
        "'removeListeners': true" +
        "}" +
        "}";
    @SuppressWarnings({"rawtypes"})
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, removeTriggerCommand);
    @SuppressWarnings({"rawtypes"})
    NamedList response = cluster.getSolrClient().request(req);
    assertEquals(response.get("result").toString(), "success");

    JettySolrRunner lostJetty = random().nextBoolean()? jetty1 : jetty2;
    String lostNodeName = lostJetty.getNodeName();
    List<CloudDescriptor> cloudDescriptors = lostJetty.getCoreContainer().getCores().stream()
        .map(solrCore -> solrCore.getCoreDescriptor().getCloudDescriptor())
        .collect(Collectors.toList());
    
    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();

    lostJetty.stop();
    
    cluster.waitForJettyToStop(lostJetty);

    reader.waitForLiveNodes(30, TimeUnit.SECONDS, missingLiveNode(lostNodeName));


    @SuppressWarnings({"rawtypes"})
    List<SolrRequest> operations = getOperations(jetty3, lostNodeName);
    assertOperations(collection1, operations, lostNodeName, cloudDescriptors,  null);

    lostJetty.start();
    cluster.waitForAllNodes(30);
    
    cluster.waitForActiveCollection(collection1, 2, 4);
    cluster.waitForActiveCollection(collection2, 1, 2);
    cluster.waitForActiveCollection(collection3, 3, 3);
    
    assertTrue("Timeout waiting for all live and active", ClusterStateUtil.waitForAllActiveAndLiveReplicas(cluster.getSolrClient().getZkStateReader(), 30000));
    
    String setClusterPreferencesCommand = "{" +
        "'set-cluster-preferences': [" +
        "{'minimize': 'cores','precision': 0}]" +
        "}";
    req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPreferencesCommand);
    
    // you can hit a stale connection from pool when restarting jetty
    try (CloudSolrClient cloudClient = new CloudSolrClient.Builder(Collections.singletonList(cluster.getZkServer().getZkAddress()),
        Optional.empty())
            .withSocketTimeout(45000).withConnectionTimeout(15000).build()) {
      response = cloudClient.request(req);
    }

    assertEquals(response.get("result").toString(), "success");

    lostJetty = random().nextBoolean()? jetty1 : jetty2;
    String lostNodeName2 = lostJetty.getNodeName();
    cloudDescriptors = lostJetty.getCoreContainer().getCores().stream()
        .map(solrCore -> solrCore.getCoreDescriptor().getCloudDescriptor())
        .collect(Collectors.toList());
    

    
    lostJetty.stop();
   
    reader.waitForLiveNodes(30, TimeUnit.SECONDS, missingLiveNode(lostNodeName2));

    try {
      operations = getOperations(jetty3, lostNodeName2);
    } catch (SolrException e) {
      // we might get a stale connection from the pool after jetty restarts
      operations = getOperations(jetty3, lostNodeName2);
    }
    
    assertOperations(collection1, operations, lostNodeName2, cloudDescriptors, jetty3);

    lostJetty.start();
    cluster.waitForAllNodes(30);
    
    cluster.waitForActiveCollection(collection1, 2, 4);
    cluster.waitForActiveCollection(collection2, 1, 2);
    cluster.waitForActiveCollection(collection3, 3, 3);
    
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
    String lostNodeName3 = lostJetty.getNodeName();
    
    lostJetty.stop();
    
    reader.waitForLiveNodes(30, TimeUnit.SECONDS, missingLiveNode(lostNodeName3));
    
    operations = getOperations(jetty3, lostNodeName3);
    assertNull(operations);
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to create unique id")
  @SuppressWarnings({"rawtypes"})
  private List<SolrRequest> getOperations(JettySolrRunner actionJetty, String lostNodeName) throws Exception {
    try (AutoAddReplicasPlanAction action = new AutoAddReplicasPlanAction()) {
      action.configure(actionJetty.getCoreContainer().getResourceLoader(), actionJetty.getCoreContainer().getZkController().getSolrCloudManager(), new HashMap<>());
      TriggerEvent lostNode = new NodeLostTrigger.NodeLostEvent(TriggerEventType.NODELOST, ".auto_add_replicas", Collections.singletonList(System.currentTimeMillis()), Collections.singletonList(lostNodeName), CollectionParams.CollectionAction.MOVEREPLICA.toLower());
      ActionContext context = new ActionContext(actionJetty.getCoreContainer().getZkController().getSolrCloudManager(), null, new HashMap<>());
      action.process(lostNode, context);
      @SuppressWarnings({"unchecked", "rawtypes"})
      List<SolrRequest> operations = (List) context.getProperty("operations");
      return operations;
    }
  }

  private void assertOperations(String collection,
                                @SuppressWarnings({"rawtypes"})List<SolrRequest> operations, String lostNodeName,
                                List<CloudDescriptor> cloudDescriptors, JettySolrRunner destJetty) {
    assertEquals("Replicas of " + collection + " is not fully moved, operations="+operations,
        cloudDescriptors.stream().filter(cd -> cd.getCollectionName().equals(collection)).count(), operations.size());
    for (@SuppressWarnings({"rawtypes"})SolrRequest solrRequest : operations) {
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
