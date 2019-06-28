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

package org.apache.solr.cloud.autoscaling.sim;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils.AutoScalingRequest;
import org.apache.solr.cloud.CloudUtil;
import org.apache.solr.cloud.autoscaling.ActionContext;
import org.apache.solr.cloud.autoscaling.ExecutePlanAction;
import org.apache.solr.cloud.autoscaling.NodeLostTrigger;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Test for {@link ExecutePlanAction}
 */
@LogLevel("org.apache.solr.cloud=DEBUG")
public class TestSimExecutePlanAction extends SimSolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final TimeSource SIM_TIME_SOURCE = TimeSource.get("simTime:50");
  private static final int NODE_COUNT = 2;

  @Before
  public void setupCluster() throws Exception {
    configureCluster(NODE_COUNT, SIM_TIME_SOURCE);
  }

  @After
  public void printState() throws Exception {
    if (null == cluster) {
      // test didn't init, nothing to do
      return;
    }
                          
    log.info("-------------_ FINAL STATE --------------");
    log.info("* Node values: " + Utils.toJSONString(cluster.getSimNodeStateProvider().simGetAllNodeValues()));
    log.info("* Live nodes: " + cluster.getClusterStateProvider().getLiveNodes());
    ClusterState state = cluster.getClusterStateProvider().getClusterState();
    for (String coll: cluster.getSimClusterStateProvider().simListCollections()) {
      log.info("* Collection " + coll + " state: " + state.getCollection(coll));
    }
    shutdownCluster();
  }

  @Test
  // commented out on: 24-Dec-2018   @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 28-June-2018
  public void testExecute() throws Exception {
    SolrClient solrClient = cluster.simGetSolrClient();
    String collectionName = "testExecute";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 1, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);

    log.info("Collection ready after " + CloudUtil.waitForState(cluster, collectionName, 120, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 2, false, true)) + "ms");

    String sourceNodeName = cluster.getSimClusterStateProvider().simGetRandomNode();
    ClusterState clusterState = cluster.getClusterStateProvider().getClusterState();
    DocCollection docCollection = clusterState.getCollection(collectionName);
    List<Replica> replicas = docCollection.getReplicas(sourceNodeName);
    assertNotNull(replicas);
    assertFalse(replicas.isEmpty());

    List<String> otherNodes = cluster.getClusterStateProvider().getLiveNodes().stream()
        .filter(node -> !node.equals(sourceNodeName)).collect(Collectors.toList());
    assertFalse(otherNodes.isEmpty());
    String survivor = otherNodes.get(0);

    try (ExecutePlanAction action = new ExecutePlanAction()) {
      action.configure(cluster.getLoader(), cluster, Collections.singletonMap("name", "execute_plan"));

      // used to signal if we found that ExecutePlanAction did in fact create the right znode before executing the operation
      AtomicBoolean znodeCreated = new AtomicBoolean(false);

      CollectionAdminRequest.AsyncCollectionAdminRequest moveReplica = new CollectionAdminRequest.MoveReplica(collectionName, replicas.get(0).getName(), survivor);
      CollectionAdminRequest.AsyncCollectionAdminRequest mockRequest = new CollectionAdminRequest.AsyncCollectionAdminRequest(CollectionParams.CollectionAction.OVERSEERSTATUS) {
        @Override
        public void setAsyncId(String asyncId) {
          super.setAsyncId(asyncId);
          String parentPath = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/xyz/execute_plan";
          try {
            if (cluster.getDistribStateManager().hasData(parentPath)) {
              java.util.List<String> children = cluster.getDistribStateManager().listData(parentPath);
              if (!children.isEmpty()) {
                String child = children.get(0);
                VersionedData data = cluster.getDistribStateManager().getData(parentPath + "/" + child);
                Map m = (Map) Utils.fromJSON(data.getData());
                if (m.containsKey("requestid")) {
                  znodeCreated.set(m.get("requestid").equals(asyncId));
                }
              }
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

        }
      };
      List<CollectionAdminRequest.AsyncCollectionAdminRequest> operations = Lists.asList(moveReplica, new CollectionAdminRequest.AsyncCollectionAdminRequest[]{mockRequest});
      NodeLostTrigger.NodeLostEvent nodeLostEvent = new NodeLostTrigger.NodeLostEvent(TriggerEventType.NODELOST,
          "mock_trigger_name", Collections.singletonList(SIM_TIME_SOURCE.getTimeNs()),
          Collections.singletonList(sourceNodeName), CollectionParams.CollectionAction.MOVEREPLICA.toLower());
      ActionContext actionContext = new ActionContext(cluster, null,
          new HashMap<>(Collections.singletonMap("operations", operations)));
      action.process(nodeLostEvent, actionContext);

//      assertTrue("ExecutePlanAction should have stored the requestid in ZK before executing the request", znodeCreated.get());
      List<NamedList<Object>> responses = (List<NamedList<Object>>) actionContext.getProperty("responses");
      assertNotNull(responses);
      assertEquals(2, responses.size());
      NamedList<Object> response = responses.get(0);
      assertNull(response.get("failure"));
      assertNotNull(response.get("success"));
    }

    log.info("Collection ready after " + CloudUtil.waitForState(cluster, collectionName, 300, TimeUnit.SECONDS,
        CloudUtil.clusterShape(1, 2, false, true)) + "ms");
  }

  @Test
  public void testIntegration() throws Exception  {
    SolrClient solrClient = cluster.simGetSolrClient();

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'execute_plan','class':'solr.ExecutePlanAction'}]" +
        "}}";
    SolrRequest req = AutoScalingRequest.create(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    assertAutoscalingUpdateComplete();

    String collectionName = "testIntegration";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 1, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);

    CloudUtil.waitForState(cluster, "Timed out waiting for replicas of new collection to be active",
        collectionName, CloudUtil.clusterShape(1, 2, false, true));

    String sourceNodeName = cluster.getSimClusterStateProvider().simGetRandomNode();
    ClusterState clusterState = cluster.getClusterStateProvider().getClusterState();
    DocCollection docCollection = clusterState.getCollection(collectionName);
    List<Replica> replicas = docCollection.getReplicas(sourceNodeName);
    assertNotNull(replicas);
    assertFalse(replicas.isEmpty());

    List<String> otherNodes = cluster.getClusterStateProvider().getLiveNodes().stream()
        .filter(node -> !node.equals(sourceNodeName)).collect(Collectors.toList());
    assertFalse(otherNodes.isEmpty());
    String survivor = otherNodes.get(0);

    cluster.simRemoveNode(sourceNodeName, false);

    cluster.getTimeSource().sleep(3000);

    CloudUtil.waitForState(cluster, "Timed out waiting for replicas of collection to be 2 again",
        collectionName, CloudUtil.clusterShape(1, 2, false, true));

    clusterState = cluster.getClusterStateProvider().getClusterState();
    docCollection = clusterState.getCollection(collectionName);
    List<Replica> replicasOnSurvivor = docCollection.getReplicas(survivor);
    assertNotNull(replicasOnSurvivor);
    assertEquals(2, replicasOnSurvivor.size());
  }
}
