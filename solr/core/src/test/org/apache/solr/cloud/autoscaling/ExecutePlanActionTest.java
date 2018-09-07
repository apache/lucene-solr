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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.LogLevel;
import org.apache.solr.common.util.TimeSource;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 * Test for {@link ExecutePlanAction}
 */
@LogLevel("org.apache.solr.cloud.autoscaling=DEBUG")
public class ExecutePlanActionTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int NODE_COUNT = 2;

  private SolrResourceLoader loader;
  private SolrCloudManager cloudManager;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(NODE_COUNT)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  public void setUp() throws Exception  {
    super.setUp();
    // clear any persisted auto scaling configuration
    Stat stat = zkClient().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), true);

    if (cluster.getJettySolrRunners().size() < NODE_COUNT) {
      // start some to get to original state
      int numJetties = cluster.getJettySolrRunners().size();
      for (int i = 0; i < NODE_COUNT - numJetties; i++) {
        cluster.startJettySolrRunner();
      }
    }
    cluster.waitForAllNodes(30);
    loader = cluster.getJettySolrRunner(0).getCoreContainer().getResourceLoader();
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
  }

  @Test
  public void testExecute() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "testExecute";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 1, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionName, clusterShape(1, 2));

    JettySolrRunner sourceNode = cluster.getRandomJetty(random());
    String sourceNodeName = sourceNode.getNodeName();
    ClusterState clusterState = solrClient.getZkStateReader().getClusterState();
    DocCollection docCollection = clusterState.getCollection(collectionName);
    List<Replica> replicas = docCollection.getReplicas(sourceNodeName);
    assertNotNull(replicas);
    assertFalse(replicas.isEmpty());

    List<JettySolrRunner> otherJetties = cluster.getJettySolrRunners().stream()
        .filter(jettySolrRunner -> jettySolrRunner != sourceNode).collect(Collectors.toList());
    assertFalse(otherJetties.isEmpty());
    JettySolrRunner survivor = otherJetties.get(0);

    try (ExecutePlanAction action = new ExecutePlanAction()) {
      action.configure(loader, cloudManager, Collections.singletonMap("name", "execute_plan"));

      // used to signal if we found that ExecutePlanAction did in fact create the right znode before executing the operation
      AtomicBoolean znodeCreated = new AtomicBoolean(false);

      CollectionAdminRequest.AsyncCollectionAdminRequest moveReplica = new CollectionAdminRequest.MoveReplica(collectionName, replicas.get(0).getName(), survivor.getNodeName());
      CollectionAdminRequest.AsyncCollectionAdminRequest mockRequest = new CollectionAdminRequest.AsyncCollectionAdminRequest(CollectionParams.CollectionAction.OVERSEERSTATUS) {
        @Override
        public void setAsyncId(String asyncId) {
          super.setAsyncId(asyncId);
          String parentPath = ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH + "/xyz/execute_plan";
          try {
            if (zkClient().exists(parentPath, true)) {
              java.util.List<String> children = zkClient().getChildren(parentPath, null, true);
              if (!children.isEmpty()) {
                String child = children.get(0);
                byte[] data = zkClient().getData(parentPath + "/" + child, null, null, true);
                Map m = (Map) Utils.fromJSON(data);
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
          "mock_trigger_name", Collections.singletonList(TimeSource.CURRENT_TIME.getTimeNs()),
          Collections.singletonList(sourceNodeName), CollectionParams.CollectionAction.MOVEREPLICA.toLower());
      ActionContext actionContext = new ActionContext(survivor.getCoreContainer().getZkController().getSolrCloudManager(), null,
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

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionName, clusterShape(1, 2));
  }

  @Test
  public void testIntegration() throws Exception  {
    CloudSolrClient solrClient = cluster.getSolrClient();

    String setTriggerCommand = "{" +
        "'set-trigger' : {" +
        "'name' : 'node_lost_trigger'," +
        "'event' : 'nodeLost'," +
        "'waitFor' : '1s'," +
        "'enabled' : true," +
        "'actions' : [{'name':'compute_plan', 'class' : 'solr.ComputePlanAction'}," +
        "{'name':'execute_plan','class':'solr.ExecutePlanAction'}]" +
        "}}";
    SolrRequest req = AutoScalingHandlerTest.createAutoScalingRequest(SolrRequest.METHOD.POST, setTriggerCommand);
    NamedList<Object> response = solrClient.request(req);
    assertEquals(response.get("result").toString(), "success");

    String collectionName = "testIntegration";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName,
        "conf", 1, 2);
    create.setMaxShardsPerNode(1);
    create.process(solrClient);

    waitForState("Timed out waiting for replicas of new collection to be active",
        collectionName, clusterShape(1, 2));

    JettySolrRunner sourceNode = cluster.getRandomJetty(random());
    String sourceNodeName = sourceNode.getNodeName();
    ClusterState clusterState = solrClient.getZkStateReader().getClusterState();
    DocCollection docCollection = clusterState.getCollection(collectionName);
    List<Replica> replicas = docCollection.getReplicas(sourceNodeName);
    assertNotNull(replicas);
    assertFalse(replicas.isEmpty());

    List<JettySolrRunner> otherJetties = cluster.getJettySolrRunners().stream()
        .filter(jettySolrRunner -> jettySolrRunner != sourceNode).collect(Collectors.toList());
    assertFalse(otherJetties.isEmpty());
    JettySolrRunner survivor = otherJetties.get(0);

    for (int i = 0; i < cluster.getJettySolrRunners().size(); i++) {
      JettySolrRunner runner = cluster.getJettySolrRunner(i);
      if (runner == sourceNode) {
        cluster.stopJettySolrRunner(i);
      }
    }

    cluster.waitForAllNodes(30);
    waitForState("Timed out waiting for replicas of collection to be 2 again",
        collectionName, clusterShape(1, 2));

    clusterState = solrClient.getZkStateReader().getClusterState();
    docCollection = clusterState.getCollection(collectionName);
    List<Replica> replicasOnSurvivor = docCollection.getReplicas(survivor.getNodeName());
    assertNotNull(replicasOnSurvivor);
    assertEquals(2, replicasOnSurvivor.size());
  }
}
