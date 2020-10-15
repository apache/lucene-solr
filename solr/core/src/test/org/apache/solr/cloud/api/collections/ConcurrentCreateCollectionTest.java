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
package org.apache.solr.cloud.api.collections;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudTestUtils;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConcurrentCreateCollectionTest extends SolrCloudTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static int NODES = 2;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(NODES)
         .addConfig("conf", configset("cloud-minimal"))
        //.addConfig("conf", configset("_default"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    cluster.deleteAllCollections();
  }


  private CollectionAdminRequest.Create createCollectionRequest(String cname, int numShards, int numReplicas) throws Exception {
    CollectionAdminRequest.Create creq = CollectionAdminRequest
        //  .createCollection(cname, "conf", NODES - 1, NODES - 1)
        .createCollection(cname, "conf", numShards, numReplicas)
        .setMaxShardsPerNode(100);
    creq.setWaitForFinalState(true);
    creq.setAutoAddReplicas(true);
    return creq;
  }

  public void testConcurrentCreatePlacement() throws Exception {
    final int nThreads = 2;
    final int createsPerThread = 1;
    final int nShards = 1;
    final int repFactor = 2;
    final boolean useClusterPolicy = false;
    final boolean useCollectionPolicy = true;
    final boolean startUnbalanced = true; // can help make a smaller test that can still reproduce an issue.
    final int unbalancedSize = 1; // the number of replicas to create first
    final boolean stopNode = false;  // only applicable when startUnbalanced==true... stops a node during first collection creation, then restarts

    final CloudSolrClient client = cluster.getSolrClient();


    if (startUnbalanced) {
      /*** This produces a failure (multiple replicas of single shard on same node) when run with NODES=4 and
       final int nThreads = 2;
       final int createsPerThread = 1;
       final int nShards = 2;
       final int repFactor = 2;
       final boolean useClusterPolicy = false;
       final boolean useCollectionPolicy = true;
       final boolean startUnbalanced = true;
       // NOTE: useClusterPolicy=true seems to fix it! So does putting both creates in a single thread!
       // NOTE: even creating a single replica to start with causes failure later on.

       Also reproduced with smaller cluster: NODES=2 and
       final int nThreads = 2;
       final int createsPerThread = 1;
       final int nShards = 1;
       final int repFactor = 2;
       final boolean useClusterPolicy = false;
       final boolean useCollectionPolicy = true;
       final boolean startUnbalanced = true;

       Also, with NODES=3:
       final int nThreads = 2;
       final int createsPerThread = 1;
       final int nShards = 1;
       final int repFactor = 2;
       final boolean useClusterPolicy = false;
       final boolean useCollectionPolicy = true;
       final boolean startUnbalanced = false;

       // Also succeeded in replicating a bug where all 5 replicas were on a single node: CORES=5, nThreads=5, repFactor=5,
       //    unbalancedSize = 16 (4 replicas on each of the up nodes), stopNode=true
       ***/


      JettySolrRunner downJetty = cluster.getJettySolrRunners().get(0);
      if (stopNode) {
        cluster.stopJettySolrRunner(downJetty);
      }

      String cname = "STARTCOLLECTION";
      CollectionAdminRequest.Create creq = CollectionAdminRequest
          //  .createCollection(cname, "conf", NODES - 1, NODES - 1)
          .createCollection(cname, "conf", unbalancedSize, 1)
          .setMaxShardsPerNode(100);
      creq.setWaitForFinalState(true);
      // creq.setAutoAddReplicas(true);
      if (useCollectionPolicy) { creq.setPolicy("policy1"); }
      creq.process(client);

      if (stopNode) {
        // this will start it with a new port.... does it matter?
        cluster.startJettySolrRunner(downJetty);
      }
    }



    if (useClusterPolicy) {
      String setClusterPolicyCommand = "{" +
          " 'set-cluster-policy': [" +
          // "      {'cores':'<100', 'node':'#ANY'}," +
          "      {'replica':'<2', 'shard': '#EACH', 'node': '#ANY'}," +
          // "      {'replica':'<2', 'node': '#ANY'}," +
          "    ]" +
          "}";

      @SuppressWarnings({"rawtypes"})
      SolrRequest req = CloudTestUtils.AutoScalingRequest.create(SolrRequest.METHOD.POST, setClusterPolicyCommand);
      client.request(req);
    }

    if (useCollectionPolicy) {
      // NOTE: the mere act of setting this named policy prevents LegacyAssignStrategy from being used, even if the policy is
      // not used during collection creation.
      String commands =  "{set-policy : {" +
          " policy1 : [{replica:'<2' , node:'#ANY'}]" +
          ",policy2 : [{replica:'<2' , shard:'#EACH', node:'#ANY'}]" +
          "}}";
      client.request(CloudTestUtils.AutoScalingRequest.create(SolrRequest.METHOD.POST, commands));

      /*** take defaults for cluster preferences
      String cmd = "{" +
          " 'set-cluster-preferences': [" +
          // "      {'cores':'<100', 'node':'#ANY'}," +
          "      {minimize:cores}" +
          "    ]" +
          "}";

      SolrRequest req = CloudTestUtils.AutoScalingRequest.create(SolrRequest.METHOD.POST, cmd);
      client.request(req);
       ***/
    }

    /***
    SolrRequest req = CloudTestUtils.AutoScalingRequest.create(SolrRequest.METHOD.GET, null);
    SolrResponse response = req.process(client);
    log.info("######### AUTOSCALE {}", response);
     ***/


    byte[] data = client.getZkStateReader().getZkClient().getData("/autoscaling.json", null, null, true);
    if (log.isInfoEnabled()) {
      log.info("AUTOSCALE DATA: {}", new String(data, "UTF-8"));
    }

    final AtomicInteger collectionNum = new AtomicInteger();
    Thread[] indexThreads = new Thread[nThreads];

    for (int i=0; i<nThreads; i++) {
      indexThreads[i] = new Thread(() -> {
        try {
          for (int j=0; j<createsPerThread; j++) {
            int num = collectionNum.incrementAndGet();
            // Thread.sleep(num*1000);
            String collectionName = "collection" + num;
            CollectionAdminRequest.Create createReq = CollectionAdminRequest
                .createCollection(collectionName, "conf", nShards, repFactor)
                // .setMaxShardsPerNode(1) // should be default
                ;
            createReq.setWaitForFinalState(false);
            if (useCollectionPolicy) {
              createReq.setPolicy("policy1");
            }
            createReq.setAutoAddReplicas(true);

            createReq.process(client);
            // cluster.waitForActiveCollection(collectionName, 1, repFactor);
            // Thread.sleep(10000);
          }
        } catch (Exception e) {
          fail(e.getMessage());
        }
      });
    }

    for (Thread thread : indexThreads) {
      thread.start();
    }

    for (Thread thread : indexThreads) {
      thread.join();
    }

    int expectedTotalReplicas = unbalancedSize + nThreads * createsPerThread * nShards * repFactor;
    int expectedPerNode = expectedTotalReplicas / NODES;
    boolean expectBalanced = (expectedPerNode * NODES == expectedTotalReplicas);

    Map<String,List<Replica>> replicaMap = new HashMap<>();
    ClusterState cstate = client.getZkStateReader().getClusterState();
    for (DocCollection collection : cstate.getCollectionsMap().values()) {
      for (Replica replica : collection.getReplicas()) {
        String url = replica.getBaseUrl();
        List<Replica> replicas = replicaMap.get(url);
        if (replicas == null) {
          replicas = new ArrayList<>();
          replicaMap.put(url, replicas);
        }
        replicas.add(replica);
      }
    }

    // check if nodes are balanced
    boolean failed = false;
    for (List<Replica> replicas : replicaMap.values()) {
      if (replicas.size() != expectedPerNode ) {
        if (expectBalanced) {
          failed = true;
        }
        log.error("UNBALANCED CLUSTER: expected replicas per node {} but got {}", expectedPerNode, replicas.size());
      }
    }

    // check if there were multiple replicas of the same shard placed on the same node
    for (DocCollection collection : cstate.getCollectionsMap().values()) {
      for (Slice slice : collection.getSlices()) {
        Map<String, Replica> nodeToReplica = new HashMap<>();
        for (Replica replica : slice.getReplicas()) {
          Replica prev = nodeToReplica.put(replica.getBaseUrl(), replica);
          if (prev != null) {
            failed = true;
            // NOTE: with a replication factor > 2, this will print multiple times per bad slice.
            log.error("MULTIPLE REPLICAS OF SINGLE SHARD ON SAME NODE: r1={} r2={}", prev, replica);
          }
        }
      }
    }

    if (failed) {
      log.error("Cluster state {}", cstate.getCollectionsMap());
    }

    assertEquals(replicaMap.size(),  NODES);  // make sure something was created

    assertTrue(!failed);
  }


  
}
