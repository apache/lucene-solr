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
package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.HashBasedRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TestInjection;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.NUM_SLICES;
import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;

@Slow
public class ShardSplitTest extends BasicDistributedZkTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String SHARD1_0 = SHARD1 + "_0";
  public static final String SHARD1_1 = SHARD1 + "_1";

  public ShardSplitTest() {
    schemaString = "schema15.xml";      // we need a string id
  }

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    useFactory(null);
  }

  @Test
  public void test() throws Exception {

    waitForThingsToLevelOut(15);

    if (usually()) {
      log.info("Using legacyCloud=false for cluster");
      CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, "false")
          .process(cloudClient);
    }
    incompleteOrOverlappingCustomRangeTest();
    splitByUniqueKeyTest();
    splitByRouteFieldTest();
    splitByRouteKeyTest();

    // todo can't call waitForThingsToLevelOut because it looks for jettys of all shards
    // and the new sub-shards don't have any.
    waitForRecoveriesToFinish(true);
    //waitForThingsToLevelOut(15);
  }

  /*
  Creates a collection with replicationFactor=1, splits a shard. Restarts the sub-shard leader node.
  Add a replica. Ensure count matches in leader and replica.
   */
  public void testSplitStaticIndexReplication() throws Exception {
    waitForThingsToLevelOut(15);

    DocCollection defCol = cloudClient.getZkStateReader().getClusterState().getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION);
    Replica replica = defCol.getReplicas().get(0);
    String nodeName = replica.getNodeName();

    String collectionName = "testSplitStaticIndexReplication";
    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1);
    create.setMaxShardsPerNode(5); // some high number so we can create replicas without hindrance
    create.setCreateNodeSet(nodeName); // we want to create the leader on a fixed node so that we know which one to restart later
    create.process(cloudClient);
    try (CloudSolrClient client = getCloudSolrClient(zkServer.getZkAddress(), true, cloudClient.getLbClient().getHttpClient())) {
      client.setDefaultCollection(collectionName);
      StoppableIndexingThread thread = new StoppableIndexingThread(controlClient, client, "i1", true);
      try {
        thread.start();
        Thread.sleep(1000); // give the indexer sometime to do its work
        thread.safeStop();
        thread.join();
        client.commit();
        controlClient.commit();

        CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName);
        splitShard.setShardName(SHARD1);
        String asyncId = splitShard.processAsync(client);
        RequestStatusState state = CollectionAdminRequest.requestStatus(asyncId).waitFor(client, 120);
        if (state == RequestStatusState.COMPLETED)  {
          waitForRecoveriesToFinish(collectionName, true);
          // let's wait to see parent shard become inactive
          CountDownLatch latch = new CountDownLatch(1);
          client.getZkStateReader().registerCollectionStateWatcher(collectionName, new CollectionStateWatcher() {
            @Override
            public boolean onStateChanged(Set<String> liveNodes, DocCollection collectionState) {
              Slice parent = collectionState.getSlice(SHARD1);
              Slice slice10 = collectionState.getSlice(SHARD1_0);
              Slice slice11 = collectionState.getSlice(SHARD1_1);
              if (slice10 != null && slice11 != null &&
                  parent.getState() == Slice.State.INACTIVE &&
                  slice10.getState() == Slice.State.ACTIVE &&
                  slice11.getState() == Slice.State.ACTIVE) {
                latch.countDown();
                return true; // removes the watch
              }
              return false;
            }
          });
          latch.await(1, TimeUnit.MINUTES);
          if (latch.getCount() != 0)  {
            // sanity check
            fail("Sub-shards did not become active even after waiting for 1 minute");
          }

          int liveNodeCount = client.getZkStateReader().getClusterState().getLiveNodes().size();

          // restart the sub-shard leader node
          boolean restarted = false;
          for (JettySolrRunner jetty : jettys) {
            int port = jetty.getBaseUrl().getPort();
            if (replica.getStr(BASE_URL_PROP).contains(":" + port))  {
              ChaosMonkey.kill(jetty);
              ChaosMonkey.start(jetty);
              restarted = true;
              break;
            }
          }
          if (!restarted) {
            // sanity check
            fail("We could not find a jetty to kill for replica: " + replica.getCoreUrl());
          }

          // add a new replica for the sub-shard
          CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest.addReplicaToShard(collectionName, SHARD1_0);
          // use control client because less chances of it being the node being restarted
          // this is to avoid flakiness of test because of NoHttpResponseExceptions
          String control_collection = client.getZkStateReader().getClusterState().getCollection("control_collection").getReplicas().get(0).getStr(BASE_URL_PROP);
          try (HttpSolrClient control = new HttpSolrClient.Builder(control_collection).withHttpClient(client.getLbClient().getHttpClient()).build())  {
            state = addReplica.processAndWait(control, 30);
          }
          if (state == RequestStatusState.COMPLETED)  {
            CountDownLatch newReplicaLatch = new CountDownLatch(1);
            client.getZkStateReader().registerCollectionStateWatcher(collectionName, new CollectionStateWatcher() {
              @Override
              public boolean onStateChanged(Set<String> liveNodes, DocCollection collectionState) {
                if (liveNodes.size() != liveNodeCount)  {
                  return false;
                }
                Slice slice = collectionState.getSlice(SHARD1_0);
                if (slice.getReplicas().size() == 2)  {
                  if (!slice.getReplicas().stream().anyMatch(r -> r.getState() == Replica.State.RECOVERING)) {
                    // we see replicas and none of them are recovering
                    newReplicaLatch.countDown();
                    return true;
                  }
                }
                return false;
              }
            });
            newReplicaLatch.await(30, TimeUnit.SECONDS);
            // check consistency of sub-shard replica explicitly because checkShardConsistency methods doesn't
            // handle new shards/replica so well.
            ClusterState clusterState = client.getZkStateReader().getClusterState();
            DocCollection collection = clusterState.getCollection(collectionName);
            int numReplicasChecked = assertConsistentReplicas(collection.getSlice(SHARD1_0));
            assertEquals("We should have checked consistency for exactly 2 replicas of shard1_0", 2, numReplicasChecked);
          } else  {
            fail("Adding a replica to sub-shard did not complete even after waiting for 30 seconds!. Saw state = " + state.getKey());
          }
        } else {
          fail("We expected shard split to succeed on a static index but it didn't. Found state = " + state.getKey());
        }
      } finally {
        thread.safeStop();
        thread.join();
      }
    }
  }

  private int assertConsistentReplicas(Slice shard) throws SolrServerException, IOException {
    long numFound = Long.MIN_VALUE;
    int count = 0;
    for (Replica replica : shard.getReplicas()) {
      HttpSolrClient client = new HttpSolrClient.Builder(replica.getCoreUrl())
          .withHttpClient(cloudClient.getLbClient().getHttpClient()).build();
      QueryResponse response = client.query(new SolrQuery("q", "*:*", "distrib", "false"));
      log.info("Found numFound={} on replica: {}", response.getResults().getNumFound(), replica.getCoreUrl());
      if (numFound == Long.MIN_VALUE)  {
        numFound = response.getResults().getNumFound();
      } else  {
        assertEquals("Shard " + shard.getName() + " replicas do not have same number of documents", numFound, response.getResults().getNumFound());
      }
      count++;
    }
    return count;
  }

  /**
   * Used to test that we can split a shard when a previous split event
   * left sub-shards in construction or recovery state.
   *
   * See SOLR-9439
   */
  @Test
  public void testSplitAfterFailedSplit() throws Exception {
    waitForThingsToLevelOut(15);

    TestInjection.splitFailureBeforeReplicaCreation = "true:100"; // we definitely want split to fail
    try {
      try {
        CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION);
        splitShard.setShardName(SHARD1);
        splitShard.process(cloudClient);
        fail("Shard split was not supposed to succeed after failure injection!");
      } catch (Exception e) {
        // expected
      }

      // assert that sub-shards cores exist and sub-shard is in construction state
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      zkStateReader.forceUpdateCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION);
      ClusterState state = zkStateReader.getClusterState();
      DocCollection collection = state.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION);

      Slice shard10 = collection.getSlice(SHARD1_0);
      assertEquals(Slice.State.CONSTRUCTION, shard10.getState());
      assertEquals(1, shard10.getReplicas().size());

      Slice shard11 = collection.getSlice(SHARD1_1);
      assertEquals(Slice.State.CONSTRUCTION, shard11.getState());
      assertEquals(1, shard11.getReplicas().size());

      // lets retry the split
      TestInjection.reset(); // let the split succeed
      try {
        CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION);
        splitShard.setShardName(SHARD1);
        splitShard.process(cloudClient);
        // Yay!
      } catch (Exception e) {
        log.error("Shard split failed", e);
        fail("Shard split did not succeed after a previous failed split attempt left sub-shards in construction state");
      }

    } finally {
      TestInjection.reset();
    }
  }

  @Test
  public void testSplitWithChaosMonkey() throws Exception {
    waitForThingsToLevelOut(15);

    List<StoppableIndexingThread> indexers = new ArrayList<>();
    try {
      for (int i = 0; i < 1; i++) {
        StoppableIndexingThread thread = new StoppableIndexingThread(controlClient, cloudClient, String.valueOf(i), true);
        indexers.add(thread);
        thread.start();
      }
      Thread.sleep(1000); // give the indexers some time to do their work
    } catch (Exception e) {
      log.error("Error in test", e);
    } finally {
      for (StoppableIndexingThread indexer : indexers) {
        indexer.safeStop();
        indexer.join();
      }
    }

    cloudClient.commit();
    controlClient.commit();

    AtomicBoolean stop = new AtomicBoolean();
    AtomicBoolean killed = new AtomicBoolean(false);
    Runnable monkey = new Runnable() {
      @Override
      public void run() {
        ZkStateReader zkStateReader = cloudClient.getZkStateReader();
        zkStateReader.registerCollectionStateWatcher(AbstractDistribZkTestBase.DEFAULT_COLLECTION, new CollectionStateWatcher() {
          @Override
          public boolean onStateChanged(Set<String> liveNodes, DocCollection collectionState) {
            if (stop.get()) {
              return true; // abort and remove the watch
            }
            Slice slice = collectionState.getSlice(SHARD1_0);
            if (slice != null && slice.getReplicas().size() > 1) {
              // ensure that only one watcher invocation thread can kill!
              if (killed.compareAndSet(false, true))  {
                log.info("Monkey thread found 2 replicas for {} {}", AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1);
                CloudJettyRunner cjetty = shardToLeaderJetty.get(SHARD1);
                try {
                  Thread.sleep(1000 + random().nextInt(500));
                  ChaosMonkey.kill(cjetty);
                  stop.set(true);
                  return true;
                } catch (Exception e) {
                  log.error("Monkey unable to kill jetty at port " + cjetty.jetty.getLocalPort(), e);
                }
              }
            }
            log.info("Monkey thread found only one replica for {} {}", AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1);
            return false;
          }
        });
      }
    };

    Thread monkeyThread = null;
    /*
     somehow the cluster state object inside this zk state reader has static copy of the collection which is never updated
     so any call to waitForRecoveriesToFinish just keeps looping until timeout.
     We workaround by explicitly registering the collection as an interesting one so that it is watched by ZkStateReader
     see SOLR-9440. Todo remove this hack after SOLR-9440 is fixed.
    */
    cloudClient.getZkStateReader().registerCore(AbstractDistribZkTestBase.DEFAULT_COLLECTION);

    monkeyThread = new Thread(monkey);
    monkeyThread.start();
    try {
      CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION);
      splitShard.setShardName(SHARD1);
      String asyncId = splitShard.processAsync(cloudClient);
      RequestStatusState splitStatus = null;
      try {
        splitStatus = CollectionAdminRequest.requestStatus(asyncId).waitFor(cloudClient, 120);
      } catch (Exception e) {
        log.warn("Failed to get request status, maybe because the overseer node was shutdown by monkey", e);
      }

      // we don't care if the split failed because we are injecting faults and it is likely
      // that the split has failed but in any case we want to assert that all docs that got
      // indexed are available in SolrCloud and if the split succeeded then all replicas of the sub-shard
      // must be consistent (i.e. have same numdocs)

      log.info("Shard split request state is COMPLETED");
      stop.set(true);
      monkeyThread.join();
      Set<String> addFails = new HashSet<>();
      Set<String> deleteFails = new HashSet<>();
      for (StoppableIndexingThread indexer : indexers) {
        addFails.addAll(indexer.getAddFails());
        deleteFails.addAll(indexer.getDeleteFails());
      }

      CloudJettyRunner cjetty = shardToLeaderJetty.get(SHARD1);
      log.info("Starting shard1 leader jetty at port {}", cjetty.jetty.getLocalPort());
      ChaosMonkey.start(cjetty.jetty);
      cloudClient.getZkStateReader().forceUpdateCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION);
      log.info("Current collection state: {}", printClusterStateInfo(AbstractDistribZkTestBase.DEFAULT_COLLECTION));

      boolean replicaCreationsFailed = false;
      if (splitStatus == RequestStatusState.FAILED)  {
        // either one or more replica creation failed (because it may have been created on the same parent shard leader node)
        // or the split may have failed while trying to soft-commit *after* all replicas have been created
        // the latter counts as a successful switch even if the API doesn't say so
        // so we must find a way to distinguish between the two
        // an easy way to do that is to look at the sub-shard replicas and check if the replica core actually exists
        // instead of existing solely inside the cluster state
        DocCollection collectionState = cloudClient.getZkStateReader().getClusterState().getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION);
        Slice slice10 = collectionState.getSlice(SHARD1_0);
        Slice slice11 = collectionState.getSlice(SHARD1_1);
        if (slice10 != null && slice11 != null) {
          for (Replica replica : slice10) {
            if (!doesReplicaCoreExist(replica)) {
              replicaCreationsFailed = true;
              break;
            }
          }
          for (Replica replica : slice11) {
            if (!doesReplicaCoreExist(replica)) {
              replicaCreationsFailed = true;
              break;
            }
          }
        }
      }

      // true if sub-shard states switch to 'active' eventually
      AtomicBoolean areSubShardsActive = new AtomicBoolean(false);

      if (!replicaCreationsFailed)  {
        // all sub-shard replicas were created successfully so all cores must recover eventually
        waitForRecoveriesToFinish(AbstractDistribZkTestBase.DEFAULT_COLLECTION, true);
        // let's wait for the overseer to switch shard states
        CountDownLatch latch = new CountDownLatch(1);
        cloudClient.getZkStateReader().registerCollectionStateWatcher(AbstractDistribZkTestBase.DEFAULT_COLLECTION, new CollectionStateWatcher() {
          @Override
          public boolean onStateChanged(Set<String> liveNodes, DocCollection collectionState) {
            Slice parent = collectionState.getSlice(SHARD1);
            Slice slice10 = collectionState.getSlice(SHARD1_0);
            Slice slice11 = collectionState.getSlice(SHARD1_1);
            if (slice10 != null && slice11 != null &&
                parent.getState() == Slice.State.INACTIVE &&
                slice10.getState() == Slice.State.ACTIVE &&
                slice11.getState() == Slice.State.ACTIVE) {
              areSubShardsActive.set(true);
              latch.countDown();
              return true; // removes the watch
            } else if (slice10 != null && slice11 != null &&
                parent.getState() == Slice.State.ACTIVE &&
                slice10.getState() == Slice.State.RECOVERY_FAILED &&
                slice11.getState() == Slice.State.RECOVERY_FAILED) {
              areSubShardsActive.set(false);
              latch.countDown();
              return true;
            }
            return false;
          }
        });

        latch.await(2, TimeUnit.MINUTES);

        if (latch.getCount() != 0)  {
          // sanity check
          fail("We think that split was successful but sub-shard states were not updated even after 2 minutes.");
        }
      }

      cloudClient.commit(); // for visibility of results on sub-shards

      checkShardConsistency(true, true, addFails, deleteFails);
      long ctrlDocs = controlClient.query(new SolrQuery("*:*")).getResults().getNumFound();
      // ensure we have added more than 0 docs
      long cloudClientDocs = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
      assertTrue("Found " + ctrlDocs + " control docs", cloudClientDocs > 0);
      assertEquals("Found " + ctrlDocs + " control docs and " + cloudClientDocs + " cloud docs", ctrlDocs, cloudClientDocs);

      // check consistency of sub-shard replica explicitly because checkShardConsistency methods doesn't
      // handle new shards/replica so well.
      if (areSubShardsActive.get()) {
        ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
        DocCollection collection = clusterState.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION);
        int numReplicasChecked = assertConsistentReplicas(collection.getSlice(SHARD1_0));
        assertEquals("We should have checked consistency for exactly 2 replicas of shard1_0", 2, numReplicasChecked);
        numReplicasChecked = assertConsistentReplicas(collection.getSlice(SHARD1_1));
        assertEquals("We should have checked consistency for exactly 2 replicas of shard1_1", 2, numReplicasChecked);
      }
    } finally {
      stop.set(true);
      monkeyThread.join();
    }
  }

  private boolean doesReplicaCoreExist(Replica replica) throws IOException {
    try (HttpSolrClient client = new HttpSolrClient.Builder(replica.getStr(BASE_URL_PROP))
        .withHttpClient(cloudClient.getLbClient().getHttpClient()).build())  {
      String coreName = replica.getCoreName();
      try {
        CoreAdminResponse status = CoreAdminRequest.getStatus(coreName, client);
        if (status.getCoreStatus(coreName) == null || status.getCoreStatus(coreName).size() == 0) {
          return false;
        }
      } catch (Exception e) {
        log.warn("Error gettting core status of replica " + replica + ". Perhaps it does not exist!", e);
        return false;
      }
    }
    return true;
  }

  @Test
  public void testSplitShardWithRule() throws Exception {
    waitForThingsToLevelOut(15);

    if (usually()) {
      log.info("Using legacyCloud=false for cluster");
      CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, "false")
          .process(cloudClient);
    }

    log.info("Starting testSplitShardWithRule");
    String collectionName = "shardSplitWithRule";
    CollectionAdminRequest.Create createRequest = new CollectionAdminRequest.Create()
        .setCollectionName(collectionName)
        .setNumShards(1)
        .setReplicationFactor(2)
        .setRule("shard:*,replica:<2,node:*");
    CollectionAdminResponse response = createRequest.process(cloudClient);
    assertEquals(0, response.getStatus());

    CollectionAdminRequest.SplitShard splitShardRequest = new CollectionAdminRequest.SplitShard()
        .setCollectionName(collectionName)
        .setShardName("shard1");
    response = splitShardRequest.process(cloudClient);
    assertEquals(String.valueOf(response.getErrorMessages()), 0, response.getStatus());
  }

  private void incompleteOrOverlappingCustomRangeTest() throws Exception  {
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    final DocRouter router = clusterState.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION).getRouter();
    Slice shard1 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1);
    DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();

    List<DocRouter.Range> subRanges = new ArrayList<>();
    List<DocRouter.Range> ranges = router.partitionRange(4, shard1Range);

    // test with only one range
    subRanges.add(ranges.get(0));
    try {
      splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1, subRanges, null);
      fail("Shard splitting with just one custom hash range should not succeed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      log.info("Expected exception:", e);
    }
    subRanges.clear();

    // test with ranges with a hole in between them
    subRanges.add(ranges.get(3)); // order shouldn't matter
    subRanges.add(ranges.get(0));
    try {
      splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1, subRanges, null);
      fail("Shard splitting with missing hashes in between given ranges should not succeed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      log.info("Expected exception:", e);
    }
    subRanges.clear();

    // test with overlapping ranges
    subRanges.add(ranges.get(0));
    subRanges.add(ranges.get(1));
    subRanges.add(ranges.get(2));
    subRanges.add(new DocRouter.Range(ranges.get(3).min - 15, ranges.get(3).max));
    try {
      splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1, subRanges, null);
      fail("Shard splitting with overlapping ranges should not succeed");
    } catch (HttpSolrClient.RemoteSolrException e) {
      log.info("Expected exception:", e);
    }
    subRanges.clear();
  }

  private void splitByUniqueKeyTest() throws Exception {
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    final DocRouter router = clusterState.getCollection(AbstractDistribZkTestBase.DEFAULT_COLLECTION).getRouter();
    Slice shard1 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1);
    DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();
    List<DocRouter.Range> subRanges = new ArrayList<>();
    if (usually())  {
      List<DocRouter.Range> ranges = router.partitionRange(4, shard1Range);
      // 75% of range goes to shard1_0 and the rest to shard1_1
      subRanges.add(new DocRouter.Range(ranges.get(0).min, ranges.get(2).max));
      subRanges.add(ranges.get(3));
    } else  {
      subRanges = router.partitionRange(2, shard1Range);
    }
    final List<DocRouter.Range> ranges = subRanges;
    final int[] docCounts = new int[ranges.size()];
    int numReplicas = shard1.getReplicas().size();

    del("*:*");
    for (int id = 0; id <= 100; id++) {
      String shardKey = "" + (char)('a' + (id % 26)); // See comment in ShardRoutingTest for hash distribution
      indexAndUpdateCount(router, ranges, docCounts, shardKey + "!" + String.valueOf(id), id);
    }
    commit();

    Thread indexThread = new Thread() {
      @Override
      public void run() {
        Random random = random();
        int max = atLeast(random, 401);
        int sleep = atLeast(random, 25);
        log.info("SHARDSPLITTEST: Going to add " + max + " number of docs at 1 doc per " + sleep + "ms");
        Set<String> deleted = new HashSet<>();
        for (int id = 101; id < max; id++) {
          try {
            indexAndUpdateCount(router, ranges, docCounts, String.valueOf(id), id);
            Thread.sleep(sleep);
            if (usually(random))  {
              String delId = String.valueOf(random.nextInt(id - 101 + 1) + 101);
              if (deleted.contains(delId))  continue;
              try {
                deleteAndUpdateCount(router, ranges, docCounts, delId);
                deleted.add(delId);
              } catch (Exception e) {
                log.error("Exception while deleting docs", e);
              }
            }
          } catch (Exception e) {
            log.error("Exception while adding doc id = " + id, e);
            // do not select this id for deletion ever
            deleted.add(String.valueOf(id));
          }
        }
      }
    };
    indexThread.start();

    try {
      for (int i = 0; i < 3; i++) {
        try {
          splitShard(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1, subRanges, null);
          log.info("Layout after split: \n");
          printLayout();
          break;
        } catch (HttpSolrClient.RemoteSolrException e) {
          if (e.code() != 500)  {
            throw e;
          }
          log.error("SPLITSHARD failed. " + (i < 2 ? " Retring split" : ""), e);
          if (i == 2) {
            fail("SPLITSHARD was not successful even after three tries");
          }
        }
      }
    } finally {
      try {
        indexThread.join();
      } catch (InterruptedException e) {
        log.error("Indexing thread interrupted", e);
      }
    }

    waitForRecoveriesToFinish(true);
    checkDocCountsAndShardStates(docCounts, numReplicas);
  }


  public void splitByRouteFieldTest() throws Exception  {
    log.info("Starting testSplitWithRouteField");
    String collectionName = "routeFieldColl";
    int numShards = 4;
    int replicationFactor = 2;
    int maxShardsPerNode = (((numShards * replicationFactor) / getCommonCloudSolrClient()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    HashMap<String, List<Integer>> collectionInfos = new HashMap<>();
    String shard_fld = "shard_s";
    try (CloudSolrClient client = createCloudClient(null)) {
      Map<String, Object> props = Utils.makeMap(
          REPLICATION_FACTOR, replicationFactor,
          MAX_SHARDS_PER_NODE, maxShardsPerNode,
          NUM_SLICES, numShards,
          "router.field", shard_fld);

      createCollection(collectionInfos, collectionName,props,client);
    }

    List<Integer> list = collectionInfos.get(collectionName);
    checkForCollection(collectionName, list, null);

    waitForRecoveriesToFinish(false);

    String url = getUrlFromZk(getCommonCloudSolrClient().getZkStateReader().getClusterState(), collectionName);

    try (HttpSolrClient collectionClient = getHttpSolrClient(url)) {

      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      final DocRouter router = clusterState.getCollection(collectionName).getRouter();
      Slice shard1 = clusterState.getSlice(collectionName, SHARD1);
      DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();
      final List<DocRouter.Range> ranges = router.partitionRange(2, shard1Range);
      final int[] docCounts = new int[ranges.size()];

      for (int i = 100; i <= 200; i++) {
        String shardKey = "" + (char) ('a' + (i % 26)); // See comment in ShardRoutingTest for hash distribution

        collectionClient.add(getDoc(id, i, "n_ti", i, shard_fld, shardKey));
        int idx = getHashRangeIdx(router, ranges, shardKey);
        if (idx != -1) {
          docCounts[idx]++;
        }
      }

      for (int i = 0; i < docCounts.length; i++) {
        int docCount = docCounts[i];
        log.info("Shard {} docCount = {}", "shard1_" + i, docCount);
      }

      collectionClient.commit();

      for (int i = 0; i < 3; i++) {
        try {
          splitShard(collectionName, SHARD1, null, null);
          break;
        } catch (HttpSolrClient.RemoteSolrException e) {
          if (e.code() != 500) {
            throw e;
          }
          log.error("SPLITSHARD failed. " + (i < 2 ? " Retring split" : ""), e);
          if (i == 2) {
            fail("SPLITSHARD was not successful even after three tries");
          }
        }
      }

      waitForRecoveriesToFinish(collectionName, false);

      assertEquals(docCounts[0], collectionClient.query(new SolrQuery("*:*").setParam("shards", "shard1_0")).getResults().getNumFound());
      assertEquals(docCounts[1], collectionClient.query(new SolrQuery("*:*").setParam("shards", "shard1_1")).getResults().getNumFound());
    }
  }

  private void splitByRouteKeyTest() throws Exception {
    log.info("Starting splitByRouteKeyTest");
    String collectionName = "splitByRouteKeyTest";
    int numShards = 4;
    int replicationFactor = 2;
    int maxShardsPerNode = (((numShards * replicationFactor) / getCommonCloudSolrClient()
        .getZkStateReader().getClusterState().getLiveNodes().size())) + 1;

    HashMap<String, List<Integer>> collectionInfos = new HashMap<>();

    try (CloudSolrClient client = createCloudClient(null)) {
      Map<String, Object> props = Utils.makeMap(
          REPLICATION_FACTOR, replicationFactor,
          MAX_SHARDS_PER_NODE, maxShardsPerNode,
          NUM_SLICES, numShards);

      createCollection(collectionInfos, collectionName,props,client);
    }

    List<Integer> list = collectionInfos.get(collectionName);
    checkForCollection(collectionName, list, null);

    waitForRecoveriesToFinish(false);

    String url = getUrlFromZk(getCommonCloudSolrClient().getZkStateReader().getClusterState(), collectionName);

    try (HttpSolrClient collectionClient = getHttpSolrClient(url)) {

      String splitKey = "b!";

      ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
      final DocRouter router = clusterState.getCollection(collectionName).getRouter();
      Slice shard1 = clusterState.getSlice(collectionName, SHARD1);
      DocRouter.Range shard1Range = shard1.getRange() != null ? shard1.getRange() : router.fullRange();
      final List<DocRouter.Range> ranges = ((CompositeIdRouter) router).partitionRangeByKey(splitKey, shard1Range);
      final int[] docCounts = new int[ranges.size()];

      int uniqIdentifier = (1 << 12);
      int splitKeyDocCount = 0;
      for (int i = 100; i <= 200; i++) {
        String shardKey = "" + (char) ('a' + (i % 26)); // See comment in ShardRoutingTest for hash distribution

        String idStr = shardKey + "!" + i;
        collectionClient.add(getDoc(id, idStr, "n_ti", (shardKey + "!").equals(splitKey) ? uniqIdentifier : i));
        int idx = getHashRangeIdx(router, ranges, idStr);
        if (idx != -1) {
          docCounts[idx]++;
        }
        if (splitKey.equals(shardKey + "!"))
          splitKeyDocCount++;
      }

      for (int i = 0; i < docCounts.length; i++) {
        int docCount = docCounts[i];
        log.info("Shard {} docCount = {}", "shard1_" + i, docCount);
      }
      log.info("Route key doc count = {}", splitKeyDocCount);

      collectionClient.commit();

      for (int i = 0; i < 3; i++) {
        try {
          splitShard(collectionName, null, null, splitKey);
          break;
        } catch (HttpSolrClient.RemoteSolrException e) {
          if (e.code() != 500) {
            throw e;
          }
          log.error("SPLITSHARD failed. " + (i < 2 ? " Retring split" : ""), e);
          if (i == 2) {
            fail("SPLITSHARD was not successful even after three tries");
          }
        }
      }

      waitForRecoveriesToFinish(collectionName, false);
      SolrQuery solrQuery = new SolrQuery("*:*");
      assertEquals("DocCount on shard1_0 does not match", docCounts[0], collectionClient.query(solrQuery.setParam("shards", "shard1_0")).getResults().getNumFound());
      assertEquals("DocCount on shard1_1 does not match", docCounts[1], collectionClient.query(solrQuery.setParam("shards", "shard1_1")).getResults().getNumFound());
      assertEquals("DocCount on shard1_2 does not match", docCounts[2], collectionClient.query(solrQuery.setParam("shards", "shard1_2")).getResults().getNumFound());

      solrQuery = new SolrQuery("n_ti:" + uniqIdentifier);
      assertEquals("shard1_0 must have 0 docs for route key: " + splitKey, 0, collectionClient.query(solrQuery.setParam("shards", "shard1_0")).getResults().getNumFound());
      assertEquals("Wrong number of docs on shard1_1 for route key: " + splitKey, splitKeyDocCount, collectionClient.query(solrQuery.setParam("shards", "shard1_1")).getResults().getNumFound());
      assertEquals("shard1_2 must have 0 docs for route key: " + splitKey, 0, collectionClient.query(solrQuery.setParam("shards", "shard1_2")).getResults().getNumFound());
    }
  }

  protected void checkDocCountsAndShardStates(int[] docCounts, int numReplicas) throws Exception {
    ClusterState clusterState = null;
    Slice slice1_0 = null, slice1_1 = null;
    int i = 0;
    for (i = 0; i < 10; i++) {
      ZkStateReader zkStateReader = cloudClient.getZkStateReader();
      clusterState = zkStateReader.getClusterState();
      slice1_0 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, "shard1_0");
      slice1_1 = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, "shard1_1");
      if (slice1_0.getState() == Slice.State.ACTIVE && slice1_1.getState() == Slice.State.ACTIVE) {
        break;
      }
      Thread.sleep(500);
    }

    log.info("ShardSplitTest waited for {} ms for shard state to be set to active", i * 500);

    assertNotNull("Cluster state does not contain shard1_0", slice1_0);
    assertNotNull("Cluster state does not contain shard1_0", slice1_1);
    assertSame("shard1_0 is not active", Slice.State.ACTIVE, slice1_0.getState());
    assertSame("shard1_1 is not active", Slice.State.ACTIVE, slice1_1.getState());
    assertEquals("Wrong number of replicas created for shard1_0", numReplicas, slice1_0.getReplicas().size());
    assertEquals("Wrong number of replicas created for shard1_1", numReplicas, slice1_1.getReplicas().size());

    commit();

    // can't use checkShardConsistency because it insists on jettys and clients for each shard
    checkSubShardConsistency(SHARD1_0);
    checkSubShardConsistency(SHARD1_1);

    SolrQuery query = new SolrQuery("*:*").setRows(1000).setFields("id", "_version_");
    query.set("distrib", false);

    ZkCoreNodeProps shard1_0 = getLeaderUrlFromZk(AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1_0);
    QueryResponse response;
    try (HttpSolrClient shard1_0Client = getHttpSolrClient(shard1_0.getCoreUrl())) {
      response = shard1_0Client.query(query);
    }
    long shard10Count = response.getResults().getNumFound();

    ZkCoreNodeProps shard1_1 = getLeaderUrlFromZk(
        AbstractDistribZkTestBase.DEFAULT_COLLECTION, SHARD1_1);
    QueryResponse response2;
    try (HttpSolrClient shard1_1Client = getHttpSolrClient(shard1_1.getCoreUrl())) {
      response2 = shard1_1Client.query(query);
    }
    long shard11Count = response2.getResults().getNumFound();

    logDebugHelp(docCounts, response, shard10Count, response2, shard11Count);

    assertEquals("Wrong doc count on shard1_0. See SOLR-5309", docCounts[0], shard10Count);
    assertEquals("Wrong doc count on shard1_1. See SOLR-5309", docCounts[1], shard11Count);
  }

  protected void checkSubShardConsistency(String shard) throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery("*:*").setRows(1000).setFields("id", "_version_");
    query.set("distrib", false);

    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    Slice slice = clusterState.getSlice(AbstractDistribZkTestBase.DEFAULT_COLLECTION, shard);
    long[] numFound = new long[slice.getReplicasMap().size()];
    int c = 0;
    for (Replica replica : slice.getReplicas()) {
      String coreUrl = new ZkCoreNodeProps(replica).getCoreUrl();
      QueryResponse response;
      try (HttpSolrClient client = getHttpSolrClient(coreUrl)) {
        response = client.query(query);
      }
      numFound[c++] = response.getResults().getNumFound();
      log.info("Shard: " + shard + " Replica: {} has {} docs", coreUrl, String.valueOf(response.getResults().getNumFound()));
      assertTrue("Shard: " + shard + " Replica: " + coreUrl + " has 0 docs", response.getResults().getNumFound() > 0);
    }
    for (int i = 0; i < slice.getReplicasMap().size(); i++) {
      assertEquals(shard + " is not consistent", numFound[0], numFound[i]);
    }
  }

  protected void splitShard(String collection, String shardId, List<DocRouter.Range> subRanges, String splitKey) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionParams.CollectionAction.SPLITSHARD.toString());
    params.set("collection", collection);
    if (shardId != null)  {
      params.set("shard", shardId);
    }
    if (subRanges != null)  {
      StringBuilder ranges = new StringBuilder();
      for (int i = 0; i < subRanges.size(); i++) {
        DocRouter.Range subRange = subRanges.get(i);
        ranges.append(subRange.toString());
        if (i < subRanges.size() - 1)
          ranges.append(",");
      }
      params.set("ranges", ranges.toString());
    }
    if (splitKey != null) {
      params.set("split.key", splitKey);
    }
    SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    String baseUrl = ((HttpSolrClient) shardToJetty.get(SHARD1).get(0).client.solrClient)
        .getBaseURL();
    baseUrl = baseUrl.substring(0, baseUrl.length() - "collection1".length());

    try (HttpSolrClient baseServer = getHttpSolrClient(baseUrl)) {
      baseServer.setConnectionTimeout(30000);
      baseServer.setSoTimeout(60000 * 5);
      baseServer.request(request);
    }
  }

  protected void indexAndUpdateCount(DocRouter router, List<DocRouter.Range> ranges, int[] docCounts, String id, int n) throws Exception {
    index("id", id, "n_ti", n);

    int idx = getHashRangeIdx(router, ranges, id);
    if (idx != -1)  {
      docCounts[idx]++;
    }
  }

  protected void deleteAndUpdateCount(DocRouter router, List<DocRouter.Range> ranges, int[] docCounts, String id) throws Exception {
    controlClient.deleteById(id);
    cloudClient.deleteById(id);

    int idx = getHashRangeIdx(router, ranges, id);
    if (idx != -1)  {
      docCounts[idx]--;
    }
  }

  public static int getHashRangeIdx(DocRouter router, List<DocRouter.Range> ranges, String id) {
    int hash = 0;
    if (router instanceof HashBasedRouter) {
      HashBasedRouter hashBasedRouter = (HashBasedRouter) router;
      hash = hashBasedRouter.sliceHash(id, null, null,null);
    }
    for (int i = 0; i < ranges.size(); i++) {
      DocRouter.Range range = ranges.get(i);
      if (range.includes(hash))
        return i;
    }
    return -1;
  }

  protected void logDebugHelp(int[] docCounts, QueryResponse response, long shard10Count, QueryResponse response2, long shard11Count) {
    for (int i = 0; i < docCounts.length; i++) {
      int docCount = docCounts[i];
      log.info("Expected docCount for shard1_{} = {}", i, docCount);
    }

    log.info("Actual docCount for shard1_0 = {}", shard10Count);
    log.info("Actual docCount for shard1_1 = {}", shard11Count);
    Map<String, String> idVsVersion = new HashMap<>();
    Map<String, SolrDocument> shard10Docs = new HashMap<>();
    Map<String, SolrDocument> shard11Docs = new HashMap<>();
    for (int i = 0; i < response.getResults().size(); i++) {
      SolrDocument document = response.getResults().get(i);
      idVsVersion.put(document.getFieldValue("id").toString(), document.getFieldValue("_version_").toString());
      SolrDocument old = shard10Docs.put(document.getFieldValue("id").toString(), document);
      if (old != null) {
        log.error("EXTRA: ID: " + document.getFieldValue("id") + " on shard1_0. Old version: " + old.getFieldValue("_version_") + " new version: " + document.getFieldValue("_version_"));
      }
    }
    for (int i = 0; i < response2.getResults().size(); i++) {
      SolrDocument document = response2.getResults().get(i);
      String value = document.getFieldValue("id").toString();
      String version = idVsVersion.get(value);
      if (version != null) {
        log.error("DUPLICATE: ID: " + value + " , shard1_0Version: " + version + " shard1_1Version:" + document.getFieldValue("_version_"));
      }
      SolrDocument old = shard11Docs.put(document.getFieldValue("id").toString(), document);
      if (old != null) {
        log.error("EXTRA: ID: " + document.getFieldValue("id") + " on shard1_1. Old version: " + old.getFieldValue("_version_") + " new version: " + document.getFieldValue("_version_"));
      }
    }
  }

  @Override
  protected SolrClient createNewSolrClient(String collection, String baseUrl) {
    HttpSolrClient client = (HttpSolrClient) super.createNewSolrClient(collection, baseUrl);
    client.setSoTimeout(5 * 60 * 1000);
    return client;
  }

  @Override
  protected SolrClient createNewSolrClient(int port) {
    HttpSolrClient client = (HttpSolrClient) super.createNewSolrClient(port);
    client.setSoTimeout(5 * 60 * 1000);
    return client;
  }

  @Override
  protected CloudSolrClient createCloudClient(String defaultCollection) {
    CloudSolrClient client = super.createCloudClient(defaultCollection);
    return client;
  }
}

