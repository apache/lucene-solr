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

import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocCollectionWatcher;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZkStateReaderAccessor;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ZkContainer;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.Replica.State.DOWN;


public class DeleteReplicaTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.zkclienttimeout", "45000");
    System.setProperty("distribUpdateSoTimeout", "15000");

  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("solr.zkclienttimeout", "45000");
    System.setProperty("distribUpdateSoTimeout", "15000");
    
    // these tests need to be isolated, so we dont share the minicluster
    configureCluster(4)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }
  
  @After
  @Override
  public void tearDown() throws Exception {
    shutdownCluster();
    super.tearDown();
  }

  @Test
  // commented out on: 01-Apr-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void deleteLiveReplicaTest() throws Exception {

    final String collectionName = "delLiveColl";

    Create req = CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2);
    req.process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(collectionName, 2, 4);

    DocCollection state = getCollectionState(collectionName);
    Slice shard = getRandomShard(state);
    
    // don't choose the leader to shutdown, it just complicates things unneccessarily
    Replica replica = getRandomReplica(shard, (r) ->
                                       ( r.getState() == Replica.State.ACTIVE &&
                                         ! r.equals(shard.getLeader())));

    CoreStatus coreStatus = getCoreStatus(replica);
    Path dataDir = Paths.get(coreStatus.getDataDirectory());

    Exception e = expectThrows(Exception.class, () -> {
      CollectionAdminRequest.deleteReplica(collectionName, shard.getName(), replica.getName())
          .setOnlyIfDown(true)
          .process(cluster.getSolrClient());
    });
    assertTrue("Unexpected error message: " + e.getMessage(), e.getMessage().contains("state is 'active'"));
    assertTrue("Data directory for " + replica.getName() + " should not have been deleted", Files.exists(dataDir));

    JettySolrRunner replicaJetty = cluster.getReplicaJetty(replica);
    ZkStateReaderAccessor accessor = new ZkStateReaderAccessor(replicaJetty.getCoreContainer().getZkController().getZkStateReader());

    final long preDeleteWatcherCount = countUnloadCoreOnDeletedWatchers
      (accessor.getStateWatchers(collectionName));
    
    CollectionAdminRequest.deleteReplica(collectionName, shard.getName(), replica.getName())
        .process(cluster.getSolrClient());
    waitForState("Expected replica " + replica.getName() + " to have been removed", collectionName, (n, c) -> {
      Slice testShard = c.getSlice(shard.getName());
      return testShard.getReplica(replica.getName()) == null;
    });
    
    // the core should no longer have a watch collection state since it was removed
    // the core should no longer have a watch collection state since it was removed
    TimeOut timeOut = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor("Waiting for core's watcher to be removed", () -> {
        final long postDeleteWatcherCount = countUnloadCoreOnDeletedWatchers
          (accessor.getStateWatchers(collectionName));
        log.info("preDeleteWatcherCount={} vs postDeleteWatcherCount={}",
                 preDeleteWatcherCount, postDeleteWatcherCount);
        return (preDeleteWatcherCount - 1L == postDeleteWatcherCount);
      });

    assertFalse("Data directory for " + replica.getName() + " should have been removed", Files.exists(dataDir));

  }

  @Test
  public void deleteReplicaAndVerifyDirectoryCleanup() throws Exception {

    final String collectionName = "deletereplica_test";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2).process(cluster.getSolrClient());

    Replica leader = cluster.getSolrClient().getZkStateReader().getLeaderRetry(collectionName, "shard1");

    //Confirm that the instance and data directory exist
    CoreStatus coreStatus = getCoreStatus(leader);
    assertTrue("Instance directory doesn't exist", Files.exists(Paths.get(coreStatus.getInstanceDirectory())));
    assertTrue("DataDirectory doesn't exist", Files.exists(Paths.get(coreStatus.getDataDirectory())));

    CollectionAdminRequest.deleteReplica(collectionName, "shard1",leader.getName())
        .process(cluster.getSolrClient());

    Replica newLeader = cluster.getSolrClient().getZkStateReader().getLeaderRetry(collectionName, "shard1");

    assertFalse(leader.equals(newLeader));

    //Confirm that the instance and data directory were deleted by default
    assertFalse("Instance directory still exists", Files.exists(Paths.get(coreStatus.getInstanceDirectory())));
    assertFalse("DataDirectory still exists", Files.exists(Paths.get(coreStatus.getDataDirectory())));
  }

  @Test
  public void deleteReplicaByCount() throws Exception {

    final String collectionName = "deleteByCount";

    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 3).process(cluster.getSolrClient());
    waitForState("Expected a single shard with three replicas", collectionName, clusterShape(1, 3));

    CollectionAdminRequest.deleteReplicasFromShard(collectionName, "shard1", 2).process(cluster.getSolrClient());
    waitForState("Expected a single shard with a single replica", collectionName, clusterShape(1, 1));

    SolrException e = expectThrows(SolrException.class,
        "Can't delete the last replica by count",
        () -> CollectionAdminRequest.deleteReplicasFromShard(collectionName, "shard1", 1).process(cluster.getSolrClient())
    );
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, e.code());
    assertTrue(e.getMessage().contains("There is only one replica available"));
    DocCollection docCollection = getCollectionState(collectionName);
    // We know that since leaders are preserved, PULL replicas should not be left alone in the shard
    assertEquals(0, docCollection.getSlice("shard1").getReplicas(EnumSet.of(Replica.Type.PULL)).size());
  }

  @Test
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void deleteReplicaByCountForAllShards() throws Exception {

    final String collectionName = "deleteByCountNew";
    Create req = CollectionAdminRequest.createCollection(collectionName, "conf", 2, 2);
    req.process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(collectionName, 2, 4);
    
    waitForState("Expected two shards with two replicas each", collectionName, clusterShape(2, 4));

    CollectionAdminRequest.deleteReplicasFromAllShards(collectionName, 1).process(cluster.getSolrClient());
    waitForState("Expected two shards with one replica each", collectionName, clusterShape(2, 2));

  }

  @Test
  public void deleteReplicaFromClusterState() throws Exception {
    deleteReplicaFromClusterState("false");
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, null).process(cluster.getSolrClient());
  }
  
  @Test
  public void deleteReplicaFromClusterStateLegacy() throws Exception {
    deleteReplicaFromClusterState("true"); 
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, null).process(cluster.getSolrClient());
  }

  private void deleteReplicaFromClusterState(String legacyCloud) throws Exception {
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, legacyCloud).process(cluster.getSolrClient());
    final String collectionName = "deleteFromClusterState_"+legacyCloud;
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 3)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(collectionName, 1, 3);
    
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1"));
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "2"));
    cluster.getSolrClient().commit(collectionName);

    cluster.waitForActiveCollection(collectionName, 1, 3);

    Slice shard = getCollectionState(collectionName).getSlice("shard1");

    // don't choose the leader to shutdown, it just complicates things unneccessarily
    Replica replica = getRandomReplica(shard, (r) ->
                                       ( r.getState() == Replica.State.ACTIVE &&
                                         ! r.equals(shard.getLeader())));
    
    JettySolrRunner replicaJetty = cluster.getReplicaJetty(replica);
    ZkStateReaderAccessor accessor = new ZkStateReaderAccessor(replicaJetty.getCoreContainer().getZkController().getZkStateReader());

    final long preDeleteWatcherCount = countUnloadCoreOnDeletedWatchers
      (accessor.getStateWatchers(collectionName));

    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
        ZkStateReader.CORE_NAME_PROP, replica.getCoreName(),
        ZkStateReader.NODE_NAME_PROP, replica.getNodeName(),
        ZkStateReader.COLLECTION_PROP, collectionName,
        ZkStateReader.CORE_NODE_NAME_PROP, replica.getName(),
        ZkStateReader.BASE_URL_PROP, replica.getBaseUrl());

    cluster.getOpenOverseer().getStateUpdateQueue().offer(Utils.toJSON(m));

    waitForState("Timeout waiting for replica get deleted", collectionName,
        (liveNodes, collectionState) -> collectionState.getSlice("shard1").getReplicas().size() == 2);

    TimeOut timeOut = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor("Waiting for replica get unloaded", () ->
        replicaJetty.getCoreContainer().getCoreDescriptor(replica.getCoreName()) == null
    );
    
    // the core should no longer have a watch collection state since it was removed
    timeOut = new TimeOut(60, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor("Waiting for core's watcher to be removed", () -> {
        final long postDeleteWatcherCount = countUnloadCoreOnDeletedWatchers
          (accessor.getStateWatchers(collectionName));
        log.info("preDeleteWatcherCount={} vs postDeleteWatcherCount={}",
                 preDeleteWatcherCount, postDeleteWatcherCount);
        return (preDeleteWatcherCount - 1L == postDeleteWatcherCount);
      });
    
    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
  }

  @Test
  @Slow
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void raceConditionOnDeleteAndRegisterReplica() throws Exception {
    raceConditionOnDeleteAndRegisterReplica("false");
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, null).process(cluster.getSolrClient());
  }
  
  @Test
  @Slow
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void raceConditionOnDeleteAndRegisterReplicaLegacy() throws Exception {
    raceConditionOnDeleteAndRegisterReplica("true");
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, null).process(cluster.getSolrClient());
  }

  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void raceConditionOnDeleteAndRegisterReplica(String legacyCloud) throws Exception {
    
    CollectionAdminRequest.setClusterProperty(ZkStateReader.LEGACY_CLOUD, legacyCloud).process(cluster.getSolrClient());
    final String collectionName = "raceDeleteReplica_"+legacyCloud;
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(collectionName, 1, 2);
    
    waitForState("Expected 1x2 collections", collectionName, clusterShape(1, 2));

    Slice shard1 = getCollectionState(collectionName).getSlice("shard1");
    Replica leader = shard1.getLeader();
    JettySolrRunner leaderJetty = getJettyForReplica(leader);
    Replica replica1 = shard1.getReplicas(replica -> !replica.getName().equals(leader.getName())).get(0);
    assertFalse(replica1.getName().equals(leader.getName()));

    JettySolrRunner replica1Jetty = getJettyForReplica(replica1);

    String replica1JettyNodeName = replica1Jetty.getNodeName();

    Semaphore waitingForReplicaGetDeleted = new Semaphore(0);
    // for safety, we only want this hook get triggered one time
    AtomicInteger times = new AtomicInteger(0);
    ZkContainer.testing_beforeRegisterInZk = cd -> {
      if (cd.getCloudDescriptor() == null) return false;
      if (replica1.getName().equals(cd.getCloudDescriptor().getCoreNodeName())
          && collectionName.equals(cd.getCloudDescriptor().getCollectionName())) {
        if (times.incrementAndGet() > 1) {
          return false;
        }
        log.info("Running delete core {}",cd);

        try {
          ZkNodeProps m = new ZkNodeProps(
              Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
              ZkStateReader.CORE_NAME_PROP, replica1.getCoreName(),
              ZkStateReader.NODE_NAME_PROP, replica1.getNodeName(),
              ZkStateReader.COLLECTION_PROP, collectionName,
              ZkStateReader.CORE_NODE_NAME_PROP, replica1.getName(),
              ZkStateReader.BASE_URL_PROP, replica1.getBaseUrl());
          cluster.getOpenOverseer().getStateUpdateQueue().offer(Utils.toJSON(m));

          boolean replicaDeleted = false;
          TimeOut timeOut = new TimeOut(20, TimeUnit.SECONDS, TimeSource.NANO_TIME);
          while (!timeOut.hasTimedOut()) {
            try {
              ZkStateReader stateReader = replica1Jetty.getCoreContainer().getZkController().getZkStateReader();
              stateReader.forceUpdateCollection(collectionName);
              Slice shard = stateReader.getClusterState().getCollection(collectionName).getSlice("shard1");
              if (shard.getReplicas().size() == 1) {
                replicaDeleted = true;
                waitingForReplicaGetDeleted.release();
                break;
              }
              Thread.sleep(500);
            } catch (NullPointerException | SolrException e) {
              e.printStackTrace();
              Thread.sleep(500);
            }
          }
          if (!replicaDeleted) {
            fail("Timeout for waiting replica get deleted");
          }
        } catch (Exception e) {
          e.printStackTrace();
          fail("Failed to delete replica");
        } finally {
          //avoiding deadlock
          waitingForReplicaGetDeleted.release();
        }
        return true;
      }
      return false;
    };

    try {
      replica1Jetty.stop();
      waitForNodeLeave(replica1JettyNodeName);
      waitForState("Expected replica:"+replica1+" get down", collectionName, (liveNodes, collectionState)
          -> collectionState.getSlice("shard1").getReplica(replica1.getName()).getState() == DOWN);
      replica1Jetty.start();
      waitingForReplicaGetDeleted.acquire();
    } finally {
      ZkContainer.testing_beforeRegisterInZk = null;
    }

    TimeOut timeOut = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    timeOut.waitFor("Timeout adding replica to shard", () -> {
      try {
        CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
            .process(cluster.getSolrClient());
        return true;
      } catch (Exception e) {
        // expected, when the node is not fully started
        return false;
      }
    });
    waitForState("Expected 1x2 collections", collectionName, clusterShape(1, 2));

    shard1 = getCollectionState(collectionName).getSlice("shard1");
    Replica latestLeader = shard1.getLeader();
    leaderJetty = getJettyForReplica(latestLeader);
    String leaderJettyNodeName = leaderJetty.getNodeName();
    leaderJetty.stop();
    waitForNodeLeave(leaderJettyNodeName);

    waitForState("Expected new active leader", collectionName, (liveNodes, collectionState) -> {
      Slice shard = collectionState.getSlice("shard1");
      Replica newLeader = shard.getLeader();
      return newLeader != null && newLeader.getState() == Replica.State.ACTIVE && !newLeader.getName().equals(latestLeader.getName());
    });

    leaderJetty.start();
    cluster.waitForActiveCollection(collectionName, 1, 2);

    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
  }

  private JettySolrRunner getJettyForReplica(Replica replica) {
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      String nodeName = jetty.getNodeName();
      if (nodeName != null && nodeName.equals(replica.getNodeName())) return jetty;
    }
    throw new IllegalArgumentException("Can not find jetty for replica "+ replica);
  }


  private void waitForNodeLeave(String lostNodeName) throws InterruptedException {
    ZkStateReader reader = cluster.getSolrClient().getZkStateReader();
    TimeOut timeOut = new TimeOut(20, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (reader.getClusterState().getLiveNodes().contains(lostNodeName)) {
      Thread.sleep(100);
      if (timeOut.hasTimedOut()) fail("Wait for " + lostNodeName + " to leave failed!");
    }
  }

  @Test
  public void deleteReplicaOnIndexing() throws Exception {
    final String collectionName = "deleteReplicaOnIndexing";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 2)
        .process(cluster.getSolrClient());
    waitForState("", collectionName, clusterShape(1, 2));
    AtomicBoolean closed = new AtomicBoolean(false);
    Thread[] threads = new Thread[100];
    for (int i = 0; i < threads.length; i++) {
      int finalI = i;
      threads[i] = new Thread(() -> {
        int doc = finalI * 10000;
        while (!closed.get()) {
          try {
            cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", String.valueOf(doc++)));
          } catch (Exception e) {
            log.error("Failed on adding document to {}", collectionName, e);
          }
        }
      });
      threads[i].start();
    }

    Slice shard1 = getCollectionState(collectionName).getSlice("shard1");
    Replica nonLeader = shard1.getReplicas(rep -> !rep.getName().equals(shard1.getLeader().getName())).get(0);
    CollectionAdminRequest.deleteReplica(collectionName, "shard1", nonLeader.getName()).process(cluster.getSolrClient());
    closed.set(true);
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }

    try {
      cluster.getSolrClient().waitForState(collectionName, 20, TimeUnit.SECONDS, (liveNodes, collectionState) -> collectionState.getReplicas().size() == 1);
    } catch (TimeoutException e) {
      log.info("Timeout wait for state {}", getCollectionState(collectionName));
      throw e;
    }
  }

  /** 
   * Helper method for counting the number of instances of <code>UnloadCoreOnDeletedWatcher</code>
   * that exist on a given node.
   *
   * This is useful for verifying that deleting a replica correctly removed it's watchers.
   *
   * (Note: tests should not assert specific values, since multiple replicas may exist on the same 
   * node. Instead tests should only assert that the number of watchers has decreased by 1 per known 
   * replica removed)
   */
  private static final long countUnloadCoreOnDeletedWatchers(final Set<DocCollectionWatcher> watchers) {
    return watchers.stream().filter(w -> w instanceof ZkController.UnloadCoreOnDeletedWatcher).count();
  }
}

