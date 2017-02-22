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

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import static org.apache.solr.cloud.AbstractDistribZkTestBase.verifyReplicaStatus;

@Slow
public class OverseerTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final int TIMEOUT = 30000;

  private List<Overseer> overseers = new ArrayList<>();
  private List<ZkStateReader> readers = new ArrayList<>();
  private List<HttpShardHandlerFactory> httpShardHandlerFactorys = new ArrayList<>();
  private List<UpdateShardHandler> updateShardHandlers = new ArrayList<>();
  
  final private String collection = SolrTestCaseJ4.DEFAULT_TEST_COLLECTION_NAME;
  
  public static class MockZKController{
    
    private final SolrZkClient zkClient;
    private final ZkStateReader zkStateReader;
    private final String nodeName;
    private final Map<String, ElectionContext> electionContext = Collections.synchronizedMap(new HashMap<String, ElectionContext>());
    
    public MockZKController(String zkAddress, String nodeName) throws InterruptedException, TimeoutException, IOException, KeeperException {
      this.nodeName = nodeName;
      zkClient = new SolrZkClient(zkAddress, TIMEOUT);

      ZkController.createClusterZkNodes(zkClient);

      zkStateReader = new ZkStateReader(zkClient);
      zkStateReader.createClusterStateWatchersAndUpdate();
      
      // live node
      final String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
      zkClient.makePath(nodePath, CreateMode.EPHEMERAL, true);
    }

    private void deleteNode(final String path) {
      
      try {
        zkClient.delete(path, -1, true);
      } catch (NoNodeException e) {
        // fine
        log.warn("cancelElection did not find election node to remove");
      } catch (KeeperException e) {
        fail("Unexpected KeeperException!" + e);
      } catch (InterruptedException e) {
        fail("Unexpected InterruptedException!" + e);
      }
    }

    public void close() {
      for (ElectionContext ec : electionContext.values()) {
        try {
          ec.cancelElection();
        } catch (Exception e) {
          log.warn(String.format(Locale.ROOT, "Error cancelling election for %s", ec.id), e);
        }
      }
      deleteNode(ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName);
      zkStateReader.close();
      zkClient.close();
    }
    
    public String publishState(String collection, String coreName, String coreNodeName, Replica.State stateName, int numShards)
        throws KeeperException, InterruptedException, IOException {
      if (stateName == null) {
        ElectionContext ec = electionContext.remove(coreName);
        if (ec != null) {
          ec.cancelElection();
        }
        ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
            ZkStateReader.NODE_NAME_PROP, nodeName,
            ZkStateReader.CORE_NAME_PROP, coreName,
            ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName,
            ZkStateReader.COLLECTION_PROP, collection);
            DistributedQueue q = Overseer.getStateUpdateQueue(zkClient);
            q.offer(Utils.toJSON(m));
         return null;
      } else {
        ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
        ZkStateReader.STATE_PROP, stateName.toString(),
        ZkStateReader.NODE_NAME_PROP, nodeName,
        ZkStateReader.CORE_NAME_PROP, coreName,
        ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName,
        ZkStateReader.COLLECTION_PROP, collection,
        ZkStateReader.NUM_SHARDS_PROP, Integer.toString(numShards),
        ZkStateReader.BASE_URL_PROP, "http://" + nodeName + "/solr/");
        DistributedQueue q = Overseer.getStateUpdateQueue(zkClient);
        q.offer(Utils.toJSON(m));
      }
      
      if (collection.length() > 0) {
        for (int i = 0; i < 120; i++) {
          String shardId = getShardId(collection, coreNodeName);
          if (shardId != null) {
            ElectionContext prevContext = electionContext.get(coreName);
            if (prevContext != null) {
              prevContext.cancelElection();
            }

            try {
              zkClient.makePath("/collections/" + collection + "/leader_elect/"
                  + shardId + "/election", true);
            } catch (NodeExistsException nee) {}
            ZkNodeProps props = new ZkNodeProps(ZkStateReader.BASE_URL_PROP,
                "http://" + nodeName + "/solr/", ZkStateReader.NODE_NAME_PROP,
                nodeName, ZkStateReader.CORE_NAME_PROP, coreName,
                ZkStateReader.SHARD_ID_PROP, shardId,
                ZkStateReader.COLLECTION_PROP, collection,
                ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName);
            LeaderElector elector = new LeaderElector(zkClient);
            ShardLeaderElectionContextBase ctx = new ShardLeaderElectionContextBase(
                elector, shardId, collection, nodeName + "_" + coreName, props,
                zkStateReader);
            elector.setup(ctx);
            electionContext.put(coreName, ctx);
            elector.joinElection(ctx, false);
            return shardId;
          }
          Thread.sleep(500);
        }
      }
      return null;
    }
    
    private String getShardId(String collection, String coreNodeName) {
      Map<String,Slice> slices = zkStateReader.getClusterState().getSlicesMap(collection);
      if (slices != null) {
        for (Slice slice : slices.values()) {
          for (Replica replica : slice.getReplicas()) {
            String cnn = replica.getName();
            if (coreNodeName.equals(cnn)) {
              return slice.getName();
            }
          }
        }
      }
      return null;
    }
  }    
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    Thread.sleep(3000); //XXX wait for threads to die...
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    for (Overseer overseer : overseers) {
      overseer.close();
    }
    overseers.clear();
    for (ZkStateReader reader : readers) {
      reader.close();
    }
    readers.clear();
    
    for (HttpShardHandlerFactory handlerFactory : httpShardHandlerFactorys) {
      handlerFactory.close();
    }
    httpShardHandlerFactorys.clear();
    
    for (UpdateShardHandler updateShardHandler : updateShardHandlers) {
      updateShardHandler.close();
    }
    updateShardHandlers.clear();
  }

  @Test
  public void testShardAssignment() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);

    MockZKController zkController = null;
    SolrZkClient zkClient = null;
    SolrZkClient overseerClient = null;

    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      overseerClient = electNewOverseer(server.getZkAddress());

      ZkStateReader reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      
      zkController = new MockZKController(server.getZkAddress(), "127.0.0.1");

      final int numShards=6;
      
      for (int i = 0; i < numShards; i++) {
        assertNotNull("shard got no id?", zkController.publishState(collection, "core" + (i+1), "node" + (i+1), Replica.State.ACTIVE, 3));
      }
      final Map<String,Replica> rmap = reader.getClusterState().getSlice(collection, "shard1").getReplicasMap();
      assertEquals(rmap.toString(), 2, rmap.size());
      assertEquals(rmap.toString(), 2, reader.getClusterState().getSlice(collection, "shard2").getReplicasMap().size());
      assertEquals(rmap.toString(), 2, reader.getClusterState().getSlice(collection, "shard3").getReplicasMap().size());
      
      //make sure leaders are in cloud state
      assertNotNull(reader.getLeaderUrl(collection, "shard1", 15000));
      assertNotNull(reader.getLeaderUrl(collection, "shard2", 15000));
      assertNotNull(reader.getLeaderUrl(collection, "shard3", 15000));
      
    } finally {
      close(zkClient);
      if (zkController != null) {
        zkController.close();
      }
      close(overseerClient);
      server.shutdown();
    }
  }

  @Test
  public void testBadQueueItem() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);

    MockZKController zkController = null;
    SolrZkClient zkClient = null;
    SolrZkClient overseerClient = null;

    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      overseerClient = electNewOverseer(server.getZkAddress());

      ZkStateReader reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      
      zkController = new MockZKController(server.getZkAddress(), "127.0.0.1");

      final int numShards=3;
      
      for (int i = 0; i < numShards; i++) {
        assertNotNull("shard got no id?", zkController.publishState(collection, "core" + (i+1), "node" + (i+1), Replica.State.ACTIVE, 3));
      }

      assertEquals(1, reader.getClusterState().getSlice(collection, "shard1").getReplicasMap().size());
      assertEquals(1, reader.getClusterState().getSlice(collection, "shard2").getReplicasMap().size());
      assertEquals(1, reader.getClusterState().getSlice(collection, "shard3").getReplicasMap().size());
      
      //make sure leaders are in cloud state
      assertNotNull(reader.getLeaderUrl(collection, "shard1", 15000));
      assertNotNull(reader.getLeaderUrl(collection, "shard2", 15000));
      assertNotNull(reader.getLeaderUrl(collection, "shard3", 15000));
      
      // publish a bad queue item
      String emptyCollectionName = "";
      zkController.publishState(emptyCollectionName, "core0", "node0", Replica.State.ACTIVE, 1);
      zkController.publishState(emptyCollectionName, "core0", "node0", null, 1);
      
      // make sure the Overseer is still processing items
      for (int i = 0; i < numShards; i++) {
        assertNotNull("shard got no id?", zkController.publishState("collection2", "core" + (i + 1), "node" + (i + 1), Replica.State.ACTIVE, 3));
      }

      assertEquals(1, reader.getClusterState().getSlice("collection2", "shard1").getReplicasMap().size());
      assertEquals(1, reader.getClusterState().getSlice("collection2", "shard2").getReplicasMap().size());
      assertEquals(1, reader.getClusterState().getSlice("collection2", "shard3").getReplicasMap().size());
      
      //make sure leaders are in cloud state
      assertNotNull(reader.getLeaderUrl("collection2", "shard1", 15000));
      assertNotNull(reader.getLeaderUrl("collection2", "shard2", 15000));
      assertNotNull(reader.getLeaderUrl("collection2", "shard3", 15000));
      
    } finally {
      close(zkClient);
      if (zkController != null) {
        zkController.close();
      }
      close(overseerClient);
      server.shutdown();
    }
  }
  
  @Test
  public void testShardAssignmentBigger() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();

    final int nodeCount = random().nextInt(TEST_NIGHTLY ? 50 : 10)+(TEST_NIGHTLY ? 50 : 10)+1;   //how many simulated nodes (num of threads)
    final int coreCount = random().nextInt(TEST_NIGHTLY ? 100 : 11)+(TEST_NIGHTLY ? 100 : 11)+1; //how many cores to register
    final int sliceCount = random().nextInt(TEST_NIGHTLY ? 20 : 5)+1;  //how many slices
    
    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;
    SolrZkClient overseerClient = null;

    final MockZKController[] controllers = new MockZKController[nodeCount];
    final ExecutorService[] nodeExecutors = new ExecutorService[nodeCount];
    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);
      
      overseerClient = electNewOverseer(server.getZkAddress());

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      for (int i = 0; i < nodeCount; i++) {
        controllers[i] = new MockZKController(server.getZkAddress(), "node" + i);
      }      
      for (int i = 0; i < nodeCount; i++) {
        nodeExecutors[i] = ExecutorUtil.newMDCAwareFixedThreadPool(1, new DefaultSolrThreadFactory("testShardAssignment"));
      }
      
      final String[] ids = new String[coreCount];
      //register total of coreCount cores
      for (int i = 0; i < coreCount; i++) {
        final int slot = i;

        nodeExecutors[i % nodeCount].submit((Runnable) () -> {

          final String coreName = "core" + slot;

          try {
            ids[slot] = controllers[slot % nodeCount].publishState(collection, coreName, "node" + slot, Replica.State.ACTIVE, sliceCount);
          } catch (Throwable e) {
            e.printStackTrace();
            fail("register threw exception:" + e.getClass());
          }
        });
      }
      
      for (int i = 0; i < nodeCount; i++) {
        nodeExecutors[i].shutdown();
      }

      for (int i = 0; i < nodeCount; i++) {
        while (!nodeExecutors[i].awaitTermination(100, TimeUnit.MILLISECONDS));
      }
      
      // make sure all cores have been assigned a id in cloudstate
      int cloudStateSliceCount = 0;
      for (int i = 0; i < 40; i++) {
        cloudStateSliceCount = 0;
        ClusterState state = reader.getClusterState();
        final Map<String,Slice> slices = state.getSlicesMap(collection);
        if (slices != null) {
          for (String name : slices.keySet()) {
            cloudStateSliceCount += slices.get(name).getReplicasMap().size();
          }
          if (coreCount == cloudStateSliceCount) break;
        }

        Thread.sleep(200);
      }
      assertEquals("Unable to verify all cores have been assigned an id in cloudstate",
                   coreCount, cloudStateSliceCount);

      // make sure all cores have been returned an id
      int assignedCount = 0;
      for (int i = 0; i < 240; i++) {
        assignedCount = 0;
        for (int j = 0; j < coreCount; j++) {
          if (ids[j] != null) {
            assignedCount++;
          }
        }
        if (coreCount == assignedCount) {
          break;
        }
        Thread.sleep(1000);
      }
      assertEquals("Unable to verify all cores have been returned an id", 
                   coreCount, assignedCount);
      
      final HashMap<String, AtomicInteger> counters = new HashMap<>();
      for (int i = 1; i < sliceCount+1; i++) {
        counters.put("shard" + i, new AtomicInteger());
      }
      
      for (int i = 0; i < coreCount; i++) {
        final AtomicInteger ai = counters.get(ids[i]);
        assertNotNull("could not find counter for shard:" + ids[i], ai);
        ai.incrementAndGet();
      }

      for (String counter: counters.keySet()) {
        int count = counters.get(counter).intValue();
        int expectedCount = coreCount / sliceCount;
        int min = expectedCount - 1;
        int max = expectedCount + 1;
        if (count < min || count > max) {
          fail("Unevenly assigned shard ids, " + counter + " had " + count
              + ", expected: " + min + "-" + max);
        }
      }
      
      //make sure leaders are in cloud state
      for (int i = 0; i < sliceCount; i++) {
        assertNotNull(reader.getLeaderUrl(collection, "shard" + (i + 1), 15000));
      }

    } finally {
      close(zkClient);
      close(overseerClient);
      close(reader);
      for (int i = 0; i < controllers.length; i++)
        if (controllers[i] != null) {
          controllers[i].close();
        }
      server.shutdown();
      for (int i = 0; i < nodeCount; i++) {
        if (nodeExecutors[i] != null) {
          nodeExecutors[i].shutdownNow();
        }
      }
    }
  }

  //wait until collections are available
  private void waitForCollections(ZkStateReader stateReader, String... collections) throws InterruptedException, KeeperException {
    int maxIterations = 100;
    while (0 < maxIterations--) {
      final ClusterState state = stateReader.getClusterState();
      Set<String> availableCollections = state.getCollectionsMap().keySet();
      int availableCount = 0;
      for(String requiredCollection: collections) {
        if(availableCollections.contains(requiredCollection)) {
          availableCount++;
        }
        if(availableCount == collections.length) return;
        Thread.sleep(50);
      }
    }
    log.warn("Timeout waiting for collections: " + Arrays.asList(collections) + " state:" + stateReader.getClusterState());
  }
  
  @Test
  public void testStateChange() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    
    ZkTestServer server = new ZkTestServer(zkDir);
    
    SolrZkClient zkClient = null;
    ZkStateReader reader = null;
    SolrZkClient overseerClient = null;
    
    try {
      server.run();
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      overseerClient = electNewOverseer(server.getZkAddress());

      DistributedQueue q = Overseer.getStateUpdateQueue(zkClient);
      
      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
          ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.CORE_NAME_PROP, "core1",
          ZkStateReader.ROLES_PROP, "",
          ZkStateReader.STATE_PROP, Replica.State.RECOVERING.toString());
      
      q.offer(Utils.toJSON(m));
      
      waitForCollections(reader, collection);

      assertSame(reader.getClusterState().toString(), Replica.State.RECOVERING,
          reader.getClusterState().getSlice(collection, "shard1").getReplica("core_node1").getState());

      //publish node state (active)
      m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
          ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.CORE_NAME_PROP, "core1",
          ZkStateReader.ROLES_PROP, "",
          ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());

      q.offer(Utils.toJSON(m));

      verifyReplicaStatus(reader, "collection1", "shard1", "core_node1", Replica.State.ACTIVE);

    } finally {

      close(zkClient);
      close(overseerClient);

      close(reader);
      server.shutdown();
    }
  }
  
  private void verifyShardLeader(ZkStateReader reader, String collection, String shard, String expectedCore) throws InterruptedException, KeeperException {
    int maxIterations = 200;
    while(maxIterations-->0) {
      ZkNodeProps props =  reader.getClusterState().getLeader(collection, shard);
      if(props!=null) {
        if(expectedCore.equals(props.getStr(ZkStateReader.CORE_NAME_PROP))) {
          return;
        }
      }
      Thread.sleep(200);
    }
    
    assertEquals("Unexpected shard leader coll:" + collection + " shard:" + shard, expectedCore, (reader.getClusterState().getLeader(collection, shard)!=null)?reader.getClusterState().getLeader(collection, shard).getStr(ZkStateReader.CORE_NAME_PROP):null);
  }

  @Test
  public void testOverseerFailure() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    ZkTestServer server = new ZkTestServer(zkDir);
    

    SolrZkClient overseerClient = null;
    ZkStateReader reader = null;
    MockZKController mockController = null;
    
    SolrZkClient zkClient = null;
    try {

      final String core = "core1";
      final String core_node = "core_node1";
      final String shard = "shard1";
      final int numShards = 1;

      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);

      ZkController.createClusterZkNodes(zkClient);
      
      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      
      mockController = new MockZKController(server.getZkAddress(), "node1");
      
      overseerClient = electNewOverseer(server.getZkAddress());
      
      Thread.sleep(1000);
      mockController.publishState(collection, core, core_node,
          Replica.State.RECOVERING, numShards);
      
      waitForCollections(reader, collection);
      verifyReplicaStatus(reader, collection, "shard1", "core_node1", Replica.State.RECOVERING);
      
      int version = getClusterStateVersion(zkClient);
      
      mockController.publishState(collection, core, core_node, Replica.State.ACTIVE,
          numShards);
      
      while (version == getClusterStateVersion(zkClient));

      verifyReplicaStatus(reader, collection, "shard1", "core_node1", Replica.State.ACTIVE);
      version = getClusterStateVersion(zkClient);
      overseerClient.close();
      Thread.sleep(1000); // wait for overseer to get killed
      
      mockController.publishState(collection, core, core_node,
          Replica.State.RECOVERING, numShards);
      version = getClusterStateVersion(zkClient);
      
      overseerClient = electNewOverseer(server.getZkAddress());
      
      while (version == getClusterStateVersion(zkClient));

      verifyReplicaStatus(reader, collection, "shard1", "core_node1", Replica.State.RECOVERING);
      
      assertEquals("Live nodes count does not match", 1, reader
          .getClusterState().getLiveNodes().size());
      assertEquals(shard+" replica count does not match", 1, reader.getClusterState()
          .getSlice(collection, shard).getReplicasMap().size());
      version = getClusterStateVersion(zkClient);
      mockController.publishState(collection, core, core_node, null, numShards);
      while (version == getClusterStateVersion(zkClient));
      Thread.sleep(500);
      assertTrue(collection+" should remain after removal of the last core", // as of SOLR-5209 core removal does not cascade to remove the slice and collection
          reader.getClusterState().hasCollection(collection));
      assertTrue(core_node+" should be gone after publishing the null state",
          null == reader.getClusterState().getCollection(collection).getReplica(core_node));
    } finally {
      close(mockController);
      close(overseerClient);
      close(zkClient);
      close(reader);
      server.shutdown();
    }
  }

  @Test
  public void testOverseerStatsReset() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    ZkTestServer server = new ZkTestServer(zkDir);
    ZkStateReader reader = null;
    MockZKController mockController = null;

    SolrZkClient zkClient = null;
    try {
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);

      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      mockController = new MockZKController(server.getZkAddress(), "node1");

      LeaderElector overseerElector = new LeaderElector(zkClient);
      if (overseers.size() > 0) {
        overseers.get(overseers.size() -1).close();
        overseers.get(overseers.size() -1).getZkStateReader().getZkClient().close();
      }
      UpdateShardHandler updateShardHandler = new UpdateShardHandler(UpdateShardHandlerConfig.DEFAULT);
      updateShardHandlers.add(updateShardHandler);
      HttpShardHandlerFactory httpShardHandlerFactory = new HttpShardHandlerFactory();
      httpShardHandlerFactorys.add(httpShardHandlerFactory);
      Overseer overseer = new Overseer(httpShardHandlerFactory.getShardHandler(), updateShardHandler, "/admin/cores", reader, null,
          new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "").build());
      overseers.add(overseer);
      ElectionContext ec = new OverseerElectionContext(zkClient, overseer,
          server.getZkAddress().replaceAll("/", "_"));
      overseerElector.setup(ec);
      overseerElector.joinElection(ec, false);

      mockController.publishState(collection, "core1", "core_node1", Replica.State.ACTIVE, 1);

      assertNotNull(overseer.getStats());
      assertTrue((overseer.getStats().getSuccessCount(OverseerAction.STATE.toLower())) > 0);

      // shut it down
      overseer.close();
      ec.cancelElection();

      // start it again
      overseerElector.setup(ec);
      overseerElector.joinElection(ec, false);
      assertNotNull(overseer.getStats());
      assertEquals(0, (overseer.getStats().getSuccessCount(OverseerAction.STATE.toLower())));

    } finally {
      close(mockController);
      close(zkClient);
      close(reader);
      server.shutdown();
    }
  }
  
  private AtomicInteger killCounter = new AtomicInteger();

  private class OverseerRestarter implements Runnable{
    SolrZkClient overseerClient = null;
    public volatile boolean run = true;
    private final String zkAddress;

    public OverseerRestarter(String zkAddress) {
      this.zkAddress = zkAddress;
    }
    
    @Override
    public void run() {
      try {
        overseerClient = electNewOverseer(zkAddress);
        while (run) {
          if (killCounter.get()>0) {
            try {
              killCounter.decrementAndGet();
              log.info("Killing overseer.");
              overseerClient.close();
              overseerClient = electNewOverseer(zkAddress);
            } catch (Throwable e) {
              // e.printStackTrace();
            }
          }
          try {
            Thread.sleep(100);
          } catch (Throwable e) {
            // e.printStackTrace();
          }
        }
      } catch (Throwable t) {
        // ignore
      } finally {
        if (overseerClient != null) {
          try {
            overseerClient.close();
          } catch (Throwable t) {
            // ignore
          }
        }
      }
    }
  }
  
  @Test
  public void testShardLeaderChange() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    final ZkTestServer server = new ZkTestServer(zkDir);
    SolrZkClient controllerClient = null;
    ZkStateReader reader = null;
    MockZKController mockController = null;
    MockZKController mockController2 = null;
    OverseerRestarter killer = null;
    Thread killerThread = null;
    try {
      server.run();
      controllerClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      ZkController.createClusterZkNodes(controllerClient);

      killer = new OverseerRestarter(server.getZkAddress());
      killerThread = new Thread(killer);
      killerThread.start();

      reader = new ZkStateReader(controllerClient);
      reader.createClusterStateWatchersAndUpdate();

      for (int i = 0; i < atLeast(4); i++) {
        killCounter.incrementAndGet(); //for each round allow 1 kill
        mockController = new MockZKController(server.getZkAddress(), "node1");
        mockController.publishState(collection, "core1", "node1", Replica.State.ACTIVE,1);
        if(mockController2!=null) {
          mockController2.close();
          mockController2 = null;
        }
        mockController.publishState(collection, "core1", "node1",Replica.State.RECOVERING,1);
        mockController2 = new MockZKController(server.getZkAddress(), "node2");
        mockController.publishState(collection, "core1", "node1", Replica.State.ACTIVE,1);
        verifyShardLeader(reader, collection, "shard1", "core1");
        mockController2.publishState(collection, "core4", "node2", Replica.State.ACTIVE ,1);
        mockController.close();
        mockController = null;
        verifyShardLeader(reader, collection, "shard1", "core4");
      }
    } finally {
      if (killer != null) {
        killer.run = false;
        if (killerThread != null) {
          killerThread.join();
        }
      }
      close(mockController);
      close(mockController2);
      close(controllerClient);
      close(reader);
      server.shutdown();
    }
  }

  @Test
  public void testDoubleAssignment() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    
    ZkTestServer server = new ZkTestServer(zkDir);
    
    SolrZkClient controllerClient = null;
    SolrZkClient overseerClient = null;
    ZkStateReader reader = null;
    MockZKController mockController = null;
    
    try {
      server.run();
      controllerClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      ZkController.createClusterZkNodes(controllerClient);
      
      reader = new ZkStateReader(controllerClient);
      reader.createClusterStateWatchersAndUpdate();

      mockController = new MockZKController(server.getZkAddress(), "node1");
      
      overseerClient = electNewOverseer(server.getZkAddress());

      mockController.publishState(collection, "core1", "core_node1", Replica.State.RECOVERING, 1);

      waitForCollections(reader, "collection1");

      verifyReplicaStatus(reader, collection, "shard1", "core_node1", Replica.State.RECOVERING);

      mockController.close();

      int version = getClusterStateVersion(controllerClient);
      
      mockController = new MockZKController(server.getZkAddress(), "node1");
      mockController.publishState(collection, "core1", "core_node1", Replica.State.RECOVERING, 1);

      while (version == reader.getClusterState().getZkClusterStateVersion()) {
        Thread.sleep(100);
      }
      
      ClusterState state = reader.getClusterState();
      
      int numFound = 0;
      Map<String, DocCollection> collectionsMap = state.getCollectionsMap();
      for (Map.Entry<String, DocCollection> entry : collectionsMap.entrySet()) {
        DocCollection collection = entry.getValue();
        for (Slice slice : collection.getSlices()) {
          if (slice.getReplicasMap().get("core_node1") != null) {
            numFound++;
          }
        }
      }
      assertEquals("Shard was found more than once in ClusterState", 1,
          numFound);
    } finally {
      close(overseerClient);
      close(mockController);
      close(controllerClient);
      close(reader);
      server.shutdown();
    }
  }

  @Test
  public void testPlaceholders() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    
    ZkTestServer server = new ZkTestServer(zkDir);
    
    SolrZkClient controllerClient = null;
    SolrZkClient overseerClient = null;
    ZkStateReader reader = null;
    MockZKController mockController = null;
    
    try {
      server.run();
      controllerClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      ZkController.createClusterZkNodes(controllerClient);

      reader = new ZkStateReader(controllerClient);
      reader.createClusterStateWatchersAndUpdate();

      mockController = new MockZKController(server.getZkAddress(), "node1");
      
      overseerClient = electNewOverseer(server.getZkAddress());

      mockController.publishState(collection, "core1", "node1", Replica.State.RECOVERING, 12);

      waitForCollections(reader, collection);
      
      assertEquals("Slicecount does not match", 12, reader.getClusterState().getSlices(collection).size());
      
    } finally {
      close(overseerClient);
      close(mockController);
      close(controllerClient);
      close(reader);
      server.shutdown();
    }
  }

  @Test
  @Ignore
  public void testPerformance() throws Exception {
    String zkDir = createTempDir("OverseerTest.testPerformance").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient controllerClient = null;
    SolrZkClient overseerClient = null;
    ZkStateReader reader = null;
    MockZKController mockController = null;

    try {
      server.run();
      controllerClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      ZkController.createClusterZkNodes(controllerClient);

      reader = new ZkStateReader(controllerClient);
      reader.createClusterStateWatchersAndUpdate();

      mockController = new MockZKController(server.getZkAddress(), "node1");

      final int MAX_COLLECTIONS = 10, MAX_CORES = 10, MAX_STATE_CHANGES = 20000, STATE_FORMAT = 2;

      for (int i=0; i<MAX_COLLECTIONS; i++)  {
        ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower(),
            "name", "perf" + i,
            ZkStateReader.NUM_SHARDS_PROP, "1",
            "stateFormat", String.valueOf(STATE_FORMAT),
            ZkStateReader.REPLICATION_FACTOR, "1",
            ZkStateReader.MAX_SHARDS_PER_NODE, "1"
            );
        DistributedQueue q = Overseer.getStateUpdateQueue(controllerClient);
        q.offer(Utils.toJSON(m));
        controllerClient.makePath("/collections/perf" + i, true);
      }

      for (int i = 0, j = 0, k = 0; i < MAX_STATE_CHANGES; i++, j++, k++) {
        ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
            ZkStateReader.STATE_PROP, Replica.State.RECOVERING.toString(),
            ZkStateReader.NODE_NAME_PROP,  "node1",
            ZkStateReader.CORE_NAME_PROP, "core" + k,
            ZkStateReader.CORE_NODE_NAME_PROP, "node1",
            ZkStateReader.COLLECTION_PROP, "perf" + j,
            ZkStateReader.NUM_SHARDS_PROP, "1",
            ZkStateReader.BASE_URL_PROP, "http://" +  "node1"
            + "/solr/");
        DistributedQueue q = Overseer.getStateUpdateQueue(controllerClient);
        q.offer(Utils.toJSON(m));
        if (j >= MAX_COLLECTIONS - 1) j = 0;
        if (k >= MAX_CORES - 1) k = 0;
        if (i > 0 && i % 100 == 0) log.info("Published {} items", i);
      }

      // let's publish a sentinel collection which we'll use to wait for overseer to complete operations
      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
          ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString(),
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.CORE_NAME_PROP, "core1",
          ZkStateReader.CORE_NODE_NAME_PROP, "node1",
          ZkStateReader.COLLECTION_PROP, "perf_sentinel",
          ZkStateReader.NUM_SHARDS_PROP, "1",
          ZkStateReader.BASE_URL_PROP, "http://" + "node1"
          + "/solr/");
      DistributedQueue q = Overseer.getStateUpdateQueue(controllerClient);
      q.offer(Utils.toJSON(m));

      Timer t = new Timer();
      Timer.Context context = t.time();
      try {
        overseerClient = electNewOverseer(server.getZkAddress());
        assertTrue(overseers.size() > 0);

        while (true)  {
          ClusterState state = reader.getClusterState();
          if (state.hasCollection("perf_sentinel")) {
            break;
          }
          Thread.sleep(1000);
        }
      } finally {
        context.stop();
      }

      log.info("Overseer loop finished processing: ");
      printTimingStats(t);

      Overseer overseer = overseers.get(0);
      Overseer.Stats stats = overseer.getStats();

      String[] interestingOps = {"state", "update_state", "am_i_leader", ""};
      Arrays.sort(interestingOps);
      for (Map.Entry<String, Overseer.Stat> entry : stats.getStats().entrySet()) {
        String op = entry.getKey();
        if (Arrays.binarySearch(interestingOps, op) < 0)
          continue;
        Overseer.Stat stat = entry.getValue();
        log.info("op: {}, success: {}, failure: {}", op, stat.success.get(), stat.errors.get());
        Timer timer = stat.requestTime;
        printTimingStats(timer);
      }

    } finally {
      close(overseerClient);
      close(mockController);
      close(controllerClient);
      close(reader);
      server.shutdown();
    }
  }

  private void printTimingStats(Timer timer) {
    Snapshot snapshot = timer.getSnapshot();
    log.info("\t avgRequestsPerSecond: {}", timer.getMeanRate());
    log.info("\t 5minRateRequestsPerSecond: {}", timer.getFiveMinuteRate());
    log.info("\t 15minRateRequestsPerSecond: {}", timer.getFifteenMinuteRate());
    log.info("\t avgTimePerRequest: {}", nsToMs(snapshot.getMean()));
    log.info("\t medianRequestTime: {}", nsToMs(snapshot.getMedian()));
    log.info("\t 75thPcRequestTime: {}", nsToMs(snapshot.get75thPercentile()));
    log.info("\t 95thPcRequestTime: {}", nsToMs(snapshot.get95thPercentile()));
    log.info("\t 99thPcRequestTime: {}", nsToMs(snapshot.get99thPercentile()));
    log.info("\t 999thPcRequestTime: {}", nsToMs(snapshot.get999thPercentile()));
  }

  private static long nsToMs(double ns) {
    return TimeUnit.NANOSECONDS.convert((long)ns, TimeUnit.MILLISECONDS);
  }

  private void close(MockZKController mockController) {
    if (mockController != null) {
      mockController.close();
    }
  }

  
  @Test
  public void testReplay() throws Exception{
    String zkDir = createTempDir().toFile().getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    ZkTestServer server = new ZkTestServer(zkDir);
    SolrZkClient zkClient = null;
    SolrZkClient overseerClient = null;
    ZkStateReader reader = null;
    
    try {
      server.run();
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      ZkController.createClusterZkNodes(zkClient);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();
      //prepopulate work queue with some items to emulate previous overseer died before persisting state
      DistributedQueue queue = Overseer.getInternalWorkQueue(zkClient, new Overseer.Stats());
      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
          ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.SHARD_ID_PROP, "s1",
          ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.CORE_NAME_PROP, "core1",
          ZkStateReader.ROLES_PROP, "",
          ZkStateReader.STATE_PROP, Replica.State.RECOVERING.toString());
      queue.offer(Utils.toJSON(m));
      m = new ZkNodeProps(Overseer.QUEUE_OPERATION, "state",
          ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.SHARD_ID_PROP, "s1",
          ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.CORE_NAME_PROP, "core2",
          ZkStateReader.ROLES_PROP, "",
          ZkStateReader.STATE_PROP, Replica.State.RECOVERING.toString());
      queue.offer(Utils.toJSON(m));
      
      overseerClient = electNewOverseer(server.getZkAddress());
      
      //submit to proper queue
      queue = Overseer.getStateUpdateQueue(zkClient);
      m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
          ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.SHARD_ID_PROP, "s1",
          ZkStateReader.COLLECTION_PROP, collection,
          ZkStateReader.CORE_NAME_PROP, "core3",
          ZkStateReader.ROLES_PROP, "",
          ZkStateReader.STATE_PROP, Replica.State.RECOVERING.toString());
      queue.offer(Utils.toJSON(m));
      
      for(int i=0;i<100;i++) {
        Slice s = reader.getClusterState().getSlice(collection, "s1");
        if(s!=null && s.getReplicasMap().size()==3) break;
        Thread.sleep(100);
      }
      assertNotNull(reader.getClusterState().getSlice(collection, "s1"));
      assertEquals(3, reader.getClusterState().getSlice(collection, "s1").getReplicasMap().size());
    } finally {
      close(overseerClient);
      close(zkClient);
      close(reader);
      server.shutdown();
    }
  }

  @Test
  public void testExternalClusterStateChangeBehavior() throws Exception {
    String zkDir = createTempDir("testExternalClusterStateChangeBehavior").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader reader = null;
    SolrZkClient overseerClient = null;

    try {
      server.run();
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      ZkController.createClusterZkNodes(zkClient);

      zkClient.create("/collections/test", null, CreateMode.PERSISTENT, true);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      overseerClient = electNewOverseer(server.getZkAddress());

      DistributedQueue q = Overseer.getStateUpdateQueue(zkClient);

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
          ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.COLLECTION_PROP, "c1",
          ZkStateReader.CORE_NAME_PROP, "core1",
          ZkStateReader.ROLES_PROP, "",
          ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());

      q.offer(Utils.toJSON(m));

      waitForCollections(reader, "c1");
      verifyReplicaStatus(reader, "c1", "shard1", "core_node1", Replica.State.DOWN);

      m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
          ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.COLLECTION_PROP, "c1",
          ZkStateReader.CORE_NAME_PROP, "core1",
          ZkStateReader.ROLES_PROP, "",
          ZkStateReader.STATE_PROP, Replica.State.RECOVERING.toString());

      q.offer(Utils.toJSON(m));


      m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
          ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.COLLECTION_PROP, "c1",
          ZkStateReader.CORE_NAME_PROP, "core1",
          ZkStateReader.ROLES_PROP, "",
          ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());

      q.offer(Utils.toJSON(m));

      Stat stat = new Stat();
      byte[] data = zkClient.getData("/clusterstate.json", null, stat, true);
      // Simulate an external modification
      zkClient.setData("/clusterstate.json", data, true);

      m = new ZkNodeProps(Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower(),
          "name", "test",
          ZkStateReader.NUM_SHARDS_PROP, "1",
          ZkStateReader.REPLICATION_FACTOR, "1",
          DocCollection.STATE_FORMAT, "2"
      );
      q.offer(Utils.toJSON(m));

      m = new ZkNodeProps(Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.CREATESHARD.toLower(),
          "collection", "test",
          ZkStateReader.SHARD_ID_PROP, "x",
          ZkStateReader.REPLICATION_FACTOR, "1"
      );
      q.offer(Utils.toJSON(m));

      m = new ZkNodeProps(Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.ADDREPLICA.toLower(),
          "collection", "test",
          ZkStateReader.SHARD_ID_PROP, "x",
          ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
          ZkStateReader.NODE_NAME_PROP, "node1",
          ZkStateReader.CORE_NAME_PROP, "core1",
          ZkStateReader.STATE_PROP, Replica.State.DOWN.toString()
      );
      q.offer(Utils.toJSON(m));

      waitForCollections(reader, "test");
      verifyReplicaStatus(reader, "test", "x", "core_node1", Replica.State.DOWN);

      waitForCollections(reader, "c1");
      verifyReplicaStatus(reader, "c1", "shard1", "core_node1", Replica.State.ACTIVE);

    } finally {
      close(zkClient);
      close(overseerClient);
      close(reader);
      server.shutdown();
    }
  }

  private void close(ZkStateReader reader) {
    if (reader != null) {
      reader.close();
    }
  }

  private void close(SolrZkClient client) throws InterruptedException {
    if (client != null) {
      client.close();
    }
  }
  
  private int getClusterStateVersion(SolrZkClient controllerClient)
      throws KeeperException, InterruptedException {
    return controllerClient.exists(ZkStateReader.CLUSTER_STATE, null, false).getVersion();
  }


  private SolrZkClient electNewOverseer(String address)
      throws InterruptedException, TimeoutException, IOException,
      KeeperException, ParserConfigurationException, SAXException {
    SolrZkClient zkClient = new SolrZkClient(address, TIMEOUT);
    ZkStateReader reader = new ZkStateReader(zkClient);
    readers.add(reader);
    LeaderElector overseerElector = new LeaderElector(zkClient);
    if (overseers.size() > 0) {
      overseers.get(overseers.size() -1).close();
      overseers.get(overseers.size() -1).getZkStateReader().getZkClient().close();
    }
    UpdateShardHandler updateShardHandler = new UpdateShardHandler(UpdateShardHandlerConfig.DEFAULT);
    updateShardHandlers.add(updateShardHandler);
    HttpShardHandlerFactory httpShardHandlerFactory = new HttpShardHandlerFactory();
    httpShardHandlerFactorys.add(httpShardHandlerFactory);
    Overseer overseer = new Overseer(httpShardHandlerFactory.getShardHandler(), updateShardHandler, "/admin/cores", reader, null,
        new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "").build());
    overseers.add(overseer);
    ElectionContext ec = new OverseerElectionContext(zkClient, overseer,
        address.replaceAll("/", "_"));
    overseerElector.setup(ec);
    overseerElector.joinElection(ec, false);
    return zkClient;
  }
  
  @Test
  public void testRemovalOfLastReplica() throws Exception {

    final Integer numReplicas = 1+random().nextInt(4); // between 1 and 4 replicas
    final Integer numShards = 1+random().nextInt(4); // between 1 and 4 shards

    final String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    final ZkTestServer server = new ZkTestServer(zkDir);

    SolrZkClient zkClient = null;
    ZkStateReader zkStateReader = null;
    SolrZkClient overseerClient = null;
    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      ZkController.createClusterZkNodes(zkClient);

      zkStateReader = new ZkStateReader(zkClient);
      zkStateReader.createClusterStateWatchersAndUpdate();

      overseerClient = electNewOverseer(server.getZkAddress());

      DistributedQueue q = Overseer.getStateUpdateQueue(zkClient);

      // create collection
      {
        final Integer maxShardsPerNode = numReplicas * numShards;
        ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.CREATE.toLower(),
            "name", collection,
            ZkStateReader.NUM_SHARDS_PROP, numShards.toString(),
            ZkStateReader.REPLICATION_FACTOR, "1",
            ZkStateReader.MAX_SHARDS_PER_NODE, maxShardsPerNode.toString()
            );
        q.offer(Utils.toJSON(m));
      }
      waitForCollections(zkStateReader, collection);

      // create nodes with state recovering
      for (int rr = 1; rr <= numReplicas; ++rr) {
        for (int ss = 1; ss <= numShards; ++ss) {
          final int N = (numReplicas-rr)*numShards + ss;
          ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
              ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
              ZkStateReader.SHARD_ID_PROP, "shard"+ss,
              ZkStateReader.NODE_NAME_PROP, "node"+N,
              ZkStateReader.COLLECTION_PROP, collection,
              ZkStateReader.CORE_NAME_PROP, "core"+N,
              ZkStateReader.ROLES_PROP, "",
              ZkStateReader.STATE_PROP, Replica.State.RECOVERING.toString());

          q.offer(Utils.toJSON(m));
        }
      }
      // verify recovering
      for (int rr = 1; rr <= numReplicas; ++rr) {
        for (int ss = 1; ss <= numShards; ++ss) {
          final int N = (numReplicas-rr)*numShards + ss;
          verifyReplicaStatus(zkStateReader, collection, "shard"+ss, "core_node"+N, Replica.State.RECOVERING);
        }
      }

      // publish node states (active)
      for (int rr = 1; rr <= numReplicas; ++rr) {
        for (int ss = 1; ss <= numShards; ++ss) {
          final int N = (numReplicas-rr)*numShards + ss;
          ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.STATE.toLower(),
              ZkStateReader.BASE_URL_PROP, "http://127.0.0.1/solr",
              ZkStateReader.NODE_NAME_PROP, "node"+N,
              ZkStateReader.COLLECTION_PROP, collection,
              ZkStateReader.CORE_NAME_PROP, "core"+N,
              ZkStateReader.ROLES_PROP, "",
              ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());

          q.offer(Utils.toJSON(m));
        }
      }
      // verify active
      for (int rr = 1; rr <= numReplicas; ++rr) {
        for (int ss = 1; ss <= numShards; ++ss) {
          final int N = (numReplicas-rr)*numShards + ss;
          verifyReplicaStatus(zkStateReader, collection, "shard"+ss, "core_node"+N, Replica.State.ACTIVE);
        }
      }

      // delete node
      for (int rr = 1; rr <= numReplicas; ++rr) {
        for (int ss = 1; ss <= numShards; ++ss) {
          final int N = (numReplicas-rr)*numShards + ss;
          ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
              ZkStateReader.COLLECTION_PROP, collection,
              ZkStateReader.CORE_NODE_NAME_PROP, "core_node"+N);

          q.offer(Utils.toJSON(m));

          {
            int iterationsLeft = 100;
            while (iterationsLeft-- > 0) {
              final Slice slice = zkStateReader.getClusterState().getSlice(collection, "shard"+ss);
              if (null == slice || null == slice.getReplicasMap().get("core_node"+N)) {
                break;
              }
              if (VERBOSE) log.info("still seeing {} shard{} core_node{}, rechecking in 50ms ({} iterations left)", collection, ss, N, iterationsLeft);
              Thread.sleep(50);
            }
          }

          final DocCollection docCollection = zkStateReader.getClusterState().getCollection(collection);
          assertTrue("found no "+collection, (null != docCollection));

          final Slice slice = docCollection.getSlice("shard"+ss);
          assertTrue("found no "+collection+" shard"+ss+" slice after removal of replica "+rr+" of "+numReplicas, (null != slice));

          final Collection<Replica> replicas = slice.getReplicas();
          assertEquals("wrong number of "+collection+" shard"+ss+" replicas left, replicas="+replicas, numReplicas-rr, replicas.size());
        }
      }

    } finally {

      close(overseerClient);
      close(zkStateReader);
      close(zkClient);

      server.shutdown();
    }
  }

}
