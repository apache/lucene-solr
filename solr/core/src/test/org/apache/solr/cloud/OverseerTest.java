package org.apache.solr.cloud;

/**
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

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.CoreState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.junit.BeforeClass;
import org.junit.Test;

public class OverseerTest extends SolrTestCaseJ4 {

  static final int TIMEOUT = 10000;
  private static final boolean DEBUG = false;

  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  @Test
  public void testShardAssignment() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);

    ZkController zkController = null;
    SolrZkClient zkClient = null;
    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);

      System.setProperty(ZkStateReader.NUM_SHARDS_PROP, "3");

      zkController = new ZkController(server.getZkAddress(), TIMEOUT, 10000,
          "localhost", "8983", "solr",3, new CurrentCoreDescriptorProvider() {

            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });

      System.setProperty("bootstrap_confdir", getFile("solr/conf")
          .getAbsolutePath());
      
      CloudDescriptor collection1Desc = new CloudDescriptor();
      collection1Desc.setCollectionName("collection1");

      CoreDescriptor desc = new CoreDescriptor(null, "core1", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard1 = zkController.register("core1", desc);
      collection1Desc.setShardId(null);
      desc = new CoreDescriptor(null, "core2", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard2 = zkController.register("core2", desc);
      collection1Desc.setShardId(null);
      desc = new CoreDescriptor(null, "core3", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard3 = zkController.register("core3", desc);
      collection1Desc.setShardId(null);
      desc = new CoreDescriptor(null, "core4", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard4 = zkController.register("core4", desc);
      collection1Desc.setShardId(null);
      desc = new CoreDescriptor(null, "core5", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard5 = zkController.register("core5", desc);
      collection1Desc.setShardId(null);
      desc = new CoreDescriptor(null, "core6", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard6 = zkController.register("core6", desc);
      collection1Desc.setShardId(null);

      assertEquals("shard1", shard1);
      assertEquals("shard2", shard2);
      assertEquals("shard3", shard3);
      assertEquals("shard1", shard4);
      assertEquals("shard2", shard5);
      assertEquals("shard3", shard6);

    } finally {
      if (DEBUG) {
        if (zkController != null) {
          zkController.printLayoutToStdOut();
        }
      }
      if (zkClient != null) {
        zkClient.close();
      }
      if (zkController != null) {
        zkController.close();
      }
      server.shutdown();
    }
    
    System.clearProperty(ZkStateReader.NUM_SHARDS_PROP);
  }
  
  //wait until i slices for collection have appeared 
  private void waitForSliceCount(ZkStateReader stateReader, String collection, int i) throws InterruptedException {
    waitForCollections(stateReader, collection);
    int maxIterations = 1000;
    while (0 < maxIterations--) {
      CloudState state = stateReader.getCloudState();
      Map<String,Slice> sliceMap = state.getSlices(collection);
      if (sliceMap.keySet().size() == i) {
        return;
      }
      Thread.sleep(50);
    }
  }

  //wait until collections are available
  private void waitForCollections(ZkStateReader stateReader, String... collections) throws InterruptedException {
    int maxIterations = 100;
    while (0 < maxIterations--) {
      Set<String> availableCollections = stateReader.getCloudState().getCollections();
      int availableCount = 0;
      for(String requiredCollection: collections) {
        if(availableCollections.contains(requiredCollection)) {
          availableCount++;
        }
        if(availableCount == collections.length) return;
        Thread.sleep(50);
      }
    }
  }
  
  @Test
  public void testStateChange() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    
    ZkTestServer server = new ZkTestServer(zkDir);
    
    SolrZkClient zkClient = null;
    ZkStateReader reader = null;
    
    try {
      server.run();
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      zkClient.makePath("/live_nodes");

      System.setProperty(ZkStateReader.NUM_SHARDS_PROP, "2");

      //live node
      String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + "node1";
      zkClient.makePath(nodePath,CreateMode.EPHEMERAL);

      reader = new ZkStateReader(zkClient);
      reader.createClusterStateWatchersAndUpdate();

      OverseerElector elector1 = new OverseerElector(zkClient, reader);
      
      ElectionContext ec = new OverseerElectionContext("node1");
      elector1.setup(ec);
      elector1.joinElection(ec);
      
      Thread.sleep(1000);
      
      
      HashMap<String, String> coreProps = new HashMap<String,String>();
      coreProps.put(ZkStateReader.URL_PROP, "http://127.0.0.1/solr");
      coreProps.put(ZkStateReader.NODE_NAME_PROP, "node1");
      coreProps.put(ZkStateReader.ROLES_PROP, "");
      coreProps.put(ZkStateReader.STATE_PROP, ZkStateReader.RECOVERING);
      CoreState state = new CoreState("core1", "collection1", coreProps);
      
      nodePath = "/node_states/node1";

      try {
        zkClient.makePath(nodePath, CreateMode.EPHEMERAL);
      } catch (KeeperException ke) {
        if(ke.code()!=Code.NODEEXISTS) {
          throw ke;
        }
      }
      //publish node state (recovering)
      
      zkClient.setData(nodePath, ZkStateReader.toJSON(new CoreState[]{state}));

      //wait overseer assignment
      waitForSliceCount(reader, "collection1", 1);
      
      assertEquals("Illegal state", ZkStateReader.RECOVERING, reader.getCloudState().getSlice("collection1", "shard1").getShards().get("core1").get(ZkStateReader.STATE_PROP));

      //publish node state (active)
      coreProps.put(ZkStateReader.STATE_PROP, ZkStateReader.ACTIVE);
      
      coreProps.put(ZkStateReader.SHARD_ID_PROP, "shard1");
      state = new CoreState("core1", "collection1", coreProps);

      zkClient.setData(nodePath, ZkStateReader.toJSON(new CoreState[]{state}));
      
      Thread.sleep(2000); // wait for data to update
      
      assertEquals("Illegal state", ZkStateReader.ACTIVE, reader.getCloudState().getSlice("collection1", "shard1").getShards().get("core1").get(ZkStateReader.STATE_PROP));

    } finally {
      System.clearProperty(ZkStateReader.NUM_SHARDS_PROP);

      if (zkClient != null) {
        zkClient.close();
      }
      if (reader != null) {
        reader.close();
      }
      server.shutdown();
    }
    

  }
  
  @Test
  public void testOverseerFailure() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    
    ZkTestServer server = new ZkTestServer(zkDir);
    
    SolrZkClient zkClient = null;
    SolrZkClient zkClient2 = null;
    ZkStateReader reader = null;
    
    try {
      server.run();
      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      zkClient2 = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());
      zkClient.makePath("/live_nodes");
      
      // create collections
      Map<String,String> props = new HashMap<String,String>();
      props.put(ZkStateReader.NUM_SHARDS_PROP, "2");
      ZkNodeProps zkProps = new ZkNodeProps(props);
      zkClient.makePath("/collections/collection1",
          ZkStateReader.toJSON(zkProps));

      reader = new ZkStateReader(zkClient2);
      reader.createClusterStateWatchersAndUpdate();

      OverseerElector elector1 = new OverseerElector(zkClient, reader);
      
      ElectionContext ec = new OverseerElectionContext("node1");
      elector1.setup(ec);
      elector1.joinElection(ec);
      
      Thread.sleep(50);


      OverseerElector elector2 = new OverseerElector(zkClient2, reader);
      
      elector2.setup(ec);
      elector2.joinElection(ec);
      
      // live node
      String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + "node1";
      zkClient2.makePath(nodePath, CreateMode.EPHEMERAL);
      
      
      HashMap<String,String> coreProps = new HashMap<String,String>();
      coreProps.put(ZkStateReader.STATE_PROP, ZkStateReader.RECOVERING);
      CoreState state = new CoreState("core1", "collection1", coreProps);
      
      nodePath = "/node_states/node1";
      
      try {
        zkClient2.makePath(nodePath, CreateMode.EPHEMERAL);
      } catch (KeeperException ke) {
        if (ke.code() != Code.NODEEXISTS) {
          throw ke;
        }
      }
      // publish node state (recovering)
      zkClient2.setData(nodePath, ZkStateReader.toJSON(new CoreState[] {state}));
      
      // wait overseer assignment
      waitForSliceCount(reader, "collection1", 1);
      
      assertEquals("Illegal state", ZkStateReader.RECOVERING,
          reader.getCloudState().getSlice("collection1", "shard1").getShards()
              .get("core1").get(ZkStateReader.STATE_PROP));
      
      //zkClient2.printLayoutToStdOut();
      // close overseer client (kills current overseer)
      zkClient.close();
      zkClient = null;
      
      // publish node state (active)
      coreProps.put(ZkStateReader.STATE_PROP, ZkStateReader.ACTIVE);
      coreProps.put(ZkStateReader.SHARD_ID_PROP, "shard1");
      state = new CoreState("core1", "collection1", coreProps);
      
      zkClient2
          .setData(nodePath, ZkStateReader.toJSON(new CoreState[] {state}));
      
      Thread.sleep(1000); // wait for data to update
      
      // zkClient2.printLayoutToStdOut();
      
      assertEquals("Illegal state", ZkStateReader.ACTIVE,
          reader.getCloudState().getSlice("collection1", "shard1").getShards()
              .get("core1").get(ZkStateReader.STATE_PROP));
      
    } finally {
      
      if (zkClient != null) {
       zkClient.close();
      }
      if (zkClient2 != null) {
        zkClient2.close();
      }
      if (reader != null) {
        reader.close();
      }
      server.shutdown();
    }
  }
}