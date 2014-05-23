package org.apache.solr.cloud;

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

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.core.MockDirectoryFactory;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractDistribZkTestBase extends BaseDistributedSearchTestCase {
  
  private static final String REMOVE_VERSION_FIELD = "remove.version.field";
  private static final String ENABLE_UPDATE_LOG = "enable.update.log";
  private static final String ZK_HOST = "zkHost";
  private static final String ZOOKEEPER_FORCE_SYNC = "zookeeper.forceSync";
  protected static final String DEFAULT_COLLECTION = "collection1";
  private static final boolean DEBUG = false;
  protected ZkTestServer zkServer;
  private AtomicInteger homeCount = new AtomicInteger();

  @BeforeClass
  public static void beforeThisClass() throws Exception {
    // Only For Manual Testing: this will force an fs based dir factory
    //useFactory(null);
  }


  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    String zkDir = testDir.getAbsolutePath() + File.separator
    + "zookeeper/server1/data";
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    
    System.setProperty(ZK_HOST, zkServer.getZkAddress());
    System.setProperty(ENABLE_UPDATE_LOG, "true");
    System.setProperty(REMOVE_VERSION_FIELD, "true");
    System.setProperty(ZOOKEEPER_FORCE_SYNC, "false");
    System.setProperty(MockDirectoryFactory.SOLR_TESTS_ALLOW_READING_FILES_STILL_OPEN_FOR_WRITE, "true");

    String schema = getSchemaFile();
    if (schema == null) schema = "schema.xml";
    AbstractZkTestCase.buildZooKeeper(zkServer.getZkHost(), zkServer.getZkAddress(), getCloudSolrConfig(), schema);

    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
  }
  
  protected String getCloudSolrConfig() {
    return "solrconfig-tlog.xml";
  }
  
  @Override
  protected void createServers(int numShards) throws Exception {
    // give everyone there own solrhome
    File controlHome = new File(new File(getSolrHome()).getParentFile(), "control" + homeCount.incrementAndGet());
    FileUtils.copyDirectory(new File(getSolrHome()), controlHome);
    setupJettySolrHome(controlHome);
    
    System.setProperty("collection", "control_collection");
    String numShardsS = System.getProperty(ZkStateReader.NUM_SHARDS_PROP);
    System.setProperty(ZkStateReader.NUM_SHARDS_PROP, "1");
    controlJetty = createJetty(controlHome, null);      // let the shardId default to shard1
    System.clearProperty("collection");
    if(numShardsS != null) {
      System.setProperty(ZkStateReader.NUM_SHARDS_PROP, numShardsS);
    } else {
      System.clearProperty(ZkStateReader.NUM_SHARDS_PROP);
    }

    controlClient = createNewSolrServer(controlJetty.getLocalPort());

    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      // give everyone there own solrhome
      File jettyHome = new File(new File(getSolrHome()).getParentFile(), "jetty" + homeCount.incrementAndGet());
      setupJettySolrHome(jettyHome);
      JettySolrRunner j = createJetty(jettyHome, null, "shard" + (i + 2));
      jettys.add(j);
      clients.add(createNewSolrServer(j.getLocalPort()));
      sb.append(buildUrl(j.getLocalPort()));
    }

    shards = sb.toString();
    
    // now wait till we see the leader for each shard
    for (int i = 1; i <= numShards; i++) {
      ZkStateReader zkStateReader = ((SolrDispatchFilter) jettys.get(0)
          .getDispatchFilter().getFilter()).getCores().getZkController()
          .getZkStateReader();
      zkStateReader.getLeaderRetry("collection1", "shard" + (i + 2), 15000);
    }
  }
  
  protected void waitForRecoveriesToFinish(String collection, ZkStateReader zkStateReader, boolean verbose)
      throws Exception {
    waitForRecoveriesToFinish(collection, zkStateReader, verbose, true);
  }
  
  protected void waitForRecoveriesToFinish(String collection, ZkStateReader zkStateReader, boolean verbose, boolean failOnTimeout)
      throws Exception {
    waitForRecoveriesToFinish(collection, zkStateReader, verbose, failOnTimeout, 330);
  }
  
  protected void waitForRecoveriesToFinish(String collection,
      ZkStateReader zkStateReader, boolean verbose, boolean failOnTimeout, int timeoutSeconds)
      throws Exception {
    log.info("Wait for recoveries to finish - collection: " + collection + " failOnTimeout:" + failOnTimeout + " timeout (sec):" + timeoutSeconds);
    boolean cont = true;
    int cnt = 0;
    
    while (cont) {
      if (verbose) System.out.println("-");
      boolean sawLiveRecovering = false;
      zkStateReader.updateClusterState(true);
      ClusterState clusterState = zkStateReader.getClusterState();
      Map<String,Slice> slices = clusterState.getSlicesMap(collection);
      assertNotNull("Could not find collection:" + collection, slices);
      for (Map.Entry<String,Slice> entry : slices.entrySet()) {
        Map<String,Replica> shards = entry.getValue().getReplicasMap();
        for (Map.Entry<String,Replica> shard : shards.entrySet()) {
          if (verbose) System.out.println("replica:" + shard.getValue().getName() + " rstate:"
              + shard.getValue().getStr(ZkStateReader.STATE_PROP)
              + " live:"
              + clusterState.liveNodesContain(shard.getValue().getNodeName()));
          String state = shard.getValue().getStr(ZkStateReader.STATE_PROP);
          if ((state.equals(ZkStateReader.RECOVERING) || state
              .equals(ZkStateReader.SYNC) || state.equals(ZkStateReader.DOWN))
              && clusterState.liveNodesContain(shard.getValue().getStr(
              ZkStateReader.NODE_NAME_PROP))) {
            sawLiveRecovering = true;
          }
        }
      }
      if (!sawLiveRecovering || cnt == timeoutSeconds) {
        if (!sawLiveRecovering) {
          if (verbose) System.out.println("no one is recoverying");
        } else {
          if (verbose) System.out.println("Gave up waiting for recovery to finish..");
          if (failOnTimeout) {
            Diagnostics.logThreadDumps("Gave up waiting for recovery to finish.  THREAD DUMP:");
            printLayout();
            fail("There are still nodes recoverying - waited for " + timeoutSeconds + " seconds");
            // won't get here
            return;
          }
        }
        cont = false;
      } else {
        Thread.sleep(1000);
      }
      cnt++;
    }

    log.info("Recoveries finished - collection: " + collection);
  }

  protected void assertAllActive(String collection,ZkStateReader zkStateReader)
      throws KeeperException, InterruptedException {

      zkStateReader.updateClusterState(true);
      ClusterState clusterState = zkStateReader.getClusterState();
      Map<String,Slice> slices = clusterState.getSlicesMap(collection);
      if (slices == null) {
        throw new IllegalArgumentException("Cannot find collection:" + collection);
      }
      for (Map.Entry<String,Slice> entry : slices.entrySet()) {
        Map<String,Replica> shards = entry.getValue().getReplicasMap();
        for (Map.Entry<String,Replica> shard : shards.entrySet()) {

          String state = shard.getValue().getStr(ZkStateReader.STATE_PROP);
          if (!state.equals(ZkStateReader.ACTIVE)) {
            fail("Not all shards are ACTIVE - found a shard that is: " + state);
          }
        }
      }
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    if (DEBUG) {
      printLayout();
    }
    System.clearProperty(ZK_HOST);
    System.clearProperty("collection");
    System.clearProperty(ENABLE_UPDATE_LOG);
    System.clearProperty(REMOVE_VERSION_FIELD);
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.test.sys.prop1");
    System.clearProperty("solr.test.sys.prop2");
    System.clearProperty(ZOOKEEPER_FORCE_SYNC);
    System.clearProperty(MockDirectoryFactory.SOLR_TESTS_ALLOW_READING_FILES_STILL_OPEN_FOR_WRITE);
    
    resetExceptionIgnores();
    super.tearDown();
    zkServer.shutdown();
  }
  
  protected void printLayout() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
}
