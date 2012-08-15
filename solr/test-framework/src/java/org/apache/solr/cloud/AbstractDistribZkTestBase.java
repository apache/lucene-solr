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
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

public abstract class AbstractDistribZkTestBase extends BaseDistributedSearchTestCase {
  
  protected static final String DEFAULT_COLLECTION = "collection1";
  private static final boolean DEBUG = false;
  protected ZkTestServer zkServer;
  private AtomicInteger homeCount = new AtomicInteger();

  @BeforeClass
  public static void beforeThisClass() throws Exception {
    useFactory(null);
  }


  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createTempDir();
    
    String zkDir = testDir.getAbsolutePath() + File.separator
    + "zookeeper/server1/data";
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    
    System.setProperty("zkHost", zkServer.getZkAddress());
    System.setProperty("enable.update.log", "true");
    System.setProperty("remove.version.field", "true");


    AbstractZkTestCase.buildZooKeeper(zkServer.getZkHost(), zkServer.getZkAddress(), "solrconfig.xml", "schema.xml");

    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
  }
  
  @Override
  protected void createServers(int numShards) throws Exception {
    // give everyone there own solrhome
    File controlHome = new File(new File(getSolrHome()).getParentFile(), "control" + homeCount.incrementAndGet());
    FileUtils.copyDirectory(new File(getSolrHome()), controlHome);
    
    System.setProperty("collection", "control_collection");
    controlJetty = createJetty(controlHome, null, "control_shard");
    System.clearProperty("collection");
    controlClient = createNewSolrServer(controlJetty.getLocalPort());

    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      // give everyone there own solrhome
      File jettyHome = new File(new File(getSolrHome()).getParentFile(), "jetty" + homeCount.incrementAndGet());
      FileUtils.copyDirectory(new File(getSolrHome()), jettyHome);
      JettySolrRunner j = createJetty(jettyHome, null, "shard" + (i + 2));
      jettys.add(j);
      clients.add(createNewSolrServer(j.getLocalPort()));
      sb.append("localhost:").append(j.getLocalPort()).append(context);
    }

    shards = sb.toString();
    
    // now wait till we see the leader for each shard
    for (int i = 1; i <= numShards; i++) {
      ZkStateReader zkStateReader = ((SolrDispatchFilter) jettys.get(0)
          .getDispatchFilter().getFilter()).getCores().getZkController()
          .getZkStateReader();
      zkStateReader.getLeaderProps("collection1", "shard" + (i + 2), 15000);
    }
  }
  
  protected void waitForRecoveriesToFinish(String collection, ZkStateReader zkStateReader, boolean verbose)
      throws Exception {
    waitForRecoveriesToFinish(collection, zkStateReader, verbose, true);
  }
  
  protected void waitForRecoveriesToFinish(String collection, ZkStateReader zkStateReader, boolean verbose, boolean failOnTimeout)
      throws Exception {
    waitForRecoveriesToFinish(collection, zkStateReader, verbose, failOnTimeout, 120 * (TEST_NIGHTLY ? 2 : 1) * RANDOM_MULTIPLIER);
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
      Map<String,Slice> slices = clusterState.getSlices(collection);
      for (Map.Entry<String,Slice> entry : slices.entrySet()) {
        Map<String,ZkNodeProps> shards = entry.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : shards.entrySet()) {
          if (verbose) System.out.println("rstate:"
              + shard.getValue().get(ZkStateReader.STATE_PROP)
              + " live:"
              + clusterState.liveNodesContain(shard.getValue().get(
                  ZkStateReader.NODE_NAME_PROP)));
          String state = shard.getValue().get(ZkStateReader.STATE_PROP);
          if ((state.equals(ZkStateReader.RECOVERING) || state
              .equals(ZkStateReader.SYNC) || state.equals(ZkStateReader.DOWN))
              && clusterState.liveNodesContain(shard.getValue().get(
                  ZkStateReader.NODE_NAME_PROP))) {
            sawLiveRecovering = true;
          }
        }
      }
      if (!sawLiveRecovering || cnt == timeoutSeconds) {
        if (!sawLiveRecovering) {
          if (verbose) System.out.println("no one is recoverying");
        } else {
          if (failOnTimeout) {
            fail("There are still nodes recoverying");
            printLayout();
            return;
          }
          if (verbose) System.out
              .println("gave up waiting for recovery to finish..");
        }
        cont = false;
      } else {
        Thread.sleep(1000);
      }
      cnt++;
    }
  }

  protected void assertAllActive(String collection,ZkStateReader zkStateReader)
      throws KeeperException, InterruptedException {

      zkStateReader.updateClusterState(true);
      ClusterState clusterState = zkStateReader.getClusterState();
      Map<String,Slice> slices = clusterState.getSlices(collection);
      if (slices == null) {
        throw new IllegalArgumentException("Cannot find collection:" + collection);
      }
      for (Map.Entry<String,Slice> entry : slices.entrySet()) {
        Map<String,ZkNodeProps> shards = entry.getValue().getShards();
        for (Map.Entry<String,ZkNodeProps> shard : shards.entrySet()) {

          String state = shard.getValue().get(ZkStateReader.STATE_PROP);
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
    zkServer.shutdown();
    System.clearProperty("zkHost");
    System.clearProperty("collection");
    System.clearProperty("enable.update.log");
    System.clearProperty("remove.version.field");
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.test.sys.prop1");
    System.clearProperty("solr.test.sys.prop2");
    resetExceptionIgnores();
    super.tearDown();
    
    JettySolrRunner.assertStoppedJetties();
  }
  
  protected void printLayout() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStdOut();
    zkClient.close();
  }
}
