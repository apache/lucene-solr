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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;

import org.apache.zookeeper.CreateMode;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stress test LiveNodes watching.
 *
 * Does bursts of adds to live_nodes using parallel threads to and verifies that after each 
 * burst a ZkStateReader detects the correct set.
 */
@Slow
public class TestStressLiveNodes extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** A basic cloud client, we'll be testing the behavior of it's ZkStateReader */
  private static CloudSolrClient CLOUD_CLIENT;
  
  /** The addr of the zk server used in this test */
  private static String ZK_SERVER_ADDR;

  /* how many seconds we're willing to wait for our executor tasks to finish before failing the test */
  private final static int WAIT_TIME = TEST_NIGHTLY ? 60 : 30;

  @BeforeClass
  private static void createMiniSolrCloudCluster() throws Exception {

    // we only need 1 node, and we don't care about any configs or collections
    // we're going to fake all the live_nodes changes we want to fake.
    configureCluster(1).configure();
    
    CLOUD_CLIENT = cluster.getSolrClient();
    CLOUD_CLIENT.connect(); // force connection even though we aren't sending any requests
    
    ZK_SERVER_ADDR = cluster.getZkServer().getZkAddress();
    
  }
  
  @AfterClass
  private static void afterClass() throws Exception {
    if (null != CLOUD_CLIENT) {
      CLOUD_CLIENT.close();
      CLOUD_CLIENT = null;
    }
  }

  private static SolrZkClient newSolrZkClient() {
    assertNotNull(ZK_SERVER_ADDR);
    // WTF is CloudConfigBuilder.DEFAULT_ZK_CLIENT_TIMEOUT private?
    return new SolrZkClient(ZK_SERVER_ADDR, 15000);
  }

  /** returns the true set of live nodes (currently in zk) as a sorted list */
  private static List<String> getTrueLiveNodesFromZk() throws Exception {
    SolrZkClient client = newSolrZkClient();
    try {
      ArrayList<String> result = new ArrayList<>(client.getChildren(ZkStateReader.LIVE_NODES_ZKNODE, null, true));
      Collections.sort(result);
      return result;
    } finally {
      client.close();
    }
  }

  /** 
   * returns the cached set of live nodes (according to the ZkStateReader in our CloudSolrClient) 
   * as a sorted list. 
   * This is done in a sleep+retry loop until the result matches the expectedCount, or a few iters have passed
   * (this way we aren't testing how fast the watchers complete, just that they got the correct result)
   */
  private static List<String> getCachedLiveNodesFromLocalState(final int expectedCount) throws Exception {
    ArrayList<String> result = null;

    for (int i = 0; i < 10; i++) {
      result = new ArrayList<>(CLOUD_CLIENT.getZkStateReader().getClusterState().getLiveNodes());
      if (expectedCount != result.size()) {
        if (log.isInfoEnabled()) {
          log.info("sleeping #{} to give watchers a chance to finish: {} != {}",
              i, expectedCount, result.size());
        }
        Thread.sleep(200);
      } else {
        break;
      }
    }
    if (expectedCount != result.size()) {
      log.error("gave up waiting for live nodes to match expected size: {} != {}",
                expectedCount, result.size());
    }
    Collections.sort(result);
    return result;
  }
  
  public void testStress() throws Exception {

    // do many iters, so we have "bursts" of adding nodes that we then check
    final int numIters = atLeast(TEST_NIGHTLY ? 1000 : 100);
    for (int iter = 0; iter < numIters; iter++) {

      // sanity check that ZK says there is in fact 1 live node
      List<String> actualLiveNodes = getTrueLiveNodesFromZk();
      assertEquals("iter"+iter+": " + actualLiveNodes.toString(),
                   1, actualLiveNodes.size());

      // only here do we forcibly update the cached live nodes so we don't have to wait for it to catch up
      // with all the ephemeral nodes that vanished after the last iteration
      CLOUD_CLIENT.getZkStateReader().updateLiveNodes();
      
      // sanity check that our Cloud Client's local state knows about the 1 (real) live node in our cluster
      List<String> cachedLiveNodes = getCachedLiveNodesFromLocalState(actualLiveNodes.size());
      assertEquals("iter"+iter+" " + actualLiveNodes.size() + " != " + cachedLiveNodes.size(),
                   actualLiveNodes, cachedLiveNodes);
      
      
      // start spinning up some threads to add some live_node children in parallel

      // we don't need a lot of threads or nodes (we don't want to swamp the CPUs
      // just bursts of concurrent adds) but we do want to randomize it a bit so we increase the
      // odds of concurrent watchers firing regardless of the num CPUs or load on the machine running
      // the test (but we deliberately don't look at availableProcessors() since we want randomization
      // consistency across all machines for a given seed)
      final int numThreads = TestUtil.nextInt(random(), 2, 5);
      
      // use same num for all thrashers, to increase likely hood of them all competing
      // (diff random number would mean heavy concurrency only for ~ the first N=lowest num requests)
      //
      // this does not need to be a large number -- in fact, the higher it is, the more
      // likely we are to see a mistake in early watcher triggers get "corrected" by a later one
      // and overlook a possible bug
      final int numNodesPerThrasher = TestUtil.nextInt(random(), 1, 5);
      
      log.info("preparing parallel adds to live nodes: iter={}, numThreads={} numNodesPerThread={}",
               iter, numThreads, numNodesPerThrasher);
      
      // NOTE: using ephemeral nodes
      // so we can't close any of these thrashers until we are done with our assertions
      final List<LiveNodeTrasher> thrashers = new ArrayList<>(numThreads);
      for (int i = 0; i < numThreads; i++) {
        thrashers.add(new LiveNodeTrasher("T"+iter+"_"+i, numNodesPerThrasher));
      }
      try {
        final ExecutorService executorService = ExecutorUtil.newMDCAwareFixedThreadPool
          (thrashers.size()+1, new SolrNamedThreadFactory("test_live_nodes_thrasher_iter"+iter));
        
        executorService.invokeAll(thrashers);
        executorService.shutdown();
        if (! executorService.awaitTermination(WAIT_TIME, TimeUnit.SECONDS)) {
          for (LiveNodeTrasher thrasher : thrashers) {
            thrasher.stop();
          }
        }
        assertTrue("iter"+iter+": thrashers didn't finish even after explicitly stopping",
                   executorService.awaitTermination(WAIT_TIME, TimeUnit.SECONDS));

        // sanity check the *real* live_nodes entries from ZK match what the thrashers added
        int totalAdded = 1; // 1 real live node when we started
        for (LiveNodeTrasher thrasher : thrashers) {
          totalAdded += thrasher.getNumAdded();
        }
        actualLiveNodes = getTrueLiveNodesFromZk();
        assertEquals("iter"+iter, totalAdded, actualLiveNodes.size());
        
        // verify our local client knows the correct set of live nodes
        cachedLiveNodes = getCachedLiveNodesFromLocalState(actualLiveNodes.size());
        assertEquals("iter"+iter+" " + actualLiveNodes.size() + " != " + cachedLiveNodes.size(),
                     actualLiveNodes, cachedLiveNodes);
        
      } finally {
        for (LiveNodeTrasher thrasher : thrashers) {
          // shutdown our zk connection, freeing our ephemeral nodes
          thrasher.close();
        }
      }
    }
  }

  /** NOTE: has internal counter which is not thread safe, only call() in one thread at a time */
  public static final class LiveNodeTrasher implements Callable<Integer> {
    private final String id;
    private final int numNodesToAdd;
    private final SolrZkClient client;

    private boolean running = false;;
    private int numAdded = 0;
    
    /** ID should ideally be unique amongst any other instances */
    public LiveNodeTrasher(String id, int numNodesToAdd) {
      this.id = id;
      this.numNodesToAdd = numNodesToAdd;
      this.client = newSolrZkClient();
    }
    /** returns the number of nodes actually added w/o error */
    public Integer call() {
      running = true;
      // NOTE: test includes 'running'
      for (int i = 0; running && i < numNodesToAdd; i++) {
        final String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/thrasher-" + id + "-" + i;
        try {
          client.makePath(nodePath, CreateMode.EPHEMERAL, true);
          numAdded++;
        } catch (Exception e) {
          log.error("failed to create: {}", nodePath, e);
        }
      }
      return numAdded;
    }
    public int getNumAdded() {
      return numAdded;
    }
    public void close() {
      client.close();
    }
    public void stop() {
      running = false;
    }
  }
}
