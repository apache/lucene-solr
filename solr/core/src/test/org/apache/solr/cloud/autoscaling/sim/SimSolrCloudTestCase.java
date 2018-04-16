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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.SOLR_AUTOSCALING_CONF_PATH;

/**
 * Base class for simulated test cases. Tests that use this class should configure the simulated cluster
 * in <code>@BeforeClass</code> like this:
 * <pre>
 *   @BeforeClass
 *   public static void setupCluster() throws Exception {
 *     cluster = configureCluster(5, TimeSource.get("simTime:50"));
 *   }
 * </pre>
 */
public class SimSolrCloudTestCase extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** The cluster. */
  protected static SimCloudManager cluster;
  protected static int clusterNodeCount = 0;

  protected static void configureCluster(int nodeCount, TimeSource timeSource) throws Exception {
    cluster = SimCloudManager.createCluster(nodeCount, timeSource);
    clusterNodeCount = nodeCount;
  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    cluster = null;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (cluster != null) {
      log.info(cluster.dumpClusterState(false));
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    if (cluster != null) {
      // clear any persisted configuration
      cluster.getDistribStateManager().setData(SOLR_AUTOSCALING_CONF_PATH, Utils.toJSON(new ZkNodeProps()), -1);
      cluster.getDistribStateManager().setData(ZkStateReader.ROLES, Utils.toJSON(new HashMap<>()), -1);
      // restore the expected number of nodes
      int currentSize = cluster.getLiveNodesSet().size();
      if (currentSize < clusterNodeCount) {
        int addCnt = clusterNodeCount - currentSize;
        while (addCnt-- > 0) {
          cluster.simAddNode();
        }
      } else if (currentSize > clusterNodeCount) {
        cluster.simRemoveRandomNodes(currentSize - clusterNodeCount, true, random());
      }
      // clean any persisted trigger state or events
      removeChildren(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH);
      removeChildren(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH);
      removeChildren(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH);
      removeChildren(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH);
      cluster.getSimClusterStateProvider().simDeleteAllCollections();
      cluster.simClearSystemCollection();
      // clear any dead nodes
      cluster.getSimNodeStateProvider().simRemoveDeadNodes();
      cluster.getSimClusterStateProvider().simResetLeaderThrottles();
      cluster.simRestartOverseer(null);
      cluster.getTimeSource().sleep(5000);
      cluster.simResetOpCounts();
    }
  }

  @Before
  public void checkClusterConfiguration() {
    if (cluster == null)
      throw new RuntimeException("SimCloudManager not configured - have you called configureCluster()?");
  }

  protected void removeChildren(String path) throws Exception {
    if (!cluster.getDistribStateManager().hasData(path)) {
      return;
    }
    cluster.getDistribStateManager().removeRecursively(path, true, false);
  }

  /* Cluster helper methods ************************************/

  /**
   * Get the collection state for a particular collection
   */
  protected DocCollection getCollectionState(String collectionName) throws IOException {
    return cluster.getClusterStateProvider().getClusterState().getCollection(collectionName);
  }

  /**
   * Get a (reproducibly) random shard from a {@link DocCollection}
   */
  protected static Slice getRandomShard(DocCollection collection) {
    List<Slice> shards = new ArrayList<>(collection.getActiveSlices());
    if (shards.size() == 0)
      fail("Couldn't get random shard for collection as it has no shards!\n" + collection.toString());
    Collections.shuffle(shards, random());
    return shards.get(0);
  }

  /**
   * Get a (reproducibly) random replica from a {@link Slice}
   */
  protected static Replica getRandomReplica(Slice slice) {
    List<Replica> replicas = new ArrayList<>(slice.getReplicas());
    if (replicas.size() == 0)
      fail("Couldn't get random replica from shard as it has no replicas!\n" + slice.toString());
    Collections.shuffle(replicas, random());
    return replicas.get(0);
  }

  /**
   * Get a (reproducibly) random replica from a {@link Slice} matching a predicate
   */
  protected static Replica getRandomReplica(Slice slice, Predicate<Replica> matchPredicate) {
    List<Replica> replicas = new ArrayList<>(slice.getReplicas());
    if (replicas.size() == 0)
      fail("Couldn't get random replica from shard as it has no replicas!\n" + slice.toString());
    Collections.shuffle(replicas, random());
    for (Replica replica : replicas) {
      if (matchPredicate.test(replica))
        return replica;
    }
    fail("Couldn't get random replica that matched conditions\n" + slice.toString());
    return null;  // just to keep the compiler happy - fail will always throw an Exception
  }
}
