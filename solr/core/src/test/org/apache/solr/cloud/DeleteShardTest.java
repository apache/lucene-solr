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
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.Slice.State;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DeleteShardTest extends SolrCloudTestCase {

  // TODO: Custom hash slice deletion test

  @Before
  public void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }
  
  @After
  public void teardownCluster() throws Exception {
    shutdownCluster();
  }

  @Test
  public void test() throws Exception {

    final String collection = "deleteShard";

    CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 2, 2);

    DocCollection state = getCollectionState(collection);
    assertEquals(State.ACTIVE, state.getSlice("shard1").getState());
    assertEquals(State.ACTIVE, state.getSlice("shard2").getState());

    // Can't delete an ACTIVE shard
    expectThrows(Exception.class, () -> {
      CollectionAdminRequest.deleteShard(collection, "shard1").process(cluster.getSolrClient());
    });

    setSliceState(collection, "shard1", Slice.State.INACTIVE);

    // Can delete an INATIVE shard
    CollectionAdminRequest.deleteShard(collection, "shard1").process(cluster.getSolrClient());
    waitForState("Expected 'shard1' to be removed", collection, (n, c) -> {
      return c.getSlice("shard1") == null;
    });

    // Can delete a shard under construction
    setSliceState(collection, "shard2", Slice.State.CONSTRUCTION);
    CollectionAdminRequest.deleteShard(collection, "shard2").process(cluster.getSolrClient());
    waitForState("Expected 'shard2' to be removed", collection, (n, c) -> {
      return c.getSlice("shard2") == null;
    });

  }

  protected void setSliceState(String collection, String slice, State state) throws Exception {

    CloudSolrClient client = cluster.getSolrClient();

    // TODO can this be encapsulated better somewhere?
    DistributedQueue inQueue =  cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getOverseer().getStateUpdateQueue();
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
    propMap.put(slice, state.toString());
    propMap.put(ZkStateReader.COLLECTION_PROP, collection);
    ZkNodeProps m = new ZkNodeProps(propMap);
    inQueue.offer(Utils.toJSON(m));

    waitForState("Expected shard " + slice + " to be in state " + state.toString(), collection, (n, c) -> {
      return c.getSlice(slice).getState() == state;
    });

  }

  @Test
  // commented 4-Sep-2018  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 09-Aug-2018
  public void testDirectoryCleanupAfterDeleteShard() throws InterruptedException, IOException, SolrServerException {

    final String collection = "deleteshard_test";
    CollectionAdminRequest.createCollectionWithImplicitRouter(collection, "conf", "a,b,c", 1)
        .setMaxShardsPerNode(2)
        .process(cluster.getSolrClient());
    
    cluster.waitForActiveCollection(collection, 3, 3);

    // Get replica details
    Replica leader = getCollectionState(collection).getLeader("a");

    CoreStatus coreStatus = getCoreStatus(leader);
    assertTrue("Instance directory doesn't exist", FileUtils.fileExists(coreStatus.getInstanceDirectory()));
    assertTrue("Data directory doesn't exist", FileUtils.fileExists(coreStatus.getDataDirectory()));

    assertEquals(3, getCollectionState(collection).getActiveSlices().size());

    // Delete shard 'a'
    CollectionAdminRequest.deleteShard(collection, "a").process(cluster.getSolrClient());
    
    waitForState("Expected 'a' to be removed", collection, (n, c) -> {
      return c.getSlice("a") == null;
    });

    assertEquals(2, getCollectionState(collection).getActiveSlices().size());
    assertFalse("Instance directory still exists", FileUtils.fileExists(coreStatus.getInstanceDirectory()));
    assertFalse("Data directory still exists", FileUtils.fileExists(coreStatus.getDataDirectory()));

    leader = getCollectionState(collection).getLeader("b");
    coreStatus = getCoreStatus(leader);

    // Delete shard 'b'
    CollectionAdminRequest.deleteShard(collection, "b")
        .setDeleteDataDir(false)
        .setDeleteInstanceDir(false)
        .process(cluster.getSolrClient());

    waitForState("Expected 'b' to be removed", collection, (n, c) -> {
      return c.getSlice("b") == null;
    });
    
    assertEquals(1, getCollectionState(collection).getActiveSlices().size());
    assertTrue("Instance directory still exists", FileUtils.fileExists(coreStatus.getInstanceDirectory()));
    assertTrue("Data directory still exists", FileUtils.fileExists(coreStatus.getDataDirectory()));
  }
}
