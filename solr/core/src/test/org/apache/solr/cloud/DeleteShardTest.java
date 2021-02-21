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
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistributedQueue;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.Slice.State;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// MRM TODO: set slice state same way as efficient state updates
public class DeleteShardTest extends SolrCloudTestCase {

  // TODO: Custom hash slice deletion test

  @Before
  public void setup() throws Exception {
    super.setUp();
    configureCluster(2)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .configure();
  }
  
  @After
  public void tearDown() throws Exception {
    shutdownCluster();
    super.tearDown();
  }

  @Test
  // MRM TODO: we need to pump slice changes through the StatePublish mechanism
  public void test() throws Exception {

    final String collection = "deleteShard";

    CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
        .process(cluster.getSolrClient());

    DocCollection state = getCollectionState(collection);
    assertEquals(State.ACTIVE, state.getSlice("s1").getState());
    assertEquals(State.ACTIVE, state.getSlice("s2").getState());

    // Can't delete an ACTIVE shard
    LuceneTestCase.expectThrows(Exception.class, () -> {
      CollectionAdminRequest.deleteShard(collection, "s1").process(cluster.getSolrClient());
    });

    setSliceState(collection, "s1", Slice.State.INACTIVE);

    // Can delete an INATIVE shard
    CollectionAdminRequest.deleteShard(collection, "s1").process(cluster.getSolrClient());

    // Can delete a shard under construction
    setSliceState(collection, "s2", Slice.State.CONSTRUCTION);

    CollectionAdminRequest.deleteShard(collection, "s2").process(cluster.getSolrClient());
  }

  protected void setSliceState(String collection, String slice, State state) throws Exception {

    // TODO can this be encapsulated better somewhere?
    DistributedQueue inQueue =  cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getOverseer().getStateUpdateQueue();
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
    propMap.put(slice, state.toString());
    propMap.put(ZkStateReader.COLLECTION_PROP, collection);
    ZkNodeProps m = new ZkNodeProps(propMap);
    inQueue.offer(Utils.toJSON(m));

    cluster.getSolrClient().getZkStateReader().waitForState(collection, 5, TimeUnit.SECONDS, (l, c)->{
      if (c == null) return false;
      if (c.getSlice(slice) == null) return false;

      return c.getSlice(slice).getState() == state;
    });
  }

  @Test
  // commented 4-Sep-2018  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 09-Aug-2018
  public void testDirectoryCleanupAfterDeleteShard() throws InterruptedException, IOException, SolrServerException {

    final String collection = "deleteshard_test";
    CollectionAdminRequest.createCollectionWithImplicitRouter(collection, "conf", "a,b,c", 1)
        .setMaxShardsPerNode(3)
        .process(cluster.getSolrClient());

    // Get replica details
    Replica leader = getCollectionState(collection).getLeader("a");

    CoreStatus coreStatus = getCoreStatus(leader);
    assertTrue("Instance directory doesn't exist", FileUtils.fileExists(coreStatus.getInstanceDirectory()));
    assertTrue("Data directory doesn't exist", FileUtils.fileExists(coreStatus.getDataDirectory()));

    assertEquals(3, getCollectionState(collection).getActiveSlices().size());

    // Delete shard 'a'
    CollectionAdminRequest.deleteShard(collection, "a").process(cluster.getSolrClient());

    coreStatus = getCoreStatus(leader);
    assertEquals(2, getCollectionState(collection).getActiveSlices().size());

    // MRM TODO:
    if (coreStatus != null && coreStatus.getResponse() != null) {
      assertFalse("Instance directory still exists", FileUtils.fileExists(coreStatus.getInstanceDirectory()));
      assertFalse("Data directory still exists", FileUtils.fileExists(coreStatus.getDataDirectory()));
    }

    leader = getCollectionState(collection).getLeader("b");
    coreStatus = getCoreStatus(leader);

    // Delete shard 'b'
    CollectionAdminRequest.deleteShard(collection, "b")
        .process(cluster.getSolrClient());
    
    assertEquals(1, getCollectionState(collection).getActiveSlices().size());
    assertFalse("Instance directory still exists", FileUtils.fileExists(coreStatus.getInstanceDirectory()));
    assertFalse("Data directory still exists", FileUtils.fileExists(coreStatus.getDataDirectory()));
  }
}
