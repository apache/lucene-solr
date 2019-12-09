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
package org.apache.solr.update.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.store.blob.client.CoreStorageClient;
import org.apache.solr.store.blob.process.CoreUpdateTracker;
import org.apache.solr.store.shared.SolrCloudSharedStoreTestCase;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for the {@link DistributedZkUpdateProcessor}.
 */
public class DistributedZkUpdateProcessorTest extends SolrCloudSharedStoreTestCase {    

  private static Path sharedStoreRootPath;
  private static CoreStorageClient storageClient;
  
  @BeforeClass
  public static void setupTestClass() throws Exception {
    assumeWorkingMockito();
    sharedStoreRootPath = createTempDir("tempDir");
    storageClient = setupLocalBlobStoreClient(sharedStoreRootPath, DEFAULT_BLOB_DIR_NAME);
    
    assumeWorkingMockito();
  }
  
  @After
  public void teardownTest() throws Exception {
    cluster.deleteAllCollections();
    shutdownCluster();
    File blobPath = sharedStoreRootPath.toFile();
    FileUtils.cleanDirectory(blobPath);
  }
  
  /**
   * Test that shared replicas can only route update requests from followers to leaders
   * by verifying that non leaders fail on forwarded updates. 
   * Note we assume shared replicas always route update requests such that commit=true 
   */
  @Test
  public void testNonLeaderSharedReplicaFailsOnForwardedCommit() throws Exception {
    setupCluster(1);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), cluster.getJettySolrRunner(0));

    String collectionName = "sharedCollection";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    setupSharedCollectionWithShardNames(collectionName, 2, 2, "shard1");

    // get the replica that's not the leader for the shard
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Slice slice = collection.getSlice("shard1");
    Replica leaderReplica = collection.getLeader("shard1");
    Replica follower = null;
    for (Replica repl : slice.getReplicas()) {
      if (!repl.getName().equals(leaderReplica.getName())) {
        follower = repl;
        break;
      }
    }

    if (follower == null) {
      fail("This test has been misconfigured");
    }

    DistributedZkUpdateProcessor processor = null;
    SolrCore core = getCoreContainer(follower.getNodeName()).getCore(follower.getCoreName());
    try {
      // Setup request such that the COMMIT_END_POINT is set to replicas. This indicates that
      // a leader has forwarded an update to its replicas which isn't possible but a safety 
      // check exists nonetheless. 
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(DistributedZkUpdateProcessor.COMMIT_END_POINT, "replicas");
      SolrQueryRequest req = new LocalSolrQueryRequest(core, params);

      // have the follower process the command as if it was forwarded it
      SolrQueryResponse rsp = new SolrQueryResponse();
      CoreUpdateTracker tracker = Mockito.mock(CoreUpdateTracker.class);
      processor = new DistributedZkUpdateProcessor(req, rsp, null, tracker);
      CommitUpdateCommand cmd = new CommitUpdateCommand(req, false);

      processor.processCommit(cmd);
      fail("Exception should have been thrown on processCommit");
    } catch (SolrException ex) {
      assertTrue(ex.getMessage().contains("Unexpected indexing forwarding"));
    } finally {
      if (processor != null) {
        processor.finish();
        processor.close();
      }
      core.close();
    }
  }
  
  /**
   * Create shared collection, create SHARED replica, index data, and confirm that commit writes to shared store.
   */
  @Test
  public void testSharedReplicaUpdate() throws Exception {
    boolean isUpdateAnIsolatedCommit = false;
    // if update has something to add or delete(i.e. not an isolated commit)
    // we expect a write to shared store
    boolean isWriteToSharedStoreExpected = true;
    testReplicaUpdatesZk(Replica.Type.SHARED, isUpdateAnIsolatedCommit, isWriteToSharedStoreExpected);
  }

  /**
   * Create shared collection, create SHARED replica, and confirm that isolated commit does not write to shared store.
   */
  @Test
  public void testSharedReplicaIsolatedCommit() throws Exception {
    boolean isUpdateAnIsolatedCommit = true;
    // if update has nothing to add or delete(i.e. just an isolated commit)
    // we expect no write to shared store
    boolean isWriteToSharedStoreExpected = false;
    testReplicaUpdatesZk(Replica.Type.SHARED, isUpdateAnIsolatedCommit, isWriteToSharedStoreExpected);
  }

  /**
   * Creates a collection with desired ({@code replicaType}), makes a desired {@code isUpdateAnIsolatedCommit} update
   * to the collection and ensure that write to shared store happened as expected {@code isWriteToSharedStoreExpected}.
   * 
   * @param replicaType only SHARED and NRT are supported
   * @param isUpdateAnIsolatedCommit if true add one document and then commit, otherwise, just an isolated commit
   *                                 Few ways isolated commit can manifest in actual usage:
   *                                 1. Client does indexing for while before issuing a separate commit.
   *                                 2. SolrJ client issuing a separate follow up commit command to affected shards than
   *                                    actual indexing request even when SolrJ client's caller issued a single update
   *                                    command with commit=true.
   * @param isWriteToSharedStoreExpected whether write to shared store is expected or not
   */
  private void testReplicaUpdatesZk(Replica.Type replicaType, boolean isUpdateAnIsolatedCommit, boolean isWriteToSharedStoreExpected) throws Exception {
    setupCluster(1);
    setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), cluster.getJettySolrRunner(0));

    // Set collection name and create client
    String collectionName = "testCollection";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // Create collection
    if (replicaType == Replica.Type.SHARED) {
      setupSharedCollectionWithShardNames(collectionName, 1, 1, "shard1");
    } else if (replicaType == Replica.Type.NRT) {
      CollectionAdminRequest.Create create = CollectionAdminRequest
          .createCollectionWithImplicitRouter(collectionName, "conf", "shard2", 0)
          .setSharedIndex(false)
          .setNrtReplicas(1);
      create.process(cloudClient);
    } else {
      throw new IllegalArgumentException(replicaType.name() + " replica type is not supported.");
    }

    // Verify that collection was created
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);

    DistributedZkUpdateProcessor processor = null;
    CoreUpdateTracker tracker = Mockito.mock(CoreUpdateTracker.class);
    // Get core from collection
    Replica newReplica = collection.getReplicas().get(0);
    SolrCore core = getCoreContainer(newReplica.getNodeName()).getCore(newReplica.getCoreName());
    try {
      // Verify that replica type is as expected
      assertEquals("wrong replica type", core.getCoreDescriptor().getCloudDescriptor().getReplicaType(), replicaType);

      // Mock out DistributedZkUpdateProcessor
      SolrQueryResponse rsp = new SolrQueryResponse();
      rsp.addResponseHeader(new SimpleOrderedMap<>());
      SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
      processor = new DistributedZkUpdateProcessor(req, rsp, null, tracker);
      if (!isUpdateAnIsolatedCommit) {
        // index a doc
        AddUpdateCommand cmd = new AddUpdateCommand(req);
        cmd.solrDoc = new SolrInputDocument();
        cmd.solrDoc.addField("id", "1");
        processor.processAdd(cmd);
      }

      // Make a commit
      processor.processCommit(new CommitUpdateCommand(req, false));
    } finally {
      if (processor != null) {
        processor.finish();
        processor.close();
      }
      core.close();
    }

    if (isWriteToSharedStoreExpected) {
      // Verify that core tracker was updated
      verify(tracker).persistShardIndexToSharedStore(any(), any(), any(), any());
    } else {
      // Verify that core tracker was not updated
      verify(tracker, never()).persistShardIndexToSharedStore(any(), any(), any(), any());
    }
  }

  /**
   * Create collection, create NRT replica, index data, and confirm that commit does not write to shared store.
   */
  @Test
  public void testNRTReplicaUpdate() throws Exception {
    boolean isIsolatedCommit = false;
    // update to non-SHARED replica should not write to shared store
    boolean isWriteToSharedStoreExpected = false;
    testReplicaUpdatesZk(Replica.Type.NRT, isIsolatedCommit, isWriteToSharedStoreExpected);
  }

  /**
   * Test update only happens on leader replica for a 1 shard shared collection
   */
  @Test
  public void testSharedReplicaSimpleUpdateOnLeaderSuccess() throws Exception {
    setupCluster(3);
    
    // configure same client for each runner, this isn't a concurrency test so this is fine
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      setupTestSharedClientForNode(getBlobStorageProviderTestInstance(storageClient), runner);
    }
    
    String collectionName = "sharedCollection";
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    setupSharedCollectionWithShardNames(collectionName, 1, 2, "shard1");
    
    // do an update
    UpdateRequest req = new UpdateRequest();
    req.add("id", "1");
    req.commit(cluster.getSolrClient(), collectionName);
    
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica leaderReplica = collection.getLeader("shard1");
    
    Replica followerReplica = null;
    for (Replica replica : collection.getReplicas()) {
      if (!replica.getName().equals(leaderReplica.getName())) {
        followerReplica = replica;
        break;
      }
    }
    
    SolrCore leaderReplicaCore = null;
    SolrCore followerReplicaCore = null;
    try {
      // verify this last update didn't happen on the follower
      CoreContainer ccLeader = getCoreContainer(leaderReplica.getNodeName());
      leaderReplicaCore = ccLeader.getCore(leaderReplica.getCoreName());
      
      CoreContainer ccFollower = getCoreContainer(followerReplica.getNodeName());
      followerReplicaCore = ccFollower.getCore(followerReplica.getCoreName());
      
      // the follower's core should only have its default segment file from creation
      assertEquals(1, followerReplicaCore.getDeletionPolicy().getLatestCommit().getFileNames().size());
      // the commit should have only happened on the leader and it should have more index files than the default 
      assertTrue(leaderReplicaCore.getDeletionPolicy().getLatestCommit().getFileNames().size() > 1);
    } finally {
      if (leaderReplica != null) {
        leaderReplicaCore.close();
      }
      if (followerReplica != null) {
        followerReplicaCore.close();
      }
    }
  }
}