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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.store.blob.process.CoreUpdateTracker;
import org.apache.solr.update.CommitUpdateCommand;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test for the {@link DistributedZkUpdateProcessor}.
 */
public class DistributedZkUpdateProcessorTest extends SolrCloudTestCase {  
  @BeforeClass
  public static void setupCluster() throws Exception {    
    configureCluster(1)
      .addConfig("conf", configset("cloud-minimal"))
      .configure();
  }
  
  /**
   * Create shared collection, create SHARED replica, index data, and confirm that
   * CoreUpdateTracker is accessed on commit.
   */
  @Test
  public void testSharedReplicaUpdatesZk() throws Exception {
    // Set collection name and create client
    String collectionName = "BlobBasedCollectionName1";
    CloudSolrClient cloudClient = cluster.getSolrClient();

    // Create collection
    CollectionAdminRequest.Create create = CollectionAdminRequest
        .createCollectionWithImplicitRouter(collectionName, "conf", "shard1", 0)
          .setSharedIndex(true)
          .setSharedReplicas(1);
    create.process(cloudClient);

    // Verify that collection was created
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);
    
    // Get core from collection
    Replica newReplica = collection.getReplicas().get(0);
    SolrCore core = getCoreContainer(newReplica.getNodeName()).getCore(newReplica.getCoreName());
    
    // Verify that replica type corresponds with sharedIndex value
    Replica.Type replicaType = core.getCoreDescriptor().getCloudDescriptor().getReplicaType();
    assertEquals(replicaType, Replica.Type.SHARED);
        
    // Mock out DistributedZkUpdateProcessor
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
    CoreUpdateTracker tracker = Mockito.mock(CoreUpdateTracker.class);
    DistributedZkUpdateProcessor processor = new DistributedZkUpdateProcessor(req, rsp, null, tracker);
    
    // Make a commit and close processor and core
    processor.processCommit(new CommitUpdateCommand(req, false));
    processor.finish();
    processor.close();
    core.close();
    
    // Verify that core tracker was updated
    verify(tracker).persistShardIndexToSharedStore(any(), any(), any(), any());
  }
  
  /**
   * Create collection, create NRT replica, index data, and confirm that CoreUpdateTracker
   * is NOT accessed on commit.
   */
  @Test
  public void testNRTReplicaUpdatesZk() throws Exception {
    // Set collection name and create client
    String collectionName = "BlobBasedCollectionName2";
    CloudSolrClient cloudClient = cluster.getSolrClient();
    
    // Create collection
    CollectionAdminRequest.Create create = CollectionAdminRequest
        .createCollectionWithImplicitRouter(collectionName, "conf", "shard2", 0)
          .setSharedIndex(false)
          .setNrtReplicas(1);
    create.process(cloudClient);

    // Verify that collection was created
    waitForState("Timed-out wait for collection to be created", collectionName, clusterShape(1, 1));
    assertTrue(cloudClient.getZkStateReader().getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, false));
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(collectionName);

    // Get core from collection
    Replica newReplica = collection.getReplicas().get(0);
    SolrCore core = getCoreContainer(newReplica.getNodeName()).getCore(newReplica.getCoreName());
    
    // Verify that replica type corresponds with sharedIndex value
    Replica.Type replicaType = core.getCoreDescriptor().getCloudDescriptor().getReplicaType();
    assertEquals(replicaType, Replica.Type.NRT);
        
    // Mock out DistributedZkUpdateProcessor
    SolrQueryResponse rsp = new SolrQueryResponse();
    SolrQueryRequest req = new LocalSolrQueryRequest(core, new ModifiableSolrParams());
    CoreUpdateTracker tracker = Mockito.mock(CoreUpdateTracker.class);
    DistributedZkUpdateProcessor processor = new DistributedZkUpdateProcessor(req, rsp, null, tracker);
    
    // Make a commit and close processor and core
    processor.processCommit(new CommitUpdateCommand(req, false));
    processor.finish();
    processor.close();
    core.close();
    
    // Verify that core tracker was not updated
    verify(tracker, never()).persistShardIndexToSharedStore(any(), any(), any(), any()); 
  }
  
}