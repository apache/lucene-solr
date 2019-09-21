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
package org.apache.solr.client.solrj;

import static org.apache.solr.common.params.CoreAdminParams.*;
import static org.apache.solr.common.params.CollectionAdminParams.FLUSH;

import java.util.Iterator;
import java.util.Set;

import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;

import com.google.common.collect.Sets;

/**
 * Tests that default {@link CollectionAdminRequest#getParams()} returns only
 * the required parameters of this request, and none other.
 */
public class CollectionAdminRequestRequiredParamsTest extends SolrTestCase {

  public void testBalanceShardUnique() {
    CollectionAdminRequest.BalanceShardUnique request = CollectionAdminRequest.balanceReplicaProperty("foo","prop");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, "property");

    request.setShardUnique(true);
    assertContainsParams(request.getParams(), ACTION, COLLECTION, "property","shardUnique");
    
    request.setOnlyActiveNodes(false);
    assertContainsParams(request.getParams(), ACTION, COLLECTION, "property","shardUnique","onlyactivenodes");
    
    request.setShardUnique(null);
    assertContainsParams(request.getParams(), ACTION, COLLECTION, "property","onlyactivenodes");
    
  }
  
  public void testClusterProp() {
    CollectionAdminRequest.ClusterProp request = CollectionAdminRequest.setClusterProperty("foo","bar");
    assertContainsParams(request.getParams(), ACTION, NAME, "val");
  }

  public void testCollectionProp() {
    final CollectionAdminRequest.CollectionProp request = CollectionAdminRequest.setCollectionProperty("foo", "bar", "baz");
    assertContainsParams(request.getParams(), ACTION, CoreAdminParams.NAME, CollectionAdminParams.PROPERTY_NAME, CollectionAdminParams.PROPERTY_VALUE);
  }

  public void testAddRole() {
    CollectionAdminRequest.AddRole request = CollectionAdminRequest.addRole("node","role");
    assertContainsParams(request.getParams(), ACTION, "node", "role");
  }
  
  public void testRemoveRole() {
    CollectionAdminRequest.RemoveRole request = CollectionAdminRequest.removeRole("node","role");
    assertContainsParams(request.getParams(), ACTION, "node", "role");
  }
  
  public void testAddReplica() {
    // with shard parameter and "client side" implicit type param
    CollectionAdminRequest.AddReplica request = CollectionAdminRequest.addReplicaToShard("collection", "shard");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD, ZkStateReader.REPLICA_TYPE);
    
    // with only shard parameter and "server side" implicit type, so no param
    request = CollectionAdminRequest.addReplicaToShard("collection", "shard", null);
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD);
    
    // with route parameter
    request = CollectionAdminRequest.addReplicaByRouteKey("collection","route");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, ShardParams._ROUTE_);
    
    // with explicit type parameter
    request = CollectionAdminRequest.addReplicaToShard("collection", "shard", Replica.Type.NRT);
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD, ZkStateReader.REPLICA_TYPE);
  }
  
  public void testAddReplicaProp() {
    final CollectionAdminRequest.AddReplicaProp request = CollectionAdminRequest.addReplicaProperty
      ("collection", "shard", "replica", "prop", "value");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD, REPLICA, "property", "property.value");
  }
  
  public void testClusterStatus() {
    final CollectionAdminRequest.ClusterStatus request = CollectionAdminRequest.getClusterStatus();
    assertContainsParams(request.getParams(), ACTION);

    request.setCollectionName("foo");
    assertContainsParams(request.getParams(), ACTION, COLLECTION);
    
    request.setShardName("foo");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD);

    request.setRouteKey("foo");
    request.setShardName(null);
    assertContainsParams(request.getParams(), ACTION, COLLECTION, ShardParams._ROUTE_);
    
  }
  
  public void testCreateShard() {
    final CollectionAdminRequest.CreateShard request = CollectionAdminRequest.createShard("collection","shard");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD);
  }
  
  public void testDeleteReplica() {
    final CollectionAdminRequest.DeleteReplica request = CollectionAdminRequest.deleteReplica("collection","shard","replica");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD, REPLICA);

  }
  
  public void testDeleteReplicaProp() {
    final CollectionAdminRequest.DeleteReplicaProp request = CollectionAdminRequest.deleteReplicaProperty
      ("collection", "shard", "replica", "foo");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD, REPLICA, "property");
  }
  
  public void testDeleteShard() {
    final CollectionAdminRequest.DeleteShard request = CollectionAdminRequest.deleteShard("collection","shard");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD);
  }
  
  public void testSplitShard() {
    final CollectionAdminRequest.SplitShard request = CollectionAdminRequest.splitShard("collection")
            .setShardName("shard");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD);
  }

  public void testCreateCollection() {
    // shortest form
    assertContainsParams(CollectionAdminRequest.createCollection("foo", null, 1, 1).getParams(),
                         ACTION, NAME, ZkStateReader.NUM_SHARDS_PROP, ZkStateReader.NRT_REPLICAS);
    // shortest form w/ "explicitly" choosing "implicit" router
    assertContainsParams(CollectionAdminRequest.createCollectionWithImplicitRouter("foo", null, "bar", 1).getParams(),
                         ACTION, NAME, "shards", "router.name", ZkStateReader.NRT_REPLICAS);
  }
  
  public void testReloadCollection() {
    final CollectionAdminRequest.Reload request = CollectionAdminRequest.reloadCollection("collection");
    assertContainsParams(request.getParams(), ACTION, NAME);
  }
  
  public void testDeleteCollection() {
    final CollectionAdminRequest.Delete request = CollectionAdminRequest.deleteCollection("collection");
    assertContainsParams(request.getParams(), ACTION, NAME);
  }
  
  public void testCreateAlias() {
    final CollectionAdminRequest.CreateAlias request = CollectionAdminRequest.createAlias("name","collections");
    assertContainsParams(request.getParams(), ACTION, NAME, "collections");
  }
  
  public void testDeleteAlias() {
    final CollectionAdminRequest.DeleteAlias request = CollectionAdminRequest.deleteAlias("name");
    assertContainsParams(request.getParams(), ACTION, NAME);
  }
  
  public void testListCollections() {
    final CollectionAdminRequest.List request = new CollectionAdminRequest.List();
    assertContainsParams(request.getParams(), ACTION);
  }

  public void testMigrate() {
    final CollectionAdminRequest.Migrate request = CollectionAdminRequest.migrateData("collection","targer","splitKey");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, "target.collection", "split.key");
  }
  
  public void testOverseerStatus() {
    final CollectionAdminRequest.OverseerStatus request = new CollectionAdminRequest.OverseerStatus();
    assertContainsParams(request.getParams(), ACTION);
  }
  
  public void testRequestStatus() {
    final CollectionAdminRequest.RequestStatus request = CollectionAdminRequest.requestStatus("request");
    assertContainsParams(request.getParams(), ACTION, REQUESTID);
  }
  
  public void testDeleteStatus() {
    assertContainsParams(CollectionAdminRequest.deleteAsyncId("foo").getParams(),
                         ACTION, REQUESTID);
    assertContainsParams(CollectionAdminRequest.deleteAllAsyncIds().getParams(),
                         ACTION, FLUSH);
  }
  
  public void testForceLeader() {
    assertContainsParams(CollectionAdminRequest.forceLeaderElection("foo","bar").getParams(),
                         ACTION, COLLECTION, SHARD);
  }

  private void assertContainsParams(SolrParams solrParams, String... requiredParams) {
    final Set<String> requiredParamsSet = Sets.newHashSet(requiredParams);
    final Set<String> solrParamsSet = Sets.newHashSet();
    for (Iterator<String> iter = solrParams.getParameterNamesIterator(); iter.hasNext();) {
      solrParamsSet.add(iter.next());
    }
    assertTrue("required params missing: required=" + requiredParamsSet + ", params=" + solrParamsSet, 
        solrParamsSet.containsAll(requiredParamsSet));
    assertTrue("extra parameters included in request: required=" + requiredParamsSet + ", params=" + solrParams, 
        Sets.difference(solrParamsSet, requiredParamsSet).isEmpty());
  }
  
}
