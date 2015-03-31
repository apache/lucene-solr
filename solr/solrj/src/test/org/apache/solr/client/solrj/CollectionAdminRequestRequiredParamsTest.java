package org.apache.solr.client.solrj;

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

import static org.apache.solr.common.params.CoreAdminParams.*;

import java.util.Iterator;
import java.util.Set;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;

import com.google.common.collect.Sets;

/**
 * Tests that default {@link CollectionAdminRequest#getParams()} returns only
 * the required parameters of this request, and none other.
 */
public class CollectionAdminRequestRequiredParamsTest extends LuceneTestCase {

  public void testBalanceShardUnique() {
    final CollectionAdminRequest.BalanceShardUnique request = new CollectionAdminRequest.BalanceShardUnique();
    request.setCollection("foo");
    request.setPropertyName("prop");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, "property");
  }
  
  public void testClusterProp() {
    final CollectionAdminRequest.ClusterProp request = new CollectionAdminRequest.ClusterProp();
    request.setPropertyName("foo");
    request.setPropertyValue("bar");
    assertContainsParams(request.getParams(), ACTION, NAME, "val");
  }
  
  public void testAddRole() {
    final CollectionAdminRequest.AddRole request = new CollectionAdminRequest.AddRole();
    request.setNode("node");
    request.setRole("role");
    assertContainsParams(request.getParams(), ACTION, "node", "role");
  }
  
  public void testRemoveRole() {
    final CollectionAdminRequest.RemoveRole request = new CollectionAdminRequest.RemoveRole();
    request.setNode("node");
    request.setRole("role");
    assertContainsParams(request.getParams(), ACTION, "node", "role");
  }
  
  public void testAddReplica() {
    // with shard parameter
    CollectionAdminRequest.AddReplica request = new CollectionAdminRequest.AddReplica();
    request.setShardName("shard");
    request.setCollectionName("collection");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD);
    
    // with route parameter
    request = new CollectionAdminRequest.AddReplica();
    request.setRouteKey("route");
    request.setCollectionName("collection");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, ShardParams._ROUTE_);
  }
  
  public void testAddReplicaProp() {
    final CollectionAdminRequest.AddReplicaProp request = new CollectionAdminRequest.AddReplicaProp();
    request.setShardName("shard");
    request.setCollectionName("collection");
    request.setReplica("replica");
    request.setPropertyName("prop");
    request.setPropertyValue("value");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD, REPLICA, "property", "property.value");
  }
  
  public void testClusterStatus() {
    final CollectionAdminRequest.ClusterStatus request = new CollectionAdminRequest.ClusterStatus();
    assertContainsParams(request.getParams(), ACTION);
  }
  
  public void testCreateShard() {
    final CollectionAdminRequest.CreateShard request = new CollectionAdminRequest.CreateShard();
    request.setCollectionName("collection");
    request.setShardName("shard");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD);
  }
  
  public void testDeleteReplica() {
    final CollectionAdminRequest.DeleteReplica request = new CollectionAdminRequest.DeleteReplica();
    request.setCollectionName("collection");
    request.setShardName("shard");
    request.setReplica("replica");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD, REPLICA);
  }
  
  public void testDeleteReplicaProp() {
    final CollectionAdminRequest.DeleteReplicaProp request = new CollectionAdminRequest.DeleteReplicaProp();
    request.setCollectionName("collection");
    request.setShardName("shard");
    request.setReplica("replica");
    request.setPropertyName("foo");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD, REPLICA, "property");
  }
  
  public void testDeleteShard() {
    final CollectionAdminRequest.DeleteShard request = new CollectionAdminRequest.DeleteShard();
    request.setCollectionName("collection");
    request.setShardName("shard");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD);
  }
  
  public void testSplitShard() {
    final CollectionAdminRequest.SplitShard request = new CollectionAdminRequest.SplitShard();
    request.setCollectionName("collection");
    request.setShardName("shard");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, SHARD);
  }

  public void testCreateCollection() {
    final CollectionAdminRequest.Create request = new CollectionAdminRequest.Create();
    request.setCollectionName("collection");
    assertContainsParams(request.getParams(), ACTION, NAME);
  }
  
  public void testReloadCollection() {
    final CollectionAdminRequest.Reload request = new CollectionAdminRequest.Reload();
    request.setCollectionName("collection");
    assertContainsParams(request.getParams(), ACTION, NAME);
  }
  
  public void testDeleteCollection() {
    final CollectionAdminRequest.Delete request = new CollectionAdminRequest.Delete();
    request.setCollectionName("collection");
    assertContainsParams(request.getParams(), ACTION, NAME);
  }
  
  public void testCreateAlias() {
    final CollectionAdminRequest.CreateAlias request = new CollectionAdminRequest.CreateAlias();
    request.setAliasName("name");
    request.setAliasedCollections("collections");
    assertContainsParams(request.getParams(), ACTION, NAME, "collections");
  }
  
  public void testDeleteAlias() {
    final CollectionAdminRequest.DeleteAlias request = new CollectionAdminRequest.DeleteAlias();
    request.setAliasName("name");
    assertContainsParams(request.getParams(), ACTION, NAME);
  }
  
  public void testListCollections() {
    final CollectionAdminRequest.List request = new CollectionAdminRequest.List();
    assertContainsParams(request.getParams(), ACTION);
  }

  public void testMigrate() {
    final CollectionAdminRequest.Migrate request = new CollectionAdminRequest.Migrate();
    request.setCollectionName("collection");
    request.setTargetCollection("target");
    request.setSplitKey("splitKey");
    assertContainsParams(request.getParams(), ACTION, COLLECTION, "target.collection", "split.key");
  }
  
  public void testOverseerStatus() {
    final CollectionAdminRequest.OverseerStatus request = new CollectionAdminRequest.OverseerStatus();
    assertContainsParams(request.getParams(), ACTION);
  }
  
  public void testRequestStatus() {
    final CollectionAdminRequest.RequestStatus request = new CollectionAdminRequest.RequestStatus();
    request.setRequestId("request");
    assertContainsParams(request.getParams(), ACTION, REQUESTID);
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
