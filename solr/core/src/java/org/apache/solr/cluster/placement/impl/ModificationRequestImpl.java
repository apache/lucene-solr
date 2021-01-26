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

package org.apache.solr.cluster.placement.impl;

import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.DeleteCollectionRequest;
import org.apache.solr.cluster.placement.DeleteReplicasRequest;
import org.apache.solr.cluster.placement.DeleteShardsRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;

import java.util.HashSet;
import java.util.Set;

/**
 * Helper class to create modification request instances.
 */
public class ModificationRequestImpl {

  public static DeleteCollectionRequest createDeleteCollectionRequest(DocCollection docCollection) {
    SolrCollection solrCollection = SimpleClusterAbstractionsImpl.SolrCollectionImpl.fromDocCollection(docCollection);
    return () -> solrCollection;
  }

  /**
   * Create a delete replicas request.
   * @param collection collection to delete replicas from
   * @param replicas replicas to delete
   */
  public static DeleteReplicasRequest createDeleteReplicasRequest(SolrCollection collection, Set<Replica> replicas) {
    return new DeleteReplicasRequest() {
      @Override
      public Set<Replica> getReplicas() {
        return replicas;
      }

      @Override
      public SolrCollection getCollection() {
        return collection;
      }

      @Override
      public String toString() {
        return "DeleteReplicasRequest{collection=" + collection.getName() +
            ",replicas=" + replicas;
      }
    };
  }

  /**
   * Create a delete replicas request using the internal Solr API.
   * @param docCollection Solr collection.
   * @param shardName shard name.
   * @param replicas Solr replicas (belonging to the shard).
   */
  public static DeleteReplicasRequest createDeleteReplicasRequest(DocCollection docCollection, String shardName, Set<org.apache.solr.common.cloud.Replica> replicas) {
    SolrCollection solrCollection = SimpleClusterAbstractionsImpl.SolrCollectionImpl.fromDocCollection(docCollection);
    Shard shard = solrCollection.getShard(shardName);
    Slice slice = docCollection.getSlice(shardName);
    Set<Replica> solrReplicas = new HashSet<>();
    replicas.forEach(replica -> {
      solrReplicas.add(shard.getReplica(replica.getName()));
    });
    return createDeleteReplicasRequest(solrCollection, solrReplicas);
  }


  public static DeleteShardsRequest createDeleteShardsRequest(SolrCollection collection, Set<String> shardNames) {
    return new DeleteShardsRequest() {
      @Override
      public Set<String> getShardNames() {
        return shardNames;
      }

      @Override
      public SolrCollection getCollection() {
        return collection;
      }

      @Override
      public String toString() {
        return "DeleteShardsRequest{collection=" + collection.getName() +
            ",shards=" + shardNames;
      }
    };
  }

  public static DeleteShardsRequest createDeleteShardsRequest(DocCollection docCollection, Set<String> shardNames) {
    SolrCollection solrCollection = SimpleClusterAbstractionsImpl.SolrCollectionImpl.fromDocCollection(docCollection);
    return createDeleteShardsRequest(solrCollection, shardNames);
  }
}
