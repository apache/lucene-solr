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
package org.apache.solr.client.solrj.cloud.autoscaling;

import java.util.Collections;
import java.util.Set;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;

/**
 * This suggester produces a DELETEREPLICA request using provided {@link org.apache.solr.client.solrj.cloud.autoscaling.Suggester.Hint#COLL_SHARD} and
 * {@link org.apache.solr.client.solrj.cloud.autoscaling.Suggester.Hint#NUMBER} hints to specify the collection, shard and number of replicas to delete.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
class DeleteReplicaSuggester extends Suggester {

  @Override
  public CollectionParams.CollectionAction getAction() {
    return CollectionParams.CollectionAction.DELETEREPLICA;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  SolrRequest init() {
    @SuppressWarnings({"unchecked"})
    Set<Pair<String, String>> shards = (Set<Pair<String, String>>) hints.getOrDefault(Hint.COLL_SHARD, Collections.emptySet());
    if (shards.isEmpty()) {
      throw new RuntimeException("delete-replica requires 'collection' and 'shard'");
    }
    if (shards.size() > 1) {
      throw new RuntimeException("delete-replica requires exactly one pair of 'collection' and 'shard'");
    }
    Pair<String, String> collShard = shards.iterator().next();
    @SuppressWarnings({"unchecked"})
    Set<Number> counts = (Set<Number>) hints.getOrDefault(Hint.NUMBER, Collections.emptySet());
    Integer count = null;
    if (!counts.isEmpty()) {
      if (counts.size() > 1) {
        throw new RuntimeException("delete-replica allows at most one number hint specifying the number of replicas to delete");
      }
      Number n = counts.iterator().next();
      count = n.intValue();
    }
    @SuppressWarnings({"unchecked"})
    Set<String> replicas = (Set<String>) hints.getOrDefault(Hint.REPLICA, Collections.emptySet());
    String replica = null;
    if (!replicas.isEmpty()) {
      if (replicas.size() > 1) {
        throw new RuntimeException("delete-replica allows at most one 'replica' hint");
      }
      replica = replicas.iterator().next();
    }
    if (replica == null && count == null) {
      throw new RuntimeException("delete-replica requires either 'replica' or 'number' hint");
    }
    if (replica != null) {
      return CollectionAdminRequest.deleteReplica(collShard.first(), collShard.second(), replica);
    } else {
      return CollectionAdminRequest.deleteReplica(collShard.first(), collShard.second(), count);
    }
  }
}
