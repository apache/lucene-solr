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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.solr.cluster.placement.PropertyValueSource;
import org.apache.solr.cluster.placement.Shard;
import org.apache.solr.cluster.placement.SolrCollection;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;

class SolrCollectionImpl implements SolrCollection {
  private final String collectionName;
  /** Map from {@link Shard#getShardName()} to {@link Shard} */
  private final Map<String, Shard> shards;
  private final DocCollection docCollection;

  static Optional<SolrCollection> createCollectionFacade(ClusterState clusterState, String collectionName) {
    DocCollection docCollection = clusterState.getCollectionOrNull(collectionName);

    if (docCollection == null) {
      return Optional.empty();
    } else {
      return Optional.of(new SolrCollectionImpl(docCollection));
    }
  }

  SolrCollectionImpl(DocCollection docCollection) {
    this.collectionName = docCollection.getName();
    this.shards = ShardImpl.getShards(this, docCollection.getSlices());
    this.docCollection = docCollection;
  }

  @Override
  public String getName() {
    return collectionName;
  }

  @Override
  public Map<String, Shard> getShards() {
    return shards;
  }

  @Override
  public String getCustomProperty(String customPropertyName) {
    return docCollection.getStr(customPropertyName);
  }

  /**
   * This class implements {@link PropertyValueSource} and will end up as a key in a Map for fetching {@link org.apache.solr.cluster.placement.PropertyKey}'s
   */
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) { return false; }
    SolrCollectionImpl other = (SolrCollectionImpl) obj;
    return Objects.equals(this.collectionName, other.collectionName)
        && Objects.equals(this.shards, other.shards);
  }

  public int hashCode() {
    return Objects.hashCode(collectionName);
  }
}
