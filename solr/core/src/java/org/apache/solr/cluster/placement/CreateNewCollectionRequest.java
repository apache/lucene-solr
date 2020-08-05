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

package org.apache.solr.cluster.placement;

import java.util.Set;

/**
 * <p>Request for creating a new collection with a given set of shards and replication factor for various replica types.
 * The expected {@link WorkOrder} corresponding to this {@link Request} is created using
 * {@link WorkOrderFactory#createWorkOrderNewCollection}
 *
 * <p>Note there is no need at this stage to allow the plugin to know each shard hash range for example, this can be handled
 * by the Solr side implementation of this interface without needing the plugin to worry about it (the implementation of this interface on
 * the Solr side can maintain the ranges for each shard).
 *
 * <p>Same goes for the {@link org.apache.solr.core.ConfigSet} name or other collection parameters. They are needed for
 * creating a Collection but likely do not have to be exposed to the plugin (this can easily be changed if needed by
 * adding accessors here, the underlying Solr side implementation of this interface has the information).
 */
public interface CreateNewCollectionRequest extends Request {
  /**
   * <p>The name of the collection to be created and for which placement should be computed.
   *
   * <p>Compare this method with {@link AddReplicasRequest#getCollection()}, there the collection already exists so can be
   * directly passed in the {@link Request}.
   *
   * <p>When processing this request, plugin code doesn't have to worry about existing {@link Replica}'s for the collection
   * given that the collection is assumed not to exist.
   */
  String getCollectionName();

  Set<String> getShardNames();

  /**
   * <p>Properties passed through the Collection API by the client creating the collection.
   * See {@link SolrCollection#getCustomProperty(String)}.
   *
   * <p>Given this {@link Request} is for creating a new collection, it is not possible to pass the custom property values through
   * the {@link SolrCollection} object. That instance does not exist yet, and is the reason {@link #getCollectionName()} exists
   * rather than a method returning {@link SolrCollection}...
   */
  String getCustomProperty(String customPropertyName);

  int getNrtReplicationFactor();
  int getTlogReplicationFactor();
  int getPullReplicationFactor();
}
