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

import org.apache.solr.cluster.*;

import java.util.Set;

/**
 * A cluster related placement request that Solr asks a {@link PlacementPlugin} plugin to resolve and compute a
 * {@link PlacementPlan} placing one or more {@link Replica}'s of one or more {@link Shard}'s of an existing {@link SolrCollection}.
 * The shard might or might not already exist, plugin code can easily find out by calling {@link SolrCollection#getShard(String)}
 * with the shard name(s) returned by {@link #getShardNames()}.
 *
 * <p>The set of {@link Node}s on which the replicas should be placed
 * is specified (defaults to being equal to the set returned by {@link Cluster#getLiveNodes()}).
 */
public interface PlacementRequest {
  /**
   * The {@link SolrCollection} to add {@link Replica}(s) to.
   */
  SolrCollection getCollection();

  /**
   * <p>Shard name(s) for which new replicas placement should be computed. The shard(s) might exist or not (that's why this
   * method returns a {@link Set} of {@link String}'s and not directly a set of {@link Shard} instances).
   *
   * <p>Note the Collection API allows specifying the shard name or a {@code _route_} parameter. The Solr implementation will
   * convert either specification into the relevant shard name so the plugin code doesn't have to worry about this.
   */
  Set<String> getShardNames();

  /**
   * <p>Replicas should only be placed on nodes in the set returned by this method.
   *
   * <p>When Collection API calls do not specify a specific set of target nodes, replicas can be placed on any live node of
   * the cluster. In such cases, this set will be equal to the set of all live nodes. The plugin placement code does not
   * need to worry (or care) if a set of nodes was explicitly specified or not.
   *
   * @return never {@code null} and never empty set (if that set was to be empty for any reason, no placement would be
   * possible and the Solr infrastructure driving the plugin code would detect the error itself rather than calling the plugin).
   */
  Set<Node> getTargetNodes();

  /**
   * Returns the number of replica to create for the given replica type.
   */
  int getCountReplicasToCreate(Replica.ReplicaType replicaType);
}
