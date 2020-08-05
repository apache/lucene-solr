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
 * <p>Request for creating one or more {@link Replica}'s for one or more {@link Shard}'s of an existing {@link SolrCollection}.
 * The shard might or might not already exist, plugin code can easily find out by using {@link SolrCollection#getShards()}
 * and verifying if the shard name(s) from {@link #getShardNames()} are there.
 *
 * <p>As opposed to {@link CreateNewCollectionRequest}, the set of {@link Node}s on which the replicas should be placed
 * is specified (defaults to being equal to the set returned by {@link Cluster#getLiveNodes()}).
 *
 * <p>There is no extension between this interface and {@link CreateNewCollectionRequest} in either direction
 * or from a common ancestor for readability. An ancestor could make sense and would be an "abstract interface" not intended
 * to be implemented directly, but this does not exist in Java.
 *
 * <p>Plugin code would likely treat the two types of requests differently since here existing {@link Replica}'s must be taken
 * into account for placement whereas in {@link CreateNewCollectionRequest} no {@link Replica}'s are assumed to exist.
 */
public interface AddReplicasRequest extends Request {
  /**
   * The {@link SolrCollection} to add {@link Replica}(s) to. The replicas are to be added to a shard that might or might
   * not yet exist when the plugin's {@link PlacementPlugin#computePlacement} is called.
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

  /** Replicas should only be placed on nodes from the set returned by this method. */
  Set<Node> getTargetNodes();

  /** Number of NRT replicas to create. */
  int getCountNrtReplicas();
  /** Number of TLOG replicas to create. */
  int getCountTlogReplicas();
  /** Number of PULL replicas to create. */
  int getCountPullReplicas();
}
