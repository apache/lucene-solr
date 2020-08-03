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
 * <p>Request for creating one or more replicas for a given shard of a given Collection. The shard might or might not
 * already exist but this shouldn't matter to plugin placement code. The Solr code handling the corresponding
 * {@link WorkOrder} (created using {@link WorkOrderFactory#createWorkOrderAddReplicas}
 * will take care of this.
 *
 * <p>As opposed to {@link CreateNewCollectionRequest}, the set of {@link Node}s on which the replicas should be placed
 * is specified (defaults to being equal to the set returned by {@link Topo#getLiveNodes()}).
 *
 * <p>There is no extension between this interface and {@link CreateNewCollectionRequest} in either direction
 * or from a common ancestor for readability. An ancestor could make sense and would be an "abstract interface" not intended
 * to be implemented directly, but this does not exist in Java, so skipping. Plugin code would likely unpack
 * {@link Request}s in specific code sections per type then call shared code.
 */
public interface AddReplicasRequest extends Request {
  /**
   * In theory we could return here a {@link SolrCollection} rather than a name, but given the Shard might not exist as
   * the plugin is asked to compute placement, it would not be possible to return a {@link Shard} from {@link #getShardName}
   * so sticking to {@link String}. This comment is intended for API review and likely can be removed at some point.
   */
  String getCollectionName();

  /** Shard name for which new replicas placement should be computed. */
  String getShardName();

  /** Replicas should only be placed on nodes from the set returned by this method. */
  Set<Node> getTargetNodes();

  /** Number of NRT replicas to create. */
  int getCountNrtReplicas();
  /** Number of TLOG replicas to create. */
  int getCountTlogReplicas();
  /** Number of PULL replicas to create. */
  int getCountPullReplicas();
}
