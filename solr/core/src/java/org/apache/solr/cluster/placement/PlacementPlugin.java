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

import org.apache.solr.cluster.Cluster;

/**
 * <p>Implemented by external plugins to control replica placement and movement on the search cluster (as well as other things
 * such as cluster elasticity?) when cluster changes are required (initiated elsewhere, most likely following a Collection
 * API call).
 *
 * <p>Instances of classes implementing this interface are created by {@link PlacementPluginFactory}
 *
 * <p>Implementations of this interface <b>must</b> be reentrant. {@link #computePlacement} <b>will</b> be called concurrently
 * from many threads.
 */
public interface PlacementPlugin {
  /**
   * <p>Request from plugin code to compute placement. Note this method must be reentrant as a plugin instance may (read
   * will) get multiple such calls in parallel.
   *
   * <p>Configuration is passed upon creation of a new instance of this class by {@link PlacementPluginFactory#createPluginInstance}.
   *
   * @param cluster              initial state of the cluster. Note there are {@link java.util.Set}'s and {@link java.util.Map}'s
   *                             accessible from the {@link Cluster} and other reachable instances. These collection will not change
   *                             while the plugin is executing and will be thrown away once the plugin is done. The plugin code can
   *                             therefore modify them if needed.
   * @param placementRequest     request for placing new replicas or moving existing replicas on the cluster.
   * @param attributeFetcher     Factory used by the plugin to fetch additional attributes from the cluster nodes, such as
   *                             count of coresm ssytem properties etc..
   * @param placementPlanFactory Factory used to create instances of {@link PlacementPlan} to return computed decision.
   * @return plan satisfying the placement request.
   */
  PlacementPlan computePlacement(Cluster cluster, PlacementRequest placementRequest, AttributeFetcher attributeFetcher,
                                 PlacementPlanFactory placementPlanFactory) throws PlacementException, InterruptedException;
}
