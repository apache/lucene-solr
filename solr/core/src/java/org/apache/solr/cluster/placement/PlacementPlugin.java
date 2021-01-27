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
   * @param placementRequest     request for placing new replicas or moving existing replicas on the cluster.
   * @return plan satisfying the placement request.
   */
  PlacementPlan computePlacement(PlacementRequest placementRequest, PlacementContext placementContext) throws PlacementException, InterruptedException;

  /**
   * Verify that a collection layout modification doesn't violate constraints on replica placements
   * required by this plugin. Default implementation is a no-op (any modifications are allowed).
   * @param modificationRequest modification request.
   * @param placementContext placement context.
   * @throws PlacementModificationException if the requested modification would violate replica
   * placement constraints.
   */
  default void verifyAllowedModification(ModificationRequest modificationRequest, PlacementContext placementContext)
    throws PlacementModificationException, InterruptedException {

  }
}
