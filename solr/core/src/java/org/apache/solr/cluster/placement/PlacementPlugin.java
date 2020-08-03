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
 * Implemented by external plugins to control replica placement and movement on the search cluster (as well as other things
 * such as cluster elasticity?) when cluster changes are required (initiated elsewhere, most likely following a Collection
 * API call).
 */
public interface PlacementPlugin {
  /**
   * @param clusterTopo initial state of the cluster
   * @param placementRequest request for placing new replicas or moving existing replicas on the cluster.
   * @param propertyFactory Factory used by the plugin to build instances of {@link PropertyKey} to resolve properties
   *                        to their values.
   * @param propertyFetcher Allows resolving {@link PropertyKey}'s to {@link PropertyValue}'s by contacting the
   *                        relevant {@link PropertyValueSource} defined in each {@link PropertyKey}.
   * @param workOrderFactory Factory used to create instances of {@link WorkOrder} to return computed decision.
   * @return work order satisfying the placement request.
   */
  WorkOrder computePlacement(Topo clusterTopo, Request placementRequest, PropertyKeyFactory propertyFactory,
                                   PropertyValueFetcher propertyFetcher, WorkOrderFactory workOrderFactory) throws PlacementException, InterruptedException;
}
