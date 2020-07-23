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

package org.apache.solr.cloud.gumi;

import java.util.List;

/**
 * Implemented by external plugins to control replica placement and movement on the search cluster (as well as other things
 * such as cluster elasticity?) when cluster changes are required (initiated elsewhere, most likely following a Collection
 * API call).
 */
public interface GumiPlugin {

  /**
   * @param clusterTopo initial state of the cluster
   * @param placementRequests requests for placing new data, moving or removing data on the cluster. These are ordered
   * @param propertyFactory Factory used by the plugin to build instances of {@link PropertyKey} to resolve properties
   *                        to their values.
   * @param propertyFetcher Allows resolving {@link PropertyKey}'s to {@link PropertyValue}'s by contacting the
   *                        relevant {@link PropertyKeyTarget} defined in each {@link PropertyKey}.
   * @param workOrderFactory Factory in used to create instances of {@link WorkOrder} to return computed decisions.
   * @return work orders to be executed (in order) and that will lead to satisfying all of the placement requests.
   */
  List<WorkOrder> computePlacement(Topo clusterTopo, List<Request> placementRequests, PropertyKeyFactory propertyFactory,
                                   PropertyKeyFetcher propertyFetcher, WorkOrderFactory workOrderFactory) throws GumiException, InterruptedException;
}
