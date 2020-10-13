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

/**
 * <p>This package contains interfaces visible by plugins (i.e. contributed code) implementing cluster elasticity,
 * placement and scalability, as well as a few examples on how plugins can be implemented.
 *
 * <p>Initially, only placement related plugins are supported.
 *
 * <p>The entry point is the {@link org.apache.solr.cluster.placement.PlacementPluginFactory} building instances
 * of the {@link org.apache.solr.cluster.placement.PlacementPlugin} interface where the placement computation is implemented.
 *
 * <p>From there, one will access the interfaces that allow navigating the cluster topology, see {@link org.apache.solr.cluster}.
 *
 * <p>Plugin code:
 * <ul>
 *   <li>Gets work to be done by receiving a {@link org.apache.solr.cluster.placement.PlacementRequest},</li>
 *   <li>Can obtain more info using {@link org.apache.solr.cluster.placement.AttributeFetcher} and building an
 *   {@link org.apache.solr.cluster.placement.AttributeValues}</li>
 *   <li>Uses the values from {@link org.apache.solr.cluster.placement.AttributeValues} as well as cluster state and
 *   {@link org.apache.solr.cluster.SolrCollection#getCustomProperty} and other data to compute placement,</li>
 *   <li>Placement decisions are returned to Solr using an instance of {@link org.apache.solr.cluster.placement.PlacementPlan}
 *   built using the {@link org.apache.solr.cluster.placement.PlacementPlanFactory}</li>
 * </ul>
 */
package org.apache.solr.cluster.placement;
