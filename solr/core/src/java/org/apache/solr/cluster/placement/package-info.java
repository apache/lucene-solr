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
 * <p>This package contains interfaces visible by plugins implementing cluster elasticity, placement and scalability,
 * as well as a few examples on how plugins can be implemented.
 *
 * <p>Initially, only placement related plugins are supported.
 *
 * <p>The entry point is the {@link org.apache.solr.cluster.placement.PlacementPlugin} interface plugins implement.
 *
 * <p>From there, one will find the family of interfaces that allow navigating the cluster structure: {@link org.apache.solr.cluster.placement.Cluster},
 * {@link org.apache.solr.cluster.placement.Node}, {@link org.apache.solr.cluster.placement.SolrCollection}, {@link org.apache.solr.cluster.placement.Shard} and
 * {@link org.apache.solr.cluster.placement.Replica}.
 *
 * <p>Plugin code:
 * <ul>
 *   <li>Gets work to be done by receiving a {@link org.apache.solr.cluster.placement.PlacementRequest},</li>
 *   <li>Can obtain more info using {@link org.apache.solr.cluster.placement.PropertyKey}'s built using
 *   {@link org.apache.solr.cluster.placement.PropertyKeyFactory} then passed to
 *   {@link org.apache.solr.cluster.placement.PropertyValueFetcher},</li>
 *   <li>Uses the returned {@link org.apache.solr.cluster.placement.PropertyValue}'s as well as cluster state and
 *   {@link org.apache.solr.cluster.placement.SolrCollection#getCustomProperty} and other data to compute placement,</li>
 *   <li>Placement decisions are returned to Solr using an instance of {@link org.apache.solr.cluster.placement.PlacementPlan}
 *   built using the {@link org.apache.solr.cluster.placement.PlacementPlanFactory}</li>
 * </ul>
 */
package org.apache.solr.cluster.placement;