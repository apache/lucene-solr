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
 * <h1>Simulated environment for autoscaling.</h1>
 *
 * <h2>Goals</h2>
 *  <ul>
 *    <li>Use the actual unchanged autoscaling code for cluster state monitoring and autoscaling plan execution.</li>
 *    <li>Support testing large clusters (&gt; 100 nodes).</li>
 *    <li>Support fast testing using accelerated time (eg. 100x faster).</li>
 *    <li>Support enough of other Solr functionality for the test results to be meaningful.</li>
 *  </ul>
 *
 *  <h2>Simulated SolrCloudManager - {@link org.apache.solr.cloud.autoscaling.sim.SimCloudManager}</h2>
 *  This implementation of {@link org.apache.solr.client.solrj.cloud.SolrCloudManager}
 *  uses the following simulated components:
 *  <ul>
 *     <li>{@link org.apache.solr.cloud.autoscaling.sim.SimDistribStateManager} - in-memory ZK look-alike, with support for Watcher-s, ephemeral and sequential nodes.</li>
 *     <li>{@link org.apache.solr.cloud.autoscaling.sim.SimClusterStateProvider} - manages collection, replica infos, states and replica metrics.</li>
 *     <li>{@link org.apache.solr.cloud.autoscaling.sim.SimNodeStateProvider} - manages node metrics.</li>
 *     <li>{@link org.apache.solr.cloud.autoscaling.sim.GenericDistributedQueue} - DistributedQueue that uses SimDistribStateManager.</li>
 *  </ul>
 *  SimCloudManager also maintains an up-to-date /live_nodes in SimDistribStateManager, provides a SolrClient instance for use in tests,
 *  and provides several convenience methods for setting up simulated clusters, populating node and replica metrics, collecting
 *  autoscaling-related event history, collecting autoscaling event statistics, etc.
 *
 *  SimCloudManager runs actual {@link org.apache.solr.cloud.autoscaling.OverseerTriggerThread} so that it
 *  uses real trigger and trigger action implementations, as well as real event scheduling and processing code.
 *  It also provides methods for simulating Overseer leader change.
 *
 *  An important part of the SimCloudManager is also a request handler that processes common autoscaling
 *  and collection admin requests. Autoscaling requests are processes by an instance of
 *  {@link org.apache.solr.cloud.autoscaling.AutoScalingHandler} (and result in changes in respective
 *  data stored in {@link org.apache.solr.cloud.autoscaling.sim.SimDistribStateManager}). Collection
 *  admin commands are simulated, ie. they don't use actual {@link org.apache.solr.handler.admin.CollectionsHandler}
 *  due to the complex dependencies on real components.
 *
 *  <h2>{@link org.apache.solr.cloud.autoscaling.sim.SimClusterStateProvider}</h2>
 *  This components maintains collection and replica states:
 *  <ul>
 *    <li>Simulates delays between request and the actual cluster state changes</li>
 *    <li>Marks replicas as down when a node goes down (optionally preserving the replica metrics in order to simulate a node coming back), and keeps track of per-node cores and disk space.</li>
 *    <li>Runs a shard leader election look-alike on collection state updates.</li>
 *    <li>Maintains up-to-date /clusterstate.json and /clusterprops.json in SimDistribStateManager (which in turn notifies Watcher-s about collection updates).
 *    Currently for simplicity it uses the old single /clusterstate.json format for representing ClusterState.</li>
 *  </ul>
 *
 *  <h2>{@link org.apache.solr.cloud.autoscaling.sim.SimNodeStateProvider}</h2>
 *  This component maintains node metrics. When a simulated cluster is set up using eg.
 *  {@link org.apache.solr.cloud.autoscaling.sim.SimCloudManager#createCluster(int, org.apache.solr.common.util.TimeSource)}
 *  method, each simulated node is initialized with some basic metrics that are expected by the autoscaling
 *  framework, such as node name, fake system load average, heap usage and disk usage.
 *
 *  The number of cores and disk space metrics may be used in autoscaling calculations, so they are
 *  tracked and adjusted by {@link org.apache.solr.cloud.autoscaling.sim.SimClusterStateProvider} according
 *  to the currently active replicas located on each node.
 *
 *  <h2>Limitations of the simulation framework</h2>
 *  Currently the simulation framework is limited to testing the core autoscaling API in a single JVM.
 *  Using it for other purposes would require extensive modifications in Solr and in the framework code.
 *
 *  Specifically, the framework supports testing the following autoscaling components:
 *  <ul>
 *    <li>OverseerTriggerThread and components that it uses.</li>
 *    <li>Autoscaling config, triggers, trigger listeners, ScheduledTriggers, trigger event queues, ComputePlanAction / ExecutePlanAction, etc.</li>
 *  </ul>
 *  Overseer and CollectionsHandler Cmd implementations are NOT used, so cannot be properly tested - some of their functionality is simulated.
 *  Other SolrCloud components make too many direct references to ZkStateReader, or direct HTTP requests, or rely on too many other components and require much more complex functionality - they may be refactored later but the effort may be too high.
 *
 *  Simulation framework definitely does not support the following functionality:
 *  <ul>
 *    <li>Solr searching and indexing</li>
 *    <li>Any component that uses ZkController (eg. CoreContainer)</li>
 *    <li>Any component that uses ShardHandler (eg. CollectionsHandler Cmd-s)</li>
 *  </ul>
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
package org.apache.solr.cloud.autoscaling.sim;


