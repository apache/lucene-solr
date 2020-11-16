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

package org.apache.solr.handler.admin;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.FAILURE;
import static org.apache.solr.common.params.CommonParams.OK;
import static org.apache.solr.common.params.CommonParams.STATUS;

/**
 * Health Check Handler for reporting the health of a specific node.
 *
 * <p>
 *   By default the handler returns status <code>200 OK</code> if all checks succeed, else it returns
 *   status <code>503 UNAVAILABLE</code>:
 * </p>
 *   <ol>
 *     <li>Cores container is active.</li>
 *     <li>Node connected to zookeeper.</li>
 *     <li>Node listed in <code>live_nodes</code> in zookeeper.</li>
 *   </ol>
 *
 * <p>
 *   The handler takes an optional request parameter <code>requireHealthyCores=true</code>
 *   which will also require that all local cores that are part of an <b>active shard</b>
 *   are done initializing, i.e. not in states <code>RECOVERING</code> or <code>DOWN</code>.
 *   This parameter is designed to help during rolling restarts, to make sure each node
 *   is fully initialized and stable before proceeding with restarting the next node, and thus
 *   reduce the risk of restarting the last live replica of a shard.
 * </p>
 */
public class HealthCheckHandler extends RequestHandlerBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_REQUIRE_HEALTHY_CORES = "requireHealthyCores";
  private static final List<State> UNHEALTHY_STATES = Arrays.asList(State.DOWN, State.RECOVERING);

  CoreContainer coreContainer;

  public HealthCheckHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

  @Override
  final public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
  }

  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

    CoreContainer cores = getCoreContainer();
    rsp.setHttpCaching(false);

    // Core container should not be null and active (redundant check)
    if(cores == null || cores.isShutDown()) {
      rsp.setException(new SolrException(SolrException.ErrorCode.SERVER_ERROR, "CoreContainer is either not initialized or shutting down"));
      return;
    }
    if(!cores.isZooKeeperAware()) {
      //TODO: Support standalone instances
      rsp.setException(new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Health check is only available when running in SolrCloud mode"));
      return;
    }
    if (log.isDebugEnabled()) {
      log.debug("Invoked HealthCheckHandler on [{}]", coreContainer.getZkController().getNodeName());
    }
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();
    // Check for isConnected and isClosed
    if(zkStateReader.getZkClient().isClosed() || !zkStateReader.getZkClient().isConnected()) {
      rsp.add(STATUS, FAILURE);
      rsp.setException(new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Host Unavailable: Not connected to zk"));
      return;
    }

    // Fail if not in live_nodes
    if (!clusterState.getLiveNodes().contains(cores.getZkController().getNodeName())) {
      rsp.add(STATUS, FAILURE);
      rsp.setException(new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Host Unavailable: Not in live nodes as per zk"));
      return;
    }

    // Optionally require that all cores on this node are active if param 'requireHealthyCores=true'
    if (req.getParams().getBool(PARAM_REQUIRE_HEALTHY_CORES, false)) {
      Collection<CloudDescriptor> coreDescriptors = cores.getCores().stream()
          .map(c -> c.getCoreDescriptor().getCloudDescriptor()).collect(Collectors.toList());
      long unhealthyCores = findUnhealthyCores(coreDescriptors, clusterState);
      if (unhealthyCores > 0) {
          rsp.add(STATUS, FAILURE);
          rsp.add("num_cores_unhealthy", unhealthyCores);
          rsp.setException(new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, unhealthyCores + " out of "
              + cores.getNumAllCores() + " replicas are currently initializing or recovering"));
          return;
      }
      rsp.add("message", "All cores are healthy");
    }

    // All lights green, report healthy
    rsp.add(STATUS, OK);
  }

  /**
   * Find replicas DOWN or RECOVERING, or replicas in clusterstate that do not exist on local node.
   * We first find local cores which are either not registered or unhealthy, and check each of these against
   * the clusterstate, and return a count of unhealthy replicas
   * @param cores list of core cloud descriptors to iterate
   * @param clusterState clusterstate from ZK
   * @return number of unhealthy cores, either in DOWN or RECOVERING state
   */
  static long findUnhealthyCores(Collection<CloudDescriptor> cores, ClusterState clusterState) {
    return cores.stream()
      .filter(c -> !c.hasRegistered() || UNHEALTHY_STATES.contains(c.getLastPublished())) // Find candidates locally
      .filter(c -> clusterState.hasCollection(c.getCollectionName())) // Only care about cores for actual collections
      .filter(c -> clusterState.getCollection(c.getCollectionName()).getActiveSlicesMap().containsKey(c.getShardId()))
      .count();
  }

  @Override
  public String getDescription() {
    return "Health check handler for SolrCloud node";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}
