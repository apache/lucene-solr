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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.solr.common.params.CommonParams.*;

/*
 * Health Check Handler for reporting the health of a specific node.
 *
 * This checks if:
 * 1. Cores container is active.
 * 2. Node connected to zookeeper.
 * 3. Node listed in 'live_nodes' in zookeeper.
 * 4. No RECOVERING or DOWN cores (if request param requireHealthyCores=true)
 */
public class HealthCheckHandler extends RequestHandlerBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String PARAM_REQUIRE_HEALTHY_CORES = "requireHealthyCores";
  private static final List<State> UNHEALTHY_STATES = Arrays.asList(State.DOWN, State.RECOVERING);

  CoreContainer coreContainer;

  public HealthCheckHandler() {}

  public HealthCheckHandler(final CoreContainer coreContainer) {
    super();
    this.coreContainer = coreContainer;
  }

  @Override
  final public void init(NamedList args) {
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
    log.debug("Invoked HealthCheckHandler on [{}]", coreContainer.getZkController().getNodeName());
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

    // Optionally require that all cores on this node are active if param 'failWhenRecovering=true'
    if (req.getParams().getBool(PARAM_REQUIRE_HEALTHY_CORES, false)) {
      List<String> unhealthyCores = findUnhealthyCores(clusterState,
              cores.getNodeConfig().getNodeName(),
              cores.getAllCoreNames());
      if (unhealthyCores.size() > 0) {
          rsp.add(STATUS, FAILURE);
          rsp.setException(new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
                  "Replica(s) " + unhealthyCores + " are currently initializing or recovering"));
          return;
      }
      rsp.add("MESSAGE", "All cores are healthy");
    }

    // All lights green, report healthy
    rsp.add(STATUS, OK);
  }

  /**
   * Find replicas DOWN or RECOVERING, or replicas in clusterstate that do not exist on local node
   * @param clusterState clusterstate from ZK
   * @param nodeName this node name
   * @param allCoreNames list of all core names on current node
   * @return list of core names that are either DOWN ore RECOVERING on 'nodeName'
   */
  static List<String> findUnhealthyCores(ClusterState clusterState, String nodeName, Collection<String> allCoreNames) {
    return clusterState.getCollectionsMap().values().stream()
            .flatMap(c -> c.getActiveSlices().stream())
            .flatMap(s -> s.getReplicas(r -> nodeName.equals(r.getNodeName())).stream())
            .filter(r -> UNHEALTHY_STATES.contains(r.getState())
                    || !allCoreNames.contains(r.getCoreName()))
            .map(Replica::getCoreName)
            .collect(Collectors.toList());
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
