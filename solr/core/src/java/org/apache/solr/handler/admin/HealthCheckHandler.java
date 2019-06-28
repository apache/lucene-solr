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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
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

/*
 * Health Check Handler for reporting the health of a specific node.
 *
 * This checks if the node is:
 * 1. Cores container is active.
 * 1. Connected to zookeeper.
 * 2. Listed in 'live_nodes' in zookeeper.
 */
public class HealthCheckHandler extends RequestHandlerBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

    log.debug("Invoked HealthCheckHandler on [{}]", coreContainer.getZkController().getNodeName());
    CoreContainer cores = getCoreContainer();

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
    ZkStateReader zkStateReader = cores.getZkController().getZkStateReader();
    ClusterState clusterState = zkStateReader.getClusterState();
    // Check for isConnected and isClosed
    if(zkStateReader.getZkClient().isClosed() || !zkStateReader.getZkClient().isConnected()) {
      rsp.add(STATUS, FAILURE);
      rsp.setException(new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Host Unavailable: Not connected to zk"));
      return;
    }

    // Set status to true if this node is in live_nodes
    if (clusterState.getLiveNodes().contains(cores.getZkController().getNodeName())) {
      rsp.add(STATUS, OK);
    } else {
      rsp.add(STATUS, FAILURE);
      rsp.setException(new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Host Unavailable: Not in live nodes as per zk"));
    }

    rsp.setHttpCaching(false);
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
