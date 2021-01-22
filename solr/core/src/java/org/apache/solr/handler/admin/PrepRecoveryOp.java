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

import org.apache.solr.cloud.ZkController.NotInClusterStateException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler.CallInfo;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;


class PrepRecoveryOp implements CoreAdminHandler.CoreAdminOp {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void execute(CallInfo it) throws Exception {

    final SolrParams params = it.req.getParams();

    String cname = params.get(CoreAdminParams.CORE, null);

    String collection = params.get("collection");

    String shard = params.get(ZkStateReader.SHARD_ID_PROP);

    if (collection == null) {
      throw new IllegalArgumentException("collection cannot be null");
    }

    Replica.State waitForState = Replica.State.getState(params.get(ZkStateReader.STATE_PROP));

    log.info(
        "Going to wait for core: {}, state: {}: params={}",
        cname, waitForState, params);

    assert TestInjection.injectPrepRecoveryOpPauseForever();

    CoreContainer coreContainer = it.handler.coreContainer;

    AtomicReference<String> errorMessage = new AtomicReference<>();

    try {
      coreContainer.getZkController().getZkStateReader().waitForState(collection, 5, TimeUnit.SECONDS, (n, c) -> {
        if (c == null) {
          log.info("collection not found {}", collection);
          return false;
        }

        // wait until we are sure the recovering node is ready
        // to accept updates
        final Replica replica = c.getReplica(cname);

        if (replica != null) {
          if ((replica.getState() == waitForState || replica.getState() == Replica.State.ACTIVE) && coreContainer.getZkController().getZkStateReader().isNodeLive(replica.getNodeName())) {
            // if (log.isDebugEnabled()) log.debug("replica={} state={} waitForState={}", replica, replica.getState(), waitForState);
            log.info("replica={} state={} waitForState={} isLive={}", replica, replica.getState(), waitForState, coreContainer.getZkController().getZkStateReader().isNodeLive(replica.getNodeName()));
            return true;
          }
        }

        return false;
      });

    } catch (TimeoutException | InterruptedException e) {
      ParWork.propagateInterrupt(e);
      String error = errorMessage.get();
      if (error == null)
        error = "Timeout waiting for collection state. \n" + coreContainer.getZkController().getZkStateReader().getClusterState().getCollectionOrNull(collection);
      throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, error);
    }
  }
}
