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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController.NotInClusterStateException;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CoreAdminHandler.CallInfo;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class PrepRecoveryOp implements CoreAdminHandler.CoreAdminOp {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void execute(CallInfo it) throws Exception {
    assert TestInjection.injectPrepRecoveryOpPauseForever();

    final SolrParams params = it.req.getParams();

    String cname = params.get(CoreAdminParams.CORE, "");

    String nodeName = params.get("nodeName");
    String coreNodeName = params.get("coreNodeName");
    Replica.State waitForState = Replica.State.getState(params.get(ZkStateReader.STATE_PROP));
    Boolean checkLive = params.getBool("checkLive");
    Boolean onlyIfLeader = params.getBool("onlyIfLeader");

    CoreContainer coreContainer = it.handler.coreContainer;
    // wait long enough for the leader conflict to work itself out plus a little extra
    int conflictWaitMs = coreContainer.getZkController().getLeaderConflictResolveWait();
    log.info(
        "Going to wait for coreNodeName: {}, state: {}, checkLive: {}, onlyIfLeader: {}: {}",
        coreNodeName, waitForState, checkLive, onlyIfLeader);

    String collectionName;
    CloudDescriptor cloudDescriptor;
    try (SolrCore core = coreContainer.getCore(cname)) {
      if (core == null) {
        if (coreContainer.isCoreLoading(cname)) {
          coreContainer.waitForLoadingCore(cname, 30000);
          try (SolrCore core2 = coreContainer.getCore(cname)) {
            collectionName = core2.getCoreDescriptor().getCloudDescriptor().getCollectionName();
            cloudDescriptor = core2.getCoreDescriptor()
                    .getCloudDescriptor();
          }
        } else {
          coreContainer.waitForLoadingCore(cname, 30000);
          try (SolrCore core2 = coreContainer.getCore(cname)) {
            if (core2 == null) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "core not found:" + cname);
            }
            collectionName = core2.getCoreDescriptor().getCloudDescriptor().getCollectionName();
            cloudDescriptor = core2.getCoreDescriptor()
                .getCloudDescriptor();
          }
        }
      } else {
        collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
        cloudDescriptor = core.getCoreDescriptor()
                .getCloudDescriptor();
      }
    }
    AtomicReference<String> errorMessage = new AtomicReference<>();
    try {
      coreContainer.getZkController().getZkStateReader().waitForState(collectionName, conflictWaitMs, TimeUnit.MILLISECONDS, (n, c) -> {
        if (c == null) {
          log.info("collection not found {}",collectionName);
          return false;
        }

        // wait until we are sure the recovering node is ready
        // to accept updates
        Replica.State state = null;
        boolean live = false;
        Slice slice = c.getSlice(cloudDescriptor.getShardId());
        if (slice != null) {
          final Replica replica = slice.getReplicasMap().get(coreNodeName);
          if (replica != null) {
            state = replica.getState();
            live = n.contains(nodeName);

            final Replica.State localState = cloudDescriptor.getLastPublished();

            ZkShardTerms shardTerms = coreContainer.getZkController().getShardTerms(collectionName, slice.getName());
            // if the replica is waiting for leader to see recovery state, the leader should refresh its terms
            if (waitForState == Replica.State.RECOVERING && shardTerms.registered(coreNodeName)
                && shardTerms.skipSendingUpdatesTo(coreNodeName)) {
              // The replica changed it term, then published itself as RECOVERING.
              // This core already see replica as RECOVERING
              // so it is guarantees that a live-fetch will be enough for this core to see max term published
              shardTerms.refreshTerms(null);
            }

            if (log.isInfoEnabled()) {
              log.info(
                  "In WaitForState(" + waitForState + "): collection=" + collectionName + ", shard=" + slice.getName() +
                      ", thisCore=" + cname +
                      ", isLeader? " + cloudDescriptor.isLeader() +
                      ", live=" + live + ", checkLive=" + checkLive + ", currentState=" + state
                      + ", localState=" + localState + ", nodeName=" + nodeName +
                      ", coreNodeName=" + coreNodeName
                      + ", nodeProps: " + replica); //LOGOK
            }

            log.info("replica={} state={} waitForState={}", replica, state, waitForState);
            if (replica != null && (state == waitForState)) {
              if (checkLive == null) {
                log.info("checkLive=false, return true");
                return true;
              } else if (checkLive && live) {
                log.info("checkLive=true live={}, return true", live);
                return true;
              } else if (!checkLive && !live) {
                log.info("checkLive=false live={}, return true", live);
                return true;
              }
            }
          }
        }

        if (coreContainer.isShutDown()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Solr is shutting down");
        }

        return false;
      });

      try (SolrCore core = coreContainer.getCore(cname)) {
        if (core == null) throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "core not found:" + cname);
        if (onlyIfLeader != null && onlyIfLeader) {
          if (!core.getCoreDescriptor().getCloudDescriptor().isLeader()) {
            if (coreContainer.isShutDown()) {
              return;
            }

            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "We are not the leader");
          }
        }
      }
    } catch (TimeoutException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      String error = errorMessage.get();
      if (error == null)
        error = "Timeout waiting for collection state.";
      throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, error);
    }

  }
}
