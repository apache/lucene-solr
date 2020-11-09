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
import java.util.Collection;
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

    final SolrParams params = it.req.getParams();

    String cname = params.get(CoreAdminParams.CORE, null);

    String collection = params.get("collection");

    if (collection == null) {
      throw new IllegalArgumentException("collection cannot be null");
    }

    String shardId = params.get("shardId");
    String nodeName = params.get("nodeName");
    Replica.State waitForState = Replica.State.getState(params.get(ZkStateReader.STATE_PROP));
    Boolean checkLive = params.getBool("checkLive");
    Boolean onlyIfLeader = params.getBool("onlyIfLeader");

    log.info(
        "Going to wait for core: {}, state: {}, checkLive: {}, onlyIfLeader: {}: params={}",
        cname, waitForState, checkLive, onlyIfLeader, params);

    assert TestInjection.injectPrepRecoveryOpPauseForever();

    CoreContainer coreContainer = it.handler.coreContainer;
    // wait long enough for the leader conflict to work itself out plus a little extra
    int conflictWaitMs = coreContainer.getZkController().getLeaderConflictResolveWait();


    AtomicReference<String> errorMessage = new AtomicReference<>();
    try {
      coreContainer.getZkController().getZkStateReader().waitForState(collection, conflictWaitMs, TimeUnit.MILLISECONDS, (n, c) -> {
        if (c == null) {
          log.info("collection not found {}", collection);
          return false;
        }

        // wait until we are sure the recovering node is ready
        // to accept updates
        Replica.State state = null;
        boolean live = false;
        final Replica replica = c.getReplica(cname);
        if (replica != null) {
          if (replica != null) {
            state = replica.getState();
            live = n.contains(nodeName);

            ZkShardTerms shardTerms = coreContainer.getZkController().getShardTerms(collection, c.getSlice(replica).getName());
            // if the replica is waiting for leader to see recovery state, the leader should refresh its terms
            if (waitForState == Replica.State.RECOVERING && shardTerms.registered(cname)
                && shardTerms.skipSendingUpdatesTo(cname)) {
              // The replica changed its term, then published itself as RECOVERING.
              // This core already see replica as RECOVERING
              // so it is guarantees that a live-fetch will be enough for this core to see max term published
              log.info("refresh shard terms for core {}", cname);
              shardTerms.refreshTerms();
            }

            if (log.isInfoEnabled()) {
              log.info(
                  "In WaitForState(" + waitForState + "): collection=" + collection +
                      ", thisCore=" + cname +
                      ", live=" + live + ", checkLive=" + checkLive + ", currentState=" + state
                      + ", nodeName=" + nodeName +
                      ", core=" + cname
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

        return false;
      });

    } catch (TimeoutException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      String error = errorMessage.get();
      if (error == null)
        error = "Timeout waiting for collection state.";
      throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, error);
    }

  }
}
