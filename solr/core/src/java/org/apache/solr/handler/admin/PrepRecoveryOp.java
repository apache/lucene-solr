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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CoreAdminHandler.CallInfo;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class PrepRecoveryOp implements CoreAdminHandler.CoreAdminOp {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void execute(CallInfo it) throws Exception {
    assert TestInjection.injectPrepRecoveryOpPauseForever();

    final SolrParams params = it.req.getParams();

    String cname = params.get(CoreAdminParams.CORE);
    if (cname == null) {
      cname = "";
    }

    String nodeName = params.get("nodeName");
    String coreNodeName = params.get("coreNodeName");
    Replica.State waitForState = Replica.State.getState(params.get(ZkStateReader.STATE_PROP));
    Boolean checkLive = params.getBool("checkLive");
    Boolean onlyIfLeader = params.getBool("onlyIfLeader");
    Boolean onlyIfLeaderActive = params.getBool("onlyIfLeaderActive");

    log.info("Going to wait for coreNodeName: " + coreNodeName + ", state: " + waitForState
        + ", checkLive: " + checkLive + ", onlyIfLeader: " + onlyIfLeader
        + ", onlyIfLeaderActive: " + onlyIfLeaderActive);

    int maxTries = 0;
    Replica.State state = null;
    boolean live = false;
    int retry = 0;
    while (true) {
      CoreContainer coreContainer = it.handler.coreContainer;
      try (SolrCore core = coreContainer.getCore(cname)) {
        if (core == null && retry == 30) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "core not found:"
              + cname);
        }
        if (core != null) {
          if (onlyIfLeader != null && onlyIfLeader) {
            if (!core.getCoreDescriptor().getCloudDescriptor().isLeader()) {
              throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "We are not the leader");
            }
          }

          // wait until we are sure the recovering node is ready
          // to accept updates
          CloudDescriptor cloudDescriptor = core.getCoreDescriptor()
              .getCloudDescriptor();
          String collectionName = cloudDescriptor.getCollectionName();

          if (retry % 15 == 0) {
            if (retry > 0 && log.isInfoEnabled())
              log.info("After " + retry + " seconds, core " + cname + " (" +
                  cloudDescriptor.getShardId() + " of " +
                  cloudDescriptor.getCollectionName() + ") still does not have state: " +
                  waitForState + "; forcing ClusterState update from ZooKeeper");

            // force a cluster state update
            coreContainer.getZkController().getZkStateReader().forceUpdateCollection(collectionName);
          }

          if (maxTries == 0) {
            // wait long enough for the leader conflict to work itself out plus a little extra
            int conflictWaitMs = coreContainer.getZkController().getLeaderConflictResolveWait();
            maxTries = (int) Math.round(conflictWaitMs / 1000) + 3;
            log.info("Will wait a max of " + maxTries + " seconds to see " + cname + " (" +
                cloudDescriptor.getShardId() + " of " +
                cloudDescriptor.getCollectionName() + ") have state: " + waitForState);
          }

          ClusterState clusterState = coreContainer.getZkController().getClusterState();
          DocCollection collection = clusterState.getCollection(collectionName);
          Slice slice = collection.getSlice(cloudDescriptor.getShardId());
          if (slice != null) {
            final Replica replica = slice.getReplicasMap().get(coreNodeName);
            if (replica != null) {
              state = replica.getState();
              live = clusterState.liveNodesContain(nodeName);

              final Replica.State localState = cloudDescriptor.getLastPublished();

              // TODO: This is funky but I've seen this in testing where the replica asks the
              // leader to be in recovery? Need to track down how that happens ... in the meantime,
              // this is a safeguard
              boolean leaderDoesNotNeedRecovery = (onlyIfLeader != null &&
                  onlyIfLeader &&
                  core.getName().equals(replica.getStr("core")) &&
                  waitForState == Replica.State.RECOVERING &&
                  localState == Replica.State.ACTIVE &&
                  state == Replica.State.ACTIVE);

              if (leaderDoesNotNeedRecovery) {
                log.warn("Leader " + core.getName() + " ignoring request to be in the recovering state because it is live and active.");
              }

              boolean onlyIfActiveCheckResult = onlyIfLeaderActive != null && onlyIfLeaderActive && localState != Replica.State.ACTIVE;
              log.info("In WaitForState(" + waitForState + "): collection=" + collectionName + ", shard=" + slice.getName() +
                  ", thisCore=" + core.getName() + ", leaderDoesNotNeedRecovery=" + leaderDoesNotNeedRecovery +
                  ", isLeader? " + core.getCoreDescriptor().getCloudDescriptor().isLeader() +
                  ", live=" + live + ", checkLive=" + checkLive + ", currentState=" + state.toString() + ", localState=" + localState + ", nodeName=" + nodeName +
                  ", coreNodeName=" + coreNodeName + ", onlyIfActiveCheckResult=" + onlyIfActiveCheckResult + ", nodeProps: " + replica);

              if (!onlyIfActiveCheckResult && replica != null && (state == waitForState || leaderDoesNotNeedRecovery)) {
                if (checkLive == null) {
                  break;
                } else if (checkLive && live) {
                  break;
                } else if (!checkLive && !live) {
                  break;
                }
              }
            }
          }
        }

        if (retry++ == maxTries) {
          String collection = null;
          String leaderInfo = null;
          String shardId = null;
          try {
            CloudDescriptor cloudDescriptor =
                core.getCoreDescriptor().getCloudDescriptor();
            collection = cloudDescriptor.getCollectionName();
            shardId = cloudDescriptor.getShardId();
            leaderInfo = coreContainer.getZkController().
                getZkStateReader().getLeaderUrl(collection, shardId, 5000);
          } catch (Exception exc) {
            leaderInfo = "Not available due to: " + exc;
          }

          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "I was asked to wait on state " + waitForState + " for "
                  + shardId + " in " + collection + " on " + nodeName
                  + " but I still do not see the requested state. I see state: "
                  + state.toString() + " live:" + live + " leader from ZK: " + leaderInfo
          );
        }

        if (coreContainer.isShutDown()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Solr is shutting down");
        }

        // solrcloud_debug
        if (log.isDebugEnabled()) {
          try {
            LocalSolrQueryRequest r = new LocalSolrQueryRequest(core,
                new ModifiableSolrParams());
            CommitUpdateCommand commitCmd = new CommitUpdateCommand(r, false);
            commitCmd.softCommit = true;
            core.getUpdateHandler().commit(commitCmd);
            RefCounted<SolrIndexSearcher> searchHolder = core
                .getNewestSearcher(false);
            SolrIndexSearcher searcher = searchHolder.get();
            try {
              log.debug(core.getCoreDescriptor().getCoreContainer()
                  .getZkController().getNodeName()
                  + " to replicate "
                  + searcher.search(new MatchAllDocsQuery(), 1).totalHits
                  + " gen:"
                  + core.getDeletionPolicy().getLatestCommit().getGeneration()
                  + " data:" + core.getDataDir());
            } finally {
              searchHolder.decref();
            }
          } catch (Exception e) {
            log.debug("Error in solrcloud_debug block", e);
          }
        }
      }
      Thread.sleep(1000);
    }

    log.info("Waited coreNodeName: " + coreNodeName + ", state: " + waitForState
        + ", checkLive: " + checkLive + ", onlyIfLeader: " + onlyIfLeader + " for: " + retry + " seconds.");
  }
}
