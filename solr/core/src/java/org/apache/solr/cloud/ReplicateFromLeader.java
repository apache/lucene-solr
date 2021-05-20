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

package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;

import org.apache.lucene.index.IndexCommit;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.IndexFetcher;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.update.UpdateLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicateFromLeader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CoreContainer cc;
  private final String coreName;

  private volatile ReplicationHandler replicationProcess;
  private volatile long lastVersion = 0;

  public ReplicateFromLeader(CoreContainer cc, String coreName) {
    this.cc = cc;
    this.coreName = coreName;
  }

  /**
   * Start a replication handler thread that will periodically pull indices from the shard leader
   *
   * <p>This is separate from the ReplicationHandler that listens at /replication, used for recovery
   * and leader actions. It is simpler to discard the entire polling ReplicationHandler rather then
   * worrying about disabling polling and correctly setting all of the leader bits if we need to reset.
   *
   * <p>TODO: It may be cleaner to extract the polling logic use that directly instead of creating
   * what might be a fairly heavyweight instance here.
   *
   * @param switchTransactionLog if true, ReplicationHandler will rotate the transaction log once
   * the replication is done
   */
  public void startReplication(boolean switchTransactionLog) {
    try (SolrCore core = cc.getCore(coreName)) {
      if (core == null) {
        if (cc.isShutDown()) {
          return;
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "SolrCore not found:" + coreName + " in "
                  + CloudUtil.getLoadedCoreNamesAsString(cc));
        }
      }
      SolrConfig.UpdateHandlerInfo uinfo = core.getSolrConfig().getUpdateHandlerInfo();
      String pollIntervalStr = "00:00:03";
      if (System.getProperty("jetty.testMode") != null) {
        pollIntervalStr = "00:00:01";
      }
      if (uinfo.autoCommmitMaxTime != -1) {
        pollIntervalStr = toPollIntervalStr(uinfo.autoCommmitMaxTime/2);
      } else if (uinfo.autoSoftCommmitMaxTime != -1) {
        pollIntervalStr = toPollIntervalStr(uinfo.autoSoftCommmitMaxTime/2);
      }
      log.info("Will start replication from leader with poll interval: {}", pollIntervalStr );

      NamedList<Object> followerConfig = new NamedList<>();
      followerConfig.add("fetchFromLeader", Boolean.TRUE);

      // don't commit on leader version zero for PULL replicas as PULL should only get its index state from leader
      boolean skipCommitOnLeaderVersionZero = switchTransactionLog;
      if (!skipCommitOnLeaderVersionZero) {
        CloudDescriptor cloudDescriptor = core.getCoreDescriptor().getCloudDescriptor();
        if (cloudDescriptor != null) {
          Replica replica =
              cc.getZkController().getZkStateReader().getCollection(cloudDescriptor.getCollectionName())
                  .getSlice(cloudDescriptor.getShardId()).getReplica(cloudDescriptor.getCoreNodeName());
          if (replica != null && replica.getType() == Replica.Type.PULL) {
            skipCommitOnLeaderVersionZero = true; // only set this to true if we're a PULL replica, otherwise use value of switchTransactionLog
          }
        }
      }
      followerConfig.add(ReplicationHandler.SKIP_COMMIT_ON_LEADER_VERSION_ZERO, skipCommitOnLeaderVersionZero);

      followerConfig.add("pollInterval", pollIntervalStr);
      NamedList<Object> replicationConfig = new NamedList<>();
      replicationConfig.add("follower", followerConfig);

      String lastCommitVersion = getCommitVersion(core);
      if (lastCommitVersion != null) {
        lastVersion = Long.parseLong(lastCommitVersion);
      }

      replicationProcess = new ReplicationHandler();
      if (switchTransactionLog) {
        replicationProcess.setPollListener((solrCore, fetchResult) -> {
          if (fetchResult == IndexFetcher.IndexFetchResult.INDEX_FETCH_SUCCESS) {
            String commitVersion = getCommitVersion(core);
            if (commitVersion == null) return;
            if (Long.parseLong(commitVersion) == lastVersion) return;
            UpdateLog updateLog = solrCore.getUpdateHandler().getUpdateLog();
            SolrQueryRequest req = new LocalSolrQueryRequest(core,
                new ModifiableSolrParams());
            CommitUpdateCommand cuc = new CommitUpdateCommand(req, false);
            cuc.setVersion(Long.parseLong(commitVersion));
            updateLog.commitAndSwitchToNewTlog(cuc);
            lastVersion = Long.parseLong(commitVersion);
          }
        });
      }
      replicationProcess.init(replicationConfig);
      replicationProcess.inform(core);
    }
  }

  public static String getCommitVersion(SolrCore solrCore) {
    IndexCommit commit = solrCore.getDeletionPolicy().getLatestCommit();
    try {
      String commitVersion = commit.getUserData().get(SolrIndexWriter.COMMIT_COMMAND_VERSION);
      if (commitVersion == null) return null;
      else return commitVersion;
    } catch (Exception e) {
      log.warn("Cannot get commit command version from index commit point ",e);
      return null;
    }
  }

  private static String toPollIntervalStr(int ms) {
    int sec = ms/1000;
    int hour = sec / 3600;
    sec = sec % 3600;
    int min = sec / 60;
    sec = sec % 60;
    return hour + ":" + min + ":" + sec;
  }

  public void stopReplication() {
    if (replicationProcess != null) {
      replicationProcess.shutdown();
    }
  }
}
