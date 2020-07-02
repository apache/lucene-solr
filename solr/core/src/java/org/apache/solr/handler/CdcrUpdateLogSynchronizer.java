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
package org.apache.solr.handler;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Synchronize periodically the update log of non-leader nodes with their leaders.
 * </p>
 * <p>
 * Non-leader nodes must always buffer updates in case of leader failures. They have to periodically
 * synchronize their update logs with their leader to remove old transaction logs that will never be used anymore.
 * This is performed by a background thread that is scheduled with a fixed delay. The background thread is sending
 * the action {@link org.apache.solr.handler.CdcrParams.CdcrAction#LASTPROCESSEDVERSION} to the leader to retrieve
 * the lowest last version number processed. This version is then used to move forward the buffer log reader.
 * </p>
 */
class CdcrUpdateLogSynchronizer implements CdcrStateManager.CdcrStateObserver {

  private CdcrLeaderStateManager leaderStateManager;
  private ScheduledExecutorService scheduler;

  private final SolrCore core;
  private final String collection;
  private final String shardId;
  private final String path;

  private int timeSchedule = DEFAULT_TIME_SCHEDULE;

  private static final int DEFAULT_TIME_SCHEDULE = 60000;  // by default, every minute

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  CdcrUpdateLogSynchronizer(SolrCore core, String path, SolrParams updateLogSynchonizerConfiguration) {
    this.core = core;
    this.path = path;
    this.collection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    this.shardId = core.getCoreDescriptor().getCloudDescriptor().getShardId();
    if (updateLogSynchonizerConfiguration != null) {
      this.timeSchedule = updateLogSynchonizerConfiguration.getInt(CdcrParams.SCHEDULE_PARAM, DEFAULT_TIME_SCHEDULE);
    }
  }

  void setLeaderStateManager(final CdcrLeaderStateManager leaderStateManager) {
    this.leaderStateManager = leaderStateManager;
    this.leaderStateManager.register(this);
  }

  @Override
  public void stateUpdate() {
    // If I am not the leader, I need to synchronise periodically my update log with my leader.
    if (!leaderStateManager.amILeader()) {
      scheduler = Executors.newSingleThreadScheduledExecutor(new SolrNamedThreadFactory("cdcr-update-log-synchronizer"));
      scheduler.scheduleWithFixedDelay(new UpdateLogSynchronisation(), 0, timeSchedule, TimeUnit.MILLISECONDS);
      return;
    }

    this.shutdown();
  }

  boolean isStarted() {
    return scheduler != null;
  }

  void shutdown() {
    if (scheduler != null) {
      // interrupts are often dangerous in Lucene / Solr code, but the
      // test for this will leak threads without
      scheduler.shutdownNow();
      scheduler = null;
    }
  }

  private class UpdateLogSynchronisation implements Runnable {

    private String getLeaderUrl() {
      ZkController zkController = core.getCoreContainer().getZkController();
      ClusterState cstate = zkController.getClusterState();
      DocCollection docCollection = cstate.getCollection(collection);
      ZkNodeProps leaderProps = docCollection.getLeader(shardId);
      if (leaderProps == null) { // we might not have a leader yet, returns null
        return null;
      }
      ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);
      return nodeProps.getCoreUrl();
    }

    @Override
    public void run() {
      try {
        String leaderUrl = getLeaderUrl();
        if (leaderUrl == null) { // we might not have a leader yet, stop and try again later
          return;
        }

        HttpSolrClient server = new HttpSolrClient.Builder(leaderUrl)
            .withConnectionTimeout(15000)
            .withSocketTimeout(60000)
            .build();

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.ACTION, CdcrParams.CdcrAction.LASTPROCESSEDVERSION.toString());

        @SuppressWarnings({"rawtypes"})
        SolrRequest request = new QueryRequest(params);
        request.setPath(path);

        long lastVersion;
        try {
          @SuppressWarnings({"rawtypes"})
          NamedList response = server.request(request);
          lastVersion = (Long) response.get(CdcrParams.LAST_PROCESSED_VERSION);
          if (log.isDebugEnabled()) {
            log.debug("My leader {} says its last processed _version_ number is: {}. I am {}", leaderUrl, lastVersion,
                core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName());
          }
        } catch (IOException | SolrServerException e) {
          log.warn("Couldn't get last processed version from leader {}: ", leaderUrl, e);
          return;
        } finally {
          try {
            server.close();
          } catch (IOException ioe) {
            log.warn("Caught exception trying to close client to {}: ", leaderUrl, ioe);
          }
        }

        // if we received -1, it means that the log reader on the leader has not yet started to read log entries
        // do nothing
        if (lastVersion == -1) {
          return;
        }

        try {
          CdcrUpdateLog ulog = (CdcrUpdateLog) core.getUpdateHandler().getUpdateLog();
          if (ulog.isBuffering()) {
            log.debug("Advancing replica buffering tlog reader to {} @ {}:{}", lastVersion, collection, shardId);
            ulog.getBufferToggle().seek(lastVersion);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          log.warn("Couldn't advance replica buffering tlog reader to {} (to remove old tlogs): ", lastVersion, e);
        } catch (IOException e) {
          log.warn("Couldn't advance replica buffering tlog reader to {} (to remove old tlogs): ", lastVersion, e);
        }
      } catch (Throwable e) {
        log.warn("Caught unexpected exception", e);
        throw e;
      }
    }
  }

}

