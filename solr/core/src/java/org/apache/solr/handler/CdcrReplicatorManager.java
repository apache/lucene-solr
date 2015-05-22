package org.apache.solr.handler;

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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CdcrUpdateLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class CdcrReplicatorManager implements CdcrStateManager.CdcrStateObserver {

  private List<CdcrReplicatorState> replicatorStates;

  private final CdcrReplicatorScheduler scheduler;
  private CdcrProcessStateManager processStateManager;
  private CdcrLeaderStateManager leaderStateManager;

  private SolrCore core;
  private String path;

  protected static Logger log = LoggerFactory.getLogger(CdcrReplicatorManager.class);

  CdcrReplicatorManager(final SolrCore core, String path,
                        SolrParams replicatorConfiguration,
                        Map<String, List<SolrParams>> replicasConfiguration) {
    this.core = core;
    this.path = path;

    // create states
    replicatorStates = new ArrayList<>();
    String myCollection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    List<SolrParams> targets = replicasConfiguration.get(myCollection);
    if (targets != null) {
      for (SolrParams params : targets) {
        String zkHost = params.get(CdcrParams.ZK_HOST_PARAM);
        String targetCollection = params.get(CdcrParams.TARGET_COLLECTION_PARAM);

        CloudSolrClient client = new CloudSolrClient(zkHost, true);
        client.setDefaultCollection(targetCollection);
        replicatorStates.add(new CdcrReplicatorState(targetCollection, zkHost, client));
      }
    }

    this.scheduler = new CdcrReplicatorScheduler(this, replicatorConfiguration);
  }

  void setProcessStateManager(final CdcrProcessStateManager processStateManager) {
    this.processStateManager = processStateManager;
    this.processStateManager.register(this);
  }

  void setLeaderStateManager(final CdcrLeaderStateManager leaderStateManager) {
    this.leaderStateManager = leaderStateManager;
    this.leaderStateManager.register(this);
  }

  /**
   * <p>
   * Inform the replicator manager of a change of state, and tell him to update its own state.
   * </p>
   * <p>
   * If we are the leader and the process state is STARTED, we need to initialise the log readers and start the
   * scheduled thread poll.
   * Otherwise, if the process state is STOPPED or if we are not the leader, we need to close the log readers and stop
   * the thread pool.
   * </p>
   * <p>
   * This method is synchronised as it can both be called by the leaderStateManager and the processStateManager.
   * </p>
   */
  @Override
  public synchronized void stateUpdate() {
    if (leaderStateManager.amILeader() && processStateManager.getState().equals(CdcrParams.ProcessState.STARTED)) {
      this.initLogReaders();
      this.scheduler.start();
      return;
    }

    this.scheduler.shutdown();
    this.closeLogReaders();
  }

  List<CdcrReplicatorState> getReplicatorStates() {
    return replicatorStates;
  }

  void initLogReaders() {
    String collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    String shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();
    CdcrUpdateLog ulog = (CdcrUpdateLog) core.getUpdateHandler().getUpdateLog();

    for (CdcrReplicatorState state : replicatorStates) {
      state.closeLogReader();
      try {
        long checkpoint = this.getCheckpoint(state);
        log.info("Create new update log reader for target {} with checkpoint {} @ {}:{}", state.getTargetCollection(),
            checkpoint, collectionName, shard);
        CdcrUpdateLog.CdcrLogReader reader = ulog.newLogReader();
        reader.seek(checkpoint);
        state.init(reader);
      } catch (IOException | SolrServerException | SolrException e) {
        log.warn("Unable to instantiate the log reader for target collection " + state.getTargetCollection(), e);
      } catch (InterruptedException e) {
        log.warn("Thread interrupted while instantiate the log reader for target collection " + state.getTargetCollection(), e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private long getCheckpoint(CdcrReplicatorState state) throws IOException, SolrServerException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.ACTION, CdcrParams.CdcrAction.COLLECTIONCHECKPOINT.toString());

    SolrRequest request = new QueryRequest(params);
    request.setPath(path);

    NamedList response = state.getClient().request(request);
    return (Long) response.get(CdcrParams.CHECKPOINT);
  }

  void closeLogReaders() {
    for (CdcrReplicatorState state : replicatorStates) {
      state.closeLogReader();
    }
  }

  /**
   * Shutdown all the {@link org.apache.solr.handler.CdcrReplicatorState} by closing their
   * {@link org.apache.solr.client.solrj.impl.CloudSolrClient} and
   * {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader}.
   */
  void shutdown() {
    this.scheduler.shutdown();
    for (CdcrReplicatorState state : replicatorStates) {
      state.shutdown();
    }
    replicatorStates.clear();
  }

}

