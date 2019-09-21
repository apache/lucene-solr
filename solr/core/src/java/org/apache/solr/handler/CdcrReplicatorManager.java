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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.admin.CoreAdminHandler.RESPONSE_STATUS;

class CdcrReplicatorManager implements CdcrStateManager.CdcrStateObserver {

  private static final int MAX_BOOTSTRAP_ATTEMPTS = 5;
  private static final int BOOTSTRAP_RETRY_DELAY_MS = 2000;
  // 6 hours is hopefully long enough for most indexes
  private static final long BOOTSTRAP_TIMEOUT_SECONDS = 6L * 3600L * 3600L;

  private List<CdcrReplicatorState> replicatorStates;

  private final CdcrReplicatorScheduler scheduler;
  private CdcrProcessStateManager processStateManager;
  private CdcrLeaderStateManager leaderStateManager;

  private SolrCore core;
  private String path;

  private ExecutorService bootstrapExecutor;
  private volatile BootstrapStatusRunnable bootstrapStatusRunnable;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

        CloudSolrClient client = new Builder(Collections.singletonList(zkHost), Optional.empty())
            .withSocketTimeout(30000).withConnectionTimeout(15000)
            .sendUpdatesOnlyToShardLeaders()
            .build();
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
      if (replicatorStates.size() > 0)  {
        this.bootstrapExecutor = ExecutorUtil.newMDCAwareFixedThreadPool(replicatorStates.size(),
            new SolrjNamedThreadFactory("cdcr-bootstrap-status"));
      }
      this.initLogReaders();
      this.scheduler.start();
      return;
    }

    this.scheduler.shutdown();
    if (bootstrapExecutor != null)  {
      IOUtils.closeQuietly(bootstrapStatusRunnable);
      ExecutorUtil.shutdownAndAwaitTermination(bootstrapExecutor);
    }
    this.closeLogReaders();
    Callable callable = core.getSolrCoreState().getCdcrBootstrapCallable();
    if (callable != null)  {
      CdcrRequestHandler.BootstrapCallable bootstrapCallable = (CdcrRequestHandler.BootstrapCallable) callable;
      IOUtils.closeQuietly(bootstrapCallable);
    }
  }

  List<CdcrReplicatorState> getReplicatorStates() {
    return replicatorStates;
  }

  private void initLogReaders() {
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
        boolean seek = reader.seek(checkpoint);
        state.init(reader);
        if (!seek) {
          // targetVersion is lower than the oldest known entry.
          // In this scenario, it probably means that there is a gap in the updates log.
          // the best we can do here is to bootstrap the target leader by replicating the full index
          final String targetCollection = state.getTargetCollection();
          state.setBootstrapInProgress(true);
          log.info("Attempting to bootstrap target collection: {}, shard: {}", targetCollection, shard);
          bootstrapStatusRunnable = new BootstrapStatusRunnable(core, state);
          log.info("Submitting bootstrap task to executor");
          try {
            bootstrapExecutor.submit(bootstrapStatusRunnable);
          } catch (Exception e) {
            log.error("Unable to submit bootstrap call to executor", e);
          }
        }
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
    if (bootstrapExecutor != null)  {
      IOUtils.closeQuietly(bootstrapStatusRunnable);
      ExecutorUtil.shutdownAndAwaitTermination(bootstrapExecutor);
    }
    for (CdcrReplicatorState state : replicatorStates) {
      state.shutdown();
    }
    replicatorStates.clear();
  }

  private class BootstrapStatusRunnable implements Runnable, Closeable {
    private final CdcrReplicatorState state;
    private final String targetCollection;
    private final String shard;
    private final String collectionName;
    private final CdcrUpdateLog ulog;
    private final String myCoreUrl;

    private volatile boolean closed = false;

    BootstrapStatusRunnable(SolrCore core, CdcrReplicatorState state) {
      this.collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
      this.shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();
      this.ulog = (CdcrUpdateLog) core.getUpdateHandler().getUpdateLog();
      this.state = state;
      this.targetCollection = state.getTargetCollection();
      String baseUrl = core.getCoreContainer().getZkController().getBaseUrl();
      this.myCoreUrl = ZkCoreNodeProps.getCoreUrl(baseUrl, core.getName());
    }

    @Override
    public void close() throws IOException {
      closed = true;
      try {
        Replica leader = state.getClient().getZkStateReader().getLeaderRetry(targetCollection, shard, 30000); // assume same shard exists on target
        String leaderCoreUrl = leader.getCoreUrl();
        HttpClient httpClient = state.getClient().getLbClient().getHttpClient();
        try (HttpSolrClient client = new HttpSolrClient.Builder(leaderCoreUrl).withHttpClient(httpClient).build()) {
          sendCdcrCommand(client, CdcrParams.CdcrAction.CANCEL_BOOTSTRAP);
        } catch (SolrServerException e) {
          log.error("Error sending cancel bootstrap message to target collection: {} shard: {} leader: {}",
              targetCollection, shard, leaderCoreUrl);
        }
      } catch (InterruptedException e) {
        log.error("Interrupted while closing BootstrapStatusRunnable", e);
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void run() {
      int retries = 1;
      boolean success = false;
      try {
        while (!closed && sendBootstrapCommand() != BootstrapStatus.SUBMITTED)  {
          Thread.sleep(BOOTSTRAP_RETRY_DELAY_MS);
        }
        TimeOut timeOut = new TimeOut(BOOTSTRAP_TIMEOUT_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        while (!timeOut.hasTimedOut()) {
          if (closed) {
            log.warn("Cancelling waiting for bootstrap on target: {} shard: {} to complete", targetCollection, shard);
            state.setBootstrapInProgress(false);
            break;
          }
          BootstrapStatus status = getBoostrapStatus();
          if (status == BootstrapStatus.RUNNING) {
            try {
              log.info("CDCR bootstrap running for {} seconds, sleeping for {} ms",
                  BOOTSTRAP_TIMEOUT_SECONDS - timeOut.timeLeft(TimeUnit.SECONDS), BOOTSTRAP_RETRY_DELAY_MS);
              timeOut.sleep(BOOTSTRAP_RETRY_DELAY_MS);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          } else if (status == BootstrapStatus.COMPLETED) {
            log.info("CDCR bootstrap successful in {} seconds", BOOTSTRAP_TIMEOUT_SECONDS - timeOut.timeLeft(TimeUnit.SECONDS));
            long checkpoint = CdcrReplicatorManager.this.getCheckpoint(state);
            log.info("Create new update log reader for target {} with checkpoint {} @ {}:{}", state.getTargetCollection(),
                checkpoint, collectionName, shard);
            CdcrUpdateLog.CdcrLogReader reader1 = ulog.newLogReader();
            reader1.seek(checkpoint);
            success = true;
            break;
          } else if (status == BootstrapStatus.FAILED) {
            log.warn("CDCR bootstrap failed in {} seconds", BOOTSTRAP_TIMEOUT_SECONDS - timeOut.timeLeft(TimeUnit.SECONDS));
            // let's retry a fixed number of times before giving up
            if (retries >= MAX_BOOTSTRAP_ATTEMPTS) {
              log.error("Unable to bootstrap the target collection: {}, shard: {} even after {} retries", targetCollection, shard, retries);
              break;
            } else {
              log.info("Retry: {} - Attempting to bootstrap target collection: {} shard: {}", retries, targetCollection, shard);
              while (!closed && sendBootstrapCommand() != BootstrapStatus.SUBMITTED)  {
                Thread.sleep(BOOTSTRAP_RETRY_DELAY_MS);
              }
              timeOut = new TimeOut(BOOTSTRAP_TIMEOUT_SECONDS, TimeUnit.SECONDS, TimeSource.NANO_TIME); // reset the timer
              retries++;
            }
          } else if (status == BootstrapStatus.NOTFOUND || status == BootstrapStatus.CANCELLED) {
            log.info("CDCR bootstrap " + (status == BootstrapStatus.NOTFOUND ? "not found" : "cancelled") + "in {} seconds",
                BOOTSTRAP_TIMEOUT_SECONDS - timeOut.timeLeft(TimeUnit.SECONDS));
            // the leader of the target shard may have changed and therefore there is no record of the
            // bootstrap process so we must retry the operation
            while (!closed && sendBootstrapCommand() != BootstrapStatus.SUBMITTED)  {
              Thread.sleep(BOOTSTRAP_RETRY_DELAY_MS);
            }
            retries = 1;
            timeOut = new TimeOut(6L * 3600L * 3600L, TimeUnit.SECONDS, TimeSource.NANO_TIME); // reset the timer
          } else if (status == BootstrapStatus.UNKNOWN || status == BootstrapStatus.SUBMITTED) {
            log.info("CDCR bootstrap is " + (status == BootstrapStatus.UNKNOWN ? "unknown" : "submitted"),
                BOOTSTRAP_TIMEOUT_SECONDS - timeOut.timeLeft(TimeUnit.SECONDS));
            // we were not able to query the status on the remote end
            // so just sleep for a bit and try again
            timeOut.sleep(BOOTSTRAP_RETRY_DELAY_MS);
          }
        }
      } catch (InterruptedException e) {
        log.info("Bootstrap thread interrupted");
        state.reportError(CdcrReplicatorState.ErrorType.INTERNAL);
        Thread.currentThread().interrupt();
      } catch (IOException | SolrServerException | SolrException e) {
        log.error("Unable to bootstrap the target collection " + targetCollection + " shard: " + shard, e);
        state.reportError(CdcrReplicatorState.ErrorType.BAD_REQUEST);
      } finally {
        if (success) {
          log.info("Bootstrap successful, giving the go-ahead to replicator");
          state.setBootstrapInProgress(false);
        }
      }
    }

    private BootstrapStatus sendBootstrapCommand() throws InterruptedException {
      Replica leader = state.getClient().getZkStateReader().getLeaderRetry(targetCollection, shard, 30000); // assume same shard exists on target
      String leaderCoreUrl = leader.getCoreUrl();
      HttpClient httpClient = state.getClient().getLbClient().getHttpClient();
      try (HttpSolrClient client = new HttpSolrClient.Builder(leaderCoreUrl).withHttpClient(httpClient).build()) {
        log.info("Attempting to bootstrap target collection: {} shard: {} leader: {}", targetCollection, shard, leaderCoreUrl);
        try {
          NamedList response = sendCdcrCommand(client, CdcrParams.CdcrAction.BOOTSTRAP, ReplicationHandler.MASTER_URL, myCoreUrl);
          log.debug("CDCR Bootstrap response: {}", response);
          String status = response.get(RESPONSE_STATUS).toString();
          return BootstrapStatus.valueOf(status.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
          log.error("Exception submitting bootstrap request", e);
          return BootstrapStatus.UNKNOWN;
        }
      } catch (IOException e) {
        log.error("There shouldn't be an IOException while closing but there was!", e);
      }
      return BootstrapStatus.UNKNOWN;
    }

    private BootstrapStatus getBoostrapStatus() throws InterruptedException {
      try {
        Replica leader = state.getClient().getZkStateReader().getLeaderRetry(targetCollection, shard, 30000); // assume same shard exists on target
        String leaderCoreUrl = leader.getCoreUrl();
        HttpClient httpClient = state.getClient().getLbClient().getHttpClient();
        try (HttpSolrClient client = new HttpSolrClient.Builder(leaderCoreUrl).withHttpClient(httpClient).build()) {
          NamedList response = sendCdcrCommand(client, CdcrParams.CdcrAction.BOOTSTRAP_STATUS);
          String status = (String) response.get(RESPONSE_STATUS);
          BootstrapStatus bootstrapStatus = BootstrapStatus.valueOf(status.toUpperCase(Locale.ROOT));
          if (bootstrapStatus == BootstrapStatus.RUNNING) {
            return BootstrapStatus.RUNNING;
          } else if (bootstrapStatus == BootstrapStatus.COMPLETED) {
            return BootstrapStatus.COMPLETED;
          } else if (bootstrapStatus == BootstrapStatus.FAILED) {
            return BootstrapStatus.FAILED;
          } else if (bootstrapStatus == BootstrapStatus.NOTFOUND) {
            log.warn("Bootstrap process was not found on target collection: {} shard: {}, leader: {}", targetCollection, shard, leaderCoreUrl);
            return BootstrapStatus.NOTFOUND;
          } else if (bootstrapStatus == BootstrapStatus.CANCELLED) {
            return BootstrapStatus.CANCELLED;
          } else {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Unknown status: " + status + " returned by BOOTSTRAP_STATUS command");
          }
        }
      } catch (Exception e) {
        log.error("Exception during bootstrap status request", e);
        return BootstrapStatus.UNKNOWN;
      }
    }
  }

  private NamedList sendCdcrCommand(SolrClient client, CdcrParams.CdcrAction action, String... params) throws SolrServerException, IOException {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    solrParams.set(CommonParams.QT, "/cdcr");
    solrParams.set(CommonParams.ACTION, action.toString());
    for (int i = 0; i < params.length - 1; i+=2) {
      solrParams.set(params[i], params[i + 1]);
    }
    SolrRequest request = new QueryRequest(solrParams);
    return client.request(request);
  }

  private enum BootstrapStatus  {
    SUBMITTED,
    RUNNING,
    COMPLETED,
    FAILED,
    NOTFOUND,
    CANCELLED,
    UNKNOWN
  }
}

