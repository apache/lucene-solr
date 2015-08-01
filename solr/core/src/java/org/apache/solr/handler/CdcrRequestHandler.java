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
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * <p>
 * This request handler implements the CDCR API and is responsible of the execution of the
 * {@link CdcrReplicator} threads.
 * </p>
 * <p>
 * It relies on three classes, {@link org.apache.solr.handler.CdcrLeaderStateManager},
 * {@link org.apache.solr.handler.CdcrBufferStateManager} and {@link org.apache.solr.handler.CdcrProcessStateManager}
 * to synchronise the state of the CDCR across all the nodes.
 * </p>
 * <p>
 * The CDCR process can be either {@link org.apache.solr.handler.CdcrParams.ProcessState#STOPPED} or {@link org.apache.solr.handler.CdcrParams.ProcessState#STARTED} by using the
 * actions {@link org.apache.solr.handler.CdcrParams.CdcrAction#STOP} and {@link org.apache.solr.handler.CdcrParams.CdcrAction#START} respectively. If a node is leader and the process
 * state is {@link org.apache.solr.handler.CdcrParams.ProcessState#STARTED}, the {@link CdcrReplicatorManager} will
 * start the {@link CdcrReplicator} threads. If a node becomes non-leader or if the process state becomes
 * {@link org.apache.solr.handler.CdcrParams.ProcessState#STOPPED}, the {@link CdcrReplicator} threads are stopped.
 * </p>
 * <p>
 * The CDCR can be switched to a "buffering" mode, in which the update log will never delete old transaction log
 * files. Such a mode can be enabled or disabled using the action {@link org.apache.solr.handler.CdcrParams.CdcrAction#ENABLEBUFFER} and
 * {@link org.apache.solr.handler.CdcrParams.CdcrAction#DISABLEBUFFER} respectively.
 * </p>
 * <p>
 * Known limitations: The source and target clusters must have the same topology. Replication between clusters
 * with a different number of shards will likely results in an inconsistent index.
 * </p>
 */
public class CdcrRequestHandler extends RequestHandlerBase implements SolrCoreAware {

  protected static Logger log = LoggerFactory.getLogger(CdcrRequestHandler.class);

  private SolrCore core;
  private String collection;
  private String path;

  private SolrParams updateLogSynchronizerConfiguration;
  private SolrParams replicatorConfiguration;
  private SolrParams bufferConfiguration;
  private Map<String, List<SolrParams>> replicasConfiguration;

  private CdcrProcessStateManager processStateManager;
  private CdcrBufferStateManager bufferStateManager;
  private CdcrReplicatorManager replicatorManager;
  private CdcrLeaderStateManager leaderStateManager;
  private CdcrUpdateLogSynchronizer updateLogSynchronizer;
  private CdcrBufferManager bufferManager;

  @Override
  public void init(NamedList args) {
    super.init(args);

    if (args != null) {
      // Configuration of the Update Log Synchronizer
      Object updateLogSynchonizerParam = args.get(CdcrParams.UPDATE_LOG_SYNCHRONIZER_PARAM);
      if (updateLogSynchonizerParam != null && updateLogSynchonizerParam instanceof NamedList) {
        updateLogSynchronizerConfiguration = SolrParams.toSolrParams((NamedList) updateLogSynchonizerParam);
      }

      // Configuration of the Replicator
      Object replicatorParam = args.get(CdcrParams.REPLICATOR_PARAM);
      if (replicatorParam != null && replicatorParam instanceof NamedList) {
        replicatorConfiguration = SolrParams.toSolrParams((NamedList) replicatorParam);
      }

      // Configuration of the Buffer
      Object bufferParam = args.get(CdcrParams.BUFFER_PARAM);
      if (bufferParam != null && bufferParam instanceof NamedList) {
        bufferConfiguration = SolrParams.toSolrParams((NamedList) bufferParam);
      }

      // Configuration of the Replicas
      replicasConfiguration = new HashMap<>();
      List replicas = args.getAll(CdcrParams.REPLICA_PARAM);
      for (Object replica : replicas) {
        if (replicas != null && replica instanceof NamedList) {
          SolrParams params = SolrParams.toSolrParams((NamedList) replica);
          if (!replicasConfiguration.containsKey(params.get(CdcrParams.SOURCE_COLLECTION_PARAM))) {
            replicasConfiguration.put(params.get(CdcrParams.SOURCE_COLLECTION_PARAM), new ArrayList<SolrParams>());
          }
          replicasConfiguration.get(params.get(CdcrParams.SOURCE_COLLECTION_PARAM)).add(params);
        }
      }
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Pick the action
    SolrParams params = req.getParams();
    CdcrParams.CdcrAction action = null;
    String a = params.get(CommonParams.ACTION);
    if (a != null) {
      action = CdcrParams.CdcrAction.get(a);
    }
    if (action == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + a);
    }

    switch (action) {
      case START: {
        this.handleStartAction(req, rsp);
        break;
      }
      case STOP: {
        this.handleStopAction(req, rsp);
        break;
      }
      case STATUS: {
        this.handleStatusAction(req, rsp);
        break;
      }
      case COLLECTIONCHECKPOINT: {
        this.handleCollectionCheckpointAction(req, rsp);
        break;
      }
      case SHARDCHECKPOINT: {
        this.handleShardCheckpointAction(req, rsp);
        break;
      }
      case ENABLEBUFFER: {
        this.handleEnableBufferAction(req, rsp);
        break;
      }
      case DISABLEBUFFER: {
        this.handleDisableBufferAction(req, rsp);
        break;
      }
      case LASTPROCESSEDVERSION: {
        this.handleLastProcessedVersionAction(req, rsp);
        break;
      }
      case QUEUES: {
        this.handleQueuesAction(req, rsp);
        break;
      }
      case OPS: {
        this.handleOpsAction(req, rsp);
        break;
      }
      case ERRORS: {
        this.handleErrorsAction(req, rsp);
        break;
      }
      default: {
        throw new RuntimeException("Unknown action: " + action);
      }
    }

    rsp.setHttpCaching(false);
  }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    collection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();

    // Make sure that the core is ZKAware
    if (!core.getCoreDescriptor().getCoreContainer().isZooKeeperAware()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Solr instance is not running in SolrCloud mode.");
    }

    // Make sure that the core is using the CdcrUpdateLog implementation
    if (!(core.getUpdateHandler().getUpdateLog() instanceof CdcrUpdateLog)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Solr instance is not configured with the cdcr update log.");
    }

    // Find the registered path of the handler
    path = null;
    for (Map.Entry<String, PluginBag.PluginHolder<SolrRequestHandler>> entry : core.getRequestHandlers().getRegistry().entrySet()) {
      if (core.getRequestHandlers().isLoaded(entry.getKey()) && entry.getValue().get() == this) {
        path = entry.getKey();
        break;
      }
    }
    if (path == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "The CdcrRequestHandler is not registered with the current core.");
    }
    if (!path.startsWith("/")) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "The CdcrRequestHandler needs to be registered to a path. Typically this is '/cdcr'");
    }

    // Initialisation phase
    // If the Solr cloud is being initialised, each CDCR node will start up in its default state, i.e., STOPPED
    // and non-leader. The leader state will be updated later, when all the Solr cores have been loaded.
    // If the Solr cloud has already been initialised, and the core is reloaded (i.e., because a node died or a new node
    // is added to the cluster), the CDCR node will synchronise its state with the global CDCR state that is stored
    // in zookeeper.

    // Initialise the buffer state manager
    bufferStateManager = new CdcrBufferStateManager(core, bufferConfiguration);
    // Initialise the process state manager
    processStateManager = new CdcrProcessStateManager(core);
    // Initialise the leader state manager
    leaderStateManager = new CdcrLeaderStateManager(core);

    // Initialise the replicator states manager
    replicatorManager = new CdcrReplicatorManager(core, path, replicatorConfiguration, replicasConfiguration);
    replicatorManager.setProcessStateManager(processStateManager);
    replicatorManager.setLeaderStateManager(leaderStateManager);
    // we need to inform it of a state event since the process and leader state
    // may have been synchronised during the initialisation
    replicatorManager.stateUpdate();

    // Initialise the update log synchronizer
    updateLogSynchronizer = new CdcrUpdateLogSynchronizer(core, path, updateLogSynchronizerConfiguration);
    updateLogSynchronizer.setLeaderStateManager(leaderStateManager);
    // we need to inform it of a state event since the leader state
    // may have been synchronised during the initialisation
    updateLogSynchronizer.stateUpdate();

    // Initialise the buffer manager
    bufferManager = new CdcrBufferManager(core);
    bufferManager.setLeaderStateManager(leaderStateManager);
    bufferManager.setBufferStateManager(bufferStateManager);
    // we need to inform it of a state event since the leader state
    // may have been synchronised during the initialisation
    bufferManager.stateUpdate();

    // register the close hook
    this.registerCloseHook(core);
  }

  /**
   * register a close hook to properly shutdown the state manager and scheduler
   */
  private void registerCloseHook(SolrCore core) {
    core.addCloseHook(new CloseHook() {

      @Override
      public void preClose(SolrCore core) {
        String collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
        String shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();
        log.info("Solr core is being closed - shutting down CDCR handler @ {}:{}", collectionName, shard);

        updateLogSynchronizer.shutdown();
        replicatorManager.shutdown();
        bufferStateManager.shutdown();
        processStateManager.shutdown();
        leaderStateManager.shutdown();
      }

      @Override
      public void postClose(SolrCore core) {
      }

    });
  }

  /**
   * <p>
   * Update and synchronize the process state.
   * </p>
   * <p>
   * The process state manager must notify the replicator states manager of the change of state.
   * </p>
   */
  private void handleStartAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (processStateManager.getState() == CdcrParams.ProcessState.STOPPED) {
      processStateManager.setState(CdcrParams.ProcessState.STARTED);
      processStateManager.synchronize();
    }

    rsp.add(CdcrParams.CdcrAction.STATUS.toLower(), this.getStatus());
  }

  private void handleStopAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (processStateManager.getState() == CdcrParams.ProcessState.STARTED) {
      processStateManager.setState(CdcrParams.ProcessState.STOPPED);
      processStateManager.synchronize();
    }

    rsp.add(CdcrParams.CdcrAction.STATUS.toLower(), this.getStatus());
  }

  private void handleStatusAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    rsp.add(CdcrParams.CdcrAction.STATUS.toLower(), this.getStatus());
  }

  private NamedList getStatus() {
    NamedList status = new NamedList();
    status.add(CdcrParams.ProcessState.getParam(), processStateManager.getState().toLower());
    status.add(CdcrParams.BufferState.getParam(), bufferStateManager.getState().toLower());
    return status;
  }

  /**
   * This action is generally executed on the target cluster in order to retrieve the latest update checkpoint.
   * This checkpoint is used on the source cluster to setup the
   * {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader} of a shard leader. <br/>
   * This method will execute in parallel one
   * {@link org.apache.solr.handler.CdcrParams.CdcrAction#SHARDCHECKPOINT} request per shard leader. It will
   * then pick the lowest version number as checkpoint. Picking the lowest amongst all shards will ensure that we do not
   * pick a checkpoint that is ahead of the source cluster. This can occur when other shard leaders are sending new
   * updates to the target cluster while we are currently instantiating the
   * {@link org.apache.solr.update.CdcrUpdateLog.CdcrLogReader}.
   * This solution only works in scenarios where the topology of the source and target clusters are identical.
   */
  private void handleCollectionCheckpointAction(SolrQueryRequest req, SolrQueryResponse rsp)
      throws IOException, SolrServerException {
    ZkController zkController = core.getCoreDescriptor().getCoreContainer().getZkController();
    try {
      zkController.getZkStateReader().updateClusterState();
    } catch (Exception e) {
      log.warn("Error when updating cluster state", e);
    }
    ClusterState cstate = zkController.getClusterState();
    Collection<Slice> shards = cstate.getActiveSlices(collection);

    ExecutorService parallelExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(new DefaultSolrThreadFactory("parallelCdcrExecutor"));

    long checkpoint = Long.MAX_VALUE;
    try {
      List<Callable<Long>> callables = new ArrayList<>();
      for (Slice shard : shards) {
        ZkNodeProps leaderProps = zkController.getZkStateReader().getLeaderRetry(collection, shard.getName());
        ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);
        callables.add(new SliceCheckpointCallable(nodeProps.getCoreUrl(), path));
      }

      for (final Future<Long> future : parallelExecutor.invokeAll(callables)) {
        long version = future.get();
        if (version < checkpoint) { // we must take the lowest checkpoint from all the shards
          checkpoint = version;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error while requesting shard's checkpoints", e);
    } catch (ExecutionException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error while requesting shard's checkpoints", e);
    } finally {
      parallelExecutor.shutdown();
    }

    rsp.add(CdcrParams.CHECKPOINT, checkpoint);
  }

  /**
   * Retrieve the version number of the latest entry of the {@link org.apache.solr.update.UpdateLog}.
   */
  private void handleShardCheckpointAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (!leaderStateManager.amILeader()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Action '" + CdcrParams.CdcrAction.SHARDCHECKPOINT +
          "' sent to non-leader replica");
    }

    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
    UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates();
    List<Long> versions = recentUpdates.getVersions(1);
    long lastVersion = versions.isEmpty() ? -1 : Math.abs(versions.get(0));
    rsp.add(CdcrParams.CHECKPOINT, lastVersion);
    recentUpdates.close();
  }

  private void handleEnableBufferAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (bufferStateManager.getState() == CdcrParams.BufferState.DISABLED) {
      bufferStateManager.setState(CdcrParams.BufferState.ENABLED);
      bufferStateManager.synchronize();
    }

    rsp.add(CdcrParams.CdcrAction.STATUS.toLower(), this.getStatus());
  }

  private void handleDisableBufferAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    if (bufferStateManager.getState() == CdcrParams.BufferState.ENABLED) {
      bufferStateManager.setState(CdcrParams.BufferState.DISABLED);
      bufferStateManager.synchronize();
    }

    rsp.add(CdcrParams.CdcrAction.STATUS.toLower(), this.getStatus());
  }

  /**
   * <p>
   * We have to take care of four cases:
   * <ul>
   * <li>Replication & Buffering</li>
   * <li>Replication & No Buffering</li>
   * <li>No Replication & Buffering</li>
   * <li>No Replication & No Buffering</li>
   * </ul>
   * In the first three cases, at least one log reader should have been initialised. We should take the lowest
   * last processed version across all the initialised readers. In the last case, there isn't a log reader
   * initialised. We should instantiate one and get the version of the first entries.
   * </p>
   */
  private void handleLastProcessedVersionAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    String collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    String shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();

    if (!leaderStateManager.amILeader()) {
      log.warn("Action {} sent to non-leader replica @ {}:{}", CdcrParams.CdcrAction.LASTPROCESSEDVERSION, collectionName, shard);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Action " + CdcrParams.CdcrAction.LASTPROCESSEDVERSION +
          " sent to non-leader replica");
    }

    // take care of the first three cases
    // first check the log readers from the replicator states
    long lastProcessedVersion = Long.MAX_VALUE;
    for (CdcrReplicatorState state : replicatorManager.getReplicatorStates()) {
      long version = Long.MAX_VALUE;
      if (state.getLogReader() != null) {
        version = state.getLogReader().getLastVersion();
      }
      lastProcessedVersion = Math.min(lastProcessedVersion, version);
    }

    // next check the log reader of the buffer
    CdcrUpdateLog.CdcrLogReader bufferLogReader = ((CdcrUpdateLog) core.getUpdateHandler().getUpdateLog()).getBufferToggle();
    if (bufferLogReader != null) {
      lastProcessedVersion = Math.min(lastProcessedVersion, bufferLogReader.getLastVersion());
    }

    // the fourth case: no cdc replication, no buffering: all readers were null
    if (processStateManager.getState().equals(CdcrParams.ProcessState.STOPPED) &&
        bufferStateManager.getState().equals(CdcrParams.BufferState.DISABLED)) {
      CdcrUpdateLog.CdcrLogReader logReader = ((CdcrUpdateLog) core.getUpdateHandler().getUpdateLog()).newLogReader();
      try {
        // let the reader initialize lastVersion
        logReader.next();
        lastProcessedVersion = Math.min(lastProcessedVersion, logReader.getLastVersion());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error while fetching the last processed version", e);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Error while fetching the last processed version", e);
      } finally {
        logReader.close();
      }
    }

    log.info("Returning the lowest last processed version {}  @ {}:{}", lastProcessedVersion, collectionName, shard);
    rsp.add(CdcrParams.LAST_PROCESSED_VERSION, lastProcessedVersion);
  }

  private void handleQueuesAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    NamedList hosts = new NamedList();

    for (CdcrReplicatorState state : replicatorManager.getReplicatorStates()) {
      NamedList queueStats = new NamedList();

      CdcrUpdateLog.CdcrLogReader logReader = state.getLogReader();
      if (logReader == null) {
        String collectionName = req.getCore().getCoreDescriptor().getCloudDescriptor().getCollectionName();
        String shard = req.getCore().getCoreDescriptor().getCloudDescriptor().getShardId();
        log.warn("The log reader for target collection {} is not initialised @ {}:{}",
            state.getTargetCollection(), collectionName, shard);
        queueStats.add(CdcrParams.QUEUE_SIZE, -1l);
      } else {
        queueStats.add(CdcrParams.QUEUE_SIZE, logReader.getNumberOfRemainingRecords());
      }
      queueStats.add(CdcrParams.LAST_TIMESTAMP, state.getTimestampOfLastProcessedOperation());

      if (hosts.get(state.getZkHost()) == null) {
        hosts.add(state.getZkHost(), new NamedList());
      }
      ((NamedList) hosts.get(state.getZkHost())).add(state.getTargetCollection(), queueStats);
    }

    rsp.add(CdcrParams.QUEUES, hosts);
    UpdateLog updateLog = core.getUpdateHandler().getUpdateLog();
    rsp.add(CdcrParams.TLOG_TOTAL_SIZE, updateLog.getTotalLogsSize());
    rsp.add(CdcrParams.TLOG_TOTAL_COUNT, updateLog.getTotalLogsNumber());
    rsp.add(CdcrParams.UPDATE_LOG_SYNCHRONIZER,
        updateLogSynchronizer.isStarted() ? CdcrParams.ProcessState.STARTED.toLower() : CdcrParams.ProcessState.STOPPED.toLower());
  }

  private void handleOpsAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    NamedList hosts = new NamedList();

    for (CdcrReplicatorState state : replicatorManager.getReplicatorStates()) {
      NamedList ops = new NamedList();
      ops.add(CdcrParams.COUNTER_ALL, state.getBenchmarkTimer().getOperationsPerSecond());
      ops.add(CdcrParams.COUNTER_ADDS, state.getBenchmarkTimer().getAddsPerSecond());
      ops.add(CdcrParams.COUNTER_DELETES, state.getBenchmarkTimer().getDeletesPerSecond());

      if (hosts.get(state.getZkHost()) == null) {
        hosts.add(state.getZkHost(), new NamedList());
      }
      ((NamedList) hosts.get(state.getZkHost())).add(state.getTargetCollection(), ops);
    }

    rsp.add(CdcrParams.OPERATIONS_PER_SECOND, hosts);
  }

  private void handleErrorsAction(SolrQueryRequest req, SolrQueryResponse rsp) {
    NamedList hosts = new NamedList();

    for (CdcrReplicatorState state : replicatorManager.getReplicatorStates()) {
      NamedList errors = new NamedList();

      errors.add(CdcrParams.CONSECUTIVE_ERRORS, state.getConsecutiveErrors());
      errors.add(CdcrReplicatorState.ErrorType.BAD_REQUEST.toLower(), state.getErrorCount(CdcrReplicatorState.ErrorType.BAD_REQUEST));
      errors.add(CdcrReplicatorState.ErrorType.INTERNAL.toLower(), state.getErrorCount(CdcrReplicatorState.ErrorType.INTERNAL));

      NamedList lastErrors = new NamedList();
      for (String[] lastError : state.getLastErrors()) {
        lastErrors.add(lastError[0], lastError[1]);
      }
      errors.add(CdcrParams.LAST, lastErrors);

      if (hosts.get(state.getZkHost()) == null) {
        hosts.add(state.getZkHost(), new NamedList());
      }
      ((NamedList) hosts.get(state.getZkHost())).add(state.getTargetCollection(), errors);
    }

    rsp.add(CdcrParams.ERRORS, hosts);
  }

  @Override
  public String getDescription() {
    return "Manage Cross Data Center Replication";
  }

  /**
   * A thread subclass for executing a single
   * {@link org.apache.solr.handler.CdcrParams.CdcrAction#SHARDCHECKPOINT} action.
   */
  private static final class SliceCheckpointCallable implements Callable<Long> {

    final String baseUrl;
    final String cdcrPath;

    SliceCheckpointCallable(final String baseUrl, final String cdcrPath) {
      this.baseUrl = baseUrl;
      this.cdcrPath = cdcrPath;
    }

    @Override
    public Long call() throws Exception {
      HttpSolrClient server = new HttpSolrClient(baseUrl);
      try {
        server.setConnectionTimeout(15000);
        server.setSoTimeout(60000);

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.ACTION, CdcrParams.CdcrAction.SHARDCHECKPOINT.toString());

        SolrRequest request = new QueryRequest(params);
        request.setPath(cdcrPath);

        NamedList response = server.request(request);
        return (Long) response.get(CdcrParams.CHECKPOINT);
      } finally {
        server.close();
      }
    }

  }

}

