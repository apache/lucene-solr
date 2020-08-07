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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkShardTerms;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.PluginBag;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.CdcrUpdateLog;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.update.VersionInfo;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.admin.CoreAdminHandler.COMPLETED;
import static org.apache.solr.handler.admin.CoreAdminHandler.FAILED;
import static org.apache.solr.handler.admin.CoreAdminHandler.RESPONSE;
import static org.apache.solr.handler.admin.CoreAdminHandler.RESPONSE_MESSAGE;
import static org.apache.solr.handler.admin.CoreAdminHandler.RESPONSE_STATUS;
import static org.apache.solr.handler.admin.CoreAdminHandler.RUNNING;

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
 * @deprecated since 8.6
 */
@Deprecated
public class CdcrRequestHandler extends RequestHandlerBase implements SolrCoreAware {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private SolrCore core;
  private String collection;
  private String shard;
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
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    super.init(args);

    log.warn("CDCR (in its current form) is deprecated as of 8.6 and shall be removed in 9.0. See SOLR-14022 for details.");

    if (args != null) {
      // Configuration of the Update Log Synchronizer
      Object updateLogSynchonizerParam = args.get(CdcrParams.UPDATE_LOG_SYNCHRONIZER_PARAM);
      if (updateLogSynchonizerParam != null && updateLogSynchonizerParam instanceof NamedList) {
        updateLogSynchronizerConfiguration = ((NamedList) updateLogSynchonizerParam).toSolrParams();
      }

      // Configuration of the Replicator
      Object replicatorParam = args.get(CdcrParams.REPLICATOR_PARAM);
      if (replicatorParam != null && replicatorParam instanceof NamedList) {
        replicatorConfiguration = ((NamedList) replicatorParam).toSolrParams();
      }

      // Configuration of the Buffer
      Object bufferParam = args.get(CdcrParams.BUFFER_PARAM);
      if (bufferParam != null && bufferParam instanceof NamedList) {
        bufferConfiguration = ((NamedList) bufferParam).toSolrParams();
      }

      // Configuration of the Replicas
      replicasConfiguration = new HashMap<>();
      @SuppressWarnings({"rawtypes"})
      List replicas = args.getAll(CdcrParams.REPLICA_PARAM);
      for (Object replica : replicas) {
        if (replica != null && replica instanceof NamedList) {
          SolrParams params = ((NamedList) replica).toSolrParams();
          if (!replicasConfiguration.containsKey(params.get(CdcrParams.SOURCE_COLLECTION_PARAM))) {
            replicasConfiguration.put(params.get(CdcrParams.SOURCE_COLLECTION_PARAM), new ArrayList<>());
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
      case BOOTSTRAP: {
        this.handleBootstrapAction(req, rsp);
        break;
      }
      case BOOTSTRAP_STATUS:  {
        this.handleBootstrapStatus(req, rsp);
        break;
      }
      case CANCEL_BOOTSTRAP:  {
        this.handleCancelBootstrap(req, rsp);
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
    shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();

    // Make sure that the core is ZKAware
    if (!core.getCoreContainer().isZooKeeperAware()) {
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
        log.info("Solr core is being closed - shutting down CDCR handler @ {}:{}", collection, shard);

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

  @SuppressWarnings({"unchecked", "rawtypes"})
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
    ZkController zkController = core.getCoreContainer().getZkController();
    try {
      zkController.getZkStateReader().forceUpdateCollection(collection);
    } catch (Exception e) {
      log.warn("Error when updating cluster state", e);
    }
    ClusterState cstate = zkController.getClusterState();
    DocCollection docCollection = cstate.getCollectionOrNull(collection);
    Collection<Slice> shards = docCollection == null? null : docCollection.getActiveSlices();

    ExecutorService parallelExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("parallelCdcrExecutor"));

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
    VersionInfo versionInfo = ulog.getVersionInfo();
    try (UpdateLog.RecentUpdates recentUpdates = ulog.getRecentUpdates()) {
      long maxVersionFromRecent = recentUpdates.getMaxRecentVersion();
      long maxVersionFromIndex = versionInfo.getMaxVersionFromIndex(req.getSearcher());
      log.info("Found maxVersionFromRecent {} maxVersionFromIndex {}", maxVersionFromRecent, maxVersionFromIndex);
      // there is no race with ongoing bootstrap because we don't expect any updates to come from the source
      long maxVersion = Math.max(maxVersionFromIndex, maxVersionFromRecent);
      if (maxVersion == 0L) {
        maxVersion = -1;
      }
      rsp.add(CdcrParams.CHECKPOINT, maxVersion);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Action '" + CdcrParams.CdcrAction.SHARDCHECKPOINT +
          "' could not read max version");
    }
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

    log.debug("Returning the lowest last processed version {}  @ {}:{}", lastProcessedVersion, collectionName, shard);
    rsp.add(CdcrParams.LAST_PROCESSED_VERSION, lastProcessedVersion);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
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

  @SuppressWarnings({"unchecked", "rawtypes"})
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

  @SuppressWarnings({"unchecked", "rawtypes"})
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

  private void handleBootstrapAction(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException, InterruptedException, SolrServerException {
    String collectionName = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    String shard = core.getCoreDescriptor().getCloudDescriptor().getShardId();
    if (!leaderStateManager.amILeader()) {
      log.warn("Action {} sent to non-leader replica @ {}:{}", CdcrParams.CdcrAction.BOOTSTRAP, collectionName, shard);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Action " + CdcrParams.CdcrAction.BOOTSTRAP +
          " sent to non-leader replica");
    }
    CountDownLatch latch = new CountDownLatch(1); // latch to make sure BOOTSTRAP_STATUS gives correct response

    Runnable runnable = () -> {
      Lock recoveryLock = req.getCore().getSolrCoreState().getRecoveryLock();
      boolean locked = recoveryLock.tryLock();
      SolrCoreState coreState = core.getSolrCoreState();
      try {
        if (!locked)  {
          handleCancelBootstrap(req, rsp);
        } else if (leaderStateManager.amILeader())  {
          coreState.setCdcrBootstrapRunning(true);
          latch.countDown(); // free the latch as current bootstrap is executing
          //running.set(true);
          String leaderUrl = ReplicationHandler.getObjectWithBackwardCompatibility(req.getParams(), ReplicationHandler.LEADER_URL, ReplicationHandler.LEGACY_LEADER_URL, null);
          BootstrapCallable bootstrapCallable = new BootstrapCallable(leaderUrl, core);
          coreState.setCdcrBootstrapCallable(bootstrapCallable);
          Future<Boolean> bootstrapFuture = core.getCoreContainer().getUpdateShardHandler().getRecoveryExecutor()
              .submit(bootstrapCallable);
          coreState.setCdcrBootstrapFuture(bootstrapFuture);
          try {
            bootstrapFuture.get();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Bootstrap was interrupted", e);
          } catch (ExecutionException e) {
            log.error("Bootstrap operation failed", e);
          }
        } else  {
          log.error("Action {} sent to non-leader replica @ {}:{}. Aborting bootstrap.", CdcrParams.CdcrAction.BOOTSTRAP, collectionName, shard);
        }
      } finally {
        if (locked) {
          coreState.setCdcrBootstrapRunning(false);
          recoveryLock.unlock();
        } else {
          latch.countDown(); // free the latch as current bootstrap is executing
        }
      }
    };

    try {
      core.getCoreContainer().getUpdateShardHandler().getUpdateExecutor().submit(runnable);
      rsp.add(RESPONSE_STATUS, "submitted");
      latch.await(10000, TimeUnit.MILLISECONDS); // put the latch for current bootstrap command
    } catch (RejectedExecutionException ree)  {
      // no problem, we're probably shutting down
      rsp.add(RESPONSE_STATUS, "failed");
    }
  }

  private void handleCancelBootstrap(SolrQueryRequest req, SolrQueryResponse rsp) {
    BootstrapCallable callable = (BootstrapCallable)core.getSolrCoreState().getCdcrBootstrapCallable();
    IOUtils.closeQuietly(callable);
    rsp.add(RESPONSE_STATUS, "cancelled");
  }

  private void handleBootstrapStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException, SolrServerException {
    SolrCoreState coreState = core.getSolrCoreState();
    if (coreState.getCdcrBootstrapRunning()) {
      rsp.add(RESPONSE_STATUS, RUNNING);
      return;
    }

    Future<Boolean> future = coreState.getCdcrBootstrapFuture();
    BootstrapCallable callable = (BootstrapCallable)coreState.getCdcrBootstrapCallable();
    if (future == null) {
      rsp.add(RESPONSE_STATUS, "notfound");
      rsp.add(RESPONSE_MESSAGE, "No bootstrap found in running, completed or failed states");
    } else if (future.isCancelled() || callable.isClosed()) {
      rsp.add(RESPONSE_STATUS, "cancelled");
    } else if (future.isDone()) {
      // could be a normal termination or an exception
      try {
        Boolean result = future.get();
        if (result) {
          rsp.add(RESPONSE_STATUS, COMPLETED);
        } else {
          rsp.add(RESPONSE_STATUS, FAILED);
        }
      } catch (InterruptedException e) {
        // should not happen?
      } catch (ExecutionException e) {
        rsp.add(RESPONSE_STATUS, FAILED);
        rsp.add(RESPONSE, e);
      } catch (CancellationException ce) {
        rsp.add(RESPONSE_STATUS, FAILED);
        rsp.add(RESPONSE_MESSAGE, "Bootstrap was cancelled");
      }
    } else {
      rsp.add(RESPONSE_STATUS, RUNNING);
    }
  }

  static class BootstrapCallable implements Callable<Boolean>, Closeable {
    private final String leaderUrl;
    private final SolrCore core;
    private volatile boolean closed = false;

    BootstrapCallable(String leaderUrl, SolrCore core) {
      this.leaderUrl = leaderUrl;
      this.core = core;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      SolrRequestHandler handler = core.getRequestHandler(ReplicationHandler.PATH);
      ReplicationHandler replicationHandler = (ReplicationHandler) handler;
      replicationHandler.abortFetch();
    }

    public boolean isClosed() {
      return closed;
    }

    @Override
    public Boolean call() throws Exception {
      boolean success = false;
      UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
      // we start buffering updates as a safeguard however we do not expect
      // to receive any updates from the source during bootstrap
      ulog.bufferUpdates();
      try {
        commitOnLeader(leaderUrl);
        // use rep handler directly, so we can do this sync rather than async
        SolrRequestHandler handler = core.getRequestHandler(ReplicationHandler.PATH);
        ReplicationHandler replicationHandler = (ReplicationHandler) handler;

        if (replicationHandler == null) {
          throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
              "Skipping recovery, no " + ReplicationHandler.PATH + " handler found");
        }

        ModifiableSolrParams solrParams = new ModifiableSolrParams();
        solrParams.set(ReplicationHandler.LEGACY_LEADER_URL, leaderUrl);
        // we do not want the raw tlog files from the source
        solrParams.set(ReplicationHandler.TLOG_FILES, false);

        success = replicationHandler.doFetch(solrParams, false).getSuccessful();

        Future<UpdateLog.RecoveryInfo> future = ulog.applyBufferedUpdates();
        if (future == null) {
          // no replay needed
          log.info("No replay needed.");
        } else {
          log.info("Replaying buffered documents.");
          // wait for replay
          UpdateLog.RecoveryInfo report = future.get();
          if (report.failed) {
            SolrException.log(log, "Replay failed");
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Replay failed");
          }
        }
        if (success)  {
          ZkController zkController = core.getCoreContainer().getZkController();
          String collectionName = core.getCoreDescriptor().getCollectionName();
          ClusterState clusterState = zkController.getZkStateReader().getClusterState();
          DocCollection collection = clusterState.getCollection(collectionName);
          Slice slice = collection.getSlice(core.getCoreDescriptor().getCloudDescriptor().getShardId());
          ZkShardTerms terms = zkController.getShardTerms(collectionName, slice.getName());
          String coreNodeName = core.getCoreDescriptor().getCloudDescriptor().getCoreNodeName();
          Set<String> allExceptLeader = slice.getReplicas().stream().filter(replica -> !replica.getName().equals(coreNodeName)).map(Replica::getName).collect(Collectors.toSet());
          terms.ensureTermsIsHigher(coreNodeName, allExceptLeader);
        }
        return success;
      } finally {
        if (closed || !success) {
          // we cannot apply the buffer in this case because it will introduce newer versions in the
          // update log and then the source cluster will get those versions via collectioncheckpoint
          // causing the versions in between to be completely missed
          boolean dropped = ulog.dropBufferedUpdates();
          assert dropped;
        }
      }
    }

    private void commitOnLeader(String leaderUrl) throws SolrServerException,
        IOException {
      try (HttpSolrClient client = new HttpSolrClient.Builder(leaderUrl)
          .withConnectionTimeout(30000)
          .build()) {
        UpdateRequest ureq = new UpdateRequest();
        ureq.setParams(new ModifiableSolrParams());
        ureq.getParams().set(DistributedUpdateProcessor.COMMIT_END_POINT, true);
        ureq.getParams().set(UpdateParams.OPEN_SEARCHER, false);
        ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true).process(
            client);
      }
    }
  }

  @Override
  public String getDescription() {
    return "Manage Cross Data Center Replication";
  }

  @Override
  public Category getCategory() {
    return Category.REPLICATION;
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
      try (HttpSolrClient server = new HttpSolrClient.Builder(baseUrl)
          .withConnectionTimeout(15000)
          .withSocketTimeout(60000)
          .build()) {

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CommonParams.ACTION, CdcrParams.CdcrAction.SHARDCHECKPOINT.toString());

        @SuppressWarnings({"rawtypes"})
        SolrRequest request = new QueryRequest(params);
        request.setPath(cdcrPath);

        @SuppressWarnings({"rawtypes"})
        NamedList response = server.request(request);
        return (Long) response.get(CdcrParams.CHECKPOINT);
      }
    }

  }

}

