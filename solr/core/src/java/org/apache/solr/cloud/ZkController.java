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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.client.solrj.request.CoreAdminRequest.WaitForState;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.BeforeReconnect;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ConnectionManager;
import org.apache.solr.common.cloud.DefaultConnectionStrategy;
import org.apache.solr.common.cloud.DefaultZkACLProvider;
import org.apache.solr.common.cloud.DefaultZkCredentialsProvider;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocCollectionWatcher;
import org.apache.solr.common.cloud.LiveNodesListener;
import org.apache.solr.common.cloud.NodesSysPropsCacher;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkACLProvider;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkCredentialsProvider;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrCoreInitializationException;
import org.apache.solr.handler.admin.ConfigSetsHandlerApi;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RTimer;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;

/**
 * Handle ZooKeeper interactions.
 * <p>
 * notes: loads everything on init, creates what's not there - further updates
 * are prompted with Watches.
 * <p>
 * TODO: exceptions during close on attempts to update cloud state
 */
public class ZkController implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final int WAIT_DOWN_STATES_TIMEOUT_SECONDS = 60;

  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");

  private final ZkDistributedQueue overseerJobQueue;
  private final OverseerTaskQueue overseerCollectionQueue;
  private final OverseerTaskQueue overseerConfigSetQueue;

  private final DistributedMap overseerRunningMap;
  private final DistributedMap overseerCompletedMap;
  private final DistributedMap overseerFailureMap;
  private final DistributedMap asyncIdsMap;

  public final static String COLLECTION_PARAM_PREFIX = "collection.";
  public final static String CONFIGNAME_PROP = "configName";

  static class ContextKey {

    private String collection;
    private String coreNodeName;

    public ContextKey(String collection, String coreNodeName) {
      this.collection = collection;
      this.coreNodeName = coreNodeName;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result
          + ((collection == null) ? 0 : collection.hashCode());
      result = prime * result
          + ((coreNodeName == null) ? 0 : coreNodeName.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      ContextKey other = (ContextKey) obj;
      if (collection == null) {
        if (other.collection != null) return false;
      } else if (!collection.equals(other.collection)) return false;
      if (coreNodeName == null) {
        if (other.coreNodeName != null) return false;
      } else if (!coreNodeName.equals(other.coreNodeName)) return false;
      return true;
    }
  }

  private final Map<ContextKey, ElectionContext> electionContexts = Collections.synchronizedMap(new HashMap<>());

  private final SolrZkClient zkClient;
  public final ZkStateReader zkStateReader;
  private SolrCloudManager cloudManager;
  private CloudSolrClient cloudSolrClient;

  private final String zkServerAddress;          // example: 127.0.0.1:54062/solr

  private final int localHostPort;      // example: 54065
  private final String hostName;           // example: 127.0.0.1
  private final String nodeName;           // example: 127.0.0.1:54065_solr
  private String baseURL;            // example: http://127.0.0.1:54065/solr

  private final CloudConfig cloudConfig;
  private final NodesSysPropsCacher sysPropsCacher;

  private LeaderElector overseerElector;

  private Map<String, ReplicateFromLeader> replicateFromLeaders = new ConcurrentHashMap<>();
  private final Map<String, ZkCollectionTerms> collectionToTerms = new HashMap<>();

  // for now, this can be null in tests, in which case recovery will be inactive, and other features
  // may accept defaults or use mocks rather than pulling things from a CoreContainer
  private CoreContainer cc;

  protected volatile Overseer overseer;

  private int leaderVoteWait;
  private int leaderConflictResolveWait;

  private boolean genericCoreNodeNames;

  private int clientTimeout;

  private volatile boolean isClosed;

  private final ConcurrentHashMap<String, Throwable> replicasMetTragicEvent = new ConcurrentHashMap<>();

  @Deprecated
  // keeps track of replicas that have been asked to recover by leaders running on this node
  private final Map<String, String> replicasInLeaderInitiatedRecovery = new HashMap<String, String>();

  // This is an expert and unsupported development mode that does not create
  // an Overseer or register a /live node. This let's you monitor the cluster
  // and interact with zookeeper via the Solr admin UI on a node outside the cluster,
  // and so one that will not be killed or stopped when testing. See developer cloud-scripts.
  private boolean zkRunOnly = Boolean.getBoolean("zkRunOnly"); // expert

  // keeps track of a list of objects that need to know a new ZooKeeper session was created after expiration occurred
  // ref is held as a HashSet since we clone the set before notifying to avoid synchronizing too long
  private HashSet<OnReconnect> reconnectListeners = new HashSet<OnReconnect>();

  private class RegisterCoreAsync implements Callable<Object> {

    CoreDescriptor descriptor;
    boolean recoverReloadedCores;
    boolean afterExpiration;

    RegisterCoreAsync(CoreDescriptor descriptor, boolean recoverReloadedCores, boolean afterExpiration) {
      this.descriptor = descriptor;
      this.recoverReloadedCores = recoverReloadedCores;
      this.afterExpiration = afterExpiration;
    }

    public Object call() throws Exception {
      log.info("Registering core {} afterExpiration? {}", descriptor.getName(), afterExpiration);
      register(descriptor.getName(), descriptor, recoverReloadedCores, afterExpiration, false);
      return descriptor;
    }
  }

  // notifies registered listeners after the ZK reconnect in the background
  private static class OnReconnectNotifyAsync implements Callable<Object> {

    private final OnReconnect listener;

    OnReconnectNotifyAsync(OnReconnect listener) {
      this.listener = listener;
    }

    @Override
    public Object call() throws Exception {
      listener.command();
      return null;
    }
  }

  public ZkController(final CoreContainer cc, String zkServerAddress, int zkClientConnectTimeout, CloudConfig cloudConfig, final CurrentCoreDescriptorProvider registerOnReconnect)
      throws InterruptedException, TimeoutException, IOException {

    if (cc == null) throw new IllegalArgumentException("CoreContainer cannot be null.");
    this.cc = cc;

    this.cloudConfig = cloudConfig;

    this.genericCoreNodeNames = cloudConfig.getGenericCoreNodeNames();

    // be forgiving and strip this off leading/trailing slashes
    // this allows us to support users specifying hostContext="/" in
    // solr.xml to indicate the root context, instead of hostContext=""
    // which means the default of "solr"
    String localHostContext = trimLeadingAndTrailingSlashes(cloudConfig.getSolrHostContext());

    this.zkServerAddress = zkServerAddress;
    this.localHostPort = cloudConfig.getSolrHostPort();
    this.hostName = normalizeHostName(cloudConfig.getHost());
    this.nodeName = generateNodeName(this.hostName, Integer.toString(this.localHostPort), localHostContext);
    MDCLoggingContext.setNode(nodeName);
    this.leaderVoteWait = cloudConfig.getLeaderVoteWait();
    this.leaderConflictResolveWait = cloudConfig.getLeaderConflictResolveWait();

    this.clientTimeout = cloudConfig.getZkClientTimeout();
    DefaultConnectionStrategy strat = new DefaultConnectionStrategy();
    String zkACLProviderClass = cloudConfig.getZkACLProviderClass();
    ZkACLProvider zkACLProvider = null;
    if (zkACLProviderClass != null && zkACLProviderClass.trim().length() > 0) {
      zkACLProvider = cc.getResourceLoader().newInstance(zkACLProviderClass, ZkACLProvider.class);
    } else {
      zkACLProvider = new DefaultZkACLProvider();
    }

    String zkCredentialsProviderClass = cloudConfig.getZkCredentialsProviderClass();
    if (zkCredentialsProviderClass != null && zkCredentialsProviderClass.trim().length() > 0) {
      strat.setZkCredentialsToAddAutomatically(cc.getResourceLoader().newInstance(zkCredentialsProviderClass, ZkCredentialsProvider.class));
    } else {
      strat.setZkCredentialsToAddAutomatically(new DefaultZkCredentialsProvider());
    }
    addOnReconnectListener(getConfigDirListener());

    zkClient = new SolrZkClient(zkServerAddress, clientTimeout, zkClientConnectTimeout, strat,
        // on reconnect, reload cloud info
        new OnReconnect() {

          @Override
          public void command() throws SessionExpiredException {
            log.info("ZooKeeper session re-connected ... refreshing core states after session expiration.");
            clearZkCollectionTerms();
            try {
              // recreate our watchers first so that they exist even on any problems below
              zkStateReader.createClusterStateWatchersAndUpdate();

              // this is troublesome - we dont want to kill anything the old
              // leader accepted
              // though I guess sync will likely get those updates back? But
              // only if
              // he is involved in the sync, and he certainly may not be
              // ExecutorUtil.shutdownAndAwaitTermination(cc.getCmdDistribExecutor());
              // we need to create all of our lost watches

              // seems we dont need to do this again...
              // Overseer.createClientNodes(zkClient, getNodeName());

              // start the overseer first as following code may need it's processing
              if (!zkRunOnly) {
                ElectionContext context = new OverseerElectionContext(zkClient,
                    overseer, getNodeName());

                ElectionContext prevContext = overseerElector.getContext();
                if (prevContext != null) {
                  prevContext.cancelElection();
                  prevContext.close();
                }

                overseerElector.setup(context);
                overseerElector.joinElection(context, true);
              }

              cc.cancelCoreRecoveries();
              
              try {
                registerAllCoresAsDown(registerOnReconnect, false);
              } catch (SessionExpiredException e) {
                // zk has to reconnect and this will all be tried again
                throw e;
              } catch (Exception e) {
                // this is really best effort - in case of races or failure cases where we now need to be the leader, if anything fails,
                // just continue
                log.warn("Exception while trying to register all cores as DOWN", e);
              } 

              // we have to register as live first to pick up docs in the buffer
              createEphemeralLiveNode();

              List<CoreDescriptor> descriptors = registerOnReconnect.getCurrentDescriptors();
              // re register all descriptors
              ExecutorService executorService = (cc != null) ? cc.getCoreZkRegisterExecutorService() : null;
              if (descriptors != null) {
                for (CoreDescriptor descriptor : descriptors) {
                  // TODO: we need to think carefully about what happens when it
                  // was
                  // a leader that was expired - as well as what to do about
                  // leaders/overseers
                  // with connection loss
                  try {
                    // unload solrcores that have been 'failed over'
                    throwErrorIfReplicaReplaced(descriptor);

                    if (executorService != null) {
                      executorService.submit(new RegisterCoreAsync(descriptor, true, true));
                    } else {
                      register(descriptor.getName(), descriptor, true, true, false);
                    }
                  } catch (Exception e) {
                    SolrException.log(log, "Error registering SolrCore", e);
                  }
                }
              }

              // notify any other objects that need to know when the session was re-connected
              HashSet<OnReconnect> clonedListeners;
              synchronized (reconnectListeners) {
                clonedListeners = (HashSet<OnReconnect>)reconnectListeners.clone();
              }
              // the OnReconnect operation can be expensive per listener, so do that async in the background
              for (OnReconnect listener : clonedListeners) {
                try {
                  if (executorService != null) {
                    executorService.submit(new OnReconnectNotifyAsync(listener));
                  } else {
                    listener.command();
                  }
                } catch (Exception exc) {
                  // not much we can do here other than warn in the log
                  log.warn("Error when notifying OnReconnect listener " + listener + " after session re-connected.", exc);
                }
              }
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (SessionExpiredException e) {
              throw e;
            } catch (Exception e) {
              SolrException.log(log, "", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            }
          }

        }, new BeforeReconnect() {

      @Override
      public void command() {
        try {
          ZkController.this.overseer.close();
        } catch (Exception e) {
          log.error("Error trying to stop any Overseer threads", e);
        }
        closeOutstandingElections(registerOnReconnect);
        markAllAsNotLeader(registerOnReconnect);
      }
    }, zkACLProvider, new ConnectionManager.IsClosed() {

      @Override
      public boolean isClosed() {
        return cc.isShutDown();
      }});


    this.overseerRunningMap = Overseer.getRunningMap(zkClient);
    this.overseerCompletedMap = Overseer.getCompletedMap(zkClient);
    this.overseerFailureMap = Overseer.getFailureMap(zkClient);
    this.asyncIdsMap = Overseer.getAsyncIdsMap(zkClient);

    zkStateReader = new ZkStateReader(zkClient, () -> {
      if (cc != null) cc.securityNodeChanged();
    });

    init(registerOnReconnect);

    this.overseerJobQueue = overseer.getStateUpdateQueue();
    this.overseerCollectionQueue = overseer.getCollectionQueue(zkClient);
    this.overseerConfigSetQueue = overseer.getConfigSetQueue(zkClient);
    this.sysPropsCacher = new NodesSysPropsCacher(getSolrCloudManager().getNodeStateProvider(),
        getNodeName(), zkStateReader);

    assert ObjectReleaseTracker.track(this);
  }

  public int getLeaderVoteWait() {
    return leaderVoteWait;
  }

  public int getLeaderConflictResolveWait() {
    return leaderConflictResolveWait;
  }

  private void registerAllCoresAsDown(
      final CurrentCoreDescriptorProvider registerOnReconnect, boolean updateLastPublished) throws SessionExpiredException {
    List<CoreDescriptor> descriptors = registerOnReconnect
        .getCurrentDescriptors();
    if (isClosed) return;
    if (descriptors != null) {
      // before registering as live, make sure everyone is in a
      // down state
      publishNodeAsDown(getNodeName());
      for (CoreDescriptor descriptor : descriptors) {
        // if it looks like we are going to be the leader, we don't
        // want to wait for the following stuff
        CloudDescriptor cloudDesc = descriptor.getCloudDescriptor();
        String collection = cloudDesc.getCollectionName();
        String slice = cloudDesc.getShardId();
        try {

          int children = zkStateReader
              .getZkClient()
              .getChildren(
                  ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
                      + "/leader_elect/" + slice + "/election", null, true).size();
          if (children == 0) {
            log.debug("looks like we are going to be the leader for collection {} shard {}", collection, slice);
            continue;
          }

        } catch (NoNodeException e) {
          log.debug("looks like we are going to be the leader for collection {} shard {}", collection, slice);
          continue;
        } catch (InterruptedException e2) {
          Thread.currentThread().interrupt();
        } catch (SessionExpiredException e) {
          // zk has to reconnect
          throw e;
        } catch (KeeperException e) {
          log.warn("", e);
          Thread.currentThread().interrupt();
        }

        final String coreZkNodeName = descriptor.getCloudDescriptor().getCoreNodeName();
        try {
          log.debug("calling waitForLeaderToSeeDownState for coreZkNodeName={} collection={} shard={}", new Object[]{coreZkNodeName, collection, slice});
          waitForLeaderToSeeDownState(descriptor, coreZkNodeName);
        } catch (Exception e) {
          log.warn("There was a problem while making a best effort to ensure the leader has seen us as down, this is not unexpected as Zookeeper has just reconnected after a session expiration", e);
          if (isClosed) {
            return;
          }
        }
      }
    }
  }

  public NodesSysPropsCacher getSysPropsCacher() {
    return sysPropsCacher;
  }

  private void closeOutstandingElections(final CurrentCoreDescriptorProvider registerOnReconnect) {

    List<CoreDescriptor> descriptors = registerOnReconnect.getCurrentDescriptors();
    if (descriptors != null) {
      for (CoreDescriptor descriptor : descriptors) {
        closeExistingElectionContext(descriptor);
      }
    }
  }

  private ContextKey closeExistingElectionContext(CoreDescriptor cd) {
    // look for old context - if we find it, cancel it
    String collection = cd.getCloudDescriptor().getCollectionName();
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

    ContextKey contextKey = new ContextKey(collection, coreNodeName);
    ElectionContext prevContext = electionContexts.get(contextKey);

    if (prevContext != null) {
      prevContext.close();
      electionContexts.remove(contextKey);
    }

    return contextKey;
  }

  private void markAllAsNotLeader(
      final CurrentCoreDescriptorProvider registerOnReconnect) {
    List<CoreDescriptor> descriptors = registerOnReconnect
        .getCurrentDescriptors();
    if (descriptors != null) {
      for (CoreDescriptor descriptor : descriptors) {
        descriptor.getCloudDescriptor().setLeader(false);
        descriptor.getCloudDescriptor().setHasRegistered(false);
      }
    }
  }

  public void preClose() {
    this.isClosed = true;

    try {
      this.removeEphemeralLiveNode();
    } catch (AlreadyClosedException | SessionExpiredException | KeeperException.ConnectionLossException e) {

    } catch (Exception e) {
      log.warn("Error removing live node. Continuing to close CoreContainer", e);
    }

    try {
      if (getZkClient().getConnectionManager().isConnected()) {
        log.info("Publish this node as DOWN...");
        publishNodeAsDown(getNodeName());
      }
    } catch (Exception e) {
      log.warn("Error publishing nodes as down. Continuing to close CoreContainer", e);
    }

    ExecutorService customThreadPool = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("preCloseThreadPool"));

    try {
      synchronized (collectionToTerms) {
        customThreadPool.submit(() -> collectionToTerms.values().parallelStream().forEach(ZkCollectionTerms::close));
      }

      customThreadPool.submit(() -> replicateFromLeaders.values().parallelStream().forEach(ReplicateFromLeader::stopReplication));
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);
    }
  }

  /**
   * Closes the underlying ZooKeeper client.
   */
  public void close() {
    if (!this.isClosed)
      preClose();

    ExecutorService customThreadPool = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("closeThreadPool"));

    customThreadPool.submit(() -> Collections.singleton(overseerElector.getContext()).parallelStream().forEach(IOUtils::closeQuietly));

    customThreadPool.submit(() -> Collections.singleton(overseer).parallelStream().forEach(IOUtils::closeQuietly));

    try {
      customThreadPool.submit(() -> electionContexts.values().parallelStream().forEach(IOUtils::closeQuietly));

    } finally {

      sysPropsCacher.close();
      customThreadPool.submit(() -> Collections.singleton(cloudSolrClient).parallelStream().forEach(IOUtils::closeQuietly));
      customThreadPool.submit(() -> Collections.singleton(cloudManager).parallelStream().forEach(IOUtils::closeQuietly));

      try {
        try {
          zkStateReader.close();
        } catch (Exception e) {
          log.error("Error closing zkStateReader", e);
        }
      } finally {
        try {
          zkClient.close();
        } catch (Exception e) {
          log.error("Error closing zkClient", e);
        } finally {

          // just in case the OverseerElectionContext managed to start another Overseer
          IOUtils.closeQuietly(overseer);

          ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);
        }

      }

    }
    assert ObjectReleaseTracker.release(this);
  }

  /**
   * Best effort to give up the leadership of a shard in a core after hitting a tragic exception
   * @param cd The current core descriptor
   * @param tragicException The tragic exception from the {@code IndexWriter}
   */
  public void giveupLeadership(CoreDescriptor cd, Throwable tragicException) {
    assert tragicException != null;
    assert cd != null;
    DocCollection dc = getClusterState().getCollectionOrNull(cd.getCollectionName());
    if (dc == null) return;

    Slice shard = dc.getSlice(cd.getCloudDescriptor().getShardId());
    if (shard == null) return;

    // if this replica is not a leader, it will be put in recovery state by the leader
    if (shard.getReplica(cd.getCloudDescriptor().getCoreNodeName()) != shard.getLeader()) return;

    int numActiveReplicas = shard.getReplicas(
        rep -> rep.getState() == Replica.State.ACTIVE
            && rep.getType() != Type.PULL
            && getClusterState().getLiveNodes().contains(rep.getNodeName())
    ).size();

    // at least the leader still be able to search, we should give up leadership if other replicas can take over
    if (numActiveReplicas >= 2) {
      String key = cd.getCollectionName() + ":" + cd.getCloudDescriptor().getCoreNodeName();
      //TODO better handling the case when delete replica was failed
      if (replicasMetTragicEvent.putIfAbsent(key, tragicException) == null) {
        log.warn("Leader {} met tragic exception, give up its leadership", key, tragicException);
        try {
          // by using Overseer to remove and add replica back, we can do the task in an async/robust manner
          Map<String,Object> props = new HashMap<>();
          props.put(Overseer.QUEUE_OPERATION, "deletereplica");
          props.put(COLLECTION_PROP, cd.getCollectionName());
          props.put(SHARD_ID_PROP, shard.getName());
          props.put(REPLICA_PROP, cd.getCloudDescriptor().getCoreNodeName());
          getOverseerCollectionQueue().offer(Utils.toJSON(new ZkNodeProps(props)));

          props.clear();
          props.put(Overseer.QUEUE_OPERATION, "addreplica");
          props.put(COLLECTION_PROP, cd.getCollectionName());
          props.put(SHARD_ID_PROP, shard.getName());
          props.put(ZkStateReader.REPLICA_TYPE, cd.getCloudDescriptor().getReplicaType().name().toUpperCase(Locale.ROOT));
          props.put(CoreAdminParams.NODE, getNodeName());
          getOverseerCollectionQueue().offer(Utils.toJSON(new ZkNodeProps(props)));
        } catch (Exception e) {
          // Exceptions are not bubbled up. giveupLeadership is best effort, and is only called in case of some other
          // unrecoverable error happened
          log.error("Met exception on give up leadership for {}", key, e);
          replicasMetTragicEvent.remove(key);
          SolrZkClient.checkInterrupted(e);
        }
      }
    }
  }


  /**
   * Returns true if config file exists
   */
  public boolean configFileExists(String collection, String fileName)
      throws KeeperException, InterruptedException {
    Stat stat = zkClient.exists(ZkConfigManager.CONFIGS_ZKNODE + "/" + collection + "/" + fileName, null, true);
    return stat != null;
  }

  /**
   * @return information about the cluster from ZooKeeper
   */
  public ClusterState getClusterState() {
    return zkStateReader.getClusterState();
  }

  public SolrCloudManager getSolrCloudManager() {
    if (cloudManager != null) {
      return cloudManager;
    }
    synchronized(this) {
      if (cloudManager != null) {
        return cloudManager;
      }
      cloudSolrClient = new CloudSolrClient.Builder(new ZkClientClusterStateProvider(zkStateReader)).withSocketTimeout(30000).withConnectionTimeout(15000)
          .withHttpClient(cc.getUpdateShardHandler().getDefaultHttpClient())
          .withConnectionTimeout(15000).withSocketTimeout(30000).build();
      cloudManager = new SolrClientCloudManager(new ZkDistributedQueueFactory(zkClient), cloudSolrClient);
      cloudManager.getClusterStateProvider().connect();
    }
    return cloudManager;
  }

  /**
   * Returns config file data (in bytes)
   */
  public byte[] getConfigFileData(String zkConfigName, String fileName)
      throws KeeperException, InterruptedException {
    String zkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + zkConfigName + "/" + fileName;
    byte[] bytes = zkClient.getData(zkPath, null, null, true);
    if (bytes == null) {
      log.error("Config file contains no data:" + zkPath);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Config file contains no data:" + zkPath);
    }

    return bytes;
  }

  // normalize host removing any url scheme.
  // input can be null, host, or url_prefix://host
  private String normalizeHostName(String host) throws IOException {

    if (host == null || host.length() == 0) {
      String hostaddress;
      try {
        hostaddress = InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        hostaddress = "127.0.0.1"; // cannot resolve system hostname, fall through
      }
      // Re-get the IP again for "127.0.0.1", the other case we trust the hosts
      // file is right.
      if ("127.0.0.1".equals(hostaddress)) {
        Enumeration<NetworkInterface> netInterfaces = null;
        try {
          netInterfaces = NetworkInterface.getNetworkInterfaces();
          while (netInterfaces.hasMoreElements()) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> ips = ni.getInetAddresses();
            while (ips.hasMoreElements()) {
              InetAddress ip = ips.nextElement();
              if (ip.isSiteLocalAddress()) {
                hostaddress = ip.getHostAddress();
              }
            }
          }
        } catch (Exception e) {
          SolrException.log(log,
              "Error while looking for a better host name than 127.0.0.1", e);
        }
      }
      host = hostaddress;
    } else {
      if (URLUtil.hasScheme(host)) {
        host = URLUtil.removeScheme(host);
      }
    }

    return host;
  }

  public String getHostName() {
    return hostName;
  }

  public int getHostPort() {
    return localHostPort;
  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }

  /**
   * @return zookeeper server address
   */
  public String getZkServerAddress() {
    return zkServerAddress;
  }

  boolean isClosed() {
    return isClosed;
  }

  /**
   * Create the zknodes necessary for a cluster to operate
   *
   * @param zkClient a SolrZkClient
   * @throws KeeperException      if there is a Zookeeper error
   * @throws InterruptedException on interrupt
   */
  public static void createClusterZkNodes(SolrZkClient zkClient)
      throws KeeperException, InterruptedException, IOException {
    ZkCmdExecutor cmdExecutor = new ZkCmdExecutor(zkClient.getZkClientTimeout());
    cmdExecutor.ensureExists(ZkStateReader.LIVE_NODES_ZKNODE, zkClient);
    cmdExecutor.ensureExists(ZkStateReader.COLLECTIONS_ZKNODE, zkClient);
    cmdExecutor.ensureExists(ZkStateReader.ALIASES, zkClient);
    cmdExecutor.ensureExists(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH, zkClient);
    cmdExecutor.ensureExists(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH, zkClient);
    cmdExecutor.ensureExists(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH, zkClient);
    cmdExecutor.ensureExists(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH, zkClient);
    byte[] emptyJson = "{}".getBytes(StandardCharsets.UTF_8);
    cmdExecutor.ensureExists(ZkStateReader.CLUSTER_STATE, emptyJson, CreateMode.PERSISTENT, zkClient);
    cmdExecutor.ensureExists(ZkStateReader.SOLR_SECURITY_CONF_PATH, emptyJson, CreateMode.PERSISTENT, zkClient);
    cmdExecutor.ensureExists(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, emptyJson, CreateMode.PERSISTENT, zkClient);
    bootstrapDefaultConfigSet(zkClient);
  }

  private static void bootstrapDefaultConfigSet(SolrZkClient zkClient) throws KeeperException, InterruptedException, IOException {
    if (zkClient.exists("/configs/_default", true) == false) {
      String configDirPath = getDefaultConfigDirPath();
      if (configDirPath == null) {
        log.warn("The _default configset could not be uploaded. Please provide 'solr.default.confdir' parameter that points to a configset" +
            " intended to be the default. Current 'solr.default.confdir' value: {}", System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE));
      } else {
        ZkMaintenanceUtils.upConfig(zkClient, Paths.get(configDirPath), ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME);
      }
    }
  }

  /**
   * Gets the absolute filesystem path of the _default configset to bootstrap from.
   * First tries the sysprop "solr.default.confdir". If not found, tries to find
   * the _default dir relative to the sysprop "solr.install.dir".
   * If that fails as well (usually for unit tests), tries to get the _default from the
   * classpath. Returns null if not found anywhere.
   */
  private static String getDefaultConfigDirPath() {
    String configDirPath = null;
    String serverSubPath = "solr" + File.separator +
        "configsets" + File.separator + "_default" +
        File.separator + "conf";
    String subPath = File.separator + "server" + File.separator + serverSubPath;
    if (System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE) != null && new File(System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE)).exists()) {
      configDirPath = new File(System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE)).getAbsolutePath();
    } else if (System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE) != null &&
        new File(System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE) + subPath).exists()) {
      configDirPath = new File(System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE) + subPath).getAbsolutePath();
    } else { // find "_default" in the classpath. This one is used for tests
      configDirPath = getDefaultConfigDirFromClasspath(serverSubPath);
    }
    return configDirPath;
  }

  private static String getDefaultConfigDirFromClasspath(String serverSubPath) {
    URL classpathUrl = ZkController.class.getClassLoader().getResource(serverSubPath);
    try {
      if (classpathUrl != null && new File(classpathUrl.toURI()).exists()) {
        return new File(classpathUrl.toURI()).getAbsolutePath();
      }
    } catch (URISyntaxException ex) {}
    return null;
  }

  private void init(CurrentCoreDescriptorProvider registerOnReconnect) {

    try {
      createClusterZkNodes(zkClient);
      zkStateReader.createClusterStateWatchersAndUpdate();
      this.baseURL = zkStateReader.getBaseUrlForNodeName(this.nodeName);

      checkForExistingEphemeralNode();
      registerLiveNodesListener();

      // start the overseer first as following code may need it's processing
      if (!zkRunOnly) {
        overseerElector = new LeaderElector(zkClient);
        this.overseer = new Overseer((HttpShardHandler) cc.getShardHandlerFactory().getShardHandler(), cc.getUpdateShardHandler(),
            CommonParams.CORES_HANDLER_PATH, zkStateReader, this, cloudConfig);
        ElectionContext context = new OverseerElectionContext(zkClient,
            overseer, getNodeName());
        overseerElector.setup(context);
        overseerElector.joinElection(context, false);
      }

      Stat stat = zkClient.exists(ZkStateReader.LIVE_NODES_ZKNODE, null, true);
      if (stat != null && stat.getNumChildren() > 0) {
        publishAndWaitForDownStates();
      }

      // Do this last to signal we're up.
      createEphemeralLiveNode();
    } catch (IOException e) {
      log.error("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Can't create ZooKeeperController", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    } catch (KeeperException e) {
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    }

  }

  private void checkForExistingEphemeralNode() throws KeeperException, InterruptedException {
    if (zkRunOnly) {
      return;
    }
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;

    if (!zkClient.exists(nodePath, true)) {
      return;
    }

    final CountDownLatch deletedLatch = new CountDownLatch(1);
    Stat stat = zkClient.exists(nodePath, event -> {
      if (Watcher.Event.EventType.None.equals(event.getType())) {
        return;
      }
      if (Watcher.Event.EventType.NodeDeleted.equals(event.getType())) {
        deletedLatch.countDown();
      }
    }, true);

    if (stat == null) {
      // znode suddenly disappeared but that's okay
      return;
    }

    boolean deleted = deletedLatch.await(zkClient.getSolrZooKeeper().getSessionTimeout() * 2, TimeUnit.MILLISECONDS);
    if (!deleted) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "A previous ephemeral live node still exists. " +
          "Solr cannot continue. Please ensure that no other Solr process using the same port is running already.");
    }
  }

  private void registerLiveNodesListener() {
    // this listener is used for generating nodeLost events, so we check only if
    // some nodes went missing compared to last state
    LiveNodesListener listener = (oldNodes, newNodes) -> {
      oldNodes.removeAll(newNodes);
      if (oldNodes.isEmpty()) { // only added nodes
        return false;
      }
      if (isClosed) {
        return true;
      }
      // if this node is in the top three then attempt to create nodeLost message
      int i = 0;
      for (String n : newNodes) {
        if (n.equals(getNodeName())) {
          break;
        }
        if (i > 2) {
          return false; // this node is not in the top three
        }
        i++;
      }

      // retrieve current trigger config - if there are no nodeLost triggers
      // then don't create markers
      boolean createNodes = false;
      try {
        createNodes = zkStateReader.getAutoScalingConfig().hasTriggerForEvents(TriggerEventType.NODELOST);
      } catch (KeeperException | InterruptedException e1) {
        log.warn("Unable to read autoscaling.json", e1);
      }
      if (createNodes) {
        byte[] json = Utils.toJSON(Collections.singletonMap("timestamp", cloudManager.getTimeSource().getEpochTimeNs()));
        for (String n : oldNodes) {
          String path = ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH + "/" + n;

          try {
            zkClient.create(path, json, CreateMode.PERSISTENT, true);
          } catch (KeeperException.NodeExistsException e) {
            // someone else already created this node - ignore
          } catch (KeeperException | InterruptedException e1) {
            log.warn("Unable to register nodeLost path for " + n, e1);
          }
        }
      }
      return false;
    };
    zkStateReader.registerLiveNodesListener(listener);
  }

  public void publishAndWaitForDownStates() throws KeeperException,
  InterruptedException {
    publishAndWaitForDownStates(WAIT_DOWN_STATES_TIMEOUT_SECONDS);
  }

  public void publishAndWaitForDownStates(int timeoutSeconds) throws KeeperException,
      InterruptedException {

    publishNodeAsDown(getNodeName());

    Set<String> collectionsWithLocalReplica = ConcurrentHashMap.newKeySet();
    for (CoreDescriptor descriptor : cc.getCoreDescriptors()) {
      collectionsWithLocalReplica.add(descriptor.getCloudDescriptor().getCollectionName());
    }

    CountDownLatch latch = new CountDownLatch(collectionsWithLocalReplica.size());
    for (String collectionWithLocalReplica : collectionsWithLocalReplica) {
      zkStateReader.registerDocCollectionWatcher(collectionWithLocalReplica, (collectionState) -> {
        if (collectionState == null)  return false;
        boolean foundStates = true;
        for (CoreDescriptor coreDescriptor : cc.getCoreDescriptors()) {
          if (coreDescriptor.getCloudDescriptor().getCollectionName().equals(collectionWithLocalReplica))  {
            Replica replica = collectionState.getReplica(coreDescriptor.getCloudDescriptor().getCoreNodeName());
            if (replica == null || replica.getState() != Replica.State.DOWN) {
              foundStates = false;
            }
          }
        }

        if (foundStates && collectionsWithLocalReplica.remove(collectionWithLocalReplica))  {
          latch.countDown();
        }
        return foundStates;
      });
    }

    boolean allPublishedDown = latch.await(timeoutSeconds, TimeUnit.SECONDS);
    if (!allPublishedDown) {
      log.warn("Timed out waiting to see all nodes published as DOWN in our cluster state.");
    }
  }

  /**
   * Validates if the chroot exists in zk (or if it is successfully created).
   * Optionally, if create is set to true this method will create the path in
   * case it doesn't exist
   *
   * @return true if the path exists or is created false if the path doesn't
   * exist and 'create' = false
   */
  public static boolean checkChrootPath(String zkHost, boolean create)
      throws KeeperException, InterruptedException {
    if (!SolrZkClient.containsChroot(zkHost)) {
      return true;
    }
    log.trace("zkHost includes chroot");
    String chrootPath = zkHost.substring(zkHost.indexOf("/"), zkHost.length());

    SolrZkClient tmpClient = new SolrZkClient(zkHost.substring(0,
        zkHost.indexOf("/")), 60000, 30000, null, null, null);
    boolean exists = tmpClient.exists(chrootPath, true);
    if (!exists && create) {
      tmpClient.makePath(chrootPath, false, true);
      exists = true;
    }
    tmpClient.close();
    return exists;
  }

  public boolean isConnected() {
    return zkClient.isConnected();
  }

  private void createEphemeralLiveNode() throws KeeperException,
      InterruptedException {
    if (zkRunOnly) {
      return;
    }
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
    String nodeAddedPath = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + nodeName;
    log.info("Register node as live in ZooKeeper:" + nodePath);
    List<Op> ops = new ArrayList<>(2);
    ops.add(Op.create(nodePath, null, zkClient.getZkACLProvider().getACLsToAdd(nodePath), CreateMode.EPHEMERAL));
    // if there are nodeAdded triggers don't create nodeAdded markers
    boolean createMarkerNode = zkStateReader.getAutoScalingConfig().hasTriggerForEvents(TriggerEventType.NODEADDED);
    if (createMarkerNode && !zkClient.exists(nodeAddedPath, true)) {
      // use EPHEMERAL so that it disappears if this node goes down
      // and no other action is taken
      byte[] json = Utils.toJSON(Collections.singletonMap("timestamp", TimeSource.NANO_TIME.getEpochTimeNs()));
      ops.add(Op.create(nodeAddedPath, json, zkClient.getZkACLProvider().getACLsToAdd(nodeAddedPath), CreateMode.EPHEMERAL));
    }
    zkClient.multi(ops, true);
  }

  public void removeEphemeralLiveNode() throws KeeperException, InterruptedException {
    if (zkRunOnly) {
      return;
    }
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
    String nodeAddedPath = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + nodeName;
    log.info("Remove node as live in ZooKeeper:" + nodePath);
    List<Op> ops = new ArrayList<>(2);
    ops.add(Op.delete(nodePath, -1));
    ops.add(Op.delete(nodeAddedPath, -1));

    try {
      zkClient.multi(ops, true);
    } catch (NoNodeException e) {

    }
  }

  public String getNodeName() {
    return nodeName;
  }

  /**
   * Returns true if the path exists
   */
  public boolean pathExists(String path) throws KeeperException,
      InterruptedException {
    return zkClient.exists(path, true);
  }


  /**
   * Register shard with ZooKeeper.
   *
   * @return the shardId for the SolrCore
   */
  public String register(String coreName, final CoreDescriptor desc, boolean skipRecovery) throws Exception {
    return register(coreName, desc, false, false, skipRecovery);
  }


  /**
   * Register shard with ZooKeeper.
   *
   * @return the shardId for the SolrCore
   */
  public String register(String coreName, final CoreDescriptor desc, boolean recoverReloadedCores,
                         boolean afterExpiration, boolean skipRecovery) throws Exception {
    try (SolrCore core = cc.getCore(desc.getName())) {
      MDCLoggingContext.setCore(core);
    }
    try {
      // pre register has published our down state
      final String baseUrl = getBaseUrl();
      final CloudDescriptor cloudDesc = desc.getCloudDescriptor();
      final String collection = cloudDesc.getCollectionName();
      final String shardId = cloudDesc.getShardId();
      final String coreZkNodeName = cloudDesc.getCoreNodeName();
      assert coreZkNodeName != null : "we should have a coreNodeName by now";

      // check replica's existence in clusterstate first
      try {
        zkStateReader.waitForState(collection, Overseer.isLegacy(zkStateReader) ? 60000 : 100,
            TimeUnit.MILLISECONDS, (collectionState) -> getReplicaOrNull(collectionState, shardId, coreZkNodeName) != null);
      } catch (TimeoutException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error registering SolrCore, timeout waiting for replica present in clusterstate");
      }
      Replica replica = getReplicaOrNull(zkStateReader.getClusterState().getCollectionOrNull(collection), shardId, coreZkNodeName);
      if (replica == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error registering SolrCore, replica is removed from clusterstate");
      }

      ZkShardTerms shardTerms = getShardTerms(collection, cloudDesc.getShardId());

      if (replica.getType() != Type.PULL) {
        shardTerms.registerTerm(coreZkNodeName);
      }

      log.debug("Register replica - core:{} address:{} collection:{} shard:{}",
          coreName, baseUrl, collection, shardId);

      try {
        // If we're a preferred leader, insert ourselves at the head of the queue
        boolean joinAtHead = replica.getBool(SliceMutator.PREFERRED_LEADER_PROP, false);
        if (replica.getType() != Type.PULL) {
          joinElection(desc, afterExpiration, joinAtHead);
        } else if (replica.getType() == Type.PULL) {
          if (joinAtHead) {
            log.warn("Replica {} was designated as preferred leader but it's type is {}, It won't join election", coreZkNodeName, Type.PULL);
          }
          log.debug("Replica {} skipping election because it's type is {}", coreZkNodeName, Type.PULL);
          startReplicationFromLeader(coreName, false);
        }
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      } catch (KeeperException | IOException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      }

      // in this case, we want to wait for the leader as long as the leader might
      // wait for a vote, at least - but also long enough that a large cluster has
      // time to get its act together
      String leaderUrl = getLeader(cloudDesc, leaderVoteWait + 600000);

      String ourUrl = ZkCoreNodeProps.getCoreUrl(baseUrl, coreName);
      log.debug("We are " + ourUrl + " and leader is " + leaderUrl);
      boolean isLeader = leaderUrl.equals(ourUrl);
      assert !(isLeader && replica.getType() == Type.PULL) : "Pull replica became leader!";

      try (SolrCore core = cc.getCore(desc.getName())) {

        // recover from local transaction log and wait for it to complete before
        // going active
        // TODO: should this be moved to another thread? To recoveryStrat?
        // TODO: should this actually be done earlier, before (or as part of)
        // leader election perhaps?

        if (core == null) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "SolrCore is no longer available to register");
        }

        UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
        boolean isTlogReplicaAndNotLeader = replica.getType() == Replica.Type.TLOG && !isLeader;
        if (isTlogReplicaAndNotLeader) {
          String commitVersion = ReplicateFromLeader.getCommitVersion(core);
          if (commitVersion != null) {
            ulog.copyOverOldUpdates(Long.parseLong(commitVersion));
          }
        }
        // we will call register again after zk expiration and on reload
        if (!afterExpiration && !core.isReloaded() && ulog != null && !isTlogReplicaAndNotLeader) {
          // disable recovery in case shard is in construction state (for shard splits)
          Slice slice = getClusterState().getCollection(collection).getSlice(shardId);
          if (slice.getState() != Slice.State.CONSTRUCTION || !isLeader) {
            Future<UpdateLog.RecoveryInfo> recoveryFuture = core.getUpdateHandler().getUpdateLog().recoverFromLog();
            if (recoveryFuture != null) {
              log.info("Replaying tlog for " + ourUrl + " during startup... NOTE: This can take a while.");
              recoveryFuture.get(); // NOTE: this could potentially block for
              // minutes or more!
              // TODO: public as recovering in the mean time?
              // TODO: in the future we could do peersync in parallel with recoverFromLog
            } else {
              log.debug("No LogReplay needed for core={} baseURL={}", core.getName(), baseUrl);
            }
          }
        }
        boolean didRecovery
            = checkRecovery(recoverReloadedCores, isLeader, skipRecovery, collection, coreZkNodeName, shardId, core, cc, afterExpiration);
        if (!didRecovery) {
          if (isTlogReplicaAndNotLeader) {
            startReplicationFromLeader(coreName, true);
          }
          publish(desc, Replica.State.ACTIVE);
        }

        if (replica.getType() != Type.PULL) {
          // the watcher is added to a set so multiple calls of this method will left only one watcher
          shardTerms.addListener(new RecoveringCoreTermWatcher(core.getCoreDescriptor(), getCoreContainer()));
        }
        core.getCoreDescriptor().getCloudDescriptor().setHasRegistered(true);
      } catch (Exception e) {
        unregister(coreName, desc, false);
        throw e;
      }

      // make sure we have an update cluster state right away
      zkStateReader.forceUpdateCollection(collection);
      // the watcher is added to a set so multiple calls of this method will left only one watcher
      zkStateReader.registerDocCollectionWatcher(cloudDesc.getCollectionName(),
          new UnloadCoreOnDeletedWatcher(coreZkNodeName, shardId, desc.getName()));
      return shardId;
    } finally {
      MDCLoggingContext.clear();
    }
  }

  private Replica getReplicaOrNull(DocCollection docCollection, String shard, String coreNodeName) {
    if (docCollection == null) return null;

    Slice slice = docCollection.getSlice(shard);
    if (slice == null) return null;

    Replica replica = slice.getReplica(coreNodeName);
    if (replica == null) return null;
    if (!getNodeName().equals(replica.getNodeName())) return null;

    return replica;
  }

  public void startReplicationFromLeader(String coreName, boolean switchTransactionLog) throws InterruptedException {
    log.info("{} starting background replication from leader", coreName);
    ReplicateFromLeader replicateFromLeader = new ReplicateFromLeader(cc, coreName);
    synchronized (replicateFromLeader) { // synchronize to prevent any stop before we finish the start
      if (replicateFromLeaders.putIfAbsent(coreName, replicateFromLeader) == null) {
        replicateFromLeader.startReplication(switchTransactionLog);
      } else {
        log.warn("A replicate from leader instance already exists for core {}", coreName);
      }
    }
  }

  public void stopReplicationFromLeader(String coreName) {
    log.info("{} stopping background replication from leader", coreName);
    ReplicateFromLeader replicateFromLeader = replicateFromLeaders.remove(coreName);
    if (replicateFromLeader != null) {
      synchronized (replicateFromLeader) {
        replicateFromLeader.stopReplication();
      }
    }
  }

  // timeoutms is the timeout for the first call to get the leader - there is then
  // a longer wait to make sure that leader matches our local state
  private String getLeader(final CloudDescriptor cloudDesc, int timeoutms) {

    String collection = cloudDesc.getCollectionName();
    String shardId = cloudDesc.getShardId();
    // rather than look in the cluster state file, we go straight to the zknodes
    // here, because on cluster restart there could be stale leader info in the
    // cluster state node that won't be updated for a moment
    String leaderUrl;
    try {
      leaderUrl = getLeaderProps(collection, cloudDesc.getShardId(), timeoutms)
          .getCoreUrl();

      // now wait until our currently cloud state contains the latest leader
      String clusterStateLeaderUrl = zkStateReader.getLeaderUrl(collection,
          shardId, timeoutms * 2); // since we found it in zk, we are willing to
      // wait a while to find it in state
      int tries = 0;
      final long msInSec = 1000L;
      int maxTries = (int) Math.floor(leaderConflictResolveWait / msInSec);
      while (!leaderUrl.equals(clusterStateLeaderUrl)) {
        if (cc.isShutDown()) throw new AlreadyClosedException();
        if (tries > maxTries) {
          throw new SolrException(ErrorCode.SERVER_ERROR,
              "There is conflicting information about the leader of shard: "
                  + cloudDesc.getShardId() + " our state says:"
                  + clusterStateLeaderUrl + " but zookeeper says:" + leaderUrl);
        }
        tries++;
        if (tries % 30 == 0) {
          String warnMsg = String.format(Locale.ENGLISH, "Still seeing conflicting information about the leader "
                  + "of shard %s for collection %s after %d seconds; our state says %s, but ZooKeeper says %s",
              cloudDesc.getShardId(), collection, tries, clusterStateLeaderUrl, leaderUrl);
          log.warn(warnMsg);
        }
        Thread.sleep(msInSec);
        clusterStateLeaderUrl = zkStateReader.getLeaderUrl(collection, shardId,
            timeoutms);
        leaderUrl = getLeaderProps(collection, cloudDesc.getShardId(), timeoutms)
            .getCoreUrl();
      }

    } catch (AlreadyClosedException e) {
      throw e;
    } catch (Exception e) {
      log.error("Error getting leader from zk", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error getting leader from zk for shard " + shardId, e);
    }
    return leaderUrl;
  }

  /**
   * Get leader props directly from zk nodes.
   * @throws SessionExpiredException on zk session expiration.
   */
  public ZkCoreNodeProps getLeaderProps(final String collection,
                                        final String slice, int timeoutms) throws InterruptedException, SessionExpiredException {
    return getLeaderProps(collection, slice, timeoutms, true);
  }

  /**
   * Get leader props directly from zk nodes.
   *
   * @return leader props
   * @throws SessionExpiredException on zk session expiration.
   */
  public ZkCoreNodeProps getLeaderProps(final String collection,
                                        final String slice, int timeoutms, boolean failImmediatelyOnExpiration) throws InterruptedException, SessionExpiredException {
    int iterCount = timeoutms / 1000;
    Exception exp = null;
    while (iterCount-- > 0) {
      try {
        byte[] data = zkClient.getData(
            ZkStateReader.getShardLeadersPath(collection, slice), null, null,
            true);
        ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(
            ZkNodeProps.load(data));
        return leaderProps;
      } catch (InterruptedException e) {
        throw e;
      } catch (SessionExpiredException e) {
        if (failImmediatelyOnExpiration) {
          throw e;
        }
        exp = e;
        Thread.sleep(1000);
      } catch (Exception e) {
        exp = e;
        Thread.sleep(1000);
      }
      if (cc.isShutDown()) {
        throw new AlreadyClosedException();
      }
    }
    throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Could not get leader props", exp);
  }


  private void joinElection(CoreDescriptor cd, boolean afterExpiration, boolean joinAtHead)
      throws InterruptedException, KeeperException, IOException {
    // look for old context - if we find it, cancel it
    String collection = cd.getCloudDescriptor().getCollectionName();
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

    ContextKey contextKey = new ContextKey(collection, coreNodeName);

    ElectionContext prevContext = electionContexts.get(contextKey);

    if (prevContext != null) {
      prevContext.cancelElection();
    }

    String shardId = cd.getCloudDescriptor().getShardId();

    Map<String, Object> props = new HashMap<>();
    // we only put a subset of props into the leader node
    props.put(ZkStateReader.BASE_URL_PROP, getBaseUrl());
    props.put(ZkStateReader.CORE_NAME_PROP, cd.getName());
    props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    props.put(ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName);


    ZkNodeProps ourProps = new ZkNodeProps(props);

    LeaderElector leaderElector = new LeaderElector(zkClient, contextKey, electionContexts);
    ElectionContext context = new ShardLeaderElectionContext(leaderElector, shardId,
        collection, coreNodeName, ourProps, this, cc);

    leaderElector.setup(context);
    electionContexts.put(contextKey, context);
    leaderElector.joinElection(context, false, joinAtHead);
  }


  /**
   * Returns whether or not a recovery was started
   */
  private boolean checkRecovery(boolean recoverReloadedCores, final boolean isLeader, boolean skipRecovery,
                                final String collection, String coreZkNodeName, String shardId,
                                SolrCore core, CoreContainer cc, boolean afterExpiration) {
    if (SKIP_AUTO_RECOVERY) {
      log.warn("Skipping recovery according to sys prop solrcloud.skip.autorecovery");
      return false;
    }
    boolean doRecovery = true;
    if (!isLeader) {

      if (skipRecovery || (!afterExpiration && core.isReloaded() && !recoverReloadedCores)) {
        doRecovery = false;
      }

      if (doRecovery) {
        log.info("Core needs to recover:" + core.getName());
        core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
        return true;
      }

      ZkShardTerms zkShardTerms = getShardTerms(collection, shardId);
      if (zkShardTerms.registered(coreZkNodeName) && !zkShardTerms.canBecomeLeader(coreZkNodeName)) {
        log.info("Leader's term larger than core " + core.getName() + "; starting recovery process");
        core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
        return true;
      }
    } else {
      log.info("I am the leader, no recovery necessary");
    }

    return false;
  }


  public String getBaseUrl() {
    return baseURL;
  }

  public void publish(final CoreDescriptor cd, final Replica.State state) throws Exception {
    publish(cd, state, true, false);
  }

  /**
   * Publish core state to overseer.
   */
  public void publish(final CoreDescriptor cd, final Replica.State state, boolean updateLastState, boolean forcePublish) throws Exception {
    if (!forcePublish) {
      try (SolrCore core = cc.getCore(cd.getName())) {
        if (core == null || core.isClosed()) {
          return;
        }
        MDCLoggingContext.setCore(core);
      }
    } else {
      MDCLoggingContext.setCoreDescriptor(cc, cd);
    }
    try {
      String collection = cd.getCloudDescriptor().getCollectionName();

      log.debug("publishing state={}", state.toString());
      // System.out.println(Thread.currentThread().getStackTrace()[3]);
      Integer numShards = cd.getCloudDescriptor().getNumShards();
      if (numShards == null) { // XXX sys prop hack
        log.debug("numShards not found on descriptor - reading it from system property");
        numShards = Integer.getInteger(ZkStateReader.NUM_SHARDS_PROP);
      }

      assert collection != null && collection.length() > 0;

      String shardId = cd.getCloudDescriptor().getShardId();

      String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

      Map<String,Object> props = new HashMap<>();
      props.put(Overseer.QUEUE_OPERATION, "state");
      props.put(ZkStateReader.STATE_PROP, state.toString());
      props.put(ZkStateReader.BASE_URL_PROP, getBaseUrl());
      props.put(ZkStateReader.CORE_NAME_PROP, cd.getName());
      props.put(ZkStateReader.ROLES_PROP, cd.getCloudDescriptor().getRoles());
      props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
      props.put(ZkStateReader.SHARD_ID_PROP, cd.getCloudDescriptor().getShardId());
      props.put(ZkStateReader.COLLECTION_PROP, collection);
      props.put(ZkStateReader.REPLICA_TYPE, cd.getCloudDescriptor().getReplicaType().toString());
      if (!Overseer.isLegacy(zkStateReader)) {
        props.put(ZkStateReader.FORCE_SET_STATE_PROP, "false");
      }
      if (numShards != null) {
        props.put(ZkStateReader.NUM_SHARDS_PROP, numShards.toString());
      }
      if (coreNodeName != null) {
        props.put(ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName);
      }
      try (SolrCore core = cc.getCore(cd.getName())) {
        if (core != null && state == Replica.State.ACTIVE) {
          ensureRegisteredSearcher(core);
        }
        if (core != null && core.getDirectoryFactory().isSharedStorage()) {
          if (core.getDirectoryFactory().isSharedStorage()) {
            props.put(ZkStateReader.SHARED_STORAGE_PROP, "true");
            props.put("dataDir", core.getDataDir());
            UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
            if (ulog != null) {
              props.put("ulogDir", ulog.getLogDir());
            }
          }
        }
      } catch (SolrCoreInitializationException ex) {
        // The core had failed to initialize (in a previous request, not this one), hence nothing to do here.
        log.info("The core '{}' had failed to initialize before.", cd.getName());
      }

      // pull replicas are excluded because their terms are not considered
      if (state == Replica.State.RECOVERING && cd.getCloudDescriptor().getReplicaType() != Type.PULL) {
        // state is used by client, state of replica can change from RECOVERING to DOWN without needed to finish recovery
        // by calling this we will know that a replica actually finished recovery or not
        getShardTerms(collection, shardId).startRecovering(coreNodeName);
      }
      if (state == Replica.State.ACTIVE && cd.getCloudDescriptor().getReplicaType() != Type.PULL) {
        getShardTerms(collection, shardId).doneRecovering(coreNodeName);
      }

      ZkNodeProps m = new ZkNodeProps(props);

      if (updateLastState) {
        cd.getCloudDescriptor().setLastPublished(state);
      }
      overseerJobQueue.offer(Utils.toJSON(m));
    } finally {
      MDCLoggingContext.clear();
    }
  }

  public ZkShardTerms getShardTerms(String collection, String shardId) {
    return getCollectionTerms(collection).getShard(shardId);
  }

  private ZkCollectionTerms getCollectionTerms(String collection) {
    synchronized (collectionToTerms) {
      if (!collectionToTerms.containsKey(collection)) collectionToTerms.put(collection, new ZkCollectionTerms(collection, zkClient));
      return collectionToTerms.get(collection);
    }
  }

  public void clearZkCollectionTerms() {
    synchronized (collectionToTerms) {
      collectionToTerms.values().forEach(ZkCollectionTerms::close);
      collectionToTerms.clear();
    }
  }

  public void unregister(String coreName, CoreDescriptor cd) throws Exception {
    unregister(coreName, cd, true);
  }

  public void unregister(String coreName, CoreDescriptor cd, boolean removeCoreFromZk) throws Exception {
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();
    final String collection = cd.getCloudDescriptor().getCollectionName();
    getCollectionTerms(collection).remove(cd.getCloudDescriptor().getShardId(), cd);
    replicasMetTragicEvent.remove(collection+":"+coreNodeName);

    if (Strings.isNullOrEmpty(collection)) {
      log.error("No collection was specified.");
      assert false : "No collection was specified [" + collection + "]";
      return;
    }
    final DocCollection docCollection = zkStateReader.getClusterState().getCollectionOrNull(collection);
    Replica replica = (docCollection == null) ? null : docCollection.getReplica(coreNodeName);

    if (replica == null || replica.getType() != Type.PULL) {
      ElectionContext context = electionContexts.remove(new ContextKey(collection, coreNodeName));

      if (context != null) {
        context.cancelElection();
      }
    }
    CloudDescriptor cloudDescriptor = cd.getCloudDescriptor();
    if (removeCoreFromZk) {
      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
          OverseerAction.DELETECORE.toLower(), ZkStateReader.CORE_NAME_PROP, coreName,
          ZkStateReader.NODE_NAME_PROP, getNodeName(),
          ZkStateReader.COLLECTION_PROP, cloudDescriptor.getCollectionName(),
          ZkStateReader.BASE_URL_PROP, getBaseUrl(),
          ZkStateReader.CORE_NODE_NAME_PROP, coreNodeName);
      overseerJobQueue.offer(Utils.toJSON(m));
    }
  }

  public void createCollection(String collection) throws Exception {
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        CollectionParams.CollectionAction.CREATE.toLower(), ZkStateReader.NODE_NAME_PROP, getNodeName(),
        ZkStateReader.COLLECTION_PROP, collection);
    overseerJobQueue.offer(Utils.toJSON(m));
  }

  public ZkStateReader getZkStateReader() {
    return zkStateReader;
  }

  private void doGetShardIdAndNodeNameProcess(CoreDescriptor cd) {
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

    if (coreNodeName != null) {
      waitForShardId(cd);
    } else {
      // if no explicit coreNodeName, we want to match by base url and core name
      waitForCoreNodeName(cd);
      waitForShardId(cd);
    }
  }

  private void waitForCoreNodeName(CoreDescriptor descriptor) {
    int retryCount = 320;
    log.debug("look for our core node name");
    while (retryCount-- > 0) {
      final DocCollection docCollection = zkStateReader.getClusterState()
          .getCollectionOrNull(descriptor.getCloudDescriptor().getCollectionName());
      if (docCollection != null && docCollection.getSlicesMap() != null) {
        final Map<String, Slice> slicesMap = docCollection.getSlicesMap();
        for (Slice slice : slicesMap.values()) {
          for (Replica replica : slice.getReplicas()) {
            // TODO: for really large clusters, we could 'index' on this

            String nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);

            String msgNodeName = getNodeName();
            String msgCore = descriptor.getName();

            if (msgNodeName.equals(nodeName) && core.equals(msgCore)) {
              descriptor.getCloudDescriptor()
                  .setCoreNodeName(replica.getName());
              getCoreContainer().getCoresLocator().persist(getCoreContainer(), descriptor);
              return;
            }
          }
        }
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void waitForShardId(CoreDescriptor cd) {
    log.debug("waiting to find shard id in clusterstate for " + cd.getName());
    int retryCount = 320;
    while (retryCount-- > 0) {
      final String shardId = zkStateReader.getClusterState().getShardId(cd.getCollectionName(), getNodeName(), cd.getName());
      if (shardId != null) {
        cd.getCloudDescriptor().setShardId(shardId);
        return;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    throw new SolrException(ErrorCode.SERVER_ERROR,
        "Could not get shard id for core: " + cd.getName());
  }


  public String getCoreNodeName(CoreDescriptor descriptor) {
    String coreNodeName = descriptor.getCloudDescriptor().getCoreNodeName();
    if (coreNodeName == null && !genericCoreNodeNames) {
      // it's the default
      return getNodeName() + "_" + descriptor.getName();
    }

    return coreNodeName;
  }

  public void preRegister(CoreDescriptor cd, boolean publishState) {

    String coreNodeName = getCoreNodeName(cd);

    // before becoming available, make sure we are not live and active
    // this also gets us our assigned shard id if it was not specified
    try {
      checkStateInZk(cd);

      CloudDescriptor cloudDesc = cd.getCloudDescriptor();

      // make sure the node name is set on the descriptor
      if (cloudDesc.getCoreNodeName() == null) {
        cloudDesc.setCoreNodeName(coreNodeName);
      }

      // publishState == false on startup
      if (publishState || isPublishAsDownOnStartup(cloudDesc)) {
        publish(cd, Replica.State.DOWN, false, true);
      }
      String collectionName = cd.getCloudDescriptor().getCollectionName();
      DocCollection collection = zkStateReader.getClusterState().getCollectionOrNull(collectionName);
      log.debug(collection == null ?
              "Collection {} not visible yet, but flagging it so a watch is registered when it becomes visible" :
              "Registering watch for collection {}",
          collectionName);
    } catch (KeeperException e) {
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (NotInClusterStateException e) {
      // make the stack trace less verbose
      throw e;
    } catch (Exception e) {
      log.error("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }

    doGetShardIdAndNodeNameProcess(cd);

  }

  /**
   * On startup, the node already published all of its replicas as DOWN,
   * so in case of legacyCloud=false ( the replica must already present on Zk )
   * we can skip publish the replica as down
   * @return Should publish the replica as down on startup
   */
  private boolean isPublishAsDownOnStartup(CloudDescriptor cloudDesc) {
    if (!Overseer.isLegacy(zkStateReader)) {
      Replica replica = zkStateReader.getClusterState().getCollection(cloudDesc.getCollectionName())
          .getSlice(cloudDesc.getShardId())
          .getReplica(cloudDesc.getCoreNodeName());
      if (replica.getNodeName().equals(getNodeName())) {
        return false;
      }
    }
    return true;
  }

  private void checkStateInZk(CoreDescriptor cd) throws InterruptedException, NotInClusterStateException {
    if (!Overseer.isLegacy(zkStateReader)) {
      CloudDescriptor cloudDesc = cd.getCloudDescriptor();
      String nodeName = cloudDesc.getCoreNodeName();
      if (nodeName == null) {
        if (cc.repairCoreProperty(cd, CoreDescriptor.CORE_NODE_NAME) == false) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "No coreNodeName for " + cd);
        }
        nodeName = cloudDesc.getCoreNodeName();
        // verify that the repair worked.
        if (nodeName == null) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "No coreNodeName for " + cd);
        }
      }
      final String coreNodeName = nodeName;

      if (cloudDesc.getShardId() == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "No shard id for " + cd);
      }

      AtomicReference<String> errorMessage = new AtomicReference<>();
      AtomicReference<DocCollection> collectionState = new AtomicReference<>();
      try {
        zkStateReader.waitForState(cd.getCollectionName(), 10, TimeUnit.SECONDS, (c) -> {
          collectionState.set(c);
          if (c == null)
            return false;
          Slice slice = c.getSlice(cloudDesc.getShardId());
          if (slice == null) {
            errorMessage.set("Invalid shard: " + cloudDesc.getShardId());
            return false;
          }
          Replica replica = slice.getReplica(coreNodeName);
          if (replica == null) {
            errorMessage.set("coreNodeName " + coreNodeName + " does not exist in shard " + cloudDesc.getShardId() +
                ", ignore the exception if the replica was deleted");
            return false;
          }
          return true;
        });
      } catch (TimeoutException e) {
        String error = errorMessage.get();
        if (error == null)
          error = "coreNodeName " + coreNodeName + " does not exist in shard " + cloudDesc.getShardId() +
              ", ignore the exception if the replica was deleted";
        throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, error);
      }
    }
  }

  private ZkCoreNodeProps waitForLeaderToSeeDownState(
      CoreDescriptor descriptor, final String coreZkNodeName) throws SessionExpiredException {
    // try not to wait too long here - if we are waiting too long, we should probably
    // move along and join the election

    CloudDescriptor cloudDesc = descriptor.getCloudDescriptor();
    String collection = cloudDesc.getCollectionName();
    String shard = cloudDesc.getShardId();
    ZkCoreNodeProps leaderProps = null;

    int retries = 2;
    for (int i = 0; i < retries; i++) {
      try {
        if (isClosed) {
          throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "We have been closed");
        }

        // go straight to zk, not the cloud state - we want current info
        leaderProps = getLeaderProps(collection, shard, 5000);
        break;
      } catch (SessionExpiredException e) {
        throw e;
      } catch (Exception e) {
        log.info("Did not find the leader in Zookeeper", e);
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
        if (i == retries - 1) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "There was a problem finding the leader in zk");
        }
      }
    }

    String leaderBaseUrl = leaderProps.getBaseUrl();
    String leaderCoreName = leaderProps.getCoreName();

    String myCoreNodeName = cloudDesc.getCoreNodeName();
    String myCoreName = descriptor.getName();
    String ourUrl = ZkCoreNodeProps.getCoreUrl(getBaseUrl(), myCoreName);

    boolean isLeader = leaderProps.getCoreUrl().equals(ourUrl);
    if (!isLeader && !SKIP_AUTO_RECOVERY) {
      if (!getShardTerms(collection, shard).canBecomeLeader(myCoreNodeName)) {
        log.debug("Term of replica " + myCoreNodeName +
            " is already less than leader, so not waiting for leader to see down state.");
      } else {

        log.info("replica={} is making a best effort attempt to wait for leader={} to see it's DOWN state.", myCoreNodeName, leaderProps.getCoreUrl());

        try (HttpSolrClient client = new Builder(leaderBaseUrl)
            .withConnectionTimeout(8000) // short timeouts, we may be in a storm and this is best effort and maybe we should be the leader now
            .withSocketTimeout(30000)
            .build()) {
          WaitForState prepCmd = new WaitForState();
          prepCmd.setCoreName(leaderCoreName);
          prepCmd.setNodeName(getNodeName());
          prepCmd.setCoreNodeName(coreZkNodeName);
          prepCmd.setState(Replica.State.DOWN);

          // lets give it another chance, but without taking too long
          retries = 3;
          for (int i = 0; i < retries; i++) {
            if (isClosed) {
              throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
                  "We have been closed");
            }
            try {
              client.request(prepCmd);
              break;
            } catch (Exception e) {

              // if the core container is shutdown, don't wait
              if (cc.isShutDown()) {
                throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
                    "Core container is shutdown.");
              }

              Throwable rootCause = SolrException.getRootCause(e);
              if (rootCause instanceof IOException) {
                // if there was a communication error talking to the leader, see if the leader is even alive
                if (!zkStateReader.getClusterState().liveNodesContain(leaderProps.getNodeName())) {
                  throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
                      "Node " + leaderProps.getNodeName() + " hosting leader for " +
                          shard + " in " + collection + " is not live!");
                }
              }

              SolrException.log(log,
                  "There was a problem making a request to the leader", e);
              try {
                Thread.sleep(2000);
              } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
              }
              if (i == retries - 1) {
                throw new SolrException(ErrorCode.SERVER_ERROR,
                    "There was a problem making a request to the leader");
              }
            }
          }
        } catch (IOException e) {
          SolrException.log(log, "Error closing HttpSolrClient", e);
        }
      }
    }
    return leaderProps;
  }

  public static void linkConfSet(SolrZkClient zkClient, String collection, String confSetName) throws KeeperException, InterruptedException {
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    log.debug("Load collection config from:" + path);
    byte[] data;
    try {
      data = zkClient.getData(path, null, null, true);
    } catch (NoNodeException e) {
      // if there is no node, we will try and create it
      // first try to make in case we are pre configuring
      ZkNodeProps props = new ZkNodeProps(CONFIGNAME_PROP, confSetName);
      try {

        zkClient.makePath(path, Utils.toJSON(props),
            CreateMode.PERSISTENT, null, true);
      } catch (KeeperException e2) {
        // it's okay if the node already exists
        if (e2.code() != KeeperException.Code.NODEEXISTS) {
          throw e;
        }
        // if we fail creating, setdata
        // TODO: we should consider using version
        zkClient.setData(path, Utils.toJSON(props), true);
      }
      return;
    }
    // we found existing data, let's update it
    ZkNodeProps props = null;
    if (data != null) {
      props = ZkNodeProps.load(data);
      Map<String, Object> newProps = new HashMap<>(props.getProperties());
      newProps.put(CONFIGNAME_PROP, confSetName);
      props = new ZkNodeProps(newProps);
    } else {
      props = new ZkNodeProps(CONFIGNAME_PROP, confSetName);
    }

    // TODO: we should consider using version
    zkClient.setData(path, Utils.toJSON(props), true);

  }

  /**
   * If in SolrCloud mode, upload config sets for each SolrCore in solr.xml.
   */
  public static void bootstrapConf(SolrZkClient zkClient, CoreContainer cc, String solrHome) throws IOException {

    ZkConfigManager configManager = new ZkConfigManager(zkClient);

    //List<String> allCoreNames = cfg.getAllCoreNames();
    List<CoreDescriptor> cds = cc.getCoresLocator().discover(cc);

    log.info("bootstrapping config for " + cds.size() + " cores into ZooKeeper using solr.xml from " + solrHome);

    for (CoreDescriptor cd : cds) {
      String coreName = cd.getName();
      String confName = cd.getCollectionName();
      if (StringUtils.isEmpty(confName))
        confName = coreName;
      Path udir = cd.getInstanceDir().resolve("conf");
      log.info("Uploading directory " + udir + " with name " + confName + " for SolrCore " + coreName);
      configManager.uploadConfigDir(udir, confName);
    }
  }

  public ZkDistributedQueue getOverseerJobQueue() {
    return overseerJobQueue;
  }

  public OverseerTaskQueue getOverseerCollectionQueue() {
    return overseerCollectionQueue;
  }

  public OverseerTaskQueue getOverseerConfigSetQueue() {
    return overseerConfigSetQueue;
  }

  public DistributedMap getOverseerRunningMap() {
    return overseerRunningMap;
  }

  public DistributedMap getOverseerCompletedMap() {
    return overseerCompletedMap;
  }

  public DistributedMap getOverseerFailureMap() {
    return overseerFailureMap;
  }

  /**
   * When an operation needs to be performed in an asynchronous mode, the asyncId needs
   * to be claimed by calling this method to make sure it's not duplicate (hasn't been
   * claimed by other request). If this method returns true, the asyncId in the parameter
   * has been reserved for the operation, meaning that no other thread/operation can claim
   * it. If for whatever reason, the operation is not scheduled, the asuncId needs to be
   * cleared using {@link #clearAsyncId(String)}.
   * If this method returns false, no reservation has been made, and this asyncId can't
   * be used, since it's being used by another operation (currently or in the past)
   * @param asyncId A string representing the asyncId of an operation. Can't be null.
   * @return True if the reservation succeeds.
   *         False if this ID is already in use.
   */
  public boolean claimAsyncId(String asyncId) throws KeeperException {
    try {
      return asyncIdsMap.putIfAbsent(asyncId, new byte[0]);
    } catch (InterruptedException e) {
      log.error("Could not claim asyncId=" + asyncId, e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Clears an asyncId previously claimed by calling {@link #claimAsyncId(String)}
   * @param asyncId A string representing the asyncId of an operation. Can't be null.
   * @return True if the asyncId existed and was cleared.
   *         False if the asyncId didn't exist before.
   */
  public boolean clearAsyncId(String asyncId) throws KeeperException {
    try {
      return asyncIdsMap.remove(asyncId);
    } catch (InterruptedException e) {
      log.error("Could not release asyncId=" + asyncId, e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  public int getClientTimeout() {
    return clientTimeout;
  }

  public Overseer getOverseer() {
    return overseer;
  }

  public LeaderElector getOverseerElector() {
    return overseerElector;
  }

  /**
   * Returns the nodeName that should be used based on the specified properties.
   *
   * @param hostName    - must not be null or the empty string
   * @param hostPort    - must consist only of digits, must not be null or the empty string
   * @param hostContext - should not begin or end with a slash (leading/trailin slashes will be ignored), must not be null, may be the empty string to denote the root context
   * @lucene.experimental
   * @see ZkStateReader#getBaseUrlForNodeName
   */
  static String generateNodeName(final String hostName,
                                 final String hostPort,
                                 final String hostContext) {
    try {
      return hostName + ':' + hostPort + '_' +
          URLEncoder.encode(trimLeadingAndTrailingSlashes(hostContext), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new Error("JVM Does not seem to support UTF-8", e);
    }
  }

  /**
   * Utility method for trimming and leading and/or trailing slashes from
   * its input.  May return the empty string.  May return null if and only
   * if the input is null.
   */
  public static String trimLeadingAndTrailingSlashes(final String in) {
    if (null == in) return in;

    String out = in;
    if (out.startsWith("/")) {
      out = out.substring(1);
    }
    if (out.endsWith("/")) {
      out = out.substring(0, out.length() - 1);
    }
    return out;
  }

  public void rejoinOverseerElection(String electionNode, boolean joinAtHead) {
    try {
      if (electionNode != null) {
        // Check whether we came to this node by mistake
        if ( overseerElector.getContext() != null && overseerElector.getContext().leaderSeqPath == null 
            && !overseerElector.getContext().leaderSeqPath.endsWith(electionNode)) {
          log.warn("Asked to rejoin with wrong election node : {}, current node is {}", electionNode, overseerElector.getContext().leaderSeqPath);
          //however delete it . This is possible when the last attempt at deleting the election node failed.
          if (electionNode.startsWith(getNodeName())) {
            try {
              zkClient.delete(Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE + "/" + electionNode, -1, true);
            } catch (NoNodeException e) {
              //no problem
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } catch (Exception e) {
              log.warn("Old election node exists , could not be removed ", e);
            }
          }
        } else { // We're in the right place, now attempt to rejoin
          overseerElector.retryElection(new OverseerElectionContext(zkClient,
              overseer, getNodeName()), joinAtHead);
          return;
        }
      } else {
        overseerElector.retryElection(overseerElector.getContext(), joinAtHead);
      }
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to rejoin election", e);
    }

  }

  public void rejoinShardLeaderElection(SolrParams params) {
    try {

      String collectionName = params.get(COLLECTION_PROP);
      String shardId = params.get(SHARD_ID_PROP);
      String coreNodeName = params.get(CORE_NODE_NAME_PROP);
      String coreName = params.get(CORE_NAME_PROP);
      String electionNode = params.get(ELECTION_NODE_PROP);
      String baseUrl = params.get(BASE_URL_PROP);

      try (SolrCore core = cc.getCore(coreName)) {
        MDCLoggingContext.setCore(core);

        log.info("Rejoin the shard leader election.");

        ContextKey contextKey = new ContextKey(collectionName, coreNodeName);

        ElectionContext prevContext = electionContexts.get(contextKey);
        if (prevContext != null) prevContext.cancelElection();

        ZkNodeProps zkProps = new ZkNodeProps(BASE_URL_PROP, baseUrl, CORE_NAME_PROP, coreName, NODE_NAME_PROP, getNodeName(), CORE_NODE_NAME_PROP, coreNodeName);

        LeaderElector elect = ((ShardLeaderElectionContextBase) prevContext).getLeaderElector();
        ShardLeaderElectionContext context = new ShardLeaderElectionContext(elect, shardId, collectionName,
            coreNodeName, zkProps, this, getCoreContainer());

        context.leaderSeqPath = context.electionPath + LeaderElector.ELECTION_NODE + "/" + electionNode;
        elect.setup(context);
        electionContexts.put(contextKey, context);

        elect.retryElection(context, params.getBool(REJOIN_AT_HEAD_PROP, false));
      }
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to rejoin election", e);
    }

  }

  public void checkOverseerDesignate() {
    try {
      byte[] data = zkClient.getData(ZkStateReader.ROLES, null, new Stat(), true);
      if (data == null) return;
      Map roles = (Map) Utils.fromJSON(data);
      if (roles == null) return;
      List nodeList = (List) roles.get("overseer");
      if (nodeList == null) return;
      if (nodeList.contains(getNodeName())) {
        ZkNodeProps props = new ZkNodeProps(Overseer.QUEUE_OPERATION, CollectionParams.CollectionAction.ADDROLE.toString().toLowerCase(Locale.ROOT),
            "node", getNodeName(),
            "role", "overseer");
        log.info("Going to add role {} ", props);
        getOverseerCollectionQueue().offer(Utils.toJSON(props));
      }
    } catch (NoNodeException nne) {
      return;
    } catch (Exception e) {
      log.warn("could not read the overseer designate ", e);
    }
  }

  public CoreContainer getCoreContainer() {
    return cc;
  }

  public void throwErrorIfReplicaReplaced(CoreDescriptor desc) {
    ClusterState clusterState = getZkStateReader().getClusterState();
    if (clusterState != null) {
      DocCollection collection = clusterState.getCollectionOrNull(desc
          .getCloudDescriptor().getCollectionName());
      if (collection != null) {
        CloudUtil.checkSharedFSFailoverReplaced(cc, desc);
      }
    }
  }

  /**
   * Add a listener to be notified once there is a new session created after a ZooKeeper session expiration occurs;
   * in most cases, listeners will be components that have watchers that need to be re-created.
   */
  public void addOnReconnectListener(OnReconnect listener) {
    if (listener != null) {
      synchronized (reconnectListeners) {
        reconnectListeners.add(listener);
        log.debug("Added new OnReconnect listener "+listener);
      }
    }
  }

  /**
   * Removed a previously registered OnReconnect listener, such as when a core is removed or reloaded.
   */
  public void removeOnReconnectListener(OnReconnect listener) {
    if (listener != null) {
      boolean wasRemoved;
      synchronized (reconnectListeners) {
        wasRemoved = reconnectListeners.remove(listener);
      }
      if (wasRemoved) {
        log.debug("Removed OnReconnect listener "+listener);
      } else {
        log.warn("Was asked to remove OnReconnect listener "+listener+
            ", but remove operation did not find it in the list of registered listeners.");
      }
    }
  }

  Set<OnReconnect> getCurrentOnReconnectListeners() {
    HashSet<OnReconnect> clonedListeners;
    synchronized (reconnectListeners) {
      clonedListeners = (HashSet<OnReconnect>)reconnectListeners.clone();
    }
    return clonedListeners;
  }

  /**
   * Persists a config file to ZooKeeper using optimistic concurrency.
   *
   * @return true on success
   */
  public static int persistConfigResourceToZooKeeper(ZkSolrResourceLoader zkLoader, int znodeVersion,
                                                         String resourceName, byte[] content,
                                                         boolean createIfNotExists) {
    int latestVersion = znodeVersion;
    final ZkController zkController = zkLoader.getZkController();
    final SolrZkClient zkClient = zkController.getZkClient();
    final String resourceLocation = zkLoader.getConfigSetZkPath() + "/" + resourceName;
    String errMsg = "Failed to persist resource at {0} - old {1}";
    try {
      try {
        Stat stat = zkClient.setData(resourceLocation, content, znodeVersion, true);
        latestVersion = stat.getVersion();// if the set succeeded , it should have incremented the version by one always
        log.info("Persisted config data to node {} ", resourceLocation);
        touchConfDir(zkLoader);
      } catch (NoNodeException e) {
        if (createIfNotExists) {
          try {
            zkClient.create(resourceLocation, content, CreateMode.PERSISTENT, true);
            latestVersion = 0;//just created so version must be zero
            touchConfDir(zkLoader);
          } catch (KeeperException.NodeExistsException nee) {
            try {
              Stat stat = zkClient.exists(resourceLocation, null, true);
              log.debug("failed to set data version in zk is {} and expected version is {} ", stat.getVersion(), znodeVersion);
            } catch (Exception e1) {
              log.warn("could not get stat");
            }

            log.info(StrUtils.formatString(errMsg, resourceLocation, znodeVersion));
            throw new ResourceModifiedInZkException(ErrorCode.CONFLICT, StrUtils.formatString(errMsg, resourceLocation, znodeVersion) + ", retry.");
          }
        }
      }

    } catch (KeeperException.BadVersionException bve) {
      int v = -1;
      try {
        Stat stat = zkClient.exists(resourceLocation, null, true);
        v = stat.getVersion();
      } catch (Exception e) {
        log.error(e.getMessage());

      }
      log.info(StrUtils.formatString(errMsg + " zkVersion= " + v, resourceLocation, znodeVersion));
      throw new ResourceModifiedInZkException(ErrorCode.CONFLICT, StrUtils.formatString(errMsg, resourceLocation, znodeVersion) + ", retry.");
    } catch (ResourceModifiedInZkException e) {
      throw e;
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt(); // Restore the interrupted status
      }
      final String msg = "Error persisting resource at " + resourceLocation;
      log.error(msg, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);
    }
    return latestVersion;
  }

  public static void touchConfDir(ZkSolrResourceLoader zkLoader) {
    SolrZkClient zkClient = zkLoader.getZkController().getZkClient();
    try {
      zkClient.setData(zkLoader.getConfigSetZkPath(), new byte[]{0}, true);
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt(); // Restore the interrupted status
      }
      final String msg = "Error 'touching' conf location " + zkLoader.getConfigSetZkPath();
      log.error(msg, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);

    }
  }

  public static class ResourceModifiedInZkException extends SolrException {
    public ResourceModifiedInZkException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }

  private void unregisterConfListener(String confDir, Runnable listener) {
    synchronized (confDirectoryListeners) {
      final Set<Runnable> listeners = confDirectoryListeners.get(confDir);
      if (listeners == null) {
        log.warn(confDir + " has no more registered listeners, but a live one attempted to unregister!");
        return;
      }
      if (listeners.remove(listener)) {
        log.debug("removed listener for config directory [{}]", confDir);
      }
      if (listeners.isEmpty()) {
        // no more listeners for this confDir, remove it from the map
        log.debug("No more listeners for config directory [{}]", confDir);
        confDirectoryListeners.remove(confDir);
      }
    }
  }

  /**
   * This will give a callback to the listener whenever a child is modified in the
   * conf directory. It is the responsibility of the listener to check if the individual
   * item of interest has been modified.  When the last core which was interested in
   * this conf directory is gone the listeners will be removed automatically.
   */
  public void registerConfListenerForCore(final String confDir, SolrCore core, final Runnable listener) {
    if (listener == null) {
      throw new NullPointerException("listener cannot be null");
    }
    synchronized (confDirectoryListeners) {
      final Set<Runnable> confDirListeners = getConfDirListeners(confDir);
      confDirListeners.add(listener);
      core.addCloseHook(new CloseHook() {
        @Override
        public void preClose(SolrCore core) {
          unregisterConfListener(confDir, listener);
        }

        @Override
        public void postClose(SolrCore core) {
        }
      });
    }
  }

  // this method is called in a protected confDirListeners block
  private Set<Runnable> getConfDirListeners(final String confDir) {
    assert Thread.holdsLock(confDirectoryListeners) : "confDirListeners lock not held by thread";
    Set<Runnable> confDirListeners = confDirectoryListeners.get(confDir);
    if (confDirListeners == null) {
      log.debug("watch zkdir {}" , confDir);
      confDirListeners = new HashSet<>();
      confDirectoryListeners.put(confDir, confDirListeners);
      setConfWatcher(confDir, new WatcherImpl(confDir), null);
    }
    return confDirListeners;
  }

  private final Map<String, Set<Runnable>> confDirectoryListeners = new HashMap<>();

  private class WatcherImpl implements Watcher {
    private final String zkDir;

    private WatcherImpl(String dir) {
      this.zkDir = dir;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }

      Stat stat = null;
      try {
        stat = zkClient.exists(zkDir, null, true);
      } catch (KeeperException e) {
        //ignore , it is not a big deal
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      boolean resetWatcher = false;
      try {
        resetWatcher = fireEventListeners(zkDir);
      } finally {
        if (Event.EventType.None.equals(event.getType())) {
          log.debug("A node got unwatched for {}", zkDir);
        } else {
          if (resetWatcher) setConfWatcher(zkDir, this, stat);
          else log.debug("A node got unwatched for {}", zkDir);
        }
      }
    }
  }

  private boolean fireEventListeners(String zkDir) {
    if (isClosed || cc.isShutDown()) {
      return false;
    }
    synchronized (confDirectoryListeners) {
      // if this is not among directories to be watched then don't set the watcher anymore
      if (!confDirectoryListeners.containsKey(zkDir)) {
        log.debug("Watcher on {} is removed ", zkDir);
        return false;
      }
      final Set<Runnable> listeners = confDirectoryListeners.get(zkDir);
      if (listeners != null && !listeners.isEmpty()) {
        final Set<Runnable> listenersCopy = new HashSet<>(listeners);
        // run these in a separate thread because this can be long running
        new Thread(() -> {
          log.debug("Running listeners for {}", zkDir);
          for (final Runnable listener : listenersCopy) {
            try {
              listener.run();
            } catch (Exception e) {
              log.warn("listener throws error", e);
            }
          }
        }).start();

      }
    }
    return true;
  }

  private void setConfWatcher(String zkDir, Watcher watcher, Stat stat) {
    try {
      Stat newStat = zkClient.exists(zkDir, watcher, true);
      if (stat != null && newStat.getVersion() > stat.getVersion()) {
        //a race condition where a we missed an event fired
        //so fire the event listeners
        fireEventListeners(zkDir);
      }
    } catch (KeeperException e) {
      log.error("failed to set watcher for conf dir {} ", zkDir);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("failed to set watcher for conf dir {} ", zkDir);
    }
  }

  public OnReconnect getConfigDirListener() {
    return () -> {
      synchronized (confDirectoryListeners) {
        for (String s : confDirectoryListeners.keySet()) {
          setConfWatcher(s, new WatcherImpl(s), null);
          fireEventListeners(s);
        }
      }
    };
  }

  /** @lucene.internal */
  class UnloadCoreOnDeletedWatcher implements DocCollectionWatcher {
    String coreNodeName;
    String shard;
    String coreName;

    public UnloadCoreOnDeletedWatcher(String coreNodeName, String shard, String coreName) {
      this.coreNodeName = coreNodeName;
      this.shard = shard;
      this.coreName = coreName;
    }

    @Override
    // synchronized due to SOLR-11535
    public synchronized boolean onStateChanged(DocCollection collectionState) {
      if (getCoreContainer().getCoreDescriptor(coreName) == null) return true;

      boolean replicaRemoved = getReplicaOrNull(collectionState, shard, coreNodeName) == null;
      if (replicaRemoved) {
        try {
          log.info("Replica {} removed from clusterstate, remove it.", coreName);
          getCoreContainer().unload(coreName, true, true, true);
        } catch (SolrException e) {
          if (!e.getMessage().contains("Cannot unload non-existent core")) {
            // no need to log if the core was already unloaded
            log.warn("Failed to unregister core:{}", coreName, e);
          }
        } catch (Exception e) {
          log.warn("Failed to unregister core:{}", coreName, e);
        }
      }
      return replicaRemoved;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      UnloadCoreOnDeletedWatcher that = (UnloadCoreOnDeletedWatcher) o;
      return Objects.equals(coreNodeName, that.coreNodeName) &&
          Objects.equals(shard, that.shard) &&
          Objects.equals(coreName, that.coreName);
    }

    @Override
    public int hashCode() {

      return Objects.hash(coreNodeName, shard, coreName);
    }
  }

  /**
   * Thrown during pre register process if the replica is not present in clusterstate
   */
  public static class NotInClusterStateException extends SolrException {
    public NotInClusterStateException(ErrorCode code, String msg) {
      super(code, msg);
    }
  }

  public boolean checkIfCoreNodeNameAlreadyExists(CoreDescriptor dcore) {
    DocCollection collection = zkStateReader.getClusterState().getCollectionOrNull(dcore.getCollectionName());
    if (collection != null) {
      Collection<Slice> slices = collection.getSlices();

      for (Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        Replica r = slice.getReplica(dcore.getCloudDescriptor().getCoreNodeName());
        if (r != null) {
          return true;
        }
      }
    }
    return false;
  }


  /**
   * Best effort to set DOWN state for all replicas on node.
   *
   * @param nodeName to operate on
   */
  public void publishNodeAsDown(String nodeName) {
    log.info("Publish node={} as DOWN", nodeName);
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.DOWNNODE.toLower(),
        ZkStateReader.NODE_NAME_PROP, nodeName);
    try {
      overseer.getStateUpdateQueue().offer(Utils.toJSON(m));
    } catch (AlreadyClosedException e) {
      log.info("Not publishing node as DOWN because a resource required to do so is already closed.");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug("Publish node as down was interrupted.");
    } catch (KeeperException e) {
      log.warn("Could not publish node as down: " + e.getMessage());
    }
  }

  /**
   * Ensures that a searcher is registered for the given core and if not, waits until one is registered
   */
  private static void ensureRegisteredSearcher(SolrCore core) throws InterruptedException {
    if (!core.getSolrConfig().useColdSearcher) {
      RefCounted<SolrIndexSearcher> registeredSearcher = core.getRegisteredSearcher();
      if (registeredSearcher != null) {
        log.debug("Found a registered searcher: {} for core: {}", registeredSearcher.get(), core);
        registeredSearcher.decref();
      } else  {
        Future[] waitSearcher = new Future[1];
        log.info("No registered searcher found for core: {}, waiting until a searcher is registered before publishing as active", core.getName());
        final RTimer timer = new RTimer();
        RefCounted<SolrIndexSearcher> searcher = null;
        try {
          searcher = core.getSearcher(false, true, waitSearcher, true);
          boolean success = true;
          if (waitSearcher[0] != null)  {
            log.debug("Waiting for first searcher of core {}, id: {} to be registered", core.getName(), core);
            try {
              waitSearcher[0].get();
            } catch (ExecutionException e) {
              log.warn("Wait for a searcher to be registered for core " + core.getName() + ",id: " + core + " failed due to: " + e, e);
              success = false;
            }
          }
          if (success)  {
            if (searcher == null) {
              // should never happen
              log.debug("Did not find a searcher even after the future callback for core: {}, id: {}!!!", core.getName(), core);
            } else  {
              log.info("Found a registered searcher: {}, took: {} ms for core: {}, id: {}", searcher.get(), timer.getTime(), core.getName(), core);
            }
          }
        } finally {
          if (searcher != null) {
            searcher.decref();
          }
        }
      }
      RefCounted<SolrIndexSearcher> newestSearcher = core.getNewestSearcher(false);
      if (newestSearcher != null) {
        log.debug("Found newest searcher: {} for core: {}, id: {}", newestSearcher.get(), core.getName(), core);
        newestSearcher.decref();
      }
    }
  }
}
