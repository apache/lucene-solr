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

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.cloud.DistributedLock;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.TriggerEventType;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.BeforeReconnect;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ConnectionManager;
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
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeOut;
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
import org.apache.solr.handler.component.HttpShardHandlerFactory;
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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTIONS_ZKNODE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.invoke.MethodHandles;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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

  public static final String CLUSTER_SHUTDOWN = "/cluster/shutdown";

  static final int WAIT_DOWN_STATES_TIMEOUT_SECONDS = 60;
  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  public final int WAIT_FOR_STATE = Integer.getInteger("solr.waitForState", 10);

  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");
  private final int zkClientConnectTimeout;
  private final Supplier<List<CoreDescriptor>> descriptorsSupplier;
  private final ZkACLProvider zkACLProvider;
  private final CloseTracker closeTracker;
  private boolean closeZkClient = false;

  private volatile ZkDistributedQueue overseerJobQueue;
  private volatile OverseerTaskQueue overseerCollectionQueue;
  private volatile OverseerTaskQueue overseerConfigSetQueue;

  private volatile DistributedMap overseerRunningMap;
  private volatile DistributedMap overseerCompletedMap;
  private volatile DistributedMap overseerFailureMap;
  private volatile DistributedMap asyncIdsMap;

  public final static String COLLECTION_PARAM_PREFIX = "collection.";
  public final static String CONFIGNAME_PROP = "configName";
  private boolean shudownCalled;

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

  private static byte[] emptyJson = "{}".getBytes(StandardCharsets.UTF_8);

  private final Map<ContextKey, ElectionContext> electionContexts = new ConcurrentHashMap<>(64, 0.75f, 16) {
    @Override
    public ElectionContext put(ContextKey key, ElectionContext value) {
      if (ZkController.this.isClosed || cc.isShutDown()) {
        throw new AlreadyClosedException();
      }
      return super.put(key, value);
    }
  };

  private final Map<ContextKey, ElectionContext> overseerContexts = new ConcurrentHashMap<>(3, 0.75f, 1) {
    @Override
    public ElectionContext put(ContextKey key, ElectionContext value) {
      if (ZkController.this.isClosed || cc.isShutDown()) {
        throw new AlreadyClosedException();
      }
      return super.put(key, value);
    }
  };

  private volatile SolrZkClient zkClient;
  public volatile ZkStateReader zkStateReader;
  private volatile SolrCloudManager cloudManager;
  private volatile CloudHttp2SolrClient cloudSolrClient;

  private final String zkServerAddress;          // example: 127.0.0.1:54062/solr

  private final int localHostPort;      // example: 54065
  private final String hostName;           // example: 127.0.0.1
  private final String nodeName;           // example: 127.0.0.1:54065_solr
  private volatile String baseURL;            // example: http://127.0.0.1:54065/solr

  private final CloudConfig cloudConfig;
  private volatile NodesSysPropsCacher sysPropsCacher;

  private volatile LeaderElector overseerElector;

  private final Map<String, ReplicateFromLeader> replicateFromLeaders = new ConcurrentHashMap<>(132, 0.75f, 50);
  private final Map<String, ZkCollectionTerms> collectionToTerms = new ConcurrentHashMap<>(132, 0.75f, 50);

  // for now, this can be null in tests, in which case recovery will be inactive, and other features
  // may accept defaults or use mocks rather than pulling things from a CoreContainer
  private volatile CoreContainer cc;

  protected volatile Overseer overseer;

  private int leaderVoteWait;
  private int leaderConflictResolveWait;

  private volatile boolean genericCoreNodeNames;

  private volatile int clientTimeout;

  private volatile boolean isClosed;

  private final Object initLock = new Object();

  private final ConcurrentHashMap<String, Throwable> replicasMetTragicEvent = new ConcurrentHashMap<>(132, 0.75f, 12);

  @Deprecated
  // keeps track of replicas that have been asked to recover by leaders running on this node
  private final Map<String, String> replicasInLeaderInitiatedRecovery = new HashMap<String, String>();

  // keeps track of a list of objects that need to know a new ZooKeeper session was created after expiration occurred
  // ref is held as a HashSet since we clone the set before notifying to avoid synchronizing too long
  private final Set<OnReconnect> reconnectListeners = ConcurrentHashMap.newKeySet();

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
      if (log.isInfoEnabled()) {
        log.info("Registering core {} afterExpiration? {}", descriptor.getName(), afterExpiration);
      }
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


  public ZkController(final CoreContainer cc, String zkServerAddress, int zkClientConnectTimeout, CloudConfig cloudConfig, final Supplier<List<CoreDescriptor>> descriptorsSupplier) throws InterruptedException, IOException, TimeoutException {
    this(cc, new SolrZkClient(), cloudConfig, descriptorsSupplier);
    this.closeZkClient = true;
  }

  /**
   * @param cc Core container associated with this controller. cannot be null.
   * @param cloudConfig configuration for this controller. TODO: possibly redundant with CoreContainer
   * @param descriptorsSupplier a supplier of the current core descriptors. used to know which cores to re-register on reconnect
   */
  public ZkController(final CoreContainer cc, SolrZkClient zkClient, CloudConfig cloudConfig, final Supplier<List<CoreDescriptor>> descriptorsSupplier)
      throws InterruptedException, TimeoutException, IOException {
    closeTracker = new CloseTracker();
    if (cc == null) log.error("null corecontainer");
    if (cc == null) throw new IllegalArgumentException("CoreContainer cannot be null.");
    try {
      this.cc = cc;
      this.descriptorsSupplier = descriptorsSupplier;
      this.cloudConfig = cloudConfig;
      this.zkClientConnectTimeout = zkClient.getZkClientTimeout();
      this.genericCoreNodeNames = cloudConfig.getGenericCoreNodeNames();
      this.zkClient = zkClient;
      // be forgiving and strip this off leading/trailing slashes
      // this allows us to support users specifying hostContext="/" in
      // solr.xml to indicate the root context, instead of hostContext=""
      // which means the default of "solr"
      String localHostContext = trimLeadingAndTrailingSlashes(cloudConfig.getSolrHostContext());

      this.zkServerAddress = zkClient.getZkServerAddress();
      this.localHostPort = cloudConfig.getSolrHostPort();
      if (log.isDebugEnabled()) log.debug("normalize hostname {}", cloudConfig.getHost());
      this.hostName = normalizeHostName(cloudConfig.getHost());
      if (log.isDebugEnabled()) log.debug("generate node name");
      this.nodeName = generateNodeName(this.hostName, Integer.toString(this.localHostPort), localHostContext);
      log.info("node name={}", nodeName);
      MDCLoggingContext.setNode(nodeName);

      if (log.isDebugEnabled()) log.debug("leaderVoteWait get");
      this.leaderVoteWait = cloudConfig.getLeaderVoteWait();
      if (log.isDebugEnabled()) log.debug("leaderConflictWait get");
      this.leaderConflictResolveWait = cloudConfig.getLeaderConflictResolveWait();

      if (log.isDebugEnabled()) log.debug("clientTimeout get");
      this.clientTimeout = cloudConfig.getZkClientTimeout();
      if (log.isDebugEnabled()) log.debug("create connection strat");

      String zkACLProviderClass = cloudConfig.getZkACLProviderClass();

      if (zkACLProviderClass != null && zkACLProviderClass.trim().length() > 0) {
        zkACLProvider = cc.getResourceLoader().newInstance(zkACLProviderClass, ZkACLProvider.class);
      } else {
        zkACLProvider = new DefaultZkACLProvider();
      }
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      log.error("Exception during ZkController init", e);
      throw e;
    }

    assert ObjectReleaseTracker.track(this);
  }

  public void closeLeaderContext(CoreDescriptor cd) {
    String collection = cd.getCloudDescriptor().getCollectionName();
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

    ContextKey contextKey = new ContextKey(collection, coreNodeName);
    ElectionContext context = electionContexts.get(contextKey);
    if (context != null) {
      try {
        context.cancelElection();
      } catch (InterruptedException e) {
        ParWork.propegateInterrupt(e);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } catch (KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } finally {
        context.close();
      }
    }
  }

  public void start() throws KeeperException {

    String zkCredentialsProviderClass = cloudConfig.getZkCredentialsProviderClass();
    if (zkCredentialsProviderClass != null && zkCredentialsProviderClass.trim().length() > 0) {
      zkClient.getConnectionManager().setZkCredentialsToAddAutomatically(cc.getResourceLoader().newInstance(zkCredentialsProviderClass, ZkCredentialsProvider.class));
    } else {
      zkClient.getConnectionManager().setZkCredentialsToAddAutomatically(new DefaultZkCredentialsProvider());
    }
    addOnReconnectListener(getConfigDirListener());
    zkClient.getConnectionManager().setBeforeReconnect(new BeforeReconnect() {

      @Override
      public synchronized void command() {

        try (ParWork worker = new ParWork("disconnected", true)) {
          worker.collect("OverseerElectionContexts", overseerContexts.values());
          worker.collect("Overseer", ZkController.this.overseer);
          worker.collect("", () -> {
            clearZkCollectionTerms();
          });
          worker.collect("electionContexts", electionContexts.values());
          worker.collect("",() -> {
            markAllAsNotLeader(descriptorsSupplier);
          });
          worker.collect("",() -> {
            cc.cancelCoreRecoveries();
          });
        }
      }
    });
    zkClient.setAclProvider(zkACLProvider);
    zkClient.getConnectionManager().setOnReconnect(new OnReconnect() {

      @Override
      public void command() throws SessionExpiredException {
        synchronized (initLock) {
          if (cc.isShutDown() || !zkClient.isConnected()) return;
          log.info("ZooKeeper session re-connected ... refreshing core states after session expiration.");
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

            ElectionContext context = new OverseerElectionContext(getNodeName(), zkClient ,overseer);
            ElectionContext prevContext = overseerContexts.put(new ContextKey("overseer", "overseer"), context);
            if (prevContext != null) {
              prevContext.close();
            }
            if (overseerElector != null) {
              ParWork.close(overseerElector.getContext());
            }
            LeaderElector overseerElector = new LeaderElector(zkClient, new ContextKey("overseer", "overseer"), overseerContexts);
            ZkController.this.overseer = new Overseer((HttpShardHandler) ((HttpShardHandlerFactory) cc.getShardHandlerFactory()).getShardHandler(cc.getUpdateShardHandler().getTheSharedHttpClient()), cc.getUpdateShardHandler(),
                    CommonParams.CORES_HANDLER_PATH, zkStateReader, ZkController.this, cloudConfig);
            overseerElector.setup(context);
            overseerElector.joinElection(context, true);


            // we have to register as live first to pick up docs in the buffer
            createEphemeralLiveNode();

            List<CoreDescriptor> descriptors = descriptorsSupplier.get();
            // re register all descriptors
            try (ParWork parWork = new ParWork(this)) {
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

                    parWork.collect(new RegisterCoreAsync(descriptor, true, true));

                  } catch (Exception e) {
                    ParWork.propegateInterrupt(e);
                    SolrException.log(log, "Error registering SolrCore", e);
                  }
                }
              }
            }

            // notify any other objects that need to know when the session was re-connected

            try (ParWork parWork = new ParWork(this)) {
              // the OnReconnect operation can be expensive per listener, so do that async in the background
              for (OnReconnect listener : reconnectListeners) {
                try {
                  parWork.collect(new OnReconnectNotifyAsync(listener));
                } catch (Exception exc) {
                  SolrZkClient.checkInterrupted(exc);
                  // not much we can do here other than warn in the log
                  log.warn("Error when notifying OnReconnect listener {} after session re-connected.", listener, exc);
                }
              }
            }
          } catch (InterruptedException e) {
            ParWork.propegateInterrupt(e);
            throw new ZooKeeperException(
                    SolrException.ErrorCode.SERVER_ERROR, "", e);
          } catch (SessionExpiredException e) {
            throw e;
          } catch (AlreadyClosedException e) {
            log.info("Already closed");
            return;
          } catch (Exception e) {
            SolrException.log(log, "", e);
            throw new ZooKeeperException(
                    SolrException.ErrorCode.SERVER_ERROR, "", e);
          }
        }
      }
    });

    zkClient.setIsClosed(new ConnectionManager.IsClosed() {

      @Override
      public boolean isClosed() {
        return cc.isShutDown();
      }});
    zkClient.setDisconnectListener(() -> {

        try (ParWork worker = new ParWork("disconnected", true)) {
          worker.collect( ZkController.this.overseer);
          worker.collect("clearZkCollectionTerms", () -> {
            clearZkCollectionTerms();
          });
          if (zkClient.isConnected()) {
            worker.collect(electionContexts.values());
          }
          worker.collect("markAllAsNotLeader", () -> {
            markAllAsNotLeader(descriptorsSupplier);
          });
        }
      //  ParWork.closeExecutor(); // we are using the root exec directly, let's just make sure it's closed here to avoid a slight delay leak
    });
    init();
  }

  public int getLeaderVoteWait() {
    return leaderVoteWait;
  }

  public int getLeaderConflictResolveWait() {
    return leaderConflictResolveWait;
  }

  public NodesSysPropsCacher getSysPropsCacher() {
    return sysPropsCacher;
  }

  private void markAllAsNotLeader(final Supplier<List<CoreDescriptor>> registerOnReconnect) {
    List<CoreDescriptor> descriptors = registerOnReconnect.get();
    if (descriptors != null) {
      for (CoreDescriptor descriptor : descriptors) {
        descriptor.getCloudDescriptor().setLeader(false);
        descriptor.getCloudDescriptor().setHasRegistered(false);
      }
    }
  }

  public void disconnect() {
    try (ParWork closer = new ParWork(this, true)) {
      if (getZkClient().getConnectionManager().isConnected()) {
        closer.collect( "replicateFromLeaders", replicateFromLeaders.values());

        closer.collect("PublishNodeAsDown&RepFromLeadersClose&RemoveEmphem", () -> {

          try {
            log.info("Publish this node as DOWN...");
            publishNodeAsDown(getNodeName());
          } catch (Exception e) {
            ParWork.propegateInterrupt("Error publishing nodes as down. Continuing to close CoreContainer", e);
          }
          return "PublishDown";

        });
        closer.addCollect();
        closer.collect("removeEphemeralLiveNode", () -> {
          try {
            removeEphemeralLiveNode();
          } catch (Exception e) {
            ParWork.propegateInterrupt("Error Removing ephemeral live node. Continuing to close CoreContainer", e);
          }
          return "RemoveEphemNode";

        });
      }
    }
  }

  /**
   * Closes the underlying ZooKeeper client.
   */
  public synchronized void close() {
    log.info("Closing ZkController");
    closeTracker.close();
    this.shudownCalled = true;

    this.isClosed = true;

    try (ParWork closer = new ParWork(this, true)) {
      closer.collect(electionContexts.values());
      closer.collect(collectionToTerms.values());
      closer.collect(sysPropsCacher);
      closer.collect(cloudManager);
      closer.collect(cloudSolrClient);
      closer.collect(replicateFromLeaders.values());
      closer.collect(overseerContexts.values());
      closer.collect("Overseer", () -> {
        if (overseer != null) {
          overseer.closeAndDone();
        }
      });
      closer.addCollect();
      closer.collect(zkStateReader);
      closer.addCollect();
      if (closeZkClient) {
        closer.collect(zkClient);
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
          ParWork.propegateInterrupt(e);
          // Exceptions are not bubbled up. giveupLeadership is best effort, and is only called in case of some other
          // unrecoverable error happened
          log.error("Met exception on give up leadership for {}", key, e);
          replicasMetTragicEvent.remove(key);
        }
      }
    }
  }


  /**
   * Returns true if config file exists
   */
  public boolean configFileExists(String collection, String fileName)
      throws KeeperException, InterruptedException {
    Stat stat = zkClient.exists(ZkConfigManager.CONFIGS_ZKNODE + "/" + collection + "/" + fileName, null);
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
      cloudSolrClient = new CloudHttp2SolrClient.Builder(zkStateReader)
          .withHttpClient(cc.getUpdateShardHandler().getTheSharedHttpClient())
          .build();
      cloudManager = new SolrClientCloudManager(
          new ZkDistributedQueueFactory(zkClient),
          cloudSolrClient,
          cc.getObjectCache(), cc.getUpdateShardHandler().getDefaultHttpClient());
      cloudManager.getClusterStateProvider().connect();
    }
    return cloudManager;
  }

  public CloudHttp2SolrClient getCloudSolrClient() {
    return  cloudSolrClient;
  }

  /**
   * Returns config file data (in bytes)
   */
  public byte[] getConfigFileData(String zkConfigName, String fileName)
      throws KeeperException, InterruptedException {
    String zkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + zkConfigName + "/" + fileName;
    byte[] bytes = zkClient.getData(zkPath, null, null);
    if (bytes == null) {
      log.error("Config file contains no data:{}", zkPath);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Config file contains no data:" + zkPath);
    }

    return bytes;
  }

  // normalize host removing any url scheme.
  // input can be null, host, or url_prefix://host
  public static String normalizeHostName(String host) {

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
          ParWork.propegateInterrupt(e);
          SolrException.log(log,
              "Error while looking for a better host name than 127.0.0.1", e);
        }
      }
      host = hostaddress;
    } else {
      if (log.isDebugEnabled()) log.debug("remove host scheme");
      if (URLUtil.hasScheme(host)) {
        host = URLUtil.removeScheme(host);
      }
    }
    if (log.isDebugEnabled()) log.debug("return host {}", host);
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
    return isClosed || getCoreContainer().isShutDown();
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
    log.info("Creating cluster zk nodes");
    // we want to have a full zk layout at the start
    // this is especially important so that we don't miss creating
    // any watchers with ZkStateReader on startup

    Map<String,byte[]> paths = new HashMap<>(45);

    paths.put(ZkStateReader.LIVE_NODES_ZKNODE, null);
    paths.put(ZkStateReader.CONFIGS_ZKNODE, null);
    paths.put(ZkStateReader.ALIASES, emptyJson);

    paths.put("/overseer", null);
    paths.put(Overseer.OVERSEER_ELECT, null);
    paths.put(Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE, null);

    paths.put(Overseer.OVERSEER_QUEUE, null);
    paths.put(Overseer.OVERSEER_QUEUE_WORK, null);
    paths.put(Overseer.OVERSEER_COLLECTION_QUEUE_WORK, null);
    paths.put(Overseer.OVERSEER_COLLECTION_MAP_RUNNING, null);
    paths.put(Overseer.OVERSEER_COLLECTION_MAP_COMPLETED, null);

    paths.put(Overseer.OVERSEER_COLLECTION_MAP_FAILURE, null);
    paths.put(Overseer.OVERSEER_ASYNC_IDS, null);
    paths.put(Overseer.OVERSEER_ELECT, null);


    paths.put("/autoscaling", null);
    paths.put(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, emptyJson);
    paths.put(ZkStateReader.SOLR_AUTOSCALING_EVENTS_PATH, null);
    paths.put(ZkStateReader.SOLR_AUTOSCALING_TRIGGER_STATE_PATH, null);
    // created with ephem node
    // paths.put(ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH, null);
    paths.put(ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH, null);
    paths.put("/autoscaling/events/.scheduled_maintenance", null);
    paths.put("/autoscaling/events/.auto_add_replicas", null);
//
    paths.put(ZkStateReader.CLUSTER_STATE, emptyJson);
    //   operations.add(zkClient.createPathOp(ZkStateReader.CLUSTER_PROPS, emptyJson));
    paths.put(ZkStateReader.SOLR_PKGS_PATH, null);
    paths.put(ZkStateReader.ROLES, emptyJson);


    paths.put(COLLECTIONS_ZKNODE, null);
//

//
//    // we create the collection znode last to indicate succesful cluster init
    // operations.add(zkClient.createPathOp(ZkStateReader.COLLECTIONS_ZKNODE));
    
    zkClient.mkdirs(paths);
//
    try {
      zkClient.mkDirs(ZkStateReader.SOLR_SECURITY_CONF_PATH, emptyJson);
    } catch (KeeperException.NodeExistsException e) {
      // okay, can be prepopulated
    }
    try {
      zkClient.mkDirs(ZkStateReader.CLUSTER_PROPS, emptyJson);
    } catch (KeeperException.NodeExistsException e) {
      // okay, can be prepopulated
    }

    if (!Boolean.getBoolean("solr.suppressDefaultConfigBootstrap")) {
      bootstrapDefaultConfigSet(zkClient);
    } else {
      log.info("Supressing upload of default config set");
    }

    log.info("Creating final {} node", "/cluster/init");
    zkClient.mkdir( "/cluster/init");

  }

  private static void bootstrapDefaultConfigSet(SolrZkClient zkClient) throws KeeperException, InterruptedException, IOException {
    if (!zkClient.exists("/configs/_default")) {
      String configDirPath = getDefaultConfigDirPath();
      if (configDirPath == null) {
        log.warn("The _default configset could not be uploaded. Please provide 'solr.default.confdir' parameter that points to a configset {} {}"
            , "intended to be the default. Current 'solr.default.confdir' value:"
            , System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE));
      } else {
        ZkMaintenanceUtils.upConfig(zkClient, Paths.get(configDirPath), ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME);
      }
    }
  }

  /**
   * Gets the absolute filesystem path of the _default configset to bootstrap from.
   * First tries the sysprop "solr.default.confdir". If not found, tries to find
   * the _default dir relative to the sysprop "solr.install.dir".
   * Returns null if not found anywhere.
   *
   * @lucene.internal
   * @see SolrDispatchFilter#SOLR_DEFAULT_CONFDIR_ATTRIBUTE
   */
  public static String getDefaultConfigDirPath() {
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
    }
    return configDirPath;
  }

  private void init() {
    // nocommit
//    Runtime.getRuntime().addShutdownHook(new Thread() {
//      public void run() {
//        shutdown();
//        ParWork.close(ParWork.getExecutor());
//      }
//
//    });
    synchronized (initLock) {
      log.info("making shutdown watcher for cluster");
      try {
        zkClient.exists(CLUSTER_SHUTDOWN, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (Event.EventType.None.equals(event.getType())) {
              return;
            }

            log.info("Got even for shutdown {}" + event);
            if (event.getType().equals(Event.EventType.NodeCreated)) {
              log.info("Shutdown zk node created, shutting down");
              shutdown();
            } else {
              log.info("Remaking shutdown watcher");
              Stat stat = null;
              try {
                stat = zkClient.exists(CLUSTER_SHUTDOWN, this);
              } catch (KeeperException e) {
                SolrException.log(log, e);
                return;
              } catch (InterruptedException e) {
                SolrException.log(log, e);
                return;
              }
              if (stat != null) {
                log.info("Got shutdown even while remaking watcher, shutting down");
                shutdown();
              }
            }
          }
        });

      } catch (KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } catch (InterruptedException e) {
        ParWork.propegateInterrupt(e);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
      try {
        zkClient.mkdirs("/cluster/cluster_lock");
      } catch (KeeperException.NodeExistsException e) {
        // okay
      } catch (KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
      boolean createdClusterNodes = false;
      try {
        DistributedLock lock = new DistributedLock(zkClient, "/cluster/cluster_lock", zkClient.getZkACLProvider().getACLsToAdd("/cluster/cluster_lock"));
        if (log.isDebugEnabled()) log.debug("get cluster lock");
        while (!lock.lock()) {
          Thread.sleep(250);
        }
        try {

          if (log.isDebugEnabled()) log.debug("got cluster lock");
          CountDownLatch latch = new CountDownLatch(1);
          zkClient.getSolrZooKeeper().sync("/cluster/init", (rc, path, ctx) -> {
            latch.countDown();
          }, new Object());
          boolean success = latch.await(10, TimeUnit.SECONDS);
          if (!success) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "Timeout calling sync on collection zknode");
          }
          if (!zkClient.exists("/cluster/init")) {
            try {
              createClusterZkNodes(zkClient);
            } catch (Exception e) {
              ParWork.propegateInterrupt(e);
              log.error("Failed creating initial zk layout", e);
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            }
            createdClusterNodes = true;
          } else {
            if (log.isDebugEnabled()) log.debug("Cluster zk nodes already exist");
            int currentLiveNodes = zkClient.getChildren(ZkStateReader.LIVE_NODES_ZKNODE, null, true).size();
            if (log.isDebugEnabled()) log.debug("Current live nodes {}", currentLiveNodes);
//          if (currentLiveNodes == 0) {
//            log.info("Delete Overseer queues");
//            // cluster is in a startup state, clear zk queues
//            List<String> pathsToDelete = Arrays.asList(new String[]{Overseer.OVERSEER_QUEUE, Overseer.OVERSEER_QUEUE_WORK,
//                    Overseer.OVERSEER_COLLECTION_QUEUE_WORK, Overseer.OVERSEER_COLLECTION_MAP_RUNNING,
//                    Overseer.OVERSEER_COLLECTION_MAP_COMPLETED, Overseer.OVERSEER_COLLECTION_MAP_FAILURE, Overseer.OVERSEER_ASYNC_IDS});
//            CountDownLatch latch = new CountDownLatch(pathsToDelete.size());
//            int[] code = new int[1];
//            String[] path = new String[1];
//            boolean[] failed = new boolean[1];
//
//            for (String delPath : pathsToDelete) {
//              zkClient.getSolrZooKeeper().delete(delPath, -1,
//                      (resultCode, zkpath, context) -> {
//                        code[0] = resultCode;
//                        if (resultCode != 0) {
//                          failed[0] = true;
//                          path[0] = "" + zkpath;
//                        }
//
//                        latch.countDown();
//                      }, "");
//            }
//            boolean success = false;
//            log.info("Wait for delete Overseer queues");
//            try {
//              success = latch.await(15, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//              ParWork.propegateInterrupt(e);
//
//              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//            }
//
//            // nocommit, still haackey, do fails right
//            if (code[0] != 0) {
//              System.out.println("fail code: "+ code[0]);
//              KeeperException e = KeeperException.create(KeeperException.Code.get(code[0]), path[0]);
//              if (e instanceof  NoNodeException) {
//                // okay
//              } else {
//                throw e;
//              }
//
//            }
//
//            if (!success) {
//              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Timeout waiting for operations to complete");
//            }
//          }
          }

        } finally {
          if (log.isDebugEnabled()) log.debug("release cluster lock");
          lock.unlock();
        }
        if (!createdClusterNodes) {
          // wait?
        }

        zkStateReader = new ZkStateReader(zkClient, () -> {
          if (cc != null) cc.securityNodeChanged();
        });
        this.baseURL = zkStateReader.getBaseUrlForNodeName(this.nodeName);

        zkStateReader.createClusterStateWatchersAndUpdate();

        this.overseer = new Overseer((HttpShardHandler) ((HttpShardHandlerFactory) cc.getShardHandlerFactory()).getShardHandler(cc.getUpdateShardHandler().getTheSharedHttpClient()), cc.getUpdateShardHandler(),
                CommonParams.CORES_HANDLER_PATH, zkStateReader, this, cloudConfig);
        this.overseerRunningMap = Overseer.getRunningMap(zkClient);
        this.overseerCompletedMap = Overseer.getCompletedMap(zkClient);
        this.overseerFailureMap = Overseer.getFailureMap(zkClient);
        this.asyncIdsMap = Overseer.getAsyncIdsMap(zkClient);
        this.overseerJobQueue = overseer.getStateUpdateQueue();
        this.overseerCollectionQueue = overseer.getCollectionQueue(zkClient);
        this.overseerConfigSetQueue = overseer.getConfigSetQueue(zkClient);
        this.sysPropsCacher = new NodesSysPropsCacher(getSolrCloudManager().getNodeStateProvider(),
                getNodeName(), zkStateReader);

        try (ParWork worker = new ParWork((this))) {
          // start the overseer first as following code may need it's processing
          worker.collect("startOverseer", () -> {
            LeaderElector overseerElector = new LeaderElector(zkClient, new ContextKey("overseer", "overseer"), electionContexts);
            ElectionContext context = new OverseerElectionContext(getNodeName(), zkClient, overseer);
            ElectionContext prevContext = electionContexts.put(new ContextKey("overseer", "overser"), context);
            if (prevContext != null) {
              prevContext.close();
            }
            overseerElector.setup(context);
            try {
              overseerElector.joinElection(context, false);
            } catch (KeeperException e) {
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            } catch (InterruptedException e) {
              ParWork.propegateInterrupt(e);
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            } catch (IOException e) {
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            }
          });

          worker.collect("registerLiveNodesListener", () -> {
            registerLiveNodesListener();
          });
          worker.collect("publishDownState", () -> {
            try {
              Stat stat = zkClient.exists(ZkStateReader.LIVE_NODES_ZKNODE, null);
              if (stat != null && stat.getNumChildren() > 0) {
                publishAndWaitForDownStates();
              }
            } catch (InterruptedException e) {
              ParWork.propegateInterrupt(e);
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            } catch (KeeperException e) {
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            }
          });
        }
        // Do this last to signal we're up.
        createEphemeralLiveNode();

        //  publishAndWaitForDownStates();
      } catch (InterruptedException e) {
        ParWork.propegateInterrupt(e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
      } catch (KeeperException e) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
      }
    }
  }

  private void registerLiveNodesListener() {
    log.info("register live nodes listener");
    // this listener is used for generating nodeLost events, so we check only if
    // some nodes went missing compared to last state
    LiveNodesListener listener = new LiveNodesListener() {
      @Override
      public boolean onChange(SortedSet<String> oldNodes, SortedSet<String> newNodes) {
        {
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
            ParWork.propegateInterrupt(e1);
            log.warn("Unable to read autoscaling.json", e1);
          }
          if (createNodes) {
            byte[] json = Utils.toJSON(Collections.singletonMap("timestamp", getSolrCloudManager().getTimeSource().getEpochTimeNs()));
            for (String n : oldNodes) {
              String path = ZkStateReader.SOLR_AUTOSCALING_NODE_LOST_PATH + "/" + n;

              try {
                zkClient.create(path, json, CreateMode.PERSISTENT, true);
              } catch (KeeperException.NodeExistsException e) {
                // someone else already created this node - ignore
              } catch (KeeperException | InterruptedException e1) {
                ParWork.propegateInterrupt(e1);
                log.warn("Unable to register nodeLost path for {}", n, e1);
              }
            }
          }
          return false;
        }
      }
    };
    zkStateReader.registerLiveNodesListener(listener);
  }

  private synchronized void shutdown() {
    if (this.shudownCalled) return;
    this.shudownCalled = true;
    Thread updaterThead = overseer.getUpdaterThread();
    log.info("Cluster shutdown initiated");
    if (updaterThead != null && updaterThead.isAlive()) {
      log.info("We are the Overseer, wait for others to shutdown");

      CountDownLatch latch = new CountDownLatch(1);
      List<String> children = null;
      try {
        children = zkClient.getChildren("/solr" + ZkStateReader.LIVE_NODES_ZKNODE, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (Event.EventType.None.equals(event.getType())) {
              return;
            }
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
              Thread updaterThead = overseer.getUpdaterThread();
              if (updaterThead == null || !updaterThead.isAlive()) {
                log.info("We were the Overseer, but it seems not anymore, shutting down");
                latch.countDown();
                return;
              }

              try {
                List<String> children = zkClient.getChildren("/solr" + ZkStateReader.LIVE_NODES_ZKNODE, this, false);
                if (children.size() == 1) {
                  latch.countDown();
                }
              } catch (KeeperException e) {
                log.error("Exception on proper shutdown", e);
                return;
              } catch (InterruptedException e) {
                ParWork.propegateInterrupt(e);
                return;
              }
            }
          }
        }, false);
      } catch (KeeperException e) {
        log.error("Time out waiting to see solr live nodes go down " + children.size());
        return;
      } catch (InterruptedException e) {
        ParWork.propegateInterrupt(e);
        return;
      }

      if (children.size() > 1) {
        boolean success = false;
        try {
          success = latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          ParWork.propegateInterrupt(e);
          return;
        }
        if (!success) {
          log.error("Time out waiting to see solr live nodes go down " + children.size());
        }
      }
    }

    URL url = null;
    try {
      url = new URL(getHostName() + ":" + getHostPort() + "/shutdown?token=" + "solrrocks");
    } catch (MalformedURLException e) {
      SolrException.log(log, e);
      return;
    }
    try {
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.getResponseCode();
      log.info("Shutting down " + url + ": " + connection.getResponseCode() + " " + connection.getResponseMessage());
    } catch (SocketException e) {
      SolrException.log(log, e);
      // Okay - the server is not running
    } catch (IOException e) {
      SolrException.log(log, e);
      return;
    }
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
    return true;
//    if (!SolrZkClient.containsChroot(zkHost)) {
//      return true;
//    }
//    log.trace("zkHost includes chroot");
//    String chrootPath = zkHost.substring(zkHost.indexOf("/"), zkHost.length());
//
//    SolrZkClient tmpClient = new SolrZkClient(zkHost.substring(0,
//        zkHost.indexOf("/")), 60000, 30000, null, null, null);
//    boolean exists = tmpClient.exists(chrootPath);
//    if (!exists && create) {
//      tmpClient.makePath(chrootPath, false, true);
//      exists = true;
//    }
//    tmpClient.close();
//    return exists;
  }

  public boolean isConnected() {
    return zkClient.isConnected();
  }

  private void createEphemeralLiveNode() {
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
    String nodeAddedPath =
        ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + nodeName;
    log.info("Register node as live in ZooKeeper:" + nodePath);

    log.info("Create our ephemeral live node");

    createLiveNodeImpl(nodePath, nodeAddedPath);
  }

  private void createLiveNodeImpl(String nodePath, String nodeAddedPath) {
    Map<String, byte[]> dataMap = new HashMap<>(2);
    Map<String, CreateMode> createModeMap = new HashMap<>(2);
    dataMap.put(nodePath, null);
    createModeMap.put(nodePath, CreateMode.EPHEMERAL);
    try {
      // if there are nodeAdded triggers don't create nodeAdded markers
      boolean createMarkerNode = zkStateReader.getAutoScalingConfig().hasTriggerForEvents(TriggerEventType.NODEADDED);

      // TODO, do this optimistically
//      if (createMarkerNode && !zkClient.exists(nodeAddedPath, true)) {
//        // use EPHEMERAL so that it disappears if this node goes down
//        // and no other action is taken
//        byte[] json = Utils.toJSON(Collections.singletonMap("timestamp", TimeSource.NANO_TIME.getEpochTimeNs()));
//        dataMap.put(nodeAddedPath, json);
//        createModeMap.put(nodePath, CreateMode.EPHEMERAL);
//      }

      //   zkClient.mkDirs(dataMap, createModeMap);

      try {
        zkClient.getSolrZooKeeper().create(nodePath, null, zkClient.getZkACLProvider().getACLsToAdd(nodePath), CreateMode.EPHEMERAL);
      } catch (KeeperException.NodeExistsException e) {
        log.warn("Found our ephemeral live node already exists. This must be a quick restart after a hard shutdown, waiting for it to expire {}", nodePath);
        // TODO nocommit wait for expiration properly and try again?
        Thread.sleep(15000);
        zkClient.getSolrZooKeeper().create(nodePath, null, zkClient.getZkACLProvider().getACLsToAdd(nodePath), CreateMode.EPHEMERAL);
      }
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  public void removeEphemeralLiveNode() throws KeeperException, InterruptedException {
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
    String nodeAddedPath = ZkStateReader.SOLR_AUTOSCALING_NODE_ADDED_PATH + "/" + nodeName;

    try {
      zkClient.delete(nodePath, -1);
    } catch (NoNodeException e) {
      // okay
    }
    try {
      zkClient.delete(nodeAddedPath, -1);
    } catch (NoNodeException e) {
      // okay
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
    return zkClient.exists(path);
  }

  public void registerUnloadWatcher(String collection, String shardId, String coreNodeName, String name) {
    zkStateReader.registerDocCollectionWatcher(collection,
        new UnloadCoreOnDeletedWatcher(coreNodeName, shardId, name));
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
    MDCLoggingContext.setCoreDescriptor(cc, desc);
    try {
      if (isClosed()) {
        throw new AlreadyClosedException();
      }
      // pre register has published our down state
      final String baseUrl = getBaseUrl();
      final CloudDescriptor cloudDesc = desc.getCloudDescriptor();
      final String collection = cloudDesc.getCollectionName();
      final String shardId = cloudDesc.getShardId();
      final String coreZkNodeName = cloudDesc.getCoreNodeName();
      assert coreZkNodeName != null : "we should have a coreNodeName by now";
      log.info("Register SolrCore, baseUrl={} collection={}, shard={} coreNodeName={}", baseUrl, collection, shardId, coreZkNodeName);
      // check replica's existence in clusterstate first
      try {
        zkStateReader.waitForState(collection, Overseer.isLegacy(zkStateReader) ? 10000 : 10000,
            TimeUnit.MILLISECONDS, (collectionState) -> getReplicaOrNull(collectionState, shardId, coreZkNodeName) != null);
      } catch (TimeoutException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error registering SolrCore, timeout waiting for replica present in clusterstate");
      }
      Replica replica = getReplicaOrNull(zkStateReader.getClusterState().getCollectionOrNull(collection), shardId, coreZkNodeName);
      if (replica == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error registering SolrCore, replica is removed from clusterstate");
      }


      if (replica.getType() != Type.PULL) {
        getCollectionTerms(collection).register(cloudDesc.getShardId(), coreZkNodeName);
      }

      ZkShardTerms shardTerms = getShardTerms(collection, cloudDesc.getShardId());

      log.info("Register replica - core:{} address:{} collection:{} shard:{}",
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
        ParWork.propegateInterrupt(e);
        return null;
      } catch (KeeperException | IOException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      }


      // don't wait if we have closed
      if (cc.isShutDown()) {
        throw new AlreadyClosedException();
      }

      getZkStateReader().waitForState(collection, 10, TimeUnit.SECONDS, (n,c) -> c != null && c.getLeader(shardId) != null && c.getLeader(shardId).getState().equals(
          Replica.State.ACTIVE));

      //  there should be no stale leader state at this point, dont hit zk directly
      String leaderUrl = zkStateReader.getLeaderUrl(collection, shardId, 5000);

      String ourUrl = ZkCoreNodeProps.getCoreUrl(baseUrl, coreName);
      log.debug("We are {} and leader is {}", ourUrl, leaderUrl);
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
              log.info("Replaying tlog for {} during startup... NOTE: This can take a while.", ourUrl);
              recoveryFuture.get(); // NOTE: this could potentially block for
              // minutes or more!
              // TODO: public as recovering in the mean time?
              // TODO: in the future we could do peersync in parallel with recoverFromLog
            } else {
              if (log.isDebugEnabled()) {
                log.debug("No LogReplay needed for core={} baseURL={}", core.getName(), baseUrl);
              }
            }
          }
        }
        boolean didRecovery
            = checkRecovery(recoverReloadedCores, isLeader, skipRecovery, collection, coreZkNodeName, shardId, core, cc, afterExpiration);
        if (isClosed()) {
          throw new AlreadyClosedException();
        }
        if (!didRecovery) {
          if (isTlogReplicaAndNotLeader) {
            startReplicationFromLeader(coreName, true);
          }
          if (!isLeader) {
            publish(desc, Replica.State.ACTIVE, true, false);
          }
        }

        if (replica.getType() != Type.PULL) {
          // the watcher is added to a set so multiple calls of this method will left only one watcher
          shardTerms.addListener(new RecoveringCoreTermWatcher(core.getCoreDescriptor(), getCoreContainer()));
        }
        core.getCoreDescriptor().getCloudDescriptor().setHasRegistered(true);
      } catch (Exception e) {
        SolrZkClient.checkInterrupted(e);
        unregister(coreName, desc, false);
        throw e;
      }

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
    if (isClosed()) throw new AlreadyClosedException();
    log.info("{} starting background replication from leader", coreName);

    stopReplicationFromLeader(coreName);

    ReplicateFromLeader replicateFromLeader = new ReplicateFromLeader(cc, coreName);

    ReplicateFromLeader prev = replicateFromLeaders.putIfAbsent(coreName, replicateFromLeader);
    if (prev == null) {
      replicateFromLeader.startReplication(switchTransactionLog);
    } else {
      log.warn("A replicate from leader instance already exists for core {}", coreName);
      try {
        prev.close();
      } catch (Exception e) {
        ParWork.propegateInterrupt("Error closing previous replication attempt", e);
      }
      if (isClosed()) throw new AlreadyClosedException();
      replicateFromLeader.startReplication(switchTransactionLog);
    }

  }

  public void stopReplicationFromLeader(String coreName) {
    log.info("{} stopping background replication from leader", coreName);
    ReplicateFromLeader replicateFromLeader = replicateFromLeaders.remove(coreName);
    if (replicateFromLeader != null) {
      synchronized (replicateFromLeader) {
        ParWork.close(replicateFromLeader);
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

      zkStateReader.waitForState(collection, timeoutms * 2, TimeUnit.MILLISECONDS, (n, c) -> checkLeaderUrl(cloudDesc, leaderUrl, collection, shardId, leaderConflictResolveWait));

    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error getting leader from zk", e);
    }
    return leaderUrl;
  }

  private boolean checkLeaderUrl(CloudDescriptor cloudDesc, String leaderUrl, String collection, String shardId,
                                 int timeoutms) {
    // now wait until our currently cloud state contains the latest leader
    String clusterStateLeaderUrl;
    try {
      clusterStateLeaderUrl = zkStateReader.getLeaderUrl(collection, shardId, 10000);

      // leaderUrl = getLeaderProps(collection, cloudDesc.getShardId(), timeoutms).getCoreUrl();
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    return clusterStateLeaderUrl != null;
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
    TimeOut timeout = new TimeOut(timeoutms, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    Exception exp = null;
    while (!timeout.hasTimedOut()) {
      try {
        getZkStateReader().waitForState(collection, 10, TimeUnit.SECONDS, (n,c) -> c != null && c.getLeader(slice) != null);

        byte[] data = zkClient.getData(ZkStateReader.getShardLeadersPath(collection, slice), null, null);
        ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(ZkNodeProps.load(data));
        return leaderProps;

      } catch (Exception e) {
        SolrZkClient.checkInterrupted(e);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    return null;
  }


  private void joinElection(CoreDescriptor cd, boolean afterExpiration, boolean joinAtHead)
      throws InterruptedException, KeeperException, IOException {
    if (this.isClosed || cc.isShutDown()) {
      log.warn("cannot join election, closed");
      return;
    }
    // look for old context - if we find it, cancel it
    String collection = cd.getCloudDescriptor().getCollectionName();
    final String coreNodeName = cd.getCloudDescriptor().getCoreNodeName();

    ContextKey contextKey = new ContextKey(collection, coreNodeName);

    ElectionContext prevContext = electionContexts.get(contextKey);

    if (prevContext != null) {
      prevContext.close();
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

    if (this.isClosed || cc.isShutDown()) {
      context.close();
      return;
    }
    prevContext = electionContexts.put(contextKey, context);
    if (prevContext != null) {
      prevContext.close();
    }

    leaderElector.setup(context);

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
        if (log.isInfoEnabled()) {
          log.info("Core needs to recover:{}", core.getName());
        }
        core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
        return true;
      }

      ZkShardTerms zkShardTerms = getShardTerms(collection, shardId);
      if (zkShardTerms.registered(coreZkNodeName) && !zkShardTerms.canBecomeLeader(coreZkNodeName)) {
        if (log.isInfoEnabled()) {
          log.info("Leader's term larger than core {}; starting recovery process", core.getName());
        }
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
      }
    }
    MDCLoggingContext.setCoreDescriptor(cc, cd);

    if ((state == Replica.State.ACTIVE || state == Replica.State.RECOVERING ) && isClosed()) {
      throw new AlreadyClosedException();
    }

    try {
      String collection = cd.getCloudDescriptor().getCollectionName();

      log.info("publishing state={}", state);
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
        if (log.isInfoEnabled()) {
          log.info("The core '{}' had failed to initialize before.", cd.getName());
        }
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

    zkStateReader.unregisterCore(collection);

    synchronized (collectionToTerms) {
      ZkCollectionTerms ct = collectionToTerms.get(collection);
      if (ct != null) {
        ct.close();
        ct.remove(cd.getCloudDescriptor().getShardId(), cd);
      }
    }
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
        context.close();
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
      zkStateReader.waitForState(cloudDescriptor.getCollectionName(), 10, TimeUnit.SECONDS, (l,c) -> {
        if (c == null) return true;
        Slice slice = c.getSlice(cloudDescriptor.getShardId());
        if (slice == null) return true;
        Replica r = slice.getReplica(cloudDescriptor.getCoreNodeName());
        if (r == null) return true;
        return false;
      });
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

  private void waitForCoreNodeName(CoreDescriptor cd) {
    if (log.isDebugEnabled()) log.debug("look for our core node name");

    AtomicReference<String> errorMessage = new AtomicReference<>();
    try {
      zkStateReader.waitForState(cd.getCollectionName(), 10, TimeUnit.SECONDS, (n, c) -> { // TODO: central timeout control
        if (c == null)
          return false;
        final Map<String,Slice> slicesMap = c.getSlicesMap();
        if (slicesMap == null) {
          return false;
        }
        for (Slice slice : slicesMap.values()) {
          for (Replica replica : slice.getReplicas()) {

            String nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);

            String msgNodeName = getNodeName();
            String msgCore = cd.getName();

            if (msgNodeName.equals(nodeName) && core.equals(msgCore)) {
              cd.getCloudDescriptor()
                      .setCoreNodeName(replica.getName());
              return true;
            }
          }
        }
        return false;
      });
    } catch (TimeoutException | InterruptedException e) {
      String error = errorMessage.get();
      if (error == null)
        error = "";
      throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, "Could not get shard id for core: " + cd.getName() + " " + error);
    }
  }

  private void waitForShardId(CoreDescriptor cd) {
    if (log.isDebugEnabled()) {
      log.debug("waitForShardId(CoreDescriptor cd={}) - start", cd);
    }

    AtomicReference<String> returnId = new AtomicReference<>();
    try {
      try {
        zkStateReader.waitForState(cd.getCollectionName(), 5, TimeUnit.SECONDS, (n, c) -> { // nocommit
          if (c == null) return false;
          String shardId = c.getShardId(cd.getCloudDescriptor().getCoreNodeName());
          if (shardId != null) {
            returnId.set(shardId);
            return true;
          }
          return false;
        });
      } catch (InterruptedException e) {
        ParWork.propegateInterrupt(e);
        throw new SolrException(ErrorCode.SERVER_ERROR, "Could not get shard id for core: " + cd.getName());
      }
    } catch (TimeoutException e1) {
      log.error("waitForShardId(CoreDescriptor=" + cd + ")", e1);

      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not get shard id for core: " + cd.getName());
    }

    final String shardId = returnId.get();
    if (shardId != null) {
      cd.getCloudDescriptor().setShardId(shardId);

      if (log.isDebugEnabled()) {
        log.debug("waitForShardId(CoreDescriptor) - end coreNodeName=" + cd.getCloudDescriptor().getCoreNodeName() + " shardId=" + shardId);
      }
      return;
    }

    throw new SolrException(ErrorCode.SERVER_ERROR, "Could not get shard id for core: " + cd.getName());
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
    log.info("PreRegister SolrCore, collection={}, shard={} coreNodeName={}", cd.getCloudDescriptor().getCollectionName(), cd.getCloudDescriptor().getShardId());

    CloudDescriptor cloudDesc = cd.getCloudDescriptor();

    String coreNodeName = getCoreNodeName(cd);

    // before becoming available, make sure we are not live and active
    // this also gets us our assigned shard id if it was not specified
    try {
      checkStateInZk(cd);

      // make sure the node name is set on the descriptor
      if (cloudDesc.getCoreNodeName() == null) {
        cloudDesc.setCoreNodeName(coreNodeName);
      }
      log.info("PreRegister found coreNodename of {}", coreNodeName);

      // publishState == false on startup
      if (isPublishAsDownOnStartup(cloudDesc)) {
        publish(cd, Replica.State.DOWN, false, true);
      }
      String collectionName = cd.getCloudDescriptor().getCollectionName();
      DocCollection collection = zkStateReader.getClusterState().getCollectionOrNull(collectionName);
      if (log.isDebugEnabled()) {
        log.debug(collection == null ?
                "Collection {} not visible yet, but flagging it so a watch is registered when it becomes visible" :
                "Registering watch for collection {}",
            collectionName);
      }
    } catch (KeeperException e) {
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (InterruptedException e) {
      ParWork.propegateInterrupt(e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (NotInClusterStateException e) {
      // make the stack trace less verbose
      throw e;
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
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
      StringBuilder sb = new StringBuilder(256);
      AtomicReference<String> errorMessage = new AtomicReference<>();
      AtomicReference<DocCollection> collectionState = new AtomicReference<>();
      try {
        zkStateReader.waitForState(cd.getCollectionName(), WAIT_FOR_STATE, TimeUnit.SECONDS, (l, c) -> {
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
            sb.setLength(0);
            slice.getReplicas().stream().forEach(replica1 -> sb.append(replica1.getName() + " "));
            errorMessage.set("coreNodeName " + coreNodeName + " does not exist in shard " + cloudDesc.getShardId() +
                ", ignore the exception if the replica was deleted. Found: " + sb.toString());
            return false;
          }
          return true;
        });
      } catch (TimeoutException e) {
        String error = errorMessage.get();
        if (error == null)
          error = "coreNodeName " + coreNodeName + " does not exist in shard " + cloudDesc.getShardId() +
              ", ignore the exception if the replica was deleted" ;

        throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, error + "\n" + collectionState.get());
      }
    }
  }

  public static void linkConfSet(SolrZkClient zkClient, String collection, String confSetName) throws KeeperException, InterruptedException {
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    log.debug("Load collection config from:{}", path);
    byte[] data;
    try {
      data = zkClient.getData(path, null, null);
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
  public static void bootstrapConf(SolrZkClient zkClient, CoreContainer cc) throws IOException, KeeperException {

    ZkConfigManager configManager = new ZkConfigManager(zkClient);

    //List<String> allCoreNames = cfg.getAllCoreNames();
    List<CoreDescriptor> cds = cc.getCoresLocator().discover(cc);

    if (log.isInfoEnabled()) {
      log.info("bootstrapping config for {} cores into ZooKeeper using solr.xml from {}", cds.size(), cc.getSolrHome());
    }

    for (CoreDescriptor cd : cds) {
      String coreName = cd.getName();
      String confName = cd.getCollectionName();
      if (StringUtils.isEmpty(confName))
        confName = coreName;
      Path udir = cd.getInstanceDir().resolve("conf");
      log.info("Uploading directory {} with name {} for solrCore {}", udir, confName, coreName);
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
      return asyncIdsMap.putIfAbsent(asyncId, EMPTY_BYTE_ARRAY);
    } catch (InterruptedException e) {
      ParWork.propegateInterrupt(e);
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
      ParWork.propegateInterrupt(e);
      throw new RuntimeException(e);
    }
  }

  public int getClientTimeout() {
    return clientTimeout;
  }

  public Overseer getOverseer() {
    return overseer;
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
  public static String generateNodeName(final String hostName,
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
              zkClient.delete(Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE + "/" + electionNode, -1);
            } catch (NoNodeException e) {
              //no problem
            } catch (InterruptedException e) {
              ParWork.propegateInterrupt(e);
              return;
            } catch (Exception e) {
              log.warn("Old election node exists , could not be removed ", e);
            }
          }
        } else { // We're in the right place, now attempt to rejoin
          overseerElector.retryElection(new OverseerElectionContext(getNodeName(),
              zkClient, overseer), joinAtHead);
          return;
        }
      } else {
        overseerElector.retryElection(new OverseerElectionContext(getNodeName(),
               zkClient, overseer), joinAtHead);
      }
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to rejoin election", e);
    }

  }

  public void rejoinShardLeaderElection(SolrParams params) {

    String collectionName = params.get(COLLECTION_PROP);
    String shardId = params.get(SHARD_ID_PROP);
    String coreNodeName = params.get(CORE_NODE_NAME_PROP);
    String coreName = params.get(CORE_NAME_PROP);
    String electionNode = params.get(ELECTION_NODE_PROP);
    String baseUrl = params.get(BASE_URL_PROP);

    try {
      MDCLoggingContext.setCoreDescriptor(cc, cc.getCoreDescriptor(coreName));

      log.info("Rejoin the shard leader election.");

      ContextKey contextKey = new ContextKey(collectionName, coreNodeName);

      ElectionContext prevContext = electionContexts.get(contextKey);
      if (prevContext != null) prevContext.close();

      ZkNodeProps zkProps = new ZkNodeProps(BASE_URL_PROP, baseUrl, CORE_NAME_PROP, coreName, NODE_NAME_PROP, getNodeName(), CORE_NODE_NAME_PROP, coreNodeName);

      LeaderElector elect = ((ShardLeaderElectionContext) prevContext).getLeaderElector();
      ShardLeaderElectionContext context = new ShardLeaderElectionContext(elect, shardId, collectionName,
          coreNodeName, zkProps, this, getCoreContainer());

      context.leaderSeqPath = context.electionPath + LeaderElector.ELECTION_NODE + "/" + electionNode;
      elect.setup(context);
      prevContext = electionContexts.put(contextKey, context);
      if (prevContext != null) prevContext.close();
      elect.retryElection(context, params.getBool(REJOIN_AT_HEAD_PROP, false));
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to rejoin election", e);
    } finally {
      MDCLoggingContext.clear();
    }
  }

  public void checkOverseerDesignate() {
    try {
      byte[] data = zkClient.getData(ZkStateReader.ROLES, null, new Stat());
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
      ParWork.propegateInterrupt(e);
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
       reconnectListeners.add(listener);
       if (log.isDebugEnabled()) log.debug("Added new OnReconnect listener {}", listener);
    }
  }

  /**
   * Removed a previously registered OnReconnect listener, such as when a core is removed or reloaded.
   */
  public void removeOnReconnectListener(OnReconnect listener) {
    if (listener != null) {
      boolean wasRemoved = reconnectListeners.remove(listener);

      if (wasRemoved) {
        if (log.isDebugEnabled()) log.debug("Removed OnReconnect listener {}", listener);
      } else {
        log.warn("Was asked to remove OnReconnect listener {}, but remove operation " +
                "did not find it in the list of registered listeners."
            , listener);
      }
    }
  }

  Set<OnReconnect> getCurrentOnReconnectListeners() {
    return Collections.unmodifiableSet(reconnectListeners);
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
              Stat stat = zkClient.exists(resourceLocation, null);
              if (log.isDebugEnabled()) {
                log.debug("failed to set data version in zk is {} and expected version is {} ", stat.getVersion(), znodeVersion);
              }
            } catch (Exception e1) {
              ParWork.propegateInterrupt(e1);
              log.warn("could not get stat");
            }

            if (log.isInfoEnabled()) {
              log.info(StrUtils.formatString(errMsg, resourceLocation, znodeVersion));
            }
            throw new ResourceModifiedInZkException(ErrorCode.CONFLICT, StrUtils.formatString(errMsg, resourceLocation, znodeVersion) + ", retry.");
          }
        }
      }

    } catch (KeeperException.BadVersionException bve) {
      int v = -1;
      try {
        Stat stat = zkClient.exists(resourceLocation, null);
        v = stat.getVersion();
      } catch (Exception e) {
        ParWork.propegateInterrupt(e);
        log.error(e.getMessage());

      }
      if (log.isInfoEnabled()) {
        log.info(StrUtils.formatString("%s zkVersion= %d %s %d", errMsg, resourceLocation, znodeVersion));
      }
      throw new ResourceModifiedInZkException(ErrorCode.CONFLICT, StrUtils.formatString(errMsg, resourceLocation, znodeVersion) + ", retry.");
    } catch (ResourceModifiedInZkException e) {
      throw e;
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
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
      ParWork.propegateInterrupt(e);
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
        log.warn("{} has no more registered listeners, but a live one attempted to unregister!", confDir);
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
        stat = zkClient.exists(zkDir, null);
      } catch (KeeperException e) {
        //ignore , it is not a big deal
      } catch (InterruptedException e) {
        ParWork.propegateInterrupt(e);
        return;
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
    }
    final Set<Runnable> listeners = confDirectoryListeners.get(zkDir);
    if (listeners != null) {

      // run these in a separate thread because this can be long running

      try (ParWork worker = new ParWork(this, true)) {
        listeners
            .forEach((it) -> worker.collect("confDirectoryListener", () -> {
              it.run();
            }));
      }
    }
    return true;
  }

  private void setConfWatcher(String zkDir, Watcher watcher, Stat stat) {
    try {
      Stat newStat = zkClient.exists(zkDir, watcher);
      if (stat != null && newStat.getVersion() > stat.getVersion()) {
        //a race condition where a we missed an event fired
        //so fire the event listeners
        fireEventListeners(zkDir);
      }
    } catch (KeeperException e) {
      log.error("failed to set watcher for conf dir {} ", zkDir);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("failed to set watcher for conf dir {} ", zkDir);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
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
  public class UnloadCoreOnDeletedWatcher implements DocCollectionWatcher {
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
      if (isClosed()) { // don't accidentally delete cores on shutdown due to unreliable state
        return true;
      }

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
          ParWork.propegateInterrupt(e);
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
  public void publishNodeAsDown(String nodeName) throws KeeperException {
    log.info("Publish node={} as DOWN", nodeName);

    if (overseer == null) {
      log.warn("Could not publish node as down, no overseer was started yet");
      return;
    }

    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, OverseerAction.DOWNNODE.toLower(),
        ZkStateReader.NODE_NAME_PROP, nodeName);
    try {
      overseer.getStateUpdateQueue().offer(Utils.toJSON(m));
    } catch (AlreadyClosedException e) {
      log.info("Not publishing node as DOWN because a resource required to do so is already closed.");
      return;
    } catch (InterruptedException e) {
      ParWork.propegateInterrupt(e);
      return;
    }
//    Collection<SolrCore> cores = cc.getCores();
//    for (SolrCore core : cores) {
//      CoreDescriptor desc = core.getCoreDescriptor();
//      String collection = desc.getCollectionName();
//      try {
//        zkStateReader.waitForState(collection, 3, TimeUnit.SECONDS, (n,c) -> {
//          if (c != null) {
//            List<Replica> replicas = c.getReplicas();
//            for (Replica replica : replicas) {
//              if (replica.getNodeName().equals(getNodeName())) {
//                if (!replica.getState().equals(Replica.State.DOWN)) {
//                  log.info("Found state {} {}", replica.getState(), replica.getNodeName());
//                  return false;
//                }
//              }
//            }
//          }
//          return true;
//        });
//      } catch (InterruptedException e) {
//        ParWork.propegateInterrupt(e);
//        return;
//      } catch (TimeoutException e) {
//        log.error("Timeout", e);
//      }
//    }
  }

  /**
   * Ensures that a searcher is registered for the given core and if not, waits until one is registered
   */
  private static void ensureRegisteredSearcher(SolrCore core) throws InterruptedException {
    if (core.isClosed() || core.getCoreContainer().isShutDown()) {
      return;
    }
    if (!core.getSolrConfig().useColdSearcher) {
      RefCounted<SolrIndexSearcher> registeredSearcher = core.getRegisteredSearcher();
      if (registeredSearcher != null) {
        if (log.isDebugEnabled()) {
          log.debug("Found a registered searcher: {} for core: {}", registeredSearcher.get(), core);
        }
        registeredSearcher.decref();
      } else  {
        Future[] waitSearcher = new Future[1];
        if (log.isInfoEnabled()) {
          log.info("No registered searcher found for core: {}, waiting until a searcher is registered before publishing as active", core.getName());
        }
        final RTimer timer = new RTimer();
        RefCounted<SolrIndexSearcher> searcher = null;
        try {
          searcher = core.getSearcher(false, true, waitSearcher, true);
          boolean success = true;
          if (waitSearcher[0] != null)  {
            if (log.isDebugEnabled()) {
              log.debug("Waiting for first searcher of core {}, id: {} to be registered", core.getName(), core);
            }
            try {
              waitSearcher[0].get();
            } catch (ExecutionException e) {
              log.warn("Wait for a searcher to be registered for core {}, id: {} failed due to: {}", core.getName(), core, e, e);
              success = false;
            }
          }
          if (success)  {
            if (searcher == null) {
              // should never happen
              if (log.isDebugEnabled()) {
                log.debug("Did not find a searcher even after the future callback for core: {}, id: {}!!!", core.getName(), core);
              }
            } else  {
              if (log.isInfoEnabled()) {
                log.info("Found a registered searcher: {}, took: {} ms for core: {}, id: {}", searcher.get(), timer.getTime(), core.getName(), core);
              }
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
        if (log.isDebugEnabled()) {
          log.debug("Found newest searcher: {} for core: {}, id: {}", newestSearcher.get(), core.getName(), core);
        }
        newestSearcher.decref();
      }
    }
  }
}
