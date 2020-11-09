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
import java.util.Arrays;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.cloud.DistributedLock;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.cloud.Replica.Type;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.IOUtils;
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
import org.apache.solr.servlet.SolrLifcycleListener;
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
public class ZkController implements Closeable, Runnable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String CLUSTER_SHUTDOWN = "/cluster/shutdown";

  public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
  public final int WAIT_FOR_STATE = Integer.getInteger("solr.waitForState", 10);

  private final boolean SKIP_AUTO_RECOVERY = Boolean.getBoolean("solrcloud.skip.autorecovery");
  private final int zkClientConnectTimeout;
  private final Supplier<List<CoreDescriptor>> descriptorsSupplier;
  private final ZkACLProvider zkACLProvider;

  private CloseTracker closeTracker;
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
  private volatile boolean dcCalled;
  private volatile boolean started;

  @Override
  public void run() {
    disconnect();
  }

  public boolean isDcCalled() {
    return dcCalled;
  }

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

  private final Map<String, LeaderElector> leaderElectors = new ConcurrentHashMap<>(16);

  private final Map<ContextKey, ElectionContext> electionContexts = new ConcurrentHashMap<>(16) {
    @Override
    public ElectionContext put(ContextKey key, ElectionContext value) {
      if (ZkController.this.isClosed || cc.isShutDown()) {
        throw new AlreadyClosedException();
      }
      return super.put(key, value);
    }
  };

  private final Map<ContextKey, ElectionContext> overseerContexts = new ConcurrentHashMap<>() {
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

  private final ReentrantLock collectionToTermsLock = new ReentrantLock(true);

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
    assert (closeTracker = new CloseTracker()) != null;
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


      this.leaderVoteWait = cloudConfig.getLeaderVoteWait();
      this.leaderConflictResolveWait = cloudConfig.getLeaderConflictResolveWait();

      this.clientTimeout = cloudConfig.getZkClientTimeout();

      String zkACLProviderClass = cloudConfig.getZkACLProviderClass();

      if (zkACLProviderClass != null && zkACLProviderClass.trim().length() > 0) {
        zkACLProvider = cc.getResourceLoader().newInstance(zkACLProviderClass, ZkACLProvider.class);
      } else {
        zkACLProvider = new DefaultZkACLProvider();
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Exception during ZkController init", e);
      throw e;
    }

    assert ObjectReleaseTracker.track(this);
  }

  public void closeLeaderContext(CoreDescriptor cd) {
    String collection = cd.getCloudDescriptor().getCollectionName();
    final String coreName = cd.getName();

    ContextKey contextKey = new ContextKey(collection, coreName);
    ElectionContext context = electionContexts.get(contextKey);
    if (context != null) {
      try {
        context.cancelElection();
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } catch (KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      } finally {
        context.close();
      }
    }
  }

  public void start() {
    if (started) throw new IllegalStateException("Already started");

    started = true;

    boolean isRegistered = SolrLifcycleListener.isRegistered(this);
    if (!isRegistered) {
      SolrLifcycleListener.registerShutdown(this);
    }

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

        try (ParWork worker = new ParWork("disconnected", true, true)) {
          worker.collect("OverseerElectionContexts", overseerContexts.values());

          worker.collect("Overseer", () -> {
            if (ZkController.this.overseerElector != null) {
              ZkController.this.overseerElector.getContext().close();
            }
          });
          worker.collect("", () -> {
            clearZkCollectionTerms();
          });
          worker.collect("electionContexts", electionContexts.values());
          worker.collect("",() -> {
            markAllAsNotLeader(descriptorsSupplier);
          });
          worker.collect("",() -> {
            cc.cancelCoreRecoveries(true, false);
          });
        }
      }
    });
    overseerContexts.clear();
    electionContexts.clear();
    zkClient.setAclProvider(zkACLProvider);
    zkClient.getConnectionManager().setOnReconnect(new OnReconnect() {

      @Override
      public void command() throws SessionExpiredException {
        synchronized (initLock) {
          if (cc.isShutDown() || isClosed() || shudownCalled || !zkClient.isConnected()) {
            log.info("skipping zk reconnect logic due to shutdown");
            return;
          }
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

            ElectionContext context = getOverseerContext();

            overseerElector.setup(context);
            overseerElector.joinElection(context, true);


            // we have to register as live first to pick up docs in the buffer
            createEphemeralLiveNode();

            List<CoreDescriptor> descriptors = descriptorsSupplier.get();
            // re register all descriptors
            try (ParWork parWork = new ParWork(this, true, true)) {
              if (descriptors != null) {
                for (CoreDescriptor descriptor : descriptors) {
                  // TODO: we need to think carefully about what happens when it
                  // was
                  // a leader that was expired - as well as what to do about
                  // leaders/overseers
                  // with connection loss
                  try {
                    // unload solrcores that have been 'failed over'
                    // throwErrorIfReplicaReplaced(descriptor);

                    parWork.collect(new RegisterCoreAsync(descriptor, true, true));

                  } catch (Exception e) {
                    SolrException.log(log, "Error registering SolrCore", e);
                  }
                }
              }
            }

            // notify any other objects that need to know when the session was re-connected

            try (ParWork parWork = new ParWork(this, true, true)) {
              // the OnReconnect operation can be expensive per listener, so do that async in the background
              reconnectListeners.forEach(listener -> {
                try {
                  parWork.collect(new OnReconnectNotifyAsync(listener));
                } catch (Exception exc) {
                  // not much we can do here other than warn in the log
                  log.warn("Error when notifying OnReconnect listener {} after session re-connected.", listener, exc);
                }
              });
            }
          } catch (InterruptedException e) {
            log.warn("interrupted", e);
          } catch (SessionExpiredException e) {
            log.warn("SessionExpiredException", e);
          } catch (AlreadyClosedException e) {
            log.info("Already closed");
            return;
          } catch (Exception e) {
            SolrException.log(log, "", e);
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
      if (isClosed()) return;
        try (ParWork worker = new ParWork("disconnected", true, true)) {
          if (zkClient.isConnected()) {
            worker.collect(ZkController.this.overseerContexts);
          } else {
            worker.collect(ZkController.this.overseer);
          }
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
    });
    init();
  }

  private ElectionContext getOverseerContext() {
    ElectionContext context = new OverseerElectionContext(getNodeName(), zkClient, overseer);
    ElectionContext prevContext = overseerContexts.put(new ContextKey("overseer", "overseer"), context);
    if (prevContext != null) {
      prevContext.close();
    }
    if (overseerElector != null) {
      ParWork.close(overseerElector.getContext());
    }
    return context;
  }

  /**
   * <p>Verifies if /clusterstate.json exists in Zookeepeer, and if it does and is not empty, refuses to start and outputs
   * a helpful message regarding collection migration.</p>
   *
   * <p>If /clusterstate.json exists and is empty, it is removed.</p>
   */
  private void checkNoOldClusterstate(final SolrZkClient zkClient) throws InterruptedException {
    try {
      if (!zkClient.exists(ZkStateReader.UNSUPPORTED_CLUSTER_STATE)) {
        return;
      }

      final byte[] data = zkClient.getData(ZkStateReader.UNSUPPORTED_CLUSTER_STATE, null, null);

      if (Arrays.equals("{}".getBytes(StandardCharsets.UTF_8), data)) {
        // Empty json. This log will only occur once.
        log.warn("{} no longer supported starting with Solr 9. Found empty file on Zookeeper, deleting it.", ZkStateReader.UNSUPPORTED_CLUSTER_STATE);
        zkClient.delete(ZkStateReader.UNSUPPORTED_CLUSTER_STATE, -1);
      } else {
        // /clusterstate.json not empty: refuse to start but do not automatically delete. A bit of a pain but user shouldn't
        // have older collections at this stage anyway.
        String message = ZkStateReader.UNSUPPORTED_CLUSTER_STATE + " no longer supported starting with Solr 9. "
            + "It is present and not empty. Cannot start Solr. Please first migrate collections to stateFormat=2 using an "
            + "older version of Solr or if you don't care about the data then delete the file from "
            + "Zookeeper using a command line tool, for example: bin/solr zk rm /clusterstate.json -z host:port";
        log.error(message);
        throw new SolrException(SolrException.ErrorCode.INVALID_STATE, message);
      }
    } catch (KeeperException e) {
      // Convert checked exception to one acceptable by the caller (see also init() further down)
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
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
    log.info("disconnect");
    this.dcCalled = true;
    try (ParWork closer = new ParWork(this, true, true)) {
      closer.collect( "replicateFromLeaders", replicateFromLeaders);

      if (getZkClient().getConnectionManager().isConnected()) {
        closer.collect("PublishNodeAsDown&RepFromLeadersClose&RemoveEmphem", () -> {

          try {
            log.info("Publish this node as DOWN...");
            publishNodeAsDown(getNodeName());
          } catch (Exception e) {
            ParWork.propagateInterrupt("Error publishing nodes as down. Continuing to close CoreContainer", e);
          }
          return "PublishDown";

        });
        closer.collect("removeEphemeralLiveNode", () -> {
          try {
            removeEphemeralLiveNode();
          } catch (Exception e) {
            ParWork.propagateInterrupt("Error Removing ephemeral live node. Continuing to close CoreContainer", e);
          }
          return "RemoveEphemNode";

        });
      }
    }
  }

  /**
   * Closes the underlying ZooKeeper client.
   */
  public void close() {
    log.info("Closing ZkController");
    //assert closeTracker.close();

    IOUtils.closeQuietly(overseerElector);

    leaderElectors.forEach((s, leaderElector) -> IOUtils.closeQuietly(leaderElector));

    if (overseer != null) {
      overseer.closeAndDone();
    }

    this.shudownCalled = true;

    this.isClosed = true;

    try (ParWork closer = new ParWork(this, true, true)) {
      closer.collect(overseer);
      closer.collect(replicateFromLeaders);
      closer.collect(electionContexts);
      closer.collect(collectionToTerms);
      closer.collect(sysPropsCacher);
      closer.collect(cloudManager);
      closer.collect(cloudSolrClient);
      closer.collect(overseerContexts);
    }

    IOUtils.closeQuietly(zkStateReader);

    if (closeZkClient) {
      IOUtils.closeQuietly(zkClient);
    }

    SolrLifcycleListener.removeShutdown(this);

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
    if (shard.getReplica(cd.getName()) != shard.getLeader()) return;

    int numActiveReplicas = shard.getReplicas(
        rep -> rep.getState() == Replica.State.ACTIVE
            && rep.getType() != Type.PULL
            && getClusterState().getLiveNodes().contains(rep.getNodeName())
    ).size();

    // at least the leader still be able to search, we should give up leadership if other replicas can take over
    if (numActiveReplicas >= 2) {
      String key = cd.getCollectionName() + ":" + cd.getName();
      //TODO better handling the case when delete replica was failed
      if (replicasMetTragicEvent.putIfAbsent(key, tragicException) == null) {
        log.warn("Leader {} met tragic exception, give up its leadership", key, tragicException);
        try {
          // by using Overseer to remove and add replica back, we can do the task in an async/robust manner
          Map<String,Object> props = new HashMap<>();
          props.put(Overseer.QUEUE_OPERATION, "deletereplica");
          props.put(COLLECTION_PROP, cd.getCollectionName());
          props.put(SHARD_ID_PROP, shard.getName());
          props.put(REPLICA_PROP, cd.getName());
          getOverseerCollectionQueue().offer(Utils.toJSON(new ZkNodeProps(props)));

          props.clear();
          props.put(Overseer.QUEUE_OPERATION, "addreplica");
          props.put(COLLECTION_PROP, cd.getCollectionName());
          props.put(SHARD_ID_PROP, shard.getName());
          props.put(ZkStateReader.REPLICA_TYPE, cd.getCloudDescriptor().getReplicaType().name().toUpperCase(Locale.ROOT));
          props.put(CoreAdminParams.NODE, getNodeName());
          getOverseerCollectionQueue().offer(Utils.toJSON(new ZkNodeProps(props)));
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
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
      cloudSolrClient.connect();
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
          ParWork.propagateInterrupt(e);
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

  boolean isShudownCalled() {
    return shudownCalled;
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
    paths.put(Overseer.OVERSEER_COLLECTION_QUEUE_WORK, null);
    paths.put(Overseer.OVERSEER_COLLECTION_MAP_RUNNING, null);
    paths.put(Overseer.OVERSEER_COLLECTION_MAP_COMPLETED, null);

    paths.put(Overseer.OVERSEER_COLLECTION_MAP_FAILURE, null);
    paths.put(Overseer.OVERSEER_ASYNC_IDS, null);
    paths.put(Overseer.OVERSEER_ELECT, null);


    paths.put("/autoscaling", null);
    paths.put("/autoscaling/events/.scheduled_maintenance", null);
    paths.put("/autoscaling/events/.auto_add_replicas", null);
//
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
        log.error("Zk Exception", e);
        return;
      } catch (InterruptedException e) {
        log.info("interrupted");
        return;
      }
      try {
        zkClient.mkdirs("/cluster/cluster_lock");
      } catch (KeeperException.NodeExistsException e) {
        // okay
      } catch (KeeperException e) {
        log.error("Zk Exception", e);
        return;
      }
      boolean createdClusterNodes = false;
      try {
        DistributedLock lock = new DistributedLock(zkClient, "/cluster/cluster_lock", zkClient.getZkACLProvider().getACLsToAdd("/cluster/cluster_lock"));
        if (log.isDebugEnabled()) log.debug("get cluster lock");
        while (!lock.lock()) {
          Thread.sleep(150);
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
              ParWork.propagateInterrupt(e);
              log.error("Failed creating initial zk layout", e);
              return;
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
          if (lock.isOwner()) lock.unlock();
        }
        if (!createdClusterNodes) {
          // wait?
        }

        zkStateReader = new ZkStateReader(zkClient, () -> {
          if (cc != null) cc.securityNodeChanged();
        });
        this.baseURL = zkStateReader.getBaseUrlForNodeName(this.nodeName);

        zkStateReader.createClusterStateWatchersAndUpdate();

        this.overseer = new Overseer(cc.getUpdateShardHandler(),
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
        overseerElector = new LeaderElector(this, new ContextKey("overseer", "overseer"), overseerContexts);
        try (ParWork worker = new ParWork(this, false, true)) {
          // start the overseer first as following code may need it's processing
          worker.collect("startOverseer", () -> {
            ElectionContext context = getOverseerContext();
            log.info("Overseer setting up context {}", context.leaderProps);
            overseerElector.setup(context);
            try {
              log.info("Overseer joining election {}", context.leaderProps);
              overseerElector.joinElection(context, false);
            } catch (KeeperException e) {
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            } catch (InterruptedException e) {
              ParWork.propagateInterrupt(e);
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            } catch (IOException e) {
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            }
          });

//          worker.collect("publishDownState", () -> {
//            try {
//              Stat stat = zkClient.exists(ZkStateReader.LIVE_NODES_ZKNODE, null);
//              if (stat != null && stat.getNumChildren() > 0) {
//                publishDownStates();
//              }
//            } catch (InterruptedException e) {
//              ParWork.propagateInterrupt(e);
//              throw new SolrException(ErrorCode.SERVER_ERROR, e);
//            } catch (KeeperException e) {
//              throw new SolrException(ErrorCode.SERVER_ERROR, e);
//            }
//          });
        }
        // Do this last to signal we're up.
        createEphemeralLiveNode();

        //  publishAndWaitForDownStates();
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
      } catch (KeeperException e) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
      }
    }
  }

  private synchronized void shutdown() {
    if (this.shudownCalled) return;
    this.shudownCalled = true;

    log.info("Cluster shutdown initiated");

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

  public void publishDownStates() throws KeeperException {
    publishNodeAsDown(getNodeName());
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
    log.info("Register node as live in ZooKeeper:" + nodePath);

    log.info("Create our ephemeral live node");

    createLiveNodeImpl(nodePath);
  }

  private void createLiveNodeImpl(String nodePath) {
    Map<String, byte[]> dataMap = new HashMap<>(2);
    Map<String, CreateMode> createModeMap = new HashMap<>(2);
    dataMap.put(nodePath, null);
    createModeMap.put(nodePath, CreateMode.EPHEMERAL);
    try {
      try {
        zkClient.getSolrZooKeeper().create(nodePath, null, zkClient.getZkACLProvider().getACLsToAdd(nodePath), CreateMode.EPHEMERAL);
      } catch (KeeperException.NodeExistsException e) {
        log.warn("Found our ephemeral live node already exists. This must be a quick restart after a hard shutdown, waiting for it to expire {}", nodePath);
        // TODO nocommit wait for expiration properly and try again?
        Thread.sleep(15000);
        zkClient.getSolrZooKeeper().create(nodePath, null, zkClient.getZkACLProvider().getACLsToAdd(nodePath), CreateMode.EPHEMERAL);
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  public void removeEphemeralLiveNode() throws KeeperException, InterruptedException {
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
    try {
      zkClient.delete(nodePath, -1);
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

  public void registerUnloadWatcher(String collection, String shardId, String name) {
    // nocommit - this thing is currently bad
//    zkStateReader.registerDocCollectionWatcher(collection,
//        new UnloadCoreOnDeletedWatcher(shardId, name));
  }

  public String register(String coreName, final CoreDescriptor desc, boolean recoverReloadedCores,
      boolean afterExpiration, boolean skipRecovery) throws Exception {
    try (SolrCore core = cc.getCore(coreName)) {
     return register(core, desc, recoverReloadedCores, afterExpiration, skipRecovery);
    }
  }

  /**
   * Register shard with ZooKeeper.
   *
   * @return the shardId for the SolrCore
   */
  private String register(SolrCore core, final CoreDescriptor desc, boolean recoverReloadedCores,
                         boolean afterExpiration, boolean skipRecovery) throws Exception {
    MDCLoggingContext.setCoreDescriptor(cc, desc);
    try {
      // pre register has published our down state
      String coreName = core.getName();
      final String baseUrl = getBaseUrl();
      final CloudDescriptor cloudDesc = desc.getCloudDescriptor();
      final String collection = cloudDesc.getCollectionName();
      final String shardId = cloudDesc.getShardId();
      log.info("Register SolrCore, core={} baseUrl={} collection={}, shard={} skipRecovery={}", coreName, baseUrl, collection, shardId, skipRecovery);
      AtomicReference<DocCollection> coll = new AtomicReference<>();
      try {
        zkStateReader.waitForState(collection, 3, TimeUnit.SECONDS, (l, c) -> { // nocommit timeout
//          if (isClosed()) {
//            throw new AlreadyClosedException();
//          }

          coll.set(c);
          if (c == null) {
            return false;
          }
          if (c.getReplica(coreName) != null) {
            return true;
          }
          return false;
        });
      } catch (TimeoutException e) {
        throw new TimeoutException("Timeout waiting to see core " + coreName  + " \n" + coll.get());
      }

      Replica replica = getReplicaOrNull(zkStateReader.getClusterState().getCollectionOrNull(collection), shardId, coreName);
      if (replica == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error registering SolrCore, replica is removed from clusterstate");
      }

      // nocommit - we need to know what type we are without getting the clusterstate
      ///  if (replica.getType() != Type.PULL) {
      log.info("Register terms for replica {}", coreName);
      getCollectionTerms(collection).register(cloudDesc.getShardId(), coreName);
      //  }

      ZkShardTerms shardTerms = getShardTerms(collection, cloudDesc.getShardId());

      log.info("Register replica - core:{} address:{} collection:{} shard:{}", coreName, baseUrl, collection, shardId);
      //
      try {
        // If we're a preferred leader, insert ourselves at the head of the queue
        boolean joinAtHead = replica.getBool(SliceMutator.PREFERRED_LEADER_PROP, false);
        if (replica.getType() != Type.PULL) {
          // nocommit review
          joinElection(desc, afterExpiration, joinAtHead);
        } else if (replica.getType() == Type.PULL) {
          if (joinAtHead) {
            log.warn("Replica {} was designated as preferred leader but it's type is {}, It won't join election", coreName, Type.PULL);
          }
          log.debug("Replica {} skipping election because it's type is {}", coreName, Type.PULL);
          startReplicationFromLeader(coreName, false);
        }
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        return null;
      } catch (KeeperException | IOException e) {
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      }
      joinElection(desc, afterExpiration, false);

      log.info("Wait to see leader for {}, {}", collection, shardId);
      Replica leader = zkStateReader.getLeaderRetry(collection, shardId, 10000);

      String ourUrl = ZkCoreNodeProps.getCoreUrl(baseUrl, coreName);
      boolean isLeader = leader.getName() .equals(coreName);

      log.info("We are {} and leader is {} isLeader={}", ourUrl, leader.getCoreUrl(), isLeader);

      log.info("Check if we should recover isLeader={}", isLeader);
      //assert !(isLeader && replica.getType() == Type.PULL) : "Pull replica became leader!";

      try {

        // recover from local transaction log and wait for it to complete before
        // going active
        // TODO: should this be moved to another thread? To recoveryStrat?
        // TODO: should this actually be done earlier, before (or as part of)
        // leader election perhaps?

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
              log.info("Replaying tlog for {} during startup... NOTE: This can take a while.", core);
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
        boolean didRecovery = checkRecovery(recoverReloadedCores, isLeader, skipRecovery, collection, coreName, shardId, core, cc, afterExpiration);

        if (!didRecovery) {
          if (isTlogReplicaAndNotLeader) {
            startReplicationFromLeader(coreName, true);
          }
        }

        if (replica.getType() != Type.PULL) {
          // the watcher is added to a set so multiple calls of this method will left only one watcher
          shardTerms.addListener(new RecoveringCoreTermWatcher(core.getCoreDescriptor(), getCoreContainer()));
        }
        desc.getCloudDescriptor().setHasRegistered(true);

        // the watcher is added to a set so multiple calls of this method will left only one watcher
        getZkStateReader().registerCore(cloudDesc.getCollectionName());
        // the watcher is added to a set so multiple calls of this method will left only one watcher
        // nocommit
        registerUnloadWatcher(cloudDesc.getCollectionName(), cloudDesc.getShardId(), desc.getName());

      } catch (Exception e) {
        SolrZkClient.checkInterrupted(e);
        unregister(coreName, desc, false);
        throw e;
      }
      log.info("SolrCore Registered, core{} baseUrl={} collection={}, shard={}", coreName, baseUrl, collection, shardId);
      return shardId;
    } finally {
      MDCLoggingContext.clear();
    }
  }

  private Replica getReplicaOrNull(DocCollection docCollection, String shard, String coreName) {
    if (docCollection == null) return null;

    Replica replica = docCollection.getReplica(coreName);

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
        ParWork.propagateInterrupt("Error closing previous replication attempt", e);
      }
      if (isClosed()) throw new AlreadyClosedException();
      replicateFromLeader.startReplication(switchTransactionLog);
    }

  }

  public void stopReplicationFromLeader(String coreName) {
    log.info("{} stopping background replication from leader", coreName);
    ReplicateFromLeader replicateFromLeader = replicateFromLeaders.remove(coreName);
    if (replicateFromLeader != null) {
      ParWork.close(replicateFromLeader);
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
      ParWork.propagateInterrupt(e);
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
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    return clusterStateLeaderUrl != null;
  }

  /**
   * Get leader props directly from zk nodes.
   * @throws SessionExpiredException on zk session expiration.
   */
  public Replica getLeaderProps(final String collection,
                                        final String slice, int timeoutms) throws InterruptedException, SessionExpiredException {
    return getLeaderProps(collection, slice, timeoutms, true);
  }

  /**
   * Get leader props directly from zk nodes.
   *
   * @return leader props
   * @throws SessionExpiredException on zk session expiration.
   */
  public Replica getLeaderProps(final String collection,
                                        final String slice, int timeoutms, boolean failImmediatelyOnExpiration) throws InterruptedException, SessionExpiredException {
    TimeOut timeout = new TimeOut(timeoutms, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    Exception exp = null;
    while (!timeout.hasTimedOut()) {
      try {
        getZkStateReader().waitForState(collection, 10, TimeUnit.SECONDS, (n,c) -> c != null && c.getLeader(slice) != null);

        byte[] data = zkClient.getData(ZkStateReader.getShardLeadersPath(collection, slice), null, null);
        ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(ZkNodeProps.load(data));
        // nocommit - right key for leader name?
        return new Replica(leaderProps.getNodeProps().getStr("name"), leaderProps.getNodeProps().getProperties(), collection, slice);

      } catch (Exception e) {
        SolrZkClient.checkInterrupted(e);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    return null;
  }


  private boolean joinElection(CoreDescriptor cd, boolean afterExpiration, boolean joinAtHead)
      throws InterruptedException, KeeperException, IOException {
    log.info("joinElection {}", cd.getName());
    // look for old context - if we find it, cancel it
    String collection = cd.getCloudDescriptor().getCollectionName();

    ContextKey contextKey = new ContextKey(collection, cd.getName());

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

    Replica replica = new Replica(cd.getName(), props, null, null);
    LeaderElector leaderElector;
    synchronized (leaderElectors) {
      leaderElector = leaderElectors.get(replica.getName());
      if (leaderElector == null) {
         leaderElector = new LeaderElector(this, contextKey, electionContexts);
         leaderElectors.put(replica.getName(), leaderElector);
      }
    }

    ElectionContext context = new ShardLeaderElectionContext(leaderElector, shardId,
        collection, cd.getName(), replica, this, cc);

    prevContext = electionContexts.put(contextKey, context);
    if (prevContext != null) {
      prevContext.close();
    }

    leaderElector.setup(context);
    log.info("Joining election ...");
    return leaderElector.joinElection(context, false, joinAtHead);
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
      log.info("get shard terms {} {} {}", core.getName(), collection, shardId);
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
    MDCLoggingContext.setCoreDescriptor(cc, cd);

    try (SolrCore core = cc.getCore(cd.getName())) {
      if ((state == Replica.State.ACTIVE || state == Replica.State.RECOVERING) && (isClosed() || (core != null && core.isClosing()))) {
        throw new AlreadyClosedException();
      }
    }

    // nocommit TODO if we publish anything but ACTIVE, cancel any possible election

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

      if (numShards != null) {
        props.put(ZkStateReader.NUM_SHARDS_PROP, numShards.toString());
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
        getShardTerms(collection, shardId).startRecovering(cd.getName());
      }
      if (state == Replica.State.ACTIVE && cd.getCloudDescriptor().getReplicaType() != Type.PULL) {
        getShardTerms(collection, shardId).doneRecovering(cd.getName());
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
    collectionToTermsLock.lock();
    try {
      if (!collectionToTerms.containsKey(collection)) collectionToTerms.put(collection, new ZkCollectionTerms(collection, zkClient));
      return collectionToTerms.get(collection);
    } finally {
      collectionToTermsLock.unlock();
    }
  }

  public void clearZkCollectionTerms() {
    collectionToTermsLock.lock();
    try {
      collectionToTerms.values().forEach(ZkCollectionTerms::close);
      collectionToTerms.clear();
    } finally {
      collectionToTermsLock.unlock();
    }
  }

  public void unregister(String coreName, CoreDescriptor cd) throws Exception {
    unregister(coreName, cd, true);
  }

  public void unregister(String coreName, CoreDescriptor cd, boolean removeCoreFromZk) throws Exception {
    log.info("Unregister core from zookeeper {}", coreName);
    if (!zkClient.isConnected()) return;
    final String collection = cd.getCloudDescriptor().getCollectionName();

    ZkCollectionTerms ct = collectionToTerms.get(collection);
    if (ct != null) {
      ct.remove(cd.getCloudDescriptor().getShardId(), cd);
    }

    zkStateReader.unregisterCore(collection);

    replicasMetTragicEvent.remove(collection + ":" + coreName);

    if (Strings.isNullOrEmpty(collection)) {
      log.error("No collection was specified.");
      assert false : "No collection was specified [" + collection + "]";
      return;
    }
    final DocCollection docCollection = zkStateReader.getClusterState().getCollectionOrNull(collection);
    Replica replica = (docCollection == null) ? null : docCollection.getReplica(coreName);

    if (replica == null || replica.getType() != Type.PULL) {
      ElectionContext context = electionContexts.remove(new ContextKey(collection, coreName));

      if (context != null) {
        context.close();
      }
    }
  }

  public ZkStateReader getZkStateReader() {
    return zkStateReader;
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
      ParWork.propagateInterrupt(e);
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
      ParWork.propagateInterrupt(e);
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

  public static String generateNodeName(final String url) {
    try {
      return URLEncoder.encode(trimLeadingAndTrailingSlashes(url), "UTF-8");
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

  public void rejoinOverseerElection(boolean joinAtHead) {
    boolean closeAndDone = false;
    try {
      closeAndDone = overseer.isCloseAndDone();
    } catch (NullPointerException e) {
      // okay
    }

    if (overseerElector == null || isClosed() || shudownCalled || closeAndDone) {
      return;
    }
    try {
      String electionNode = overseerElector.getContext().electionPath;
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
              ParWork.propagateInterrupt(e);
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
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to rejoin election", e);
    }

  }

  public void rejoinShardLeaderElection(SolrParams params) {

    String collectionName = params.get(COLLECTION_PROP);
    String shardId = params.get(SHARD_ID_PROP);
    String coreName = params.get(CORE_NAME_PROP);
    String electionNode = params.get(ELECTION_NODE_PROP);
    String baseUrl = params.get(BASE_URL_PROP);

    try {
      MDCLoggingContext.setCoreDescriptor(cc, cc.getCoreDescriptor(coreName));

      log.info("Rejoin the shard leader election.");

      ContextKey contextKey = new ContextKey(collectionName, coreName);

      ElectionContext prevContext = electionContexts.get(contextKey);
      if (prevContext != null) prevContext.close();

      Map<String, Object> props = new HashMap<>();
      props.put(ZkStateReader.BASE_URL_PROP, baseUrl);
      props.put(ZkStateReader.CORE_NAME_PROP, coreName);
      props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());

      Replica replica = new Replica(coreName, props, null, null);

      LeaderElector elect = ((ShardLeaderElectionContext) prevContext).getLeaderElector();
      ShardLeaderElectionContext context = new ShardLeaderElectionContext(elect, shardId, collectionName,
          coreName, replica, this, getCoreContainer());

      context.leaderSeqPath = context.electionPath + LeaderElector.ELECTION_NODE + "/" + electionNode;
      elect.setup(context);
      prevContext = electionContexts.put(contextKey, context);
      if (prevContext != null) prevContext.close();
      elect.retryElection(context, params.getBool(REJOIN_AT_HEAD_PROP, false));
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
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
      ParWork.propagateInterrupt(e);
      log.warn("could not read the overseer designate ", e);
    }
  }

  public CoreContainer getCoreContainer() {
    return cc;
  }

  public void throwErrorIfReplicaReplaced(CoreDescriptor desc) {
    ClusterState clusterState = getZkStateReader().getClusterState();
    if (clusterState != null && desc  != null) {
      CloudUtil.checkSharedFSFailoverReplaced(cc, desc);
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
              ParWork.propagateInterrupt(e1);
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
        ParWork.propagateInterrupt(e);
        log.error(e.getMessage());

      }
      if (log.isInfoEnabled()) {
        log.info(StrUtils.formatString("%s zkVersion= %d %s %d", errMsg, resourceLocation, znodeVersion));
      }
      throw new ResourceModifiedInZkException(ErrorCode.CONFLICT, StrUtils.formatString(errMsg, resourceLocation, znodeVersion) + ", retry.");
    } catch (ResourceModifiedInZkException e) {
      throw e;
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
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
      ParWork.propagateInterrupt(e);
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

  private class
  WatcherImpl implements Watcher {
    private final String zkDir;

    private WatcherImpl(String dir) {
      this.zkDir = dir;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType()) || isClosed() || cc.isShutDown()) {
        return;
      }

      Stat stat = null;
      try {
        stat = zkClient.exists(zkDir, null);
      } catch (KeeperException e) {
        log.info(e.getMessage(), e);
      } catch (InterruptedException e) {
        log.info("WatcherImpl Interrupted");
        return;
      }

      boolean resetWatcher = false;
      try {
        resetWatcher = fireEventListeners(zkDir);
      } finally {

        if (!isClosed() && !cc.isShutDown()) {
          if (resetWatcher) {
            setConfWatcher(zkDir, this, stat);
          } else {
            log.debug("A node got unwatched for {}", zkDir);
          }
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
    String shard;
    String coreName;

    public UnloadCoreOnDeletedWatcher(String shard, String coreName) {
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

      boolean replicaRemoved = getReplicaOrNull(collectionState, shard, coreName) == null;
      if (replicaRemoved) {
        try {
          log.info("Replica {} removed from clusterstate, remove it.", coreName);
          getCoreContainer().unload(coreName, true, true, true); // nocommit - this causes bad things in tests
        } catch (SolrException e) {
          if (!e.getMessage().contains("Cannot unload non-existent core")) {
            // no need to log if the core was already unloaded
            log.warn("Failed to unregister core:{}", coreName, e);
          }
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
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
      return
          Objects.equals(shard, that.shard) &&
          Objects.equals(coreName, that.coreName);
    }

    @Override
    public int hashCode() {

      return Objects.hash(shard, coreName);
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
        Replica r = slice.getReplica(dcore.getName());
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
    } catch (AlreadyClosedException | InterruptedException e) {
      ParWork.propagateInterrupt("Not publishing node as DOWN because a resource required to do so is already closed.", null, true);
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
