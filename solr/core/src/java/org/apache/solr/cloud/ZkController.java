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

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.cloud.DistributedLock;
import org.apache.solr.client.solrj.cloud.LockListener;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DefaultZkACLProvider;
import org.apache.solr.common.cloud.DefaultZkCredentialsProvider;
import org.apache.solr.common.cloud.DocCollection;
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
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.URLUtil;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrCoreInitializationException;
import org.apache.solr.handler.admin.ConfigSetsHandlerApi;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.packagemanager.PackageUtils;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.servlet.SolrLifcycleListener;
import org.apache.solr.update.UpdateLog;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTIONS_ZKNODE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.packagemanager.PackageUtils.getMapper;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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

  private final ZkACLProvider zkACLProvider;

  private boolean closeZkClient = false;

  private volatile StatePublisher statePublisher;

  private volatile ZkDistributedQueue overseerJobQueue;
  private volatile OverseerTaskQueue overseerCollectionQueue;
  private volatile OverseerTaskQueue overseerConfigSetQueue;

  private volatile DistributedMap overseerRunningMap;
  private volatile DistributedMap overseerCompletedMap;
  private volatile DistributedMap overseerFailureMap;
  private volatile DistributedMap asyncIdsMap;

  public final static String COLLECTION_PARAM_PREFIX = "collection.";
  public final static String CONFIGNAME_PROP = "configName";
  private boolean isShutdownCalled;
  private volatile boolean dcCalled;
  private volatile boolean started;

  @Override
  public void run() {
    disconnect(true);
    if (zkClient.isConnected()) {
      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
      }
      //      log.info("Waiting to see DOWN states for node before shutdown ...");
//      Collection<SolrCore> cores = cc.getCores();
//      for (SolrCore core : cores) {
//        CoreDescriptor desc = core.getCoreDescriptor();
//        String collection = desc.getCollectionName();
//        try {
//          zkStateReader.waitForState(collection, 2, TimeUnit.SECONDS, (n, c) -> {
//            if (c == null) {
//              return false;
//            }
//            List<Replica> replicas = c.getReplicas();
//            for (Replica replica : replicas) {
//              if (replica.getNodeName().equals(getNodeName())) {
//                if (!replica.getState().equals(Replica.State.DOWN)) {
//                  // log.info("Found state {} {}", replica.getState(), replica.getNodeName());
//                  return false;
//                }
//              }
//            }
//
//            return true;
//          });
//        } catch (InterruptedException e) {
//          ParWork.propagateInterrupt(e);
//          return;
//        } catch (TimeoutException e) {
//          log.error("Timeout", e);
//          break;
//        }
//      }
    } else {
      log.info("ZkClient is not connected, won't wait to see DOWN nodes on shutdown");
    }
    log.info("Continuing to Solr shutdown");
  }

  public boolean isDcCalled() {
    return dcCalled;
  }

  public LeaderElector removeShardLeaderElector(String name) {
    LeaderElector elector = leaderElectors.remove(name);
    IOUtils.closeQuietly(elector);
    return  elector;
  }

  public LeaderElector getLeaderElector(String name) {
    return leaderElectors.get(name);
  }

  static class ContextKey {

    private final String collection;
    private final String coreNodeName;

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
        return other.coreNodeName == null;
      } else return coreNodeName.equals(other.coreNodeName);
    }
  }

  private static final byte[] emptyJson = Utils.toJSON(Collections.emptyMap());

  private final Map<String, LeaderElector> leaderElectors = new ConcurrentHashMap<>(16);

//  private final Map<ContextKey, ElectionContext> electionContexts = new ConcurrentHashMap<>(16) {
//    @Override
//    public ElectionContext put(ContextKey key, ElectionContext value) {
//      if (ZkController.this.isClosed || cc.isShutDown()) {
//        throw new AlreadyClosedException();
//      }
//      return super.put(key, value);
//    }
//  };

//  private final Map<ContextKey, ElectionContext> overseerContexts = new ConcurrentHashMap<>() {
//    @Override
//    public ElectionContext put(ContextKey key, ElectionContext value) {
//      if (ZkController.this.isClosed || cc.isShutDown()) {
//        throw new AlreadyClosedException();
//      }
//      return super.put(key, value);
//    }
//  };

  private final SolrZkClient zkClient;
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

  protected volatile LeaderElector overseerElector;

  private final Map<String, ReplicateFromLeader> replicateFromLeaders = new ConcurrentHashMap<>(16, 0.75f, 16);
  private final Map<String, ZkCollectionTerms> collectionToTerms = new ConcurrentHashMap<>(16, 0.75f, 16);

  //private final ReentrantLock collectionToTermsLock = new ReentrantLock(true);

  // for now, this can be null in tests, in which case recovery will be inactive, and other features
  // may accept defaults or use mocks rather than pulling things from a CoreContainer
  private final CoreContainer cc;

  protected volatile Overseer overseer;

  private final int leaderVoteWait;
  private final int leaderConflictResolveWait;

  private final int clientTimeout;

  private volatile boolean isClosed;

  private final Object initLock = new Object();

  private final ConcurrentHashMap<String, Throwable> replicasMetTragicEvent = new ConcurrentHashMap<>(16, 0.75f, 1);

  @Deprecated
  // keeps track of replicas that have been asked to recover by leaders running on this node
  private final Map<String, String> replicasInLeaderInitiatedRecovery = new HashMap<>();

  // keeps track of a list of objects that need to know a new ZooKeeper session was created after expiration occurred
  // ref is held as a HashSet since we clone the set before notifying to avoid synchronizing too long
  private final Set<OnReconnect> reconnectListeners = ConcurrentHashMap.newKeySet();

  private static class MyLockListener implements LockListener {
    private final CountDownLatch lockWaitLatch;

    public MyLockListener(CountDownLatch lockWaitLatch) {
      this.lockWaitLatch = lockWaitLatch;
    }

    @Override
    public void lockAcquired() {
      lockWaitLatch.countDown();
    }

    @Override
    public void lockReleased() {

    }
  }

  public static class RegisterCoreAsync implements Callable<Object> {

    private final ZkController zkController;
    final CoreDescriptor descriptor;
    final boolean afterExpiration;

    public RegisterCoreAsync(ZkController zkController, CoreDescriptor descriptor, boolean afterExpiration) {
      this.descriptor = descriptor;
      this.afterExpiration = afterExpiration;
      this.zkController = zkController;
    }

    public Object call() throws Exception {
      MDCLoggingContext.setCoreName(descriptor.getName());
      try {
        log.info("Registering core with ZK {} afterExpiration? {}", descriptor.getName(), afterExpiration);

        if (zkController.isDcCalled() || zkController.getCoreContainer().isShutDown() || (afterExpiration && !descriptor.getCloudDescriptor().hasRegistered())) {
          return null;
        }
        if (zkController.cc.getAllCoreNames().contains(descriptor.getName())) {
          try {
            zkController.register(descriptor.getName(), descriptor, afterExpiration);
          } catch (Exception e) {
            log.error("Error registering core name={} afterExpireation={}", descriptor.getName(), afterExpiration);
            throw new SolrException(ErrorCode.SERVER_ERROR, e);
          }
        }
        return descriptor;
      } finally {
        MDCLoggingContext.clear();
      }
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


  public ZkController(final CoreContainer cc, String zkServerAddress, int zkClientConnectTimeout, CloudConfig cloudConfig) throws InterruptedException, IOException, TimeoutException {
    this(cc, new SolrZkClient(), cloudConfig);
    this.closeZkClient = true;
  }

  /**
   * @param cc Core container associated with this controller. cannot be null.
   * @param cloudConfig configuration for this controller. TODO: possibly redundant with CoreContainer
   */
  public ZkController(final CoreContainer cc, SolrZkClient zkClient, CloudConfig cloudConfig)
      throws InterruptedException, TimeoutException, IOException {
    assert new CloseTracker() != null;
    if (cc == null) log.error("null corecontainer");
    if (cc == null) throw new IllegalArgumentException("CoreContainer cannot be null.");
    try {
      this.cc = cc;
      this.cloudConfig = cloudConfig;
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

  public void start() {
    if (started) throw new IllegalStateException("Already started");

    started = true;

    try {
      if (zkClient.exists( ZkStateReader.LIVE_NODES_ZKNODE + "/" + getNodeName())) {
        removeEphemeralLiveNode();
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt("Error Removing ephemeral live node. Continuing to close CoreContainer", e);
    }

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

    zkClient.setAclProvider(zkACLProvider);
    zkClient.getConnectionManager().setOnReconnect(new OnReconnect() {

      @Override
      public void command() {
        synchronized (initLock) {
          if (cc.isShutDown() || isClosed() || isShutdownCalled) {
            log.info("skipping zk reconnect logic due to shutdown");
            return;
          }
          ParWork.getRootSharedExecutor().submit(() -> {
            log.info("ZooKeeper session re-connected ... refreshing core states after session expiration.");
            try {

              removeEphemeralLiveNode();

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

              overseerElector.retryElection(false);

              List<CoreDescriptor> descriptors = getCoreContainer().getCoreDescriptors();
              // re register all descriptors

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

                    ParWork.getRootSharedExecutor().submit(new RegisterCoreAsync(ZkController.this, descriptor, true));

                  } catch (Exception e) {
                    SolrException.log(log, "Error registering SolrCore", e);
                  }
                }
              }

              // notify any other objects that need to know when the session was re-connected

              // the OnReconnect operation can be expensive per listener, so do that async in the background
              try (ParWork work = new ParWork(this, true, false)) {
                reconnectListeners.forEach(listener -> {
                  try {
                    work.collect(new OnReconnectNotifyAsync(listener));
                  } catch (Exception exc) {
                    // not much we can do here other than warn in the log
                    log.warn("Error when notifying OnReconnect listener {} after session re-connected.", listener, exc);
                  }
                });
              }

              createEphemeralLiveNode();

            } catch (AlreadyClosedException e) {
              log.info("Already closed");
              return;
            } catch (Exception e) {
              SolrException.log(log, "", e);
            }
          });

        }
      }

      @Override
      public String getName() {
        return "ZkController";
      }
    });

    zkClient.setDisconnectListener(() -> {
      try (ParWork worker = new ParWork("disconnected", true, false)) {
        worker.collect(ZkController.this.overseerElector);
        worker.collect(ZkController.this.overseer);
        worker.collect(leaderElectors.values());
        leaderElectors.clear();
        // I don't think so...
//        worker.collect("clearZkCollectionTerms", () -> {
//          clearZkCollectionTerms();
//        });
      }

    });
    init();
  }

  private ElectionContext getOverseerContext() {
    return new OverseerElectionContext(getNodeName(), zkClient, overseer);
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

  public void disconnect(boolean publishDown) {
    log.info("disconnect");
    this.dcCalled = true;

    try {
      removeEphemeralLiveNode();
    } catch (Exception e) {
      ParWork.propagateInterrupt("Error Removing ephemeral live node. Continuing to close CoreContainer", e);
    }

    try (ParWork closer = new ParWork(this, true, false)) {
      closer.collect("replicateFromLeaders", replicateFromLeaders);
      closer.collect(leaderElectors);

      if (publishDown) {
        closer.collect("PublishNodeAsDown&RepFromLeaders", () -> {
          try {
            log.info("Publish this node as DOWN...");
            publishNodeAs(getNodeName(), OverseerAction.DOWNNODE);
          } catch (Exception e) {
            ParWork.propagateInterrupt("Error publishing nodes as down. Continuing to close CoreContainer", e);
          }
          return "PublishDown";
        });
      }
    }
  }

  /**
   * Closes the underlying ZooKeeper client.
   */
  public void close() {
    if (log.isDebugEnabled()) log.debug("Closing ZkController");
    //assert closeTracker.close();

    this.isShutdownCalled = true;

    this.isClosed = true;
    try (ParWork closer = new ParWork(this, true, false)) {
      closer.collect(leaderElectors);
      closer.collect(sysPropsCacher);
      closer.collect(cloudManager);
      closer.collect(cloudSolrClient);

      closer.collect("", () -> {
        try {
          if (statePublisher != null) {
            statePublisher.submitState(StatePublisher.TERMINATE_OP);
          }
        } catch (Exception e) {
          log.error("Exception closing state publisher");
        }
      });

      collectionToTerms.forEach((s, zkCollectionTerms) -> closer.collect(zkCollectionTerms));

    } finally {
      IOUtils.closeQuietly(overseerElector);
      if (overseer != null) {
        try {
          overseer.closeAndDone();
        } catch (Exception e) {
          log.warn("Exception closing Overseer", e);
        }
      }
      IOUtils.closeQuietly(zkStateReader);

      if (closeZkClient && zkClient != null) {
        zkClient.disableCloseLock();
        IOUtils.closeQuietly(zkClient);
      }

      SolrLifcycleListener.removeShutdown(this);

      assert ObjectReleaseTracker.release(this);
    }
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
            && getZkStateReader().getLiveNodes().contains(rep.getNodeName())
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
          getOverseerCollectionQueue().offer(Utils.toJSON(new ZkNodeProps(props)), false);

          props.clear();
          props.put(Overseer.QUEUE_OPERATION, "addreplica");
          props.put(COLLECTION_PROP, cd.getCollectionName());
          props.put(SHARD_ID_PROP, shard.getName());
          props.put(ZkStateReader.REPLICA_TYPE, cd.getCloudDescriptor().getReplicaType().name().toUpperCase(Locale.ROOT));
          props.put(CoreAdminParams.NODE, getNodeName());
          getOverseerCollectionQueue().offer(Utils.toJSON(new ZkNodeProps(props)), false);
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
          cc.getObjectCache(), cc.getUpdateShardHandler().getTheSharedHttpClient());
      cloudManager.getClusterStateProvider().connect();
    }
    return cloudManager;
  }

  public CloudHttp2SolrClient getCloudSolrClient() {
    return  cloudSolrClient;
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
    return isClosed;
  }

  boolean isShutdownCalled() {
    return isShutdownCalled;
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

//
    //   operations.add(zkClient.createPathOp(ZkStateReader.CLUSTER_PROPS, emptyJson));
    paths.put(ZkStateReader.SOLR_PKGS_PATH, getMapper().writeValueAsString(Collections.emptyMap()).getBytes("UTF-8"));
    paths.put(PackageUtils.REPOSITORIES_ZK_PATH, "[]".getBytes(StandardCharsets.UTF_8));

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

    if (log.isDebugEnabled()) log.debug("Creating final {} node", "/cluster/init");
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
    String defaultConfigSet = System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE);
    log.info("{} set to {}", SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE, defaultConfigSet);
    if (defaultConfigSet != null) {
      configDirPath = new File(defaultConfigSet).getAbsolutePath();
    } else if (defaultConfigSet != null &&
        new File(System.getProperty(defaultConfigSet + subPath)).exists()) {
      configDirPath = new File(defaultConfigSet + subPath).getAbsolutePath();
    }
    return configDirPath;
  }

  private void init() {
    // MRM TODO:
    //    Runtime.getRuntime().addShutdownHook(new Thread() {
    //      public void run() {
    //        shutdown();
    //        ParWork.close(ParWork.getExecutor());
    //      }
    //
    //    });
    //   synchronized (initLock) {
    if (log.isDebugEnabled()) log.debug("making shutdown watcher for cluster");
    try {
      zkClient.exists(CLUSTER_SHUTDOWN, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          if (Event.EventType.None.equals(event.getType())) {
            return;
          }

          log.info("Got event for shutdown {}", event);
          if (event.getType().equals(Event.EventType.NodeCreated)) {
            log.info("Shutdown zk node created, shutting down");
            shutdown();
          } else {
            log.info("Remaking shutdown watcher");
            Stat stat;
            try {
              stat = zkClient.exists(CLUSTER_SHUTDOWN, this);
            } catch (KeeperException | InterruptedException e) {
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
      CountDownLatch lockWaitLatch = new CountDownLatch(1);
      boolean create = false;
      DistributedLock lock = new DistributedLock(zkClient, "/cluster/cluster_lock", zkClient.getZkACLProvider().getACLsToAdd("/cluster/cluster_lock"), new MyLockListener(lockWaitLatch));
      try {
        //  if (log.isDebugEnabled()) log.debug("get cluster lock");
        if (lock.lock()) {
          create = true;
        }
        if (create) {

          if (log.isDebugEnabled()) log.debug("got cluster lock");
          //          CountDownLatch latch = new CountDownLatch(1);
          //          zkClient.getSolrZooKeeper().sync("/cluster/init", (rc, path, ctx) -> {
          //            latch.countDown();
          //          }, new Object());
          //          boolean success = latch.await(10, TimeUnit.SECONDS);
          //          if (!success) {
          //            throw new SolrException(ErrorCode.SERVER_ERROR, "Timeout calling sync on collection zknode");
          //          }
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
            //if (log.isDebugEnabled()) log.debug("Cluster zk nodes already exist");
            //int currentLiveNodes = zkClient.getChildren(ZkStateReader.LIVE_NODES_ZKNODE, null, true).size();
            //if (log.isDebugEnabled()) log.debug("Current live nodes {}", currentLiveNodes);
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
            //            // MRM TODO:, still haackey, do fails right
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
        }
      } finally {
        if (log.isDebugEnabled()) log.debug("release cluster lock");
        lock.unlock();
        lock.close();
      }

      if (!createdClusterNodes) {
        // wait?
      }

      zkStateReader = new ZkStateReader(zkClient, () -> {
        if (cc != null) cc.securityNodeChanged();
      });
      zkStateReader.setNode(nodeName);
      zkStateReader.setCollectionRemovedListener(this::removeCollectionTerms);
      this.baseURL = zkStateReader.getBaseUrlForNodeName(this.nodeName);

      zkStateReader.createClusterStateWatchersAndUpdate();

      this.overseer = new Overseer(cc.getUpdateShardHandler(), CommonParams.CORES_HANDLER_PATH, this, cloudConfig);

      try {
        this.overseerRunningMap = Overseer.getRunningMap(zkClient);

        this.overseerCompletedMap = Overseer.getCompletedMap(zkClient);
        this.overseerFailureMap = Overseer.getFailureMap(zkClient);
        this.asyncIdsMap = Overseer.getAsyncIdsMap(zkClient);
      } catch (KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
      this.overseerJobQueue = overseer.getStateUpdateQueue();
      this.overseerCollectionQueue = overseer.getCollectionQueue(zkClient);
      this.overseerConfigSetQueue = overseer.getConfigSetQueue(zkClient);

      statePublisher = new StatePublisher(overseerJobQueue, zkStateReader, cc);
      statePublisher.start();

      this.sysPropsCacher = new NodesSysPropsCacher(getSolrCloudManager().getNodeStateProvider(), getNodeName(), zkStateReader);
      overseerElector = new LeaderElector(this);
      //try (ParWork worker = new ParWork(this, false, true)) {
        // start the overseer first as following code may need it's processing
       // worker.collect("startOverseer", () -> {
          ElectionContext context = getOverseerContext();
          if (log.isDebugEnabled()) log.debug("Overseer setting up context {}", context.leaderProps.getNodeName());
          overseerElector.setup(context);

          log.info("Overseer joining election {}", context.leaderProps.getNodeName());
          overseerElector.joinElection(false);

      publishNodeAs(getNodeName(), OverseerAction.RECOVERYNODE);

    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    } catch (KeeperException e) {
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }
    //  }
  }

  private synchronized void shutdown() {
    if (this.isShutdownCalled) return;
    this.isShutdownCalled = true;

    log.info("Cluster shutdown initiated");

    URL url;
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
    publishNodeAs(getNodeName(), OverseerAction.DOWNNODE);
  }

  /**
   * Validates if the chroot exists in zk (or if it is successfully created).
   * Optionally, if create is set to true this method will create the path in
   * case it doesn't exist
   *
   * @return true if the path exists or is created false if the path doesn't
   * exist and 'create' = false
   */
  public static boolean checkChrootPath(String zkHost, boolean create) // MRM TODO:
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

  public void createEphemeralLiveNode() {
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;

    log.info("Create our ephemeral live node {}", ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName);

    createLiveNodeImpl(nodePath);
  }

  private void createLiveNodeImpl(String nodePath) {
    try {
      try {
        zkClient.create(nodePath, (byte[]) null, CreateMode.EPHEMERAL, true);
      } catch (KeeperException.NodeExistsException e) {
        log.warn("Found our ephemeral live node already exists. This must be a quick restart after a hard shutdown? ... {}", nodePath);
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  public void removeEphemeralLiveNode() throws KeeperException {
    log.info("Removing our ephemeral live node");
    String nodeName = getNodeName();
    String nodePath = ZkStateReader.LIVE_NODES_ZKNODE + "/" + nodeName;
    try {
      zkClient.delete(nodePath, -1);
    } catch (NoNodeException | SessionExpiredException e) {
      // okay
    } catch (Exception e) {
      log.warn("Could not remove ephemeral live node {}", nodePath, e);
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
    // MRM TODO: - this thing is currently bad
//    zkStateReader.registerDocCollectionWatcher(collection,
//        new UnloadCoreOnDeletedWatcher(shardId, name));
  }

  public String register(String coreName, final CoreDescriptor desc) throws Exception {
     return register(coreName, desc, false);
  }

  public boolean isOverseerLeader() {
    return overseerElector != null && overseerElector.isLeader();
  }

  /**
   * Register shard with ZooKeeper.
   *
   * @return the shardId for the SolrCore
   */
  private String register(String coreName, final CoreDescriptor desc, boolean afterExpiration) {
    if (getCoreContainer().isShutDown() || isDcCalled()) {
      throw new AlreadyClosedException();
    }
    MDCLoggingContext.setCoreName(desc.getName());
    ZkShardTerms shardTerms = null;
    LeaderElector leaderElector = null;
    try {
      final String baseUrl = getBaseUrl();
      final CloudDescriptor cloudDesc = desc.getCloudDescriptor();
      final String collection = cloudDesc.getCollectionName();
      final String shardId = cloudDesc.getShardId();

      log.info("Register SolrCore, core={} baseUrl={} collection={}, shard={}", coreName, baseUrl, collection, shardId);
      AtomicReference<DocCollection> coll = new AtomicReference<>();
      AtomicReference<Replica> replicaRef = new AtomicReference<>();

      // the watcher is added to a set so multiple calls of this method will left only one watcher
      if (!cloudDesc.hasRegistered()) {
        getZkStateReader().registerCore(cloudDesc.getCollectionName());
      }

      log.info("Register replica - core:{} address:{} collection:{} shard:{} type={}", coreName, baseUrl, collection, shardId, cloudDesc.getReplicaType());

      log.info("Register terms for replica {}", coreName);

      registerShardTerms(collection, cloudDesc.getShardId(), coreName);

      log.info("Create leader elector for replica {}", coreName);
      leaderElector = leaderElectors.get(coreName);
      if (leaderElector == null) {
        leaderElector = new LeaderElector(this);
        LeaderElector oldElector = leaderElectors.putIfAbsent(coreName, leaderElector);

        if (oldElector != null) {
          IOUtils.closeQuietly(leaderElector);
        }

        if (cc.isShutDown()) {
          IOUtils.closeQuietly(leaderElector);
          IOUtils.closeQuietly(oldElector);
          IOUtils.closeQuietly(getShardTermsOrNull(collection, shardId));
          throw new AlreadyClosedException();
        }
      }

      // If we're a preferred leader, insert ourselves at the head of the queue
      boolean joinAtHead = false; //replica.getBool(SliceMutator.PREFERRED_LEADER_PROP, false);

      if (cloudDesc.getReplicaType() != Type.PULL) {
        //getCollectionTerms(collection).register(cloudDesc.getShardId(), coreName);
        // MRM TODO: review joinAtHead
        joinElection(desc, joinAtHead);
      }

      log.info("Wait to see leader for {}, {}", collection, shardId);
      String leaderName = null;

      for (int i = 0; i < 60; i++) {
        if (leaderElector.isLeader()) {
          leaderName = coreName;
          break;
        }
        try {
          Replica leader = zkStateReader.getLeaderRetry(collection, shardId, 6000, true);
          leaderName = leader.getName();

        } catch (TimeoutException timeoutException) {
          if (isClosed() || isDcCalled() || cc.isShutDown()) {
            throw new AlreadyClosedException();
          }

          log.info("Timeout waiting to see leader, retry");
        }
      }

      if (leaderName == null) {
        log.error("No leader found while trying to register " + coreName + " with zookeeper");
        throw new SolrException(ErrorCode.SERVER_ERROR, "No leader found while trying to register " + coreName + " with zookeeper");
      }


      boolean isLeader = leaderName.equals(coreName);

      log.info("We are {} and leader is {} isLeader={}", coreName, leaderName, isLeader);

      log.info("Check if we should recover isLeader={}", isLeader);
      //assert !(isLeader && replica.getType() == Type.PULL) : "Pull replica became leader!";

      // recover from local transaction log and wait for it to complete before
      // going active
      // TODO: should this be moved to another thread? To recoveryStrat?
      // TODO: should this actually be done earlier, before (or as part of)
      // leader election perhaps?
      try (SolrCore core = cc.getCore(coreName)) {
        if (core == null || core.isClosing() || getCoreContainer().isShutDown()) {
          throw new AlreadyClosedException();
        }

        UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
        boolean isTlogReplicaAndNotLeader = cloudDesc.getReplicaType() == Replica.Type.TLOG && !isLeader;
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

        if (cloudDesc.getReplicaType() != Type.PULL && !isLeader) {
          checkRecovery(core, cc);
        } else if (isTlogReplicaAndNotLeader) {
          startReplicationFromLeader(coreName, true);
        }

        if (cloudDesc.getReplicaType() == Type.PULL) {
          startReplicationFromLeader(coreName, false);
        }

        if (cloudDesc.getReplicaType() != Type.PULL) {
          shardTerms = getShardTerms(collection, cloudDesc.getShardId());
          // the watcher is added to a set so multiple calls of this method will left only one watcher
          if (log.isDebugEnabled()) log.debug("add shard terms listener for {}", coreName);
          shardTerms.addListener(new RecoveringCoreTermWatcher(core.getCoreDescriptor(), getCoreContainer()));
        }
      }

      // the watcher is added to a set so multiple calls of this method will left only one watcher
      // MRM TODO:
      registerUnloadWatcher(cloudDesc.getCollectionName(), cloudDesc.getShardId(), desc.getName());

      log.info("SolrCore Registered, core{} baseUrl={} collection={}, shard={}", coreName, baseUrl, collection, shardId);

      desc.getCloudDescriptor().setHasRegistered(true);

      return shardId;
    } catch (AlreadyClosedException e) {
      log.warn("Won't register with ZooKeeper, already shutting down core={}", desc.getName());
      throw e;
    } catch (Exception e) {
      log.error("Error registering SolrCore with Zookeeper core={}", desc, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error registering SolrCore with Zookeeper", e);
    } finally {
      if (isDcCalled() || isClosed()) {
        IOUtils.closeQuietly(leaderElector);
      }
      MDCLoggingContext.clear();
    }
  }

  private Replica getReplicaOrNull(DocCollection docCollection, String shard, String coreName) {
    if (docCollection == null) return null;

    return docCollection.getReplica(coreName);
  }

  public void startReplicationFromLeader(String coreName, boolean switchTransactionLog) throws InterruptedException {
    if (isClosed()) throw new AlreadyClosedException();
    log.info("{} starting background replication from leader", coreName);

    stopReplicationFromLeader(coreName);

    ReplicateFromLeader replicateFromLeader = new ReplicateFromLeader(cc, coreName);
    if (isDcCalled() || isClosed || cc.isShutDown()) {
      return;
    }
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
     // if (isClosed()) throw new AlreadyClosedException();
      replicateFromLeader.startReplication(switchTransactionLog);
    }

  }

  public void stopReplicationFromLeader(String coreName) {
    log.info("{} stopping background replication from leader", coreName);
    ReplicateFromLeader replicateFromLeader = replicateFromLeaders.remove(coreName);
    if (replicateFromLeader != null) {
      IOUtils.closeQuietly(replicateFromLeader);
    }
  }

  /**
   * Get leader props directly from zk nodes.
   * @throws SessionExpiredException on zk session expiration.
   */
  public Replica getLeaderProps(final String collection, long id,
                                        final String slice, int timeoutms) throws InterruptedException, SessionExpiredException {
    return getLeaderProps(collection, id, slice, timeoutms, true);
  }

  /**
   * Get leader props directly from zk nodes.
   *
   * @return leader props
   * @throws SessionExpiredException on zk session expiration.
   */
  public Replica getLeaderProps(final String collection, long id, final String slice, int timeoutms, boolean failImmediatelyOnExpiration)
      throws InterruptedException, SessionExpiredException { // MRM TODO: look at failImmediatelyOnExpiration

    try {
      getZkStateReader().waitForState(collection, timeoutms, TimeUnit.SECONDS, (n, c) -> c != null && c.getLeader(slice) != null);

      byte[] data = zkClient.getData(ZkStateReader.getShardLeadersPath(collection, slice), null, null);
      ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(ZkNodeProps.load(data));

      return new Replica(leaderProps.getNodeProps().getStr(CORE_NAME_PROP), leaderProps.getNodeProps().getProperties(), collection, id, slice, zkStateReader);

    } catch (Exception e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

  }


  private void joinElection(CoreDescriptor cd, boolean joinAtHead) {
    log.info("joinElection {}", cd.getName());
    // look for old context - if we find it, cancel it
    String collection = cd.getCloudDescriptor().getCollectionName();

    String shardId = cd.getCloudDescriptor().getShardId();

    Map<String, Object> props = new HashMap<>();
    // we only put a subset of props into the leader node
    props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
    props.put(CORE_NAME_PROP, cd.getName());

    String id = cd.getCoreProperty("id", "-1");
    if (id.equals("-1")) {
      throw new IllegalArgumentException("no id found props=" + cd.getCoreProperties());
    }

    props.put("id", id);

    String collId = cd.getCoreProperty("collId", "-1");
    if (collId.equals("-1")) {
      throw new IllegalArgumentException("no id found props=" + cd.getCoreProperties());
    }

    Replica replica = new Replica(cd.getName(), props, collection, Long.parseLong(collId), shardId, zkStateReader);
    LeaderElector leaderElector;

    if (isDcCalled() || isClosed) {
      return;
    }
    leaderElector = leaderElectors.get(replica.getName());
    if (leaderElector == null) {
      leaderElector = new LeaderElector(this);
      LeaderElector oldElector = leaderElectors.put(replica.getName(), leaderElector);
      IOUtils.closeQuietly(oldElector);
    } else {
      leaderElector.cancel();
    }

    ElectionContext context = new ShardLeaderElectionContext(leaderElector, shardId,
        collection, replica, this, cc, cd);


    leaderElector.setup(context);
    log.info("Joining election ...");
    leaderElector.joinElection( false, joinAtHead);
  }


  /**
   * Returns whether or not a recovery was started
   */
  private void checkRecovery(SolrCore core, CoreContainer cc) {
      if (log.isInfoEnabled()) {
        log.info("Core needs to recover:{}", core.getName());
      }
      core.getUpdateHandler().getSolrCoreState().doRecovery(cc, core.getCoreDescriptor());
  }


  public String getBaseUrl() {
    return baseURL;
  }

  public void publish(final CoreDescriptor cd, final Replica.State state) throws Exception {
    publish(cd, state, true);
  }

  /**
   * Publish core state to overseer.
   */
  public void publish(final CoreDescriptor cd, final Replica.State state, boolean updateLastState) throws Exception {
    MDCLoggingContext.setCoreName(cd.getName());
    log.info("publishing state={}", state);
    String collection = cd.getCloudDescriptor().getCollectionName();
    String shardId = cd.getCloudDescriptor().getShardId();
    Map<String,Object> props = new HashMap<>();
    try (SolrCore core = cc.getCore(cd.getName())) {

      // MRM TODO: if we publish anything but ACTIVE, cancel any possible election?

      // System.out.println(Thread.currentThread().getStackTrace()[3]);
      Integer numShards = cd.getCloudDescriptor().getNumShards();
      if (numShards == null) { // XXX sys prop hack
        log.debug("numShards not found on descriptor - reading it from system property");
      }

      props.put(Overseer.QUEUE_OPERATION, "state");
      props.put(ZkStateReader.STATE_PROP, state.toString());
      props.put("id", cd.getCoreProperty("collId", "-1") + "-" + cd.getCoreProperty("id", "-1"));
      //  props.put(ZkStateReader.ROLES_PROP, cd.getCloudDescriptor().getRoles());
      props.put(CORE_NAME_PROP, cd.getName());
      //  props.put(ZkStateReader.NODE_NAME_PROP, getNodeName());
      //  props.put(ZkStateReader.SHARD_ID_PROP, cd.getCloudDescriptor().getShardId());
      props.put(ZkStateReader.COLLECTION_PROP, collection);
      props.put(ZkStateReader.REPLICA_TYPE, cd.getCloudDescriptor().getReplicaType().toString());
      try {
        if (core.getDirectoryFactory().isSharedStorage()) {
          // MRM TODO: currently doesn't publish anywhere
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
      if ((state == Replica.State.RECOVERING || state == Replica.State.BUFFERING) && cd.getCloudDescriptor().getReplicaType() != Type.PULL) {
        // state is used by client, state of replica can change from RECOVERING to DOWN without needed to finish recovery
        // by calling this we will know that a replica actually finished recovery or not
        ZkShardTerms shardTerms = getShardTerms(collection, shardId);
        shardTerms.startRecovering(cd.getName());
        shardTerms.setTermEqualsToLeader(cd.getName());
      }
      if (state == Replica.State.ACTIVE && cd.getCloudDescriptor().getReplicaType() != Type.PULL) {
        ZkShardTerms shardTerms = getShardTerms(collection, shardId);
        shardTerms.doneRecovering(cd.getName());
      }

      ZkNodeProps m = new ZkNodeProps(props);

      if (updateLastState) {
        cd.getCloudDescriptor().setLastPublished(state);
      }
      statePublisher.submitState(m);
    } finally {
      MDCLoggingContext.clear();
    }
  }

  public void publish(ZkNodeProps message) {
    statePublisher.submitState(message);
  }

  public void registerShardTerms(String collection, String shardId, String corename) throws Exception {
    ZkCollectionTerms ct = getCollectionTerms(collection);
    if (ct == null) {
      ct = createCollectionTerms(collection);
    }
    ct.register(shardId, corename);
  }

  public ZkShardTerms getShardTerms(String collection, String shardId) throws Exception {
    ZkCollectionTerms ct = getCollectionTerms(collection);
    if (ct == null) {
      ct = createCollectionTerms(collection);
    }
    return ct.getShard(shardId);
  }

  public ZkShardTerms getShardTermsOrNull(String collection, String shardId) {
    ZkCollectionTerms ct = getCollectionTerms(collection);
    if (ct == null) return null;
    return ct.getShardOrNull(shardId);
  }


  public void removeCollectionTerms(String collection) {
    ZkCollectionTerms collectionTerms = collectionToTerms.remove(collection);
    IOUtils.closeQuietly(collectionTerms);
  }

  public ZkCollectionTerms getCollectionTerms(String collection) {
    return collectionToTerms.get(collection);
  }

  public ZkCollectionTerms createCollectionTerms(String collection) {
    ZkCollectionTerms ct = new ZkCollectionTerms(collection, zkClient);
    ZkCollectionTerms returned = collectionToTerms.putIfAbsent(collection, ct);
    if (returned == null) {
      return ct;
    } else {
      IOUtils.closeQuietly(ct);
    }
    return returned;
  }

  public void clearZkCollectionTerms() {
    collectionToTerms.values().forEach(ZkCollectionTerms::close);
  }

  public void unregister(String coreName, String collection, String shardId) throws KeeperException, InterruptedException {
    log.info("Unregister core from zookeeper {}", coreName);
    try {

      removeShardLeaderElector(coreName);

      ZkCollectionTerms ct = collectionToTerms.get(collection);
      if (ct != null) {
        ct.remove(shardId, coreName);
      }

      replicasMetTragicEvent.remove(collection + ":" + coreName);

    } finally {
      try {
        zkStateReader.unregisterCore(collection);
      } finally {
        if (statePublisher != null) {
          statePublisher.clearStatCache(coreName);
        }
      }
    }
    //    if (Strings.isNullOrEmpty(collection)) {
    //      log.error("No collection was specified.");
    //      assert false : "No collection was specified [" + collection + "]";
    //      return;
    //    }
    //    final DocCollection docCollection = zkStateReader.getClusterState().getCollectionOrNull(collection);
    //    Replica replica = (docCollection == null) ? null : docCollection.getReplica(coreName);

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
    ZkNodeProps props;
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
    return hostName + ':' + hostPort + '_' +
        URLEncoder.encode(trimLeadingAndTrailingSlashes(hostContext), StandardCharsets.UTF_8);
  }

  public static String generateNodeName(final String url) {
    return URLEncoder.encode(trimLeadingAndTrailingSlashes(url), StandardCharsets.UTF_8);
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
    boolean closeAndDone;
    try {
      closeAndDone = overseer.isCloseAndDone();
    } catch (NullPointerException e) {
      // okay
      closeAndDone = true;
    }
    ElectionContext context = overseerElector.getContext();
    if (overseerElector == null || isClosed() || isShutdownCalled || closeAndDone || context == null) {
      return;
    }
    try {
      overseerElector.retryElection(joinAtHead);
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to rejoin election", e);
    }

  }

  public void rejoinShardLeaderElection(SolrParams params) {

    String coreName = params.get(CORE_NAME_PROP);

    try {
      log.info("Rejoin the shard leader election.");
      LeaderElector elect =  leaderElectors.get(coreName);
      if (elect != null) {
        elect.retryElection(params.getBool(REJOIN_AT_HEAD_PROP, false));
      }
      try (SolrCore core = getCoreContainer().getCore(coreName)) {
        core.getSolrCoreState().doRecovery(core);
      }
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to rejoin election", e);
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
    final SolrZkClient zkClient = zkLoader.getZkClient();
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
    SolrZkClient zkClient = zkLoader.getZkClient();
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
      final ConfListeners confListeners = confDirectoryListeners.get(confDir);
      if (confListeners == null) {
        log.warn("{} has no more registered listeners, but a live one attempted to unregister!", confDir);
        return;
      }
      if (confListeners.confDirListeners.remove(listener)) {
        if (log.isDebugEnabled()) log.debug("removed listener for config directory [{}]", confDir);
      }
      if (confListeners.confDirListeners.isEmpty()) {
        confDirectoryListeners.remove(confDir);
        // no more listeners for this confDir, remove it from the map
        if (log.isDebugEnabled()) log.debug("No more listeners for config directory [{}]", confDir);
        try {
          zkClient.removeWatches(COLLECTIONS_ZKNODE, confListeners.watcher, Watcher.WatcherType.Any, true);
        } catch (KeeperException.NoWatcherException e) {

        } catch (Exception e) {
          log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
        }
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

    final ConfListeners confDirListeners = getConfDirListeners(confDir);
    confDirListeners.confDirListeners.add(listener);
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

  private static class ConfListeners {

    private Set<Runnable> confDirListeners;
    private final Watcher watcher;

    ConfListeners( Set<Runnable> confDirListeners, Watcher watcher) {
      this.confDirListeners = confDirListeners;
      this.watcher = watcher;
    }
  }

  private ConfListeners getConfDirListeners(final String confDir) {
    synchronized (confDirectoryListeners) {
      ConfListeners confDirListeners = confDirectoryListeners.get(confDir);
      if (confDirListeners == null) {
        if (log.isTraceEnabled()) log.trace("watch zkdir {}", confDir);
        ConfDirWatcher watcher = new ConfDirWatcher(confDir, cc, confDirectoryListeners);
        confDirListeners = new ConfListeners(ConcurrentHashMap.newKeySet(), watcher);
        confDirectoryListeners.put(confDir, confDirListeners);
        setConfWatcher(confDir, watcher, null, cc, confDirectoryListeners, cc.getZkController().getZkClient());
      }
      return confDirListeners;
    }
  }

  private final Map<String, ConfListeners> confDirectoryListeners = new ConcurrentHashMap<>();

  private static class ConfDirWatcher implements Watcher {
    private final String zkDir;
    private final CoreContainer cc;
    private final SolrZkClient zkClient;
    private final Map<String, ConfListeners> confDirectoryListeners;

    private ConfDirWatcher(String dir, CoreContainer cc, Map<String, ConfListeners> confDirectoryListeners) {
      this.zkDir = dir;
      this.cc = cc;
      this.zkClient = cc.getZkController().getZkClient();
      this.confDirectoryListeners = confDirectoryListeners;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (Event.EventType.None.equals(event.getType())) {
        return;
      }
      if (cc.getZkController().isClosed() || cc.isShutDown() || cc.getZkController().isDcCalled()) {
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

      fireEventListeners(zkDir, confDirectoryListeners, cc);
    }
  }

  private static boolean fireEventListeners(String zkDir, Map<String, ConfListeners> confDirectoryListeners, CoreContainer cc) {
    if (cc.isShutDown()) {
      return false;
    }

    // if this is not among directories to be watched then don't set the watcher anymore
    if (!confDirectoryListeners.containsKey(zkDir)) {
      if (log.isDebugEnabled()) log.debug("Watcher on {} is removed ", zkDir);
      return false;
    }

    final Set<Runnable> listeners = confDirectoryListeners.get(zkDir).confDirListeners;
    if (listeners != null) {
      if (cc.isShutDown() || cc.getZkController().isDcCalled()) {
        return false;
      }
      listeners.forEach(runnable -> ParWork.getRootSharedExecutor().submit(runnable));
    }
    return true;
  }

  private static void setConfWatcher(String zkDir, Watcher watcher, Stat stat, CoreContainer cc, Map<String, ConfListeners> confDirectoryListeners, SolrZkClient zkClient) {
    try {
      zkClient.addWatch(zkDir, watcher, AddWatchMode.PERSISTENT);
      Stat newStat = zkClient.exists(zkDir, null);
      if (stat != null && newStat.getVersion() > stat.getVersion()) {
        //a race condition where a we missed an event fired
        //so fire the event listeners
        fireEventListeners(zkDir, confDirectoryListeners, cc);
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
    return new ZkControllerOnReconnect(confDirectoryListeners, cc);
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
      Replica r = collection.getReplica(dcore.getName());
      return r != null;
    }
    return false;
  }


  /**
   * Best effort to set DOWN state for all replicas on node.
   *
   * @param nodeName to operate on
   */
  public void publishNodeAs(String nodeName, OverseerAction state) throws KeeperException {
    log.info("Publish node={} as {}", nodeName, state);

    if (overseer == null) {
      log.warn("Could not publish node as down, no overseer was started yet");
      return;
    }

    ZkNodeProps m = new ZkNodeProps(StatePublisher.OPERATION, state.toLower(),
        ZkStateReader.NODE_NAME_PROP, nodeName);
    try {
      statePublisher.submitState(m);
    } catch (AlreadyClosedException e) {
      ParWork.propagateInterrupt("Not publishing node as " + state + " because a resource required to do so is already closed.", null, true);
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

  private static class ZkControllerOnReconnect implements OnReconnect {

    private final Map<String, ConfListeners> confDirectoryListeners;
    private final CoreContainer cc;

    ZkControllerOnReconnect(Map<String, ConfListeners> confDirectoryListeners, CoreContainer cc) {
      this.confDirectoryListeners = confDirectoryListeners;
      this.cc = cc;
    }
    
    @Override
    public void command() {
        confDirectoryListeners.forEach((s, runnables) -> {
          setConfWatcher(s, new ConfDirWatcher(s, cc, confDirectoryListeners), null, cc, confDirectoryListeners, cc.getZkController().getZkClient());
          fireEventListeners(s, confDirectoryListeners, cc);
        });
    }

    @Override
    public String getName() {
      return null;
    }
  }
}
