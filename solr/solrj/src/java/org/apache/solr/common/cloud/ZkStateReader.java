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
package org.apache.solr.common.cloud;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.cloud.CloudInspectUtil;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.Callable;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.emptySortedSet;
import static org.apache.solr.common.util.Utils.fromJSON;

public class ZkStateReader implements SolrCloseable, Replica.NodeNameToBaseUrl {
 // public static final int STATE_UPDATE_DELAY = Integer.getInteger("solr.OverseerStateUpdateDelay", 2000);  // delay between cloud state updates
  public static final String STRUCTURE_CHANGE_NOTIFIER = "_scn";
  public static final String STATE_UPDATES = "_statupdates";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final byte[] emptyJson = Utils.toJSON(EMPTY_MAP);

  public static final String BASE_URL_PROP = "base_url";
  public static final String NODE_NAME_PROP = "node_name";

  public static final String ROLES_PROP = "roles";
  public static final String STATE_PROP = "state";

  /**
   * SolrCore name.
   */
  public static final String CORE_NAME_PROP = "core";
  public static final String COLLECTION_PROP = "collection";
  public static final String ELECTION_NODE_PROP = "election_node";
  public static final String SHARD_ID_PROP = "shard";
  public static final String REPLICA_PROP = "replica";
  public static final String SHARD_RANGE_PROP = "shard_range";
  public static final String SHARD_STATE_PROP = "shard_state";
  public static final String SHARD_PARENT_PROP = "shard_parent";
  public static final String NUM_SHARDS_PROP = "numShards";
  public static final String LEADER_PROP = "leader";
  public static final String SHARED_STORAGE_PROP = "shared_storage";
  public static final String PROPERTY_PROP = "property";
  public static final String PROPERTY_PROP_PREFIX = "property.";
  public static final String PROPERTY_VALUE_PROP = "property.value";
  public static final String MAX_AT_ONCE_PROP = "maxAtOnce";
  public static final String MAX_WAIT_SECONDS_PROP = "maxWaitSeconds";
  public static final String STATE_TIMESTAMP_PROP = "stateTimestamp";
  public static final String COLLECTIONS_ZKNODE = "/collections";
  public static final String LIVE_NODES_ZKNODE = "/live_nodes";
  public static final String ALIASES = "/aliases.json";
  public static final String STATE_JSON = "/state.json";
  /**
   * This ZooKeeper file is no longer used starting with Solr 9 but keeping the name around to check if it
   * is still present and non empty (in case of upgrade from previous Solr version). It used to contain collection
   * state for all collections in the cluster.
   */
  public static final String UNSUPPORTED_CLUSTER_STATE = "/clusterstate.json";
  public static final String CLUSTER_PROPS = "/clusterprops.json";
  public static final String COLLECTION_PROPS_ZKNODE = "collectionprops.json";
  public static final String REJOIN_AT_HEAD_PROP = "rejoinAtHead";
  public static final String SOLR_SECURITY_CONF_PATH = "/security.json";
  public static final String SOLR_PKGS_PATH = "/packages.json";

  public static final String DEFAULT_SHARD_PREFERENCES = "defaultShardPreferences";
  public static final String REPLICATION_FACTOR = "replicationFactor";
  public static final String MAX_SHARDS_PER_NODE = "maxShardsPerNode";
  public static final String MAX_CORES_PER_NODE = "maxCoresPerNode";
  public static final String PULL_REPLICAS = "pullReplicas";
  public static final String NRT_REPLICAS = "nrtReplicas";
  public static final String TLOG_REPLICAS = "tlogReplicas";
  public static final String READ_ONLY = "readOnly";

  public static final String ROLES = "/roles.json";

  public static final String CONFIGS_ZKNODE = "/configs";
  public final static String CONFIGNAME_PROP = "configName";

  public static final String SAMPLE_PERCENTAGE = "samplePercentage";

  public static final String CREATE_NODE_SET_EMPTY = "EMPTY";
  public static final String CREATE_NODE_SET = CollectionAdminParams.CREATE_NODE_SET_PARAM;

  /**
   * @deprecated use {@link org.apache.solr.common.params.CollectionAdminParams#DEFAULTS} instead.
   */
  @Deprecated
  public static final String COLLECTION_DEF = "collectionDefaults";

  public static final String URL_SCHEME = "urlScheme";

  private static final String SOLR_ENVIRONMENT = "environment";

  public static final String REPLICA_TYPE = "type";

  private CloseTracker closeTracker;

  /**
   * A view of the current state of all collections.
   */
  protected volatile ClusterState clusterState = new ClusterState(Collections.emptyMap(), -1);


  private final int GET_LEADER_RETRY_DEFAULT_TIMEOUT = Integer.parseInt(System.getProperty("zkReaderGetLeaderRetryTimeoutMs", "1000"));

  public static final String LEADER_ELECT_ZKNODE = "leader_elect";

  public static final String SHARD_LEADERS_ZKNODE = "leaders";
  public static final String ELECTION_NODE = "election";

  /**
   * "Interesting" and actively watched Collections.
   */
  private final ConcurrentHashMap<String, DocCollection> watchedCollectionStates = new ConcurrentHashMap<>();

  /**
   * "Interesting" but not actively watched Collections.
   */
  private final ConcurrentHashMap<String, LazyCollectionRef> lazyCollectionStates = new ConcurrentHashMap<>();

  /**
   * Collection properties being actively watched
   */
  private final ConcurrentHashMap<String, VersionedCollectionProps> watchedCollectionProps = new ConcurrentHashMap<>();

  /**
   * Watchers of Collection properties
   */
  private final ConcurrentHashMap<String, PropsWatcher> collectionPropsWatchers = new ConcurrentHashMap<>();

  private volatile SortedSet<String> liveNodes = emptySortedSet();

  private volatile int liveNodesVersion = -1;

  private final ReentrantLock liveNodesLock = new ReentrantLock(true);

  private final ReentrantLock clusterStateLock = new ReentrantLock(true);

  private volatile Map<String, Object> clusterProperties = Collections.emptyMap();

  private final ZkConfigManager configManager;

  private ConfigData securityData;

  private final Runnable securityNodeListener;

  private final ConcurrentHashMap<String, CollectionWatch<DocCollectionWatcher>> collectionWatches = new ConcurrentHashMap<>(32, 0.75f, 3);

  private final Map<String,CollectionStateWatcher> stateWatchersMap = new ConcurrentHashMap<>(32, 0.75f, 3);

  // named this observers so there's less confusion between CollectionPropsWatcher map and the PropsWatcher map.
  private final ConcurrentHashMap<String, CollectionWatch<CollectionPropsWatcher>> collectionPropsObservers = new ConcurrentHashMap<>();

  private Set<CloudCollectionsListener> cloudCollectionsListeners = ConcurrentHashMap.newKeySet();

  private final ExecutorService notifications = ParWork.getExecutorService(Integer.MAX_VALUE, false, false);

  private final Set<LiveNodesListener> liveNodesListeners = ConcurrentHashMap.newKeySet();

  private final Set<ClusterPropertiesListener> clusterPropertiesListeners = ConcurrentHashMap.newKeySet();

  private volatile Future<?> collectionPropsCacheCleaner; // only kept to identify if the cleaner has already been started.
  private volatile String node = null;
  private volatile LiveNodeWatcher liveNodesWatcher;
  private volatile CollectionsChildWatcher collectionsChildWatcher;

  public static interface CollectionRemoved {
    void removed(String collection);
  }
  private volatile CollectionRemoved collectionRemoved;

  private static class CollectionWatch<T> {

    volatile AtomicInteger coreRefCount = new AtomicInteger();
    final Set<DocCollectionWatcher> stateWatchers = ConcurrentHashMap.newKeySet();

    final Set<CollectionPropsWatcher> propStateWatchers = ConcurrentHashMap.newKeySet();

    public boolean canBeRemoved() {
      return coreRefCount.get() <= 0 && stateWatchers.size() <= 0;
    }

  }

  public static final Set<String> KNOWN_CLUSTER_PROPS = Set.of(
      URL_SCHEME,
      CoreAdminParams.BACKUP_LOCATION,
      DEFAULT_SHARD_PREFERENCES,
      MAX_CORES_PER_NODE,
      SAMPLE_PERCENTAGE,
      SOLR_ENVIRONMENT,
      CollectionAdminParams.DEFAULTS);

  /**
   * Returns config set name for collection.
   * TODO move to DocCollection (state.json).
   *
   * @param collection to return config set name for
   */
  public String readConfigName(String collection) throws KeeperException {

    String configName = null;

    DocCollection docCollection = watchedCollectionStates.get(collection);
    if (docCollection != null) {
      configName = docCollection.getStr(CONFIGNAME_PROP);
      if (configName != null) {
        return configName;
      }
    }

    String path = COLLECTIONS_ZKNODE + "/" + collection;
    log.debug("Loading collection config from: [{}]", path);

    try {

      byte[] data = zkClient.getData(path, null, null);
      if (data == null) {
        log.warn("No config data found at path {}.", path);
        throw new KeeperException.NoNodeException(path);
      }

      ZkNodeProps props = ZkNodeProps.load(data);
      configName = props.getStr(CONFIGNAME_PROP);

      if (configName == null) {
        log.warn("No config data found at path{}. ", path);
        throw new KeeperException.NoNodeException("No config data found at path: " + path);
      }
    } catch (InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      log.warn("Thread interrupted when loading config name for collection {}", collection);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Thread interrupted when loading config name for collection " + collection, e);
    }

    return configName;
  }


  private final SolrZkClient zkClient;

  protected final boolean closeClient;

  private volatile boolean closed = false;

  public ZkStateReader(SolrZkClient zkClient) {
    this(zkClient, null);
  }

  public ZkStateReader(SolrZkClient zkClient, Runnable securityNodeListener) {
    assert (closeTracker = new CloseTracker()) != null;
    this.zkClient = zkClient;
    this.configManager = new ZkConfigManager(zkClient);
    this.closeClient = false;
    this.securityNodeListener = securityNodeListener;
    assert ObjectReleaseTracker.track(this);
  }


  public ZkStateReader(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    // MRM TODO: check this out
    assert (closeTracker = new CloseTracker()) != null;
    this.zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
            // on reconnect, reload cloud info
            new OnReconnect() {
              @Override
              public void command() {
                ZkStateReader.this.createClusterStateWatchersAndUpdate();
              }

              @Override
              public String getName() {
                return "createClusterStateWatchersAndUpdate";
              }
            });

    this.configManager = new ZkConfigManager(zkClient);
    this.closeClient = true;
    this.securityNodeListener = null;
    try {
      zkClient.start();
    } catch (RuntimeException re) {
      log.error("Exception starting zkClient", re);
      zkClient.close(); // stuff has been opened inside the zkClient
      throw re;
    }
    assert ObjectReleaseTracker.track(this);
  }

  public ZkConfigManager getConfigManager() {
    return configManager;
  }

  // don't call this, used in one place

  public void forciblyRefreshAllClusterStateSlow() {
    // No need to set watchers because we should already have watchers registered for everything.
    try {
      refreshCollectionList();
      refreshLiveNodes();
      // Need a copy so we don't delete from what we're iterating over.
      Collection<String> safeCopy = new ArrayList<>(watchedCollectionStates.keySet());
      Set<String> updatedCollections = new HashSet<>();
      for (String coll : safeCopy) {
        DocCollection newState = fetchCollectionState(coll);
        if (updateWatchedCollection(coll, newState)) {
          updatedCollections.add(coll);
        }
      }
      constructState(updatedCollections);
    } catch (KeeperException e) {
      log.error("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

  }


  public void forciblyRefreshClusterStateSlow(String name) {
    try {
      refreshCollectionList();
      refreshLiveNodes();
      // Need a copy so we don't delete from what we're iterating over.

      Set<String> updatedCollections = new HashSet<>();

      DocCollection newState = fetchCollectionState(name);

      if (updateWatchedCollection(name, newState)) {
        updatedCollections.add(name);
      }

      constructState(updatedCollections);

    } catch (KeeperException e) {
      log.error("", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  /**
   * Refresh the set of live nodes.
   */
  public void updateLiveNodes() throws KeeperException, InterruptedException {
    refreshLiveNodes();
  }

  public Integer compareStateVersions(String coll, int version) {
    DocCollection collection = clusterState.getCollectionOrNull(coll);
    if (collection == null) return null;
    if (collection.getZNodeVersion() < version) {
      if (log.isDebugEnabled()) {
        log.debug("Server older than client {}<{}", collection.getZNodeVersion(), version);
      }
      DocCollection nu = getCollectionLive(this, coll);
      if (nu == null) return -3;
      if (nu.getZNodeVersion() > collection.getZNodeVersion()) {
        if (updateWatchedCollection(coll, nu)) {
          constructState(Collections.singleton(coll));
        }
        collection = nu;
      }
    }

    if (collection.getZNodeVersion() == version) {
      return null;
    }

    if (log.isDebugEnabled()) {
      log.debug("Wrong version from client [{}]!=[{}]", version, collection.getZNodeVersion());
    }

    return collection.getZNodeVersion();
  }

  @SuppressWarnings({"unchecked"})
  public synchronized void createClusterStateWatchersAndUpdate() {
    if (isClosed()) {
      throw new AlreadyClosedException();
    }

    if (log.isDebugEnabled()) log.debug("createClusterStateWatchersAndUpdate");
    CountDownLatch latch = new CountDownLatch(1);

    Watcher watcher = new Watcher() {

      @Override
      public void process(WatchedEvent event) {
        if (EventType.None.equals(event.getType())) {
          return;
        }
        if (log.isDebugEnabled()) log.debug("Got event on live node watcher {}", event.toString());
        if (event.getType() == EventType.NodeCreated) {
          latch.countDown();
        } else {
          try {
            Stat stat = zkClient.exists("/cluster/init", this);
            if (stat != null) {
              latch.countDown();
            }
          } catch (KeeperException e) {
            SolrException.log(log, e);
            return;
          } catch (InterruptedException e) {
            log.warn("", e);
            return;
          }
        }

      }
    };
    try {
      Stat stat = zkClient.exists("/cluster/init", watcher);
      if (stat == null) {
        if (log.isDebugEnabled()) log.debug("Collections znode not found, waiting on latch");
        try {
          boolean success = latch.await(1000, TimeUnit.MILLISECONDS);
          if (!success) {
            throw new SolrException(ErrorCode.SERVER_ERROR, "cluster not found/not ready");
          }
          if (log.isDebugEnabled()) log.debug("Done waiting on latch");
        } catch (InterruptedException e) {
          log.warn("", e);
          return;
        }
      }

    } catch (KeeperException e) {
      log.warn("", e);
      return;
    } catch (InterruptedException e) {
      log.warn("", e);
      return;
    }

    try {

      if (log.isDebugEnabled()) {
        log.debug("Updating cluster state from ZooKeeper... ");
      }

      // on reconnect of SolrZkClient force refresh and re-add watches.
      loadClusterProperties();

      if (this.liveNodesWatcher == null) {
        this.liveNodesWatcher = new LiveNodeWatcher();
      } else {
        this.liveNodesWatcher.removeWatch();
      }
      this.liveNodesWatcher.createWatch();
      this.liveNodesWatcher.refresh();

      if (this.collectionsChildWatcher == null) {
        this.collectionsChildWatcher = new CollectionsChildWatcher();
      } else {
        this.collectionsChildWatcher.removeWatch();
      }
      this.collectionsChildWatcher.createWatch();
      this.collectionsChildWatcher.refresh();

      refreshAliases(aliasesManager);

      if (securityNodeListener != null) {
        addSecurityNodeWatcher(pair -> {
          ConfigData cd = new ConfigData();
          cd.data = pair.first() == null || pair.first().length == 0 ? EMPTY_MAP : Utils.getDeepCopy((Map) fromJSON(pair.first()), 4, false);
          cd.version = pair.second() == null ? -1 : pair.second().getVersion();
          securityData = cd;
          securityNodeListener.run();
        });
        securityData = getSecurityProps(true);
      }

      collectionPropsObservers.forEach((k, v) -> {
        collectionPropsWatchers.computeIfAbsent(k, PropsWatcher::new).refreshAndWatch(true);
      });
    } catch (Exception e) {
      log.warn("", e);
      return;
    }
  }

  private void addSecurityNodeWatcher(final Callable<Pair<byte[], Stat>> callback)
      throws KeeperException, InterruptedException {
    zkClient.exists(SOLR_SECURITY_CONF_PATH,
        new Watcher() {

          @Override
          public void process(WatchedEvent event) {
            // session events are not change events, and do not remove the watcher
            if (EventType.None.equals(event.getType())) {
              return;
            }
            try {

              log.info("Updating [{}] ... ", SOLR_SECURITY_CONF_PATH);

              // remake watch
              final Stat stat = new Stat();
              byte[] data = "{}".getBytes(StandardCharsets.UTF_8);
              if (EventType.NodeDeleted.equals(event.getType())) {
                // Node deleted, just recreate watch without attempting a read - SOLR-9679
                getZkClient().exists(SOLR_SECURITY_CONF_PATH, this);
              } else {
                data = getZkClient().getData(SOLR_SECURITY_CONF_PATH, this, stat);
              }
              try {
                callback.call(new Pair<>(data, stat));
              } catch (Exception e) {
                log.error("Error running collections node listener", e);
                return;
              }

            } catch (KeeperException e) {
              log.error("A ZK error has occurred", e);
              return;
            } catch (InterruptedException e) {
              log.warn("", e);
              return;
            }
          }
        });
  }

  private void constructState(Set<String> changedCollections) {
    constructState(changedCollections, "general");
  }

  /**
   * Construct the total state view from all sources.
   *
   * @param changedCollections collections that have changed since the last call,
   *                           and that should fire notifications
   */
  private void constructState(Set<String> changedCollections, String caller) {
    if (log.isDebugEnabled()) log.debug("construct new cluster state on structure change {} {}", caller, changedCollections);

    Map<String,ClusterState.CollectionRef> result = new LinkedHashMap<>(watchedCollectionStates.size() + lazyCollectionStates.size());

    clusterStateLock.lock();
    try {
      // Add collections
      watchedCollectionStates.forEach((s, slices) -> {
        result.put(s, new ClusterState.CollectionRef(slices));
      });

      // Finally, add any lazy collections that aren't already accounted for.
      lazyCollectionStates.forEach((s, lazyCollectionRef) -> {
        result.putIfAbsent(s, lazyCollectionRef);
      });

      this.clusterState = new ClusterState(result, -1);
    } finally {
      clusterStateLock.unlock();
    }

    if (log.isDebugEnabled()) {
      log.debug("clusterStateSet: interesting [{}] watched [{}] lazy [{}] total [{}]", collectionWatches.keySet().size(), watchedCollectionStates.keySet().size(), lazyCollectionStates.keySet().size(),
          clusterState.getCollectionStates().size());
    }

    if (log.isTraceEnabled()) {
      log.trace("clusterStateSet: interesting [{}] watched [{}] lazy [{}] total [{}]", collectionWatches.keySet(), watchedCollectionStates.keySet(), lazyCollectionStates.keySet(),
          clusterState.getCollectionStates());
    }

    notifyCloudCollectionsListeners();

    for (String collection : changedCollections) {
      notifyStateWatchers(collection, clusterState.getCollectionOrNull(collection));
    }

  }

  // We don't get a Stat or track versions on getChildren() calls, so force linearization.
  private final Object refreshCollectionListLock = new Object();

  /**
   * Search for any lazy-loadable collections.
   */
  private void refreshCollectionList() throws KeeperException, InterruptedException {
    List<String> children = null;
    try {
      children = zkClient.getChildren(COLLECTIONS_ZKNODE, null, null,true, false);
    } catch (KeeperException.NoNodeException e) {
      log.warn("Error fetching collection names: [{}]", e.getMessage());
      // fall through
    }
    if (children == null || children.isEmpty()) {
      lazyCollectionStates.clear();
      return;
    }

    // Don't lock getUpdateLock() here, we don't need it and it would cause deadlock.
    // Don't mess with watchedCollections, they should self-manage.

    // First, drop any children that disappeared.
    this.lazyCollectionStates.keySet().retainAll(children);
    for (String coll : children) {
      // We will create an eager collection for any interesting collections, so don't add to lazy.
      if (!collectionWatches.containsKey(coll)) {
        // Double check contains just to avoid allocating an object.
        LazyCollectionRef existing = lazyCollectionStates.get(coll);
        if (existing == null) {
          lazyCollectionStates.putIfAbsent(coll, new LazyCollectionRef(coll));
        }
      }
    }
  }

  // We don't get a Stat or track versions on getChildren() calls, so force linearization.
 // private final Object refreshCollectionsSetLock = new Object();
  // Ensures that only the latest getChildren fetch gets applied.
  private final AtomicReference<Set<String>> lastFetchedCollectionSet = new AtomicReference<>();

  /**
   * Register a CloudCollectionsListener to be called when the set of collections within a cloud changes.
   */
  public void registerCloudCollectionsListener(CloudCollectionsListener cloudCollectionsListener) {
    cloudCollectionsListeners.add(cloudCollectionsListener);
    notifyNewCloudCollectionsListener(cloudCollectionsListener);
  }

  /**
   * Remove a registered CloudCollectionsListener.
   */
  public void removeCloudCollectionsListener(CloudCollectionsListener cloudCollectionsListener) {
    cloudCollectionsListeners.remove(cloudCollectionsListener);
  }

  private void notifyNewCloudCollectionsListener(CloudCollectionsListener listener) {
    notifications.submit(()-> listener.onChange(Collections.emptySet(), lastFetchedCollectionSet.get()));
  }

  private void notifyCloudCollectionsListeners() {
    notifyCloudCollectionsListeners(true);
  }

  private void notifyCloudCollectionsListeners(boolean notifyIfSame) {
    if (log.isDebugEnabled()) log.debug("Notify cloud collection listeners {}", notifyIfSame);
    Set<String> newCollections;
    Set<String> oldCollections;
    boolean fire = false;

    newCollections = getCurrentCollections();
    oldCollections = lastFetchedCollectionSet.getAndSet(newCollections);
    if (!newCollections.equals(oldCollections) || notifyIfSame) {
      fire = true;
    }

    if (log.isDebugEnabled()) log.debug("Should fire listeners? {}", fire);
    if (fire) {

      cloudCollectionsListeners.forEach(new CloudCollectionsListenerConsumer(oldCollections, newCollections));
    }
  }

  private Set<String> getCurrentCollections() {
    Set<String> collections = new HashSet<>();
    collections.addAll(watchedCollectionStates.keySet());
    collections.addAll(lazyCollectionStates.keySet());
    return collections;
  }

  private class LazyCollectionRef extends ClusterState.CollectionRef {
    private final String collName;
    private volatile DocCollection cachedDocCollection;

    public LazyCollectionRef(String collName) {
      super(null);
      this.collName = collName;
    }

    @Override
    public DocCollection get(boolean allowCached) {
      gets.incrementAndGet();

      boolean shouldFetch = true;
      DocCollection cached = cachedDocCollection;
      if (cached != null) {
        Stat exists = null;
        try {
          exists = zkClient.exists(getCollectionPath(collName), null, true);
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
        if (exists != null && exists.getVersion() == cached.getZNodeVersion()) {
          shouldFetch = false;
        }
      }
      if (shouldFetch) {
        cached = getCollectionLive(ZkStateReader.this, collName);
        cachedDocCollection = cached;
        return cached;
      }

      if (log.isDebugEnabled() && cachedDocCollection == null) {
        log.debug("cached collection is null");
      }
      return cachedDocCollection;
    }

    @Override
    public boolean isLazilyLoaded() {
      return true;
    }

    @Override
    public String toString() {
      return "LazyCollectionRef(" + collName + ")";
    }
  }
  /**
   * Refresh live_nodes.
   */
  private void refreshLiveNodes() throws KeeperException, InterruptedException {
    SortedSet<String> oldLiveNodes;
    SortedSet<String> newLiveNodes = null;
    liveNodesLock.lock();
    try {
      try {

        Stat stat = new Stat();
        List<String> nodeList = zkClient.getChildren(LIVE_NODES_ZKNODE, null, stat, true, false);
        this.liveNodesVersion = stat.getCversion();
        newLiveNodes = new TreeSet<>(nodeList);
      } catch (KeeperException.NoNodeException e) {
        newLiveNodes = emptySortedSet();
      }

      oldLiveNodes = this.liveNodes;
      this.liveNodes = newLiveNodes;

      if (log.isInfoEnabled()) {
        log.info("Updated live nodes from ZooKeeper... ({}) -> ({})", oldLiveNodes.size(), newLiveNodes.size());
      }

      if (log.isTraceEnabled()) {
        log.trace("Updated live nodes from ZooKeeper... {} -> {}", oldLiveNodes, newLiveNodes);
      }

      if (log.isDebugEnabled()) log.debug("Fire live node listeners");
      SortedSet<String> finalNewLiveNodes = newLiveNodes;

      liveNodesListeners.forEach(listener -> {
        notifications.submit(() -> {
          if (listener.onChange(new TreeSet<>(finalNewLiveNodes))) {
            removeLiveNodesListener(listener);
          }
        });
      });

    } catch (AlreadyClosedException e) {

    } finally {
      liveNodesLock.unlock();
    }
  }

  public void registerClusterPropertiesListener(ClusterPropertiesListener listener) {
    // fire it once with current properties
    if (listener.onChange(getClusterProperties())) {
      removeClusterPropertiesListener(listener);
    } else {
      clusterPropertiesListeners.add(listener);
    }
  }

  public void removeClusterPropertiesListener(ClusterPropertiesListener listener) {
    clusterPropertiesListeners.remove(listener);
  }

  public void registerLiveNodesListener(LiveNodesListener listener) {
    // fire it once with current live nodes

    if (listener.onChange(new TreeSet<>(liveNodes))) {
      removeLiveNodesListener(listener);
    }

    liveNodesListeners.add(listener);
  }

  public void removeLiveNodesListener(LiveNodesListener listener) {
    liveNodesListeners.remove(listener);
  }

  /**
   * @return information about the cluster from ZooKeeper
   */
  public ClusterState getClusterState() {
    return clusterState;
  }

  public Set<String> getLiveNodes() {
    return liveNodes;
  }

  public void close() {
    if (log.isDebugEnabled()) log.debug("Closing ZkStateReader");
    assert closeTracker != null ? closeTracker.close() : true;

    closed = true;
    try {
      IOUtils.closeQuietly(clusterPropertiesWatcher);
      Future<?> cpc = collectionPropsCacheCleaner;
      if (cpc != null) {
        cpc.cancel(true);
      }
      stateWatchersMap.forEach((s, stateWatcher) -> {
        IOUtils.closeQuietly(stateWatcher);
        stateWatcher.removeWatch();
      });
      stateWatchersMap.clear();

      IOUtils.closeQuietly(this.liveNodesWatcher);
      IOUtils.closeQuietly(this.collectionsChildWatcher);
      if (closeClient) {
        IOUtils.closeQuietly(zkClient);
      }

      //      if (notifications != null) {
      //        notifications.shutdownNow();
      //      }

      //      waitLatches.forEach(c -> { for (int i = 0; i < c.getCount(); i++) c.countDown(); });
      //      waitLatches.clear();

    } finally {
      assert ObjectReleaseTracker.release(this);
    }

  }

  public String getLeaderUrl(String collection, String shard, int timeout) throws InterruptedException, TimeoutException {
    Replica replica = getLeaderRetry(collection, shard, timeout);
    return replica.getCoreUrl();
  }

  public Replica getLeader(String collection, String shard) {
    return getLeader(getClusterState().getCollection(collection), shard);
  }

  private Replica getLeader(DocCollection docCollection, String shard) {
    Replica replica = docCollection != null ? docCollection.getLeader(shard) : null;
    if (replica != null && replica.getState() == Replica.State.ACTIVE) {
      return replica;
    }
    return null;
  }

//  public Replica getLeader(String collection, String shard) {
//    if (clusterState != null) {
//      DocCollection docCollection = clusterState.getCollectionOrNull(collection);
//      Replica replica = docCollection != null ? docCollection.getLeader(shard) : null;
//      if (replica != null && getClusterState().liveNodesContain(replica.getNodeName())) {
//        return replica;
//      }
//    }
//    return null;
//  }

  public boolean isNodeLive(String node) {
    return getLiveNodes().contains(node);
  }

  public void setNode(String node) {
    this.node = node;
  }

  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(String collection, String shard) throws InterruptedException, TimeoutException {
    return getLeaderRetry(collection, shard, GET_LEADER_RETRY_DEFAULT_TIMEOUT);
  }

  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(String collection, String shard, int timeout) throws InterruptedException, TimeoutException {
    return getLeaderRetry(collection, shard, timeout, true);
  }

  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(String collection, String shard, int timeout, boolean mustBeLive) throws InterruptedException, TimeoutException {
    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll != null) {
      Slice slice = coll.getSlice(shard);
      if (slice  != null) {
        Replica leader = slice.getLeader();
        if (leader != null && isNodeLive(leader.getNodeName())) {
          return leader;
        }
      }
    }
    AtomicReference<Replica> returnLeader = new AtomicReference<>();
    try {
      waitForState(collection, timeout, TimeUnit.MILLISECONDS, (n, c) -> {
        if (c == null)
          return false;
        Slice slice = c.getSlice(shard);
        if (slice == null) return false;
        Replica zkLeader = null;
        Replica leader = slice.getLeader();
        if (leader != null && leader.getState() == Replica.State.ACTIVE) {
          if (isNodeLive(leader.getNodeName())) {
            returnLeader.set(leader);
            return true;
          }

          if (!mustBeLive) {
            if (zkLeader == null) {
              zkLeader = getLeaderProps(collection, coll.getId(), shard);
            }
            if (zkLeader != null && zkLeader.getName().equals(leader.getName())) {
              returnLeader.set(leader);
              return true;
            }
          }
        }
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          if ("true".equals(replica.getProperty(LEADER_PROP)) && replica.getState() == Replica.State.ACTIVE) {
            if (isNodeLive(replica.getNodeName())) {
              returnLeader.set(replica);
              return true;
            }
            if (!mustBeLive) {
              if (zkLeader == null) {
                zkLeader = getLeaderProps(collection, coll.getId(), shard);
              }
              if (zkLeader != null && zkLeader.getName().equals(replica.getName())) {
                returnLeader.set(replica);
                return true;
              }
            }
          }
        }

        return false;
      });
    } catch (TimeoutException e) {
      throw new TimeoutException("No registered leader was found after waiting for "
          + timeout + "ms " + ", collection: " + collection + " slice: " + shard + " saw state=" + clusterState.getCollectionOrNull(collection)
          + " with live_nodes=" + liveNodes + " zkLeaderNode=" + getLeaderProps(collection, coll.getId(), shard));
    }

    Replica leader = returnLeader.get();

    if (leader == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "No registered leader was found "
          + "collection: " + collection + " slice: " + shard + " saw state=" + clusterState.getCollectionOrNull(collection)
          + " with live_nodes=" + liveNodes + " zkLeaderNode=" + getLeaderProps(collection, coll.getId(), shard));
    }

    return leader;
  }

  public Replica getLeaderProps(final String collection, long collId, final String slice) {

    try {
      byte[] data = zkClient.getData(ZkStateReader.getShardLeadersPath(collection, slice), null, null);
      ZkCoreNodeProps leaderProps = new ZkCoreNodeProps(ZkNodeProps.load(data));
      String name = leaderProps.getNodeProps().getStr(ZkStateReader.CORE_NAME_PROP);
      leaderProps.getNodeProps().getProperties().remove(ZkStateReader.CORE_NAME_PROP);

      // MRM TODO: - right key for leader name?
      return new Replica(name, leaderProps.getNodeProps().getProperties(), collection, collId, slice, this);

    } catch (KeeperException.NoNodeException e) {
      return null;
    } catch (Exception e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

  }

  /**
   * Get path where shard leader properties live in zookeeper.
   */
  public static String getShardLeadersPath(String collection, String shardId) {
    return COLLECTIONS_ZKNODE + "/" + collection + "/"
        + SHARD_LEADERS_ZKNODE + (shardId != null ? ("/" + shardId)
        : "") + "/leader";
  }

  /**
   * Get path where shard leader elections ephemeral nodes are.
   */
  public static String getShardLeadersElectPath(String collection, String shardId) {
    return COLLECTIONS_ZKNODE + "/" + collection + "/"
        + LEADER_ELECT_ZKNODE + (shardId != null ? ("/" + shardId + "/" + ELECTION_NODE)
        : "");
  }


  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName) {
    return getReplicaProps(collection, shardId, thisCoreNodeName, null);
  }

  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
                                               Replica.State mustMatchStateFilter) {
    return getReplicaProps(collection, shardId, thisCoreNodeName, mustMatchStateFilter, null);
  }

  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
                                               Replica.State mustMatchStateFilter, Replica.State mustMatchStateFilter2) {
    //TODO: We don't need all these getReplicaProps method overloading. Also, it's odd that the default is to return replicas of type TLOG and NRT only
    return getReplicaProps(collection, shardId, thisCoreNodeName, mustMatchStateFilter, mustMatchStateFilter2, EnumSet.of(Replica.Type.TLOG, Replica.Type.NRT));
  }

  public List<Replica> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
                                               Replica.State mustMatchStateFilter, Replica.State mustMatchStateFilter2, final EnumSet<Replica.Type> acceptReplicaType) {
    assert thisCoreNodeName != null;
    ClusterState clusterState = this.clusterState;
    if (clusterState == null) {
      return null;
    }
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection == null || docCollection.getSlicesMap() == null) {
      return null;
    }

    Map<String, Slice> slices = docCollection.getSlicesMap();
    Slice replicas = slices.get(shardId);
    if (replicas == null) {
      return null;
    }

    Map<String, Replica> shardMap = replicas.getReplicasMap();
    List<Replica> nodes = new ArrayList<>(shardMap.size());
    for (Entry<String, Replica> entry : shardMap.entrySet().stream().filter((e) -> acceptReplicaType.contains(e.getValue().getType())).collect(Collectors.toList())) {
      Replica nodeProps = entry.getValue();

      String coreNodeName = entry.getValue().getName();

      if (liveNodes.contains(nodeProps.getNodeName()) && !coreNodeName.equals(thisCoreNodeName)) {
        if (mustMatchStateFilter == null || (mustMatchStateFilter == nodeProps.getState() || mustMatchStateFilter2 == nodeProps.getState())) {
          nodes.add(nodeProps);
        }
      }
    }
    if (nodes.size() == 0) {
      // no replicas
      return null;
    }

    return nodes;
  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }

  /**
   * Get a cluster property
   * <p>
   * N.B. Cluster properties are updated via ZK watchers, and so may not necessarily
   * be completely up-to-date.  If you need to get the latest version, then use a
   * {@link ClusterProperties} instance.
   *
   * @param key          the property to read
   * @param defaultValue a default value to use if no such property exists
   * @param <T>          the type of the property
   * @return the cluster property, or a default if the property is not set
   */
  @SuppressWarnings("unchecked")
  public <T> T getClusterProperty(String key, T defaultValue) {
    T value = (T) Utils.getObjectByPath(clusterProperties, false, key);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Same as the above but allows a full json path as a list of parts
   *
   * @param keyPath      path to the property example ["collectionDefauls", "numShards"]
   * @param defaultValue a default value to use if no such property exists
   * @return the cluster property, or a default if the property is not set
   */
  @SuppressWarnings({"unchecked"})
  public <T> T getClusterProperty(List<String> keyPath, T defaultValue) {
    T value = (T) Utils.getObjectByPath(clusterProperties, false, keyPath);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Get all cluster properties for this cluster
   * <p>
   * N.B. Cluster properties are updated via ZK watchers, and so may not necessarily
   * be completely up-to-date.  If you need to get the latest version, then use a
   * {@link ClusterProperties} instance.
   *
   * @return a Map of cluster properties
   */
  public Map<String, Object> getClusterProperties() {
    return Collections.unmodifiableMap(clusterProperties);
  }

  private final ClusterPropsWatcher clusterPropertiesWatcher = new ClusterPropsWatcher(ZkStateReader.CLUSTER_PROPS);

  @SuppressWarnings("unchecked")
  private void loadClusterProperties() {
    try {
        try {
          IOUtils.closeQuietly(clusterPropertiesWatcher);
          byte[] data = zkClient.getData(ZkStateReader.CLUSTER_PROPS, clusterPropertiesWatcher, new Stat(), true);
          this.clusterProperties = ClusterProperties.convertCollectionDefaultsToNestedFormat((Map<String, Object>) Utils.fromJSON(data));
          log.debug("Loaded cluster properties: {}", this.clusterProperties);
          clusterPropertiesListeners.forEach((it) -> {
            notifications.submit(()-> it.onChange(getClusterProperties()));
          });
          return;
        } catch (KeeperException.NoNodeException e) {
          this.clusterProperties = Collections.emptyMap();
          if (log.isDebugEnabled()) {
            log.debug("Loaded empty cluster properties");
          }
        }
    } catch (KeeperException e) {
      log.error("Error reading cluster properties from zookeeper", SolrZkClient.checkInterrupted(e));
    } catch (InterruptedException e) {
      log.info("interrupted");
    }

  }

  /**
   * Get collection properties for a given collection. If the collection is watched, simply return it from the cache,
   * otherwise fetch it directly from zookeeper. This is a convenience for {@code getCollectionProperties(collection,0)}
   *
   * @param collection the collection for which properties are desired
   * @return a map representing the key/value properties for the collection.
   */
  public Map<String, String> getCollectionProperties(final String collection) {
    return getCollectionProperties(collection, 0);
  }

  /**
   * Get and cache collection properties for a given collection. If the collection is watched, or still cached
   * simply return it from the cache, otherwise fetch it directly from zookeeper and retain the value for at
   * least cacheForMillis milliseconds. Cached properties are watched in zookeeper and updated automatically.
   * This version of {@code getCollectionProperties} should be used when properties need to be consulted
   * frequently in the absence of an active {@link CollectionPropsWatcher}.
   *
   * @param collection     The collection for which properties are desired
   * @param cacheForMillis The minimum number of milliseconds to maintain a cache for the specified collection's
   *                       properties. Setting a {@code CollectionPropsWatcher} will override this value and retain
   *                       the cache for the life of the watcher. A lack of changes in zookeeper may allow the
   *                       caching to remain for a greater duration up to the cycle time of {@link CacheCleaner}.
   *                       Passing zero for this value will explicitly remove the cached copy if and only if it is
   *                       due to expire and no watch exists. Any positive value will extend the expiration time
   *                       if required.
   * @return a map representing the key/value properties for the collection.
   */
  public Map<String, String> getCollectionProperties(final String collection, long cacheForMillis) {
    PropsWatcher watcher = null;
    if (cacheForMillis > 0) {
      watcher = collectionPropsWatchers.compute(collection, (c, w) -> w == null ? new PropsWatcher(c, cacheForMillis) : w.renew(cacheForMillis));
    }
    VersionedCollectionProps vprops = watchedCollectionProps.get(collection);
    boolean haveUnexpiredProps = vprops != null && vprops.cacheUntilNs > System.nanoTime();
    long untilNs = System.nanoTime() + TimeUnit.NANOSECONDS.convert(cacheForMillis, TimeUnit.MILLISECONDS);
    Map<String,String> properties;
    if (haveUnexpiredProps) {
      properties = vprops.props;
      vprops.cacheUntilNs = Math.max(vprops.cacheUntilNs, untilNs);
    } else {
      try {
        VersionedCollectionProps vcp = fetchCollectionProperties(collection, watcher);
        properties = vcp.props;
        if (cacheForMillis > 0) {
          vcp.cacheUntilNs = untilNs;
          watchedCollectionProps.put(collection, vcp);
        } else {
          // we're synchronized on watchedCollectionProps and we can only get here if we have found an expired
          // vprops above, so it is safe to remove the cached value and let the GC free up some mem a bit sooner.
          if (!collectionPropsObservers.containsKey(collection)) {
            watchedCollectionProps.remove(collection);
          }
        }
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading collection properties", SolrZkClient.checkInterrupted(e));
      }
    }
    return properties;
  }

  private static class VersionedCollectionProps {
    int zkVersion;
    Map<String, String> props;
    long cacheUntilNs = 0;

    VersionedCollectionProps(int zkVersion, Map<String, String> props) {
      this.zkVersion = zkVersion;
      this.props = props;
    }
  }

  public static String getCollectionPropsPath(final String collection) {
    return COLLECTIONS_ZKNODE + '/' + collection + '/' + COLLECTION_PROPS_ZKNODE;
  }

  @SuppressWarnings("unchecked")
  private VersionedCollectionProps fetchCollectionProperties(String collection, PropsWatcher watcher) throws KeeperException, InterruptedException {
    final String znodePath = getCollectionPropsPath(collection);
    // lazy init cache cleaner once we know someone is using collection properties.
    if (collectionPropsCacheCleaner == null) {
      synchronized (this) { // There can be only one! :)
        if (collectionPropsCacheCleaner == null) {
          collectionPropsCacheCleaner = notifications.submit(new CacheCleaner());
        }
      }
    }

    try {
      IOUtils.closeQuietly(watcher);
      Stat stat = new Stat();
      byte[] data = zkClient.getData(znodePath, watcher, stat, true);
      return new VersionedCollectionProps(stat.getVersion(), (Map<String,String>) Utils.fromJSON(data));
    } catch (ClassCastException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to parse collection properties for collection " + collection, e);
    } catch (KeeperException.NoNodeException e) {
      return new VersionedCollectionProps(-1, EMPTY_MAP);
    }
  }

  /**
   * Returns the content of /security.json from ZooKeeper as a Map
   * If the files doesn't exist, it returns null.
   */
  @SuppressWarnings({"unchecked"})
  public ConfigData getSecurityProps(boolean getFresh) {
    if (!getFresh) {
      if (securityData == null) return new ConfigData(EMPTY_MAP, -1);
      return new ConfigData(securityData.data, securityData.version);
    }
    try {
      Stat stat = new Stat();
      if (getZkClient().exists(SOLR_SECURITY_CONF_PATH, true)) {
        final byte[] data = getZkClient().getData(ZkStateReader.SOLR_SECURITY_CONF_PATH, null, stat, true);
        return data != null && data.length > 0 ?
            new ConfigData((Map<String, Object>) Utils.fromJSON(data), stat.getVersion()) :
            null;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading security properties", e);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading security properties", e);
    }
    return null;
  }

  /**
   * Returns the baseURL corresponding to a given node's nodeName --
   * NOTE: does not (currently) imply that the nodeName (or resulting
   * baseURL) exists in the cluster.
   *
   * @lucene.experimental
   */
  @Override
  public String getBaseUrlForNodeName(final String nodeName) {
    return Utils.getBaseUrlForNodeName(nodeName, getClusterProperty(URL_SCHEME, "http"));
  }

  /**
   * Watches a single collection's format2 state.json.
   */
  class CollectionStateWatcher implements Watcher, Closeable {
    private final String coll;
    private volatile StateUpdateWatcher stateUpdateWatcher;

    private final ReentrantLock collectionStateLock = new ReentrantLock();
    private volatile boolean closed;

    CollectionStateWatcher(String coll) {
      this.coll = coll;
      String stateUpdatesPath = ZkStateReader.getCollectionStateUpdatesPath(coll);
      stateUpdateWatcher = new StateUpdateWatcher(stateUpdatesPath);
    }

    @Override
    public void process(WatchedEvent event) {
      if (zkClient.isClosed() || closed) return;

      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }

      if (closed) return;

      if (node != null) {
        MDCLoggingContext.setNode(node);
      }

      if (!collectionWatches.containsKey(coll)) {
        // This collection is no longer interesting, stop watching.
        if (log.isDebugEnabled()) log.debug("Uninteresting collection {}", coll);
        return;
      }

      Set<String> liveNodes = ZkStateReader.this.liveNodes;
      if (log.isInfoEnabled()) {
        log.info("A cluster state change: [{}] for collection [{}] has occurred - updating... (live nodes size: [{}])", event, coll, liveNodes.size());
      }

      refresh();
    }

    /**
     * Refresh collection state from ZK and leave a watch for future changes.
     * As a side effect, updates {@link #clusterState} and {@link #watchedCollectionStates}
     * with the results of the refresh.
     */
    public void refresh() {
      collectionStateLock.lock();
      try {
        DocCollection newState = fetchCollectionState(coll);
        updateWatchedCollection(coll, newState);
        constructState(Collections.singleton(coll), "state.json watcher");
      } catch (KeeperException e) {
        log.error("Unwatched collection: [{}]", coll, e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Unwatched collection: [{}]", coll, e);
      } finally {
        collectionStateLock.unlock();
      }
    }

    public void createWatch() {
      String collectionCSNPath = getCollectionSCNPath(coll);
      try {
        zkClient.addWatch(collectionCSNPath, this, AddWatchMode.PERSISTENT);
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }

      try {
        zkClient.addWatch(stateUpdateWatcher.stateUpdatesPath, stateUpdateWatcher, AddWatchMode.PERSISTENT);
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    public void removeWatch() {

      String collectionCSNPath = getCollectionSCNPath(coll);
      try {
        zkClient.removeWatches(collectionCSNPath, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }

      try {
        zkClient.removeWatches(stateUpdateWatcher.stateUpdatesPath, stateUpdateWatcher, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    public void refreshStateUpdates() {
      if (log.isDebugEnabled()) log.debug("watch for additional state updates {}", coll);

      try {
        processStateUpdates(stateUpdateWatcher.stateUpdatesPath);
      } catch (Exception e) {
        log.error("Unwatched collection: [{}]", coll, e);
      }
    }

    private void processStateUpdates(String stateUpdatesPath) throws KeeperException, InterruptedException {

      byte[] data = null;

      try {
        data = getZkClient().getData(stateUpdatesPath, null, null, true, false);
      } catch (NoNodeException e) {
        log.info("No node found for {}", stateUpdatesPath);
        return;
      }

      if (data == null) {
        log.info("No data found for {}", stateUpdatesPath);
        return;
      }

      Map<String,Object> m = (Map) fromJSON(data);
      if (log.isDebugEnabled()) log.debug("Got additional state updates {}", m);
      if (m.size() == 0) {
        return;
      }

      Integer version = Integer.parseInt((String) m.get("_cs_ver_"));
      if (log.isDebugEnabled()) log.debug("Got additional state updates with version {}", version);

      m.remove("_cs_ver_");

      collectionStateLock.lock();
      try {
        Set<Entry<String,Object>> entrySet = m.entrySet();
        DocCollection docCollection = clusterState.getCollectionOrNull(coll);

        Map<String,ClusterState.CollectionRef> result = new LinkedHashMap<>();
        List<DocCollection> changedCollections = new ArrayList<>();

        if (docCollection != null) {
          // || (version > docCollection.getZNodeVersion() && clusterState.getZkClusterStateVersion() == -1)) {
          if (version < docCollection.getZNodeVersion()) {
            if (log.isDebugEnabled()) log.debug("Will not apply state updates, they are for an older state.json {}, ours is now {}", version, docCollection.getZNodeVersion());
            return;
          }
          for (Entry<String,Object> entry : entrySet) {
            String id = entry.getKey();
            Replica.State state = null;
            if (!entry.getValue().equals("l")) {
              state = Replica.State.shortStateToState((String) entry.getValue());
            }

            Replica replica = docCollection.getReplicaById(id);
            if (log.isDebugEnabled()) log.debug("Got additional state update replica={} id={} ids={} {} {}", replica, id, docCollection.getReplicaByIds(), state == null ? "leader" : state);

            if (replica != null) {

              //     if (replica.getState() != state || entry.getValue().equals("l")) {
              Slice slice = docCollection.getSlice(replica.getSlice());
              Map<String,Replica> replicasMap = new HashMap(slice.getReplicasMap());
              boolean setLeader = false;
              Map properties = new HashMap(replica.getProperties());
              if (entry.getValue().equals("l")) {
                if (log.isDebugEnabled()) log.debug("state is leader, set to active and leader prop");
                properties.put(ZkStateReader.STATE_PROP, Replica.State.ACTIVE.toString());
                properties.put("leader", "true");

                for (Replica r : replicasMap.values()) {
                  if (r == replica) {
                    continue;
                  }
                  if ("true".equals(r.getProperty(LEADER_PROP))) {
                    Map<String,Object> props = new HashMap<>(r.getProperties());
                    props.remove(LEADER_PROP);
                    Replica newReplica = new Replica(r.getName(), props, coll, docCollection.getId(), r.getSlice(), ZkStateReader.this);
                    replicasMap.put(r.getName(), newReplica);
                  }
                }
              } else if (state != null && !properties.get(ZkStateReader.STATE_PROP).equals(state.toString())) {
                if (log.isDebugEnabled()) log.debug("std state, set to {}", state);
                properties.put(ZkStateReader.STATE_PROP, state.toString());
                if ("true".equals(properties.get(LEADER_PROP))) {
                  properties.remove(LEADER_PROP);
                }
              }

              Replica newReplica = new Replica(replica.getName(), properties, coll, docCollection.getId(), replica.getSlice(), ZkStateReader.this);

              if (log.isDebugEnabled()) log.debug("add new replica {}", newReplica);

              replicasMap.put(replica.getName(), newReplica);

              Slice newSlice = new Slice(slice.getName(), replicasMap, slice.getProperties(), coll, replica.id, ZkStateReader.this);

              Map<String,Slice> newSlices = new HashMap<>(docCollection.getSlicesMap());
              newSlices.put(slice.getName(), newSlice);

              if (log.isDebugEnabled()) log.debug("add new slice leader={} {}", newSlice.getLeader(), newSlice);

              DocCollection newDocCollection = new DocCollection(coll, newSlices, docCollection.getProperties(), docCollection.getRouter(), version, true);
              docCollection = newDocCollection;
              changedCollections.add(docCollection);

              result.put(coll, new ClusterState.CollectionRef(newDocCollection));

              //  }
            } else {
              if (log.isDebugEnabled()) log.debug("Could not find core to update local state {} {}", id, state);
            }
          }
          if (changedCollections.size() > 0) {
            clusterStateLock.lock();
            ClusterState cs;
            try {
              watchedCollectionStates.forEach((s, slices) -> {
                if (!s.equals(coll)) {
                  result.put(s, new ClusterState.CollectionRef(slices));
                }
              });

              // Finally, add any lazy collections that aren't already accounted for.
              lazyCollectionStates.forEach((s, lazyCollectionRef) -> {
                if (!s.equals(coll)) {
                  result.putIfAbsent(s, lazyCollectionRef);
                }

              });

              cs = new ClusterState(result, -2);
              if (log.isDebugEnabled()) log.debug("Set a new clusterstate based on update diff {}", cs);
              ZkStateReader.this.clusterState = cs;
            } finally {
              clusterStateLock.unlock();
            }

            notifyCloudCollectionsListeners(true);

            if (log.isDebugEnabled()) log.debug("Notify state watchers for changed collections {}", changedCollections);
            for (DocCollection collection : changedCollections) {
              updateWatchedCollection(collection.getName(), collection);
              notifyStateWatchers(collection.getName(), cs.getCollection(collection.getName()));
            }
          }

          //          for (Map.Entry<String,DocCollection> entry : watchedCollectionStates.entrySet()) {
          //            if (changedCollections.contains(entry.getKey())) {
          //              clusterState = clusterState.copyWith(entry.getKey(), stateClusterState.getCollectionOrNull(entry.getKey()));
          //            }
          //          }
        }

      } catch (Exception e) {
        log.error("exeption trying to process additional updates", e);
      } finally {
        collectionStateLock.unlock();
      }

    }

    @Override
    public void close() throws IOException {
      this.closed = true;
      IOUtils.closeQuietly(stateUpdateWatcher);
//      SolrZooKeeper zk = zkClient.getSolrZooKeeper();
//      if (zk != null) {
//        try {
//          zk.removeWatches(getCollectionSCNPath(coll), this, WatcherType.Any, true);
//        } catch (KeeperException.NoWatcherException e) {
//
//        } catch (Exception e) {
//          if (log.isDebugEnabled()) log.debug("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
//        }
//      }
//      IOUtils.closeQuietly(stateUpdateWatcher);
    }

    private class StateUpdateWatcher implements Watcher, Closeable {
      private final String stateUpdatesPath;
      private volatile boolean closed;

      public StateUpdateWatcher(String stateUpdatesPath) {
        this.stateUpdatesPath = stateUpdatesPath;
      }

      @Override
      public void close() throws IOException {
        this.closed = true;
//        SolrZooKeeper zk = zkClient.getSolrZooKeeper();
//        if (zk != null) {
//          if (stateUpdateWatcher != null) {
//            try {
//              zk.removeWatches(getCollectionStateUpdatesPath(coll), stateUpdateWatcher, WatcherType.Any, true);
//            } catch (KeeperException.NoWatcherException e) {
//
//            } catch (Exception e) {
//              if (log.isDebugEnabled()) log.debug("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
//            }
//          }
//        }
      }

      @Override
      public void process(WatchedEvent event) {
        if (zkClient.isClosed() || closed) return;
        if (log.isDebugEnabled()) log.debug("_statupdates event {}", event);

        try {

          //            if (event.getType() == EventType.NodeDataChanged ||
          //                event.getType() == EventType.NodeDeleted || event.getType() == EventType.NodeCreated) {
          processStateUpdates(stateUpdatesPath);
          //            }

        } catch (AlreadyClosedException e) {

        } catch (Exception e) {
          log.error("Unwatched collection: [{}]", coll, e);
        }
      }

    }
  }

  /**
   * Watches collection properties
   */
  class PropsWatcher implements Watcher, Closeable {
    private final String coll;
    private long watchUntilNs;

    PropsWatcher(String coll) {
      this.coll = coll;
      watchUntilNs = 0;
    }

    PropsWatcher(String coll, long forMillis) {
      this.coll = coll;
      watchUntilNs = System.nanoTime() + TimeUnit.NANOSECONDS.convert(forMillis, TimeUnit.MILLISECONDS);
    }

    public PropsWatcher renew(long forMillis) {
      watchUntilNs = System.nanoTime() + TimeUnit.NANOSECONDS.convert(forMillis, TimeUnit.MILLISECONDS);
      return this;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      if (closed) return;
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }
      boolean expired = System.nanoTime() > watchUntilNs;
      if (!collectionPropsObservers.containsKey(coll) && expired) {
        // No one can be notified of the change, we can ignore it and "unset" the watch
        log.debug("Ignoring property change for collection {}", coll);
        return;
      }

      log.info("A collection property change: [{}] for collection [{}] has occurred - updating...",
          event, coll);

      refreshAndWatch(true);
    }

    @Override
    public void close() throws IOException {
      String znodePath = getCollectionPropsPath(coll);

      try {
        zkClient.removeWatches(znodePath, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        if (log.isDebugEnabled()) log.debug("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }

    }

    /**
     * Refresh collection properties from ZK and leave a watch for future changes. Updates the properties in
     * watchedCollectionProps with the results of the refresh. Optionally notifies watchers
     */
    void refreshAndWatch(boolean notifyWatchers) {
      try {

        VersionedCollectionProps vcp = fetchCollectionProperties(coll, this);
        Map<String,String> properties = vcp.props;
        VersionedCollectionProps existingVcp = watchedCollectionProps.get(coll);
        if (existingVcp == null ||                   // never called before, record what we found
            vcp.zkVersion > existingVcp.zkVersion || // newer info we should update
            vcp.zkVersion == -1) {                   // node was deleted start over
          watchedCollectionProps.put(coll, vcp);
          if (notifyWatchers) {
            notifyPropsWatchers(coll, properties);
          }
          if (vcp.zkVersion == -1 && existingVcp != null) { // Collection DELETE detected

            // We should not be caching a collection that has been deleted.
            watchedCollectionProps.remove(coll);

            // core ref counting not relevant here, don't need canRemove(), we just sent
            // a notification of an empty set of properties, no reason to watch what doesn't exist.
            collectionPropsObservers.remove(coll);

            // This is the one time we know it's safe to throw this out. We just failed to set the watch
            // due to an NoNodeException, so it isn't held by ZK and can't re-set itself due to an update.
            collectionPropsWatchers.remove(coll);
          }
        }

      } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("Lost collection property watcher for {} due to ZK error", coll, e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Lost collection property watcher for {} due to the thread being interrupted", coll, e);
      }
    }
  }

  /**
   * Watches /collections children .
   */
  // MRM TODO: persistent watch
  class CollectionsChildWatcher implements Watcher, Closeable {
    volatile boolean watchRemoved = true;
    @Override
    public void process(WatchedEvent event) {
      if (ZkStateReader.this.closed) {
        return;
      }
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      if (log.isDebugEnabled()) log.debug("A collections change: [{}], has occurred - updating...", event);
      try {
        refresh();
      } catch (Exception e) {
        log.error("An error has occurred", e);
        return;
      }

      constructState(Collections.emptySet(), "collection child watcher");
    }

    public void refresh() {
      try {
        refreshCollectionList();
      } catch (AlreadyClosedException e) {

      } catch (KeeperException e) {
        log.error("A ZK error has occurred", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }

    public void createWatch() {
      watchRemoved = false;
      try {
        zkClient.addWatch(COLLECTIONS_ZKNODE, this, AddWatchMode.PERSISTENT);
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    public void removeWatch() {
      if (watchRemoved) return;
      watchRemoved = true;
      try {
        zkClient.removeWatches(COLLECTIONS_ZKNODE, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        log.warn("Exception removing watch", e);
      }
    }

    @Override
    public void close() throws IOException {
      removeWatch();
    }
  }

  /**
   * Watches the live_nodes and syncs changes.
   */
  class LiveNodeWatcher implements Watcher, Closeable {

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }

      if (log.isDebugEnabled()) {
        log.debug("A live node change: [{}], has occurred - updating... (previous live nodes size: [{}])", event, liveNodes.size());
      }
      refresh();
    }

    public void refresh() {
      try {
        refreshLiveNodes();
      } catch (KeeperException.SessionExpiredException e) {
        // okay
      } catch (Exception e) {
        log.error("A ZK error has occurred", e);
      }
    }

    public void createWatch() {
      try {
        zkClient.addWatch(LIVE_NODES_ZKNODE, this, AddWatchMode.PERSISTENT);
      } catch (Exception e) {
        log.warn("Exception creating watch", e);
        throw new SolrException(ErrorCode.SERVER_ERROR, "Exception creating watch", e);
      }
    }

    public void removeWatch() {
      try {
        zkClient.removeWatches(LIVE_NODES_ZKNODE, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        log.warn("Exception removing watch", e);
      }
    }

    @Override
    public void close() throws IOException {
      removeWatch();
    }
  }

  public static DocCollection getCollectionLive(ZkStateReader zkStateReader, String coll) {
    try {
      return zkStateReader.fetchCollectionState(coll);
    } catch (KeeperException.SessionExpiredException | InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new AlreadyClosedException("Could not load collection from ZK: " + coll, e);
    } catch (KeeperException e) {
      log.error("getCollectionLive", e);
      throw new SolrException(ErrorCode.BAD_REQUEST, "Could not load collection from ZK: " + coll, e);
    }
  }

  private DocCollection fetchCollectionState(String coll) throws KeeperException, InterruptedException {
    try {
      String collectionPath = getCollectionPath(coll);
      String collectionCSNPath = getCollectionSCNPath(coll);
      if (log.isDebugEnabled()) log.debug("Looking at fetching full clusterstate");
      Stat exists = zkClient.exists(collectionCSNPath, null, true);
      int version = 0;
      if (exists != null) {

        Stat stateStat = zkClient.exists(collectionPath, null, true, false);
        if (stateStat != null) {
          version = stateStat.getVersion();
          if (log.isDebugEnabled()) log.debug("version for cs is {}", version);
          // version we would get
          DocCollection docCollection = watchedCollectionStates.get(coll);
          if (docCollection != null) {
            int localVersion = docCollection.getZNodeVersion();
            if (log.isDebugEnabled()) log.debug("found version {}, our local version is {}, has updates {}", version, localVersion, docCollection.hasStateUpdates());
            if (docCollection.hasStateUpdates()) {
              if (localVersion > version) {
                return docCollection;
              }
            } else {
              if (localVersion >= version) {
                return docCollection;
              }
            }
          }
        }
        if (log.isDebugEnabled()) log.debug("getting latest state.json knowing it's at least {}", version);
        Stat stat = new Stat();
        byte[] data = zkClient.getData(collectionPath, null, stat, true);
        if (data == null) return null;
        ClusterState state = ClusterState.createFromJson(this, stat.getVersion(), data);
        ClusterState.CollectionRef collectionRef = state.getCollectionStates().get(coll);
        return collectionRef == null ? null : collectionRef.get();
      }

    } catch (AlreadyClosedException e) {

    }
    return null;
  }

  public static String getCollectionPathRoot(String coll) {
    return COLLECTIONS_ZKNODE + "/" + coll;
  }

  public static String getCollectionPath(String coll) {
    return getCollectionPathRoot(coll) + "/state.json";
  }

  public static String getCollectionSCNPath(String coll) {
    return getCollectionPathRoot(coll) + "/" + STRUCTURE_CHANGE_NOTIFIER;
  }

  public static String getCollectionStateUpdatesPath(String coll) {
    return getCollectionPathRoot(coll) + "/" + STATE_UPDATES;
  }
  /**
   * Notify this reader that a local Core is a member of a collection, and so that collection
   * state should be watched.
   * <p>
   * Not a public API.  This method should only be called from ZkController.
   * <p>
   * The number of cores per-collection is tracked, and adding multiple cores from the same
   * collection does not increase the number of watches.
   *
   * @param collection the collection that the core is a member of
   * @see ZkStateReader#unregisterCore(String)
   */
  public void registerCore(String collection) {

    if (log.isDebugEnabled()) log.debug("register core for collection {}", collection);
    if (collection == null) {
      throw new IllegalArgumentException("Collection cannot be null");
    }

    AtomicBoolean reconstructState = new AtomicBoolean(false);
    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) {
        reconstructState.set(true);
        v = new CollectionWatch<>();
        CollectionStateWatcher sw = new CollectionStateWatcher(collection);
        stateWatchersMap.put(collection, sw);
        sw.createWatch();
        sw.refresh();
        sw.refreshStateUpdates();
      }
      v.coreRefCount.incrementAndGet();
      return v;
    });

  }

  public boolean watched(String collection) {
    return collectionWatches.containsKey(collection);
  }

  /**
   * Notify this reader that a local core that is a member of a collection has been closed.
   * <p>
   * Not a public API.  This method should only be called from ZkController.
   * <p>
   * If no cores are registered for a collection, and there are no {@link org.apache.solr.common.cloud.CollectionStateWatcher}s
   * for that collection either, the collection watch will be removed.
   *
   * @param collection the collection that the core belongs to
   */
  public void unregisterCore(String collection) {
    if (collection == null) {
      throw new IllegalArgumentException("Collection cannot be null");
    }
    AtomicBoolean reconstructState = new AtomicBoolean(false);

    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) return null;
      if (v.coreRefCount.get() > 0)
        v.coreRefCount.decrementAndGet();
      if (v.canBeRemoved()) {
        watchedCollectionStates.remove(collection);
        CollectionStateWatcher watcher = stateWatchersMap.remove(collection);
        if (watcher != null) {
          IOUtils.closeQuietly(watcher);
          watcher.removeWatch();
        }
        lazyCollectionStates.put(collection, new LazyCollectionRef(collection));
        reconstructState.set(true);
        return null;
      }
      return v;
    });

    if (reconstructState.get()) {
      constructState(Collections.emptySet());
    }

  }

  /**
   * Register a CollectionStateWatcher to be called when the state of a collection changes
   * <em>or</em> the set of live nodes changes.
   *
   * <p>
   * The Watcher will automatically be removed when it's
   * <code>onStateChanged</code> returns <code>true</code>
   * </p>
   *
   * <p>
   * This is method is just syntactic sugar for registering both a {@link DocCollectionWatcher} and
   * a {@link LiveNodesListener}.  Callers that only care about one or the other (but not both) are
   * encouraged to use the more specific methods register methods as it may reduce the number of
   * ZooKeeper watchers needed, and reduce the amount of network/cpu used.
   * </p>
   *
   * @see #registerDocCollectionWatcher
   * @see #registerLiveNodesListener
   */
  public void registerCollectionStateWatcher(String collection, org.apache.solr.common.cloud.CollectionStateWatcher stateWatcher) {
    final DocCollectionAndLiveNodesWatcherWrapper wrapper
        = new DocCollectionAndLiveNodesWatcherWrapper(collection, stateWatcher);

    registerDocCollectionWatcher(collection, wrapper);
    registerLiveNodesListener(wrapper);

    DocCollection state = clusterState.getCollectionOrNull(collection);
    if (stateWatcher.onStateChanged(liveNodes, state) == true) {
      removeCollectionStateWatcher(collection, stateWatcher);
    }
  }

  /**
   * Register a DocCollectionWatcher to be called when the state of a collection changes
   *
   * <p>
   * The Watcher will automatically be removed when it's
   * <code>onStateChanged</code> returns <code>true</code>
   * </p>
   */
  public void registerDocCollectionWatcher(String collection, DocCollectionWatcher stateWatcher) {
    if (log.isDebugEnabled()) log.debug("registerDocCollectionWatcher {}", collection);

    if (collection == null) {
      throw new IllegalArgumentException("Collection cannot be null");
    }

    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) {
        v = new CollectionWatch<>();
        CollectionStateWatcher sw = new CollectionStateWatcher(collection);
        stateWatchersMap.put(collection, sw);
        sw.createWatch();
        sw.refresh();
        sw.refreshStateUpdates();
      }
      v.stateWatchers.add(stateWatcher);
      return v;
    });

    DocCollection state = clusterState.getCollectionOrNull(collection);
    if (stateWatcher.onStateChanged(state) == true) {
      removeDocCollectionWatcher(collection, stateWatcher);
    }

  }

  /**
   * Block until a CollectionStatePredicate returns true, or the wait times out
   *
   * <p>
   * Note that the predicate may be called again even after it has returned true, so
   * implementors should avoid changing state within the predicate call itself.
   * </p>
   *
   * <p>
   * This implementation utilizes {@link org.apache.solr.common.cloud.CollectionStateWatcher} internally.
   * Callers that don't care about liveNodes are encouraged to use a {@link DocCollection} {@link Predicate}
   * instead
   * </p>
   *
   * @param collection the collection to watch
   * @param wait       how long to wait
   * @param unit       the units of the wait parameter
   * @param predicate  the predicate to call on state changes
   * @throws InterruptedException on interrupt
   * @throws TimeoutException     on timeout
   * @see #registerCollectionStateWatcher
   */
  public void waitForState(final String collection, long wait, TimeUnit unit, CollectionStatePredicate predicate)
      throws InterruptedException, TimeoutException {

    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (predicate.matches(getLiveNodes(), coll)) {
      return;
    }
    final CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<DocCollection> docCollection = new AtomicReference<>();
    org.apache.solr.common.cloud.CollectionStateWatcher watcher = new PredicateMatcher(predicate, latch, docCollection).invoke();
    registerCollectionStateWatcher(collection, watcher);
    try {

      // wait for the watcher predicate to return true, or time out
      if (!latch.await(wait, unit)) {
        coll = clusterState.getCollectionOrNull(collection);
        if (predicate.matches(getLiveNodes(), coll)) {
          return;
        }

        throw new TimeoutException("Timeout waiting to see state for collection=" + collection + " :" + "live=" + liveNodes
                + docCollection.get());
      }
    } finally {
      removeCollectionStateWatcher(collection, watcher);
    }
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit, int shards, int totalReplicas) {
    waitForActiveCollection(collection, wait, unit, shards, totalReplicas, false);
  }

  public void waitForActiveCollection(String collection, long wait, TimeUnit unit, int shards, int totalReplicas, boolean exact) {
    log.info("waitForActiveCollection: {}", collection);
    assert collection != null;
    CollectionStatePredicate predicate = expectedShardsAndActiveReplicas(shards, totalReplicas, exact);

    AtomicReference<DocCollection> state = new AtomicReference<>();
    AtomicReference<Set<String>> liveNodesLastSeen = new AtomicReference<>();
    try {
      waitForState(collection, wait, unit, (n, c) -> {
        state.set(c);
        liveNodesLastSeen.set(n);

        return predicate.matches(n, c);
      });
    } catch (TimeoutException e) {
      throw new RuntimeException("Failed while waiting for active collection" + "\n" + e.getMessage() + " \nShards:" + shards + " Replicas:" + totalReplicas + "\nLive Nodes: " + Arrays.toString(liveNodesLastSeen.get().toArray())
          + "\nLast available state: " + state.get());
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new RuntimeException("", e);
    }

  }

  /**
   * Block until a LiveNodesStatePredicate returns true, or the wait times out
   * <p>
   * Note that the predicate may be called again even after it has returned true, so
   * implementors should avoid changing state within the predicate call itself.
   * </p>
   *
   * @param wait      how long to wait
   * @param unit      the units of the wait parameter
   * @param predicate the predicate to call on state changes
   * @throws InterruptedException on interrupt
   * @throws TimeoutException     on timeout
   */
  public void waitForLiveNodes(long wait, TimeUnit unit, LiveNodesPredicate predicate)
      throws InterruptedException, TimeoutException {

    if (predicate.matches(liveNodes)) {
      return;
    }

    final CountDownLatch latch = new CountDownLatch(1);

    LiveNodesListener listener = (n) -> {
      boolean matches = predicate.matches(n);
      if (matches)
        latch.countDown();
      return matches;
    };

    registerLiveNodesListener(listener);

    try {
      // wait for the watcher predicate to return true, or time out
      if (!latch.await(wait, unit))
        if (predicate.matches(liveNodes)) {
          return;
        }
        throw new TimeoutException("Timeout waiting for live nodes, currently they are: " + liveNodes);

    } finally {
      removeLiveNodesListener(listener);
    }
  }


  /**
   * Remove a watcher from a collection's watch list.
   * <p>
   * This allows Zookeeper watches to be removed if there is no interest in the
   * collection.
   * </p>
   *
   * @param collection the collection
   * @param watcher    the watcher
   * @see #registerCollectionStateWatcher
   */
  public void removeCollectionStateWatcher(String collection, org.apache.solr.common.cloud.CollectionStateWatcher watcher) {
    final DocCollectionAndLiveNodesWatcherWrapper wrapper
        = new DocCollectionAndLiveNodesWatcherWrapper(collection, watcher);

    removeDocCollectionWatcher(collection, wrapper);
    removeLiveNodesListener(wrapper);
  }

  /**
   * Remove a watcher from a collection's watch list.
   * <p>
   * This allows Zookeeper watches to be removed if there is no interest in the
   * collection.
   * </p>
   *
   * @param collection the collection
   * @param watcher    the watcher
   * @see #registerDocCollectionWatcher
   */
  public void removeDocCollectionWatcher(String collection, DocCollectionWatcher watcher) {
    if (log.isDebugEnabled()) log.debug("remove watcher for collection {}", collection);

    if (collection == null) {
      throw new IllegalArgumentException("Collection cannot be null");
    }

    AtomicBoolean reconstructState = new AtomicBoolean(false);

    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) return null;
      v.stateWatchers.remove(watcher);
      if (v.canBeRemoved()) {
        log.info("no longer watch collection {}", collection);
        watchedCollectionStates.remove(collection);
        lazyCollectionStates.put(collection, new LazyCollectionRef(collection));
        CollectionStateWatcher stateWatcher = stateWatchersMap.remove(collection);
        if (stateWatcher != null) {
          IOUtils.closeQuietly(stateWatcher);
          stateWatcher.removeWatch();
        }
        reconstructState.set(true);
        return null;
      }
      return v;
    });

  }

  /* package-private for testing */
  Set<DocCollectionWatcher> getStateWatchers(String collection) {
    if (collection == null) {
      throw new IllegalArgumentException("Collection cannot be null");
    }
    final Set<DocCollectionWatcher> watchers = new HashSet<>();

    collectionWatches.compute(collection, (k, v) -> {
      if (v != null) {
        watchers.addAll(v.stateWatchers);
      }
      return v;
    });

    return watchers;
  }

  // returns true if the state has changed
  private boolean updateWatchedCollection(String coll, DocCollection newState) {
    try {
      if (newState == null) {
        if (log.isDebugEnabled()) log.debug("Removing cached collection state for [{}]", coll);
        watchedCollectionStates.remove(coll);
        IOUtils.closeQuietly(stateWatchersMap.remove(coll));
        lazyCollectionStates.remove(coll);
        if (collectionRemoved != null) {
          collectionRemoved.removed(coll);
        }
        return true;
      }

      boolean updated = false;
      // CAS update loop
      //   while (true) {

      watchedCollectionStates.put(coll, newState);
      if (log.isDebugEnabled()) {
        log.debug("Add data for [{}] ver [{}]", coll, newState.getZNodeVersion());
      }
      updated = true;

      //   }

      // Resolve race with unregisterCore.
      //      if (!collectionWatches.containsKey(coll)) {
      //        watchedCollectionStates.remove(coll);
      //        log.debug("Removing uninteresting collection [{}]", coll);
      //      }

      return updated;
    } catch (Exception e) {
      log.error("Failing updating clusterstate", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  public void registerCollectionPropsWatcher(final String collection, CollectionPropsWatcher propsWatcher) {
    AtomicBoolean watchSet = new AtomicBoolean(false);
    collectionPropsObservers.compute(collection, (k, v) -> {
      if (v == null) {
        v = new CollectionWatch<>();
        watchSet.set(true);
      }
      v.propStateWatchers.add(propsWatcher);
      return v;
    });

    if (watchSet.get()) {
      collectionPropsWatchers.computeIfAbsent(collection, PropsWatcher::new).refreshAndWatch(false);
    }
  }

  public void removeCollectionPropsWatcher(String collection, CollectionPropsWatcher watcher) {
    collectionPropsObservers.compute(collection, (k, v) -> {
      if (v == null)
        return null;
      v.propStateWatchers.remove(watcher);
      if (v.canBeRemoved()) {
        // don't want this to happen in middle of other blocks that might add it back.
        synchronized (watchedCollectionProps) {
          watchedCollectionProps.remove(collection);
        }
        return null;
      }
      return v;
    });
  }

  public void setCollectionRemovedListener(CollectionRemoved listener) {
    this.collectionRemoved = listener;
  }

  public static class ConfigData {
    public Map<String, Object> data;
    public int version;

    public ConfigData() {
    }

    public ConfigData(Map<String, Object> data, int version) {
      this.data = data;
      this.version = version;

    }
  }

  private void notifyStateWatchers(String collection, DocCollection collectionState) {
    if (log.isTraceEnabled()) log.trace("Notify state watchers {} {}", collectionWatches.keySet(), collectionState);

    try {
      notifications.submit(new Notification(collection, collectionState, collectionWatches));
    } catch (RejectedExecutionException e) {
      if (!closed) {
        log.error("Couldn't run collection notifications for {}", collection, e);
      }
    }

  }

  private class Notification implements Runnable {

    final String collection;
    final DocCollection collectionState;

    private final ConcurrentHashMap<String,CollectionWatch<DocCollectionWatcher>> collectionWatches;

    public Notification(String collection, DocCollection collectionState, ConcurrentHashMap<String, CollectionWatch<DocCollectionWatcher>> collectionWatches) {
      this.collection = collection;
      this.collectionState = collectionState;
      this.collectionWatches = collectionWatches;
    }

    @Override
    public void run() {
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }

      CollectionWatch<DocCollectionWatcher> watchers = collectionWatches.get(collection);
      if (watchers != null) {
        try (ParWork work = new ParWork(this)) {
          watchers.stateWatchers.forEach(watcher -> {
           // work.collect("", () -> {
              if (log.isTraceEnabled()) log.debug("Notify DocCollectionWatcher {} {}", watcher, collectionState);
              try {
                if (watcher.onStateChanged(collectionState)) {
                  removeDocCollectionWatcher(collection, watcher);
                }
              } catch (Exception exception) {
                ParWork.propagateInterrupt(exception);
                log.warn("Error on calling watcher", exception);
              }
            });
         // });
        }
      }
    }

  }

  //
  //  Aliases related
  //

  /**
   * Access to the {@link Aliases}.
   */
  public final AliasesManager aliasesManager = new AliasesManager();

  /**
   * Get an immutable copy of the present state of the aliases. References to this object should not be retained
   * in any context where it will be important to know if aliases have changed.
   *
   * @return The current aliases, Aliases.EMPTY if not solr cloud, or no aliases have existed yet. Never returns null.
   */
  public Aliases getAliases() {
    return aliasesManager.getAliases();
  }

  // called by createClusterStateWatchersAndUpdate()
  private void refreshAliases(AliasesManager watcher) throws KeeperException, InterruptedException {
    constructState(Collections.emptySet());
    zkClient.exists(ALIASES, watcher);
    aliasesManager.update();
  }

  /**
   * A class to manage the aliases instance, including watching for changes.
   * There should only ever be one instance of this class
   * per instance of ZkStateReader. Normally it will not be useful to create a new instance since
   * this watcher automatically re-registers itself every time it is updated.
   */
  public class AliasesManager implements Watcher { // the holder is a Zk watcher
    // note: as of this writing, this class if very generic. Is it useful to use for other ZK managed things?
    private final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private volatile Aliases aliases = Aliases.EMPTY;

    public Aliases getAliases() {
      return aliases; // volatile read
    }

    /**
     * Writes an updated {@link Aliases} to zk.
     * It will retry if there are races with other modifications, giving up after 30 seconds with a SolrException.
     * The caller should understand it's possible the aliases has further changed if it examines it.
     */
    public void applyModificationAndExportToZk(UnaryOperator<Aliases> op) {
      // The current aliases hasn't been update()'ed yet -- which is impossible?  Any way just update it first.
      if (aliases.getZNodeVersion() == -1) {
        try {
          boolean updated = update();
          assert updated;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
        } catch (KeeperException e) {
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
        }
      }

      final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      // note: triesLeft tuning is based on ConcurrentCreateRoutedAliasTest
      for (int triesLeft = 5; triesLeft > 0; triesLeft--) {
        // we could synchronize on "this" but there doesn't seem to be a point; we have a retry loop.
        Aliases curAliases = getAliases();
        Aliases modAliases = op.apply(curAliases);
        final byte[] modAliasesJson = modAliases.toJSON();
        if (curAliases == modAliases) {
          log.debug("Current aliases has the desired modification; no further ZK interaction needed.");
          return;
        }

        try {
          try {
            final Stat stat = getZkClient().setData(ALIASES, modAliasesJson, curAliases.getZNodeVersion(), true);
            setIfNewer(Aliases.fromJSON(modAliasesJson, stat.getVersion()));
            return;
          } catch (KeeperException.BadVersionException e) {
            log.debug("{}", e, e);
            log.warn("Couldn't save aliases due to race with another modification; will update and retry until timeout");
            Thread.sleep(250);
            // considered a backoff here, but we really do want to compete strongly since the normal case is
            // that we will do one update and succeed. This is left as a hot loop for limited tries intentionally.
            // More failures than that here probably indicate a bug or a very strange high write frequency usage for
            // aliases.json, timeouts mean zk is being very slow to respond, or this node is being crushed
            // by other processing and just can't find any cpu cycles at all.
            update();
            if (deadlineNanos < System.nanoTime()) {
              throw new SolrException(ErrorCode.SERVER_ERROR, "Timed out trying to update aliases! " +
                  "Either zookeeper or this node may be overloaded.");
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
        } catch (KeeperException e) {
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, e.toString(), e);
        }
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Too many successive version failures trying to update aliases");
    }

    /**
     * Ensures the internal aliases is up to date. If there is a change, return true.
     *
     * @return true if an update was performed
     */
    public boolean update() throws KeeperException, InterruptedException {
      log.debug("Checking ZK for most up to date Aliases {}", ALIASES);
      // Call sync() first to ensure the subsequent read (getData) is up to date.
      // MRM TODO:
      zkClient.getSolrZooKeeper().sync(ALIASES, null, null);
      Stat stat = new Stat();
      final byte[] data = zkClient.getData(ALIASES, null, stat, true);
      return setIfNewer(Aliases.fromJSON(data, stat.getVersion()));
    }

    // ZK Watcher interface
    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }
      try {
        log.debug("Aliases: updating");

        // re-register the watch
        Stat stat = new Stat();
        final byte[] data = zkClient.getData(ALIASES, this, stat, true);
        // note: it'd be nice to avoid possibly needlessly parsing if we don't update aliases but not a big deal
        setIfNewer(Aliases.fromJSON(data, stat.getVersion()));
      } catch (NoNodeException e) {
        // /aliases.json will not always exist
      } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
        // note: aliases.json is required to be present
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("A ZK error has occurred", e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }

    /**
     * Update the internal aliases reference with a new one, provided that its ZK version has increased.
     *
     * @param newAliases the potentially newer version of Aliases
     * @return true if aliases have been updated to a new version, false otherwise
     */
    private boolean setIfNewer(Aliases newAliases) {
      assert newAliases.getZNodeVersion() >= 0;
      synchronized (this) {
        int cmp = Integer.compare(aliases.getZNodeVersion(), newAliases.getZNodeVersion());
        if (cmp < 0) {
          log.debug("Aliases: cmp={}, new definition is: {}", cmp, newAliases);
          aliases = newAliases;
          this.notifyAll();
          return true;
        } else {
          log.debug("Aliases: cmp={}, not overwriting ZK version.", cmp);
          assert cmp != 0 || Arrays.equals(aliases.toJSON(), newAliases.toJSON()) : aliases + " != " + newAliases;
          return false;
        }
      }
    }

  }

  private void notifyPropsWatchers(String collection, Map<String, String> properties) {
    try {
      notifications.submit(new PropsNotification(collection, properties));
    } catch (RejectedExecutionException e) {
      if (!closed) {
        log.error("Couldn't run collection properties notifications for {}", collection, e);
      }
    }
  }

  private class PropsNotification implements Runnable {

    private final String collection;
    private final Map<String, String> collectionProperties;
    private final List<CollectionPropsWatcher> watchers = new ArrayList<>();

    private PropsNotification(String collection, Map<String, String> collectionProperties) {
      this.collection = collection;
      this.collectionProperties = collectionProperties;
      // guarantee delivery of notification regardless of what happens to collectionPropsObservers
      // while we wait our turn in the executor by capturing the list on creation.
      collectionPropsObservers.compute(collection, (k, v) -> {
        if (v == null)
          return null;
        watchers.addAll(v.propStateWatchers);
        return v;
      });
    }

    @Override
    public void run() {
      if (node != null) {
        MDCLoggingContext.setNode(node);
      }
      for (CollectionPropsWatcher watcher : watchers) {
        if (watcher.onStateChanged(collectionProperties)) {
          removeCollectionPropsWatcher(collection, watcher);
        }
      }
    }
  }

  private class CacheCleaner implements Runnable {
    public void run() {
      while (!Thread.interrupted()) {
        try {
          Thread.sleep(60000);
        } catch (InterruptedException e) {
          // Executor shutdown will send us an interrupt
          break;
        }
        watchedCollectionProps.entrySet().removeIf(entry ->
            entry.getValue().cacheUntilNs < System.nanoTime() && !collectionPropsObservers.containsKey(entry.getKey()));
      }
    }
  }

  /**
   * Helper class that acts as both a {@link DocCollectionWatcher} and a {@link LiveNodesListener}
   * while wraping and delegating to a {@link org.apache.solr.common.cloud.CollectionStateWatcher}
   */
  private final class DocCollectionAndLiveNodesWatcherWrapper implements DocCollectionWatcher, LiveNodesListener {
    private final String collectionName;
    private final org.apache.solr.common.cloud.CollectionStateWatcher delegate;

    public int hashCode() {
      return collectionName.hashCode() * delegate.hashCode();
    }

    public boolean equals(Object other) {
      if (other instanceof DocCollectionAndLiveNodesWatcherWrapper) {
        DocCollectionAndLiveNodesWatcherWrapper that
            = (DocCollectionAndLiveNodesWatcherWrapper) other;
        return this.collectionName.equals(that.collectionName)
            && this.delegate.equals(that.delegate);
      }
      return false;
    }

    public DocCollectionAndLiveNodesWatcherWrapper(final String collectionName,
                                                   final org.apache.solr.common.cloud.CollectionStateWatcher delegate) {
      this.collectionName = collectionName;
      this.delegate = delegate;
    }

    @Override
    public boolean onStateChanged(DocCollection collectionState) {
      final boolean result = delegate.onStateChanged(ZkStateReader.this.liveNodes,
          collectionState);
      if (result) {
        // it might be a while before live nodes changes, so proactively remove ourselves
        removeDocCollectionWatcher(collectionName, this);

      }
      return result;
    }

    @Override
    public boolean onChange(SortedSet<String> newLiveNodes) {
      final DocCollection collection = ZkStateReader.this.clusterState.getCollectionOrNull(collectionName);
      final boolean result = delegate.onStateChanged(newLiveNodes, collection);
      if (result) {
        // it might be a while before collection changes, so proactively remove ourselves
        removeLiveNodesListener(this);
      }
      return result;
    }
  }

  public static CollectionStatePredicate expectedShardsAndActiveReplicas(int expectedShards, int expectedReplicas) {
    return expectedShardsAndActiveReplicas(expectedShards, expectedReplicas, false);
  }

  public static CollectionStatePredicate expectedShardsAndActiveReplicas(int expectedShards, int expectedReplicas, boolean exact) {
    return (liveNodes, collectionState) -> {
      if (collectionState == null)
        return false;
      Collection<Slice> activeSlices = collectionState.getActiveSlices();

      if (log.isTraceEnabled()) log.trace("active slices expected={} {} {} allSlices={}",expectedShards, activeSlices.size(), activeSlices, collectionState.getSlices());

      if (!exact) {
        if (activeSlices.size() < expectedShards) {
          return false;
        }
      } else {
        if (activeSlices.size() != expectedShards) {
          return false;
        }
      }

      if (expectedReplicas == 0 && !exact) {
        log.info("0 replicas expected and found, return");
        return true;
      }

      int activeReplicas = 0;
      for (Slice slice : activeSlices) {
        Replica leader = slice.getLeader();
        log.info("slice is {} and leader is {}", slice.getName(), leader);
        if (leader == null) {
          return false;
        }
        for (Replica replica : slice) {
          if (replica.getState() == Replica.State.ACTIVE && liveNodes.contains(replica.getNodeName())) {
            activeReplicas++;
          }
        }
        log.info("slice is {} and active replicas is {}, expected {} liveNodes={}", slice.getName(), activeReplicas, expectedReplicas, liveNodes);
      }
      if (!exact) {
        if (activeReplicas >= expectedReplicas) {
          return true;
        }
      } else {
        if (activeReplicas == expectedReplicas) {
          return true;
        }
      }

      return false;
    };
  }

  /* Checks both shard replcia consistency and against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(String collection) throws Exception {
    checkShardConsistency(collection, true, false);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  public void checkShardConsistency(String collection, boolean checkVsControl, boolean verbose)
      throws Exception {
    checkShardConsistency(collection, checkVsControl, verbose, null, null);
  }

  /* Checks shard consistency and optionally checks against the control shard.
   * The test will be failed if differences are found.
   */
  protected void checkShardConsistency(String collection, boolean checkVsControl, boolean verbose, Set<String> addFails, Set<String> deleteFails)
      throws Exception {

    Set<String> theShards = getClusterState().getCollection(collection).getSlicesMap().keySet();
    String failMessage = null;
    for (String shard : theShards) {
      String shardFailMessage = checkShardConsistency(collection, shard, false, verbose);
      if (shardFailMessage != null && failMessage == null) {
        failMessage = shardFailMessage;
      }
    }

    if (failMessage != null) {
      System.err.println(failMessage);
      // MRM TODO: fail test if from tests

      throw new AssertionError(failMessage);
    }

    if (!checkVsControl) return;

    SolrParams q = params("q","*:*","rows","0", "tests","checkShardConsistency(vsControl)");    // add a tag to aid in debugging via logs

//    SolrDocumentList controlDocList = controlClient.query(q).getResults();
//    long controlDocs = controlDocList.getNumFound();
//
//    SolrDocumentList cloudDocList = cloudClient.query(q).getResults();
//    long cloudClientDocs = cloudDocList.getNumFound();


    // now check that the right # are on each shard
    //    theShards = shardToJetty.keySet();
    //    int cnt = 0;
    //    for (String s : theShards) {
    //      int times = shardToJetty.get(s).size();
    //      for (int i = 0; i < times; i++) {
    //        try {
    //          CloudJettyRunner cjetty = shardToJetty.get(s).get(i);
    //          ZkNodeProps props = cjetty.info;
    //          SolrClient client = cjetty.client.solrClient;
    //          boolean active = Replica.State.getState(props.getStr(ZkStateReader.STATE_PROP)) == Replica.State.ACTIVE;
    //          if (active) {
    //            SolrQuery query = new SolrQuery("*:*");
    //            query.set("distrib", false);
    //            long results = client.query(query).getResults().getNumFound();
    //            if (verbose) System.err.println(props + " : " + results);
    //            if (verbose) System.err.println("shard:"
    //                + props.getStr(ZkStateReader.SHARD_ID_PROP));
    //            cnt += results;
    //            break;
    //          }
    //        } catch (Exception e) {
    //          ParWork.propagateInterrupt(e);
    //          // if we have a problem, try the next one
    //          if (i == times - 1) {
    //            throw e;
    //          }
    //        }
    //      }
    //    }

    //controlDocs != cnt ||
    int cnt = -1;
//    if (cloudClientDocs != controlDocs) {
//      String msg = "document count mismatch.  control=" + controlDocs + " sum(shards)="+ cnt + " cloudClient="+cloudClientDocs;
//      log.error(msg);
//
//      boolean shouldFail = CloudInspectUtil.compareResults(controlClient, cloudClient, addFails, deleteFails);
//      if (shouldFail) {
//        fail(msg);
//      }
//    }
  }

  /**
   * Returns a non-null string if replicas within the same shard do not have a
   * consistent number of documents.
   * If expectFailure==false, the exact differences found will be logged since
   * this would be an unexpected failure.
   * verbose causes extra debugging into to be displayed, even if everything is
   * consistent.
   */
  protected String checkShardConsistency(String collection, String shard, boolean expectFailure, boolean verbose)
      throws Exception {


    long num = -1;
    long lastNum = -1;
    String failMessage = null;
    if (verbose) System.err.println("\nCheck consistency of shard: " + shard);
    if (verbose) System.err.println("__________________________\n");
    int cnt = 0;

    DocCollection coll = getClusterState().getCollection(collection);

    Slice replicas = coll.getSlice(shard);

    Replica lastReplica = null;
    for (Replica replica : replicas) {

      //if (verbose) System.err.println("client" + cnt++);
      if (verbose) System.err.println("Replica: " + replica);
      try (SolrClient client = getHttpClient(replica.getCoreUrl())) {
        try {
          SolrParams query = params("q","*:*", "rows","0", "distrib","false", "tests","checkShardConsistency"); // "tests" is just a tag that won't do anything except be echoed in logs
          num = client.query(query).getResults().getNumFound();
        } catch (SolrException | SolrServerException e) {
          if (verbose) System.err.println("error contacting client: "
              + e.getMessage() + "\n");
          continue;
        }

        boolean live = false;
        String nodeName = replica.getNodeName();
        if (isNodeLive(nodeName)) {
          live = true;
        }
        if (verbose) System.err.println(" Live:" + live);
        if (verbose) System.err.println(" Count:" + num + "\n");

        boolean active = replica.getState() == Replica.State.ACTIVE;
        if (active && live) {
          if (lastNum > -1 && lastNum != num && failMessage == null) {
            failMessage = shard + " is not consistent.  Got " + lastNum + " from " + lastReplica.getCoreUrl() + " (previous client)" + " and got " + num + " from " + replica.getCoreUrl();

            if (!expectFailure || verbose) {
              System.err.println("######" + failMessage);
              SolrQuery query = new SolrQuery("*:*");
              query.set("distrib", false);
              query.set("fl", "id,_version_");
              query.set("rows", "100000");
              query.set("sort", "id asc");
              query.set("tests", "checkShardConsistency/showDiff");

              try (SolrClient lastClient = getHttpClient(lastReplica.getCoreUrl())) {
                SolrDocumentList lst1 = lastClient.query(query).getResults();
                SolrDocumentList lst2 = client.query(query).getResults();

                CloudInspectUtil.showDiff(lst1, lst2, lastReplica.getCoreUrl(), replica.getCoreUrl());
              }
            }

          }
          lastNum = num;
          lastReplica = replica;
        }
      }
    }
    return failMessage;

  }

  /**
   * Generates the correct SolrParams from an even list of strings.
   * A string in an even position will represent the name of a parameter, while the following string
   * at position (i+1) will be the assigned value.
   *
   * @param params an even list of strings
   * @return the ModifiableSolrParams generated from the given list of strings.
   */
  public static ModifiableSolrParams params(String... params) {
    if (params.length % 2 != 0) throw new RuntimeException("Params length should be even");
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i=0; i<params.length; i+=2) {
      msp.add(params[i], params[i+1]);
    }
    return msp;
  }

  public Http2SolrClient getHttpClient(String baseUrl) {
    Http2SolrClient client = new Http2SolrClient.Builder(baseUrl)
        .idleTimeout(Integer.getInteger("socketTimeout", 30000))
        .build();
    return client;
  }

  private class CloudCollectionsListenerConsumer implements Consumer<CloudCollectionsListener> {
    private final Set<String> oldCollections;
    private final Set<String> newCollections;

    public CloudCollectionsListenerConsumer(Set<String> oldCollections, Set<String> newCollections) {
      this.oldCollections = oldCollections;
      this.newCollections = newCollections;
    }

    @Override
    public void accept(CloudCollectionsListener listener) {
      if (log.isDebugEnabled()) log.debug("fire listeners {}", listener);
      notifications.submit(new ListenerOnChange(listener));
    }

    private class ListenerOnChange implements Runnable {
      private final CloudCollectionsListener listener;

      public ListenerOnChange(CloudCollectionsListener listener) {
        this.listener = listener;
      }

      @Override
      public void run() {
        listener.onChange(oldCollections, newCollections);
      }
    }
  }

  private class ClusterPropsWatcher implements Watcher, Closeable {

    private final String path;

    ClusterPropsWatcher(String path) {
      this.path = path;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      if (closed) return;
      loadClusterProperties();
    }

    @Override
    public void close() throws IOException {
      try {
        zkClient.removeWatches(path, this, WatcherType.Any, true);
      } catch (KeeperException.NoWatcherException e) {

      } catch (Exception e) {
        log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
      }
    }
  }

  private class PredicateMatcher {
    private CollectionStatePredicate predicate;
    private CountDownLatch latch;
    private AtomicReference<DocCollection> docCollection;

    public PredicateMatcher(CollectionStatePredicate predicate, CountDownLatch latch, AtomicReference<DocCollection> docCollection) {
      this.predicate = predicate;
      this.latch = latch;
      this.docCollection = docCollection;
    }

    public org.apache.solr.common.cloud.CollectionStateWatcher invoke() {
      return (n, c) -> {
        // if (isClosed()) return true;
        docCollection.set(c);
        boolean matches = predicate.matches(getLiveNodes(), c);
        if (matches)
          latch.countDown();

        return matches;
      };
    }
  }
}
