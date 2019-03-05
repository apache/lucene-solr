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
import java.lang.invoke.MethodHandles;
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
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.Callable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.emptySortedSet;
import static java.util.Collections.unmodifiableSet;
import static org.apache.solr.common.util.Utils.fromJSON;

public class ZkStateReader implements Closeable {
  public static final int STATE_UPDATE_DELAY = Integer.getInteger("solr.OverseerStateUpdateDelay", 2000);  // delay between cloud state updates
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final String BASE_URL_PROP = "base_url";
  public static final String NODE_NAME_PROP = "node_name";
  public static final String CORE_NODE_NAME_PROP = "core_node_name";
  public static final String ROLES_PROP = "roles";
  public static final String STATE_PROP = "state";
  // if this flag equals to false and the replica does not exist in cluster state, set state op become no op (default is true)
  public static final String FORCE_SET_STATE_PROP = "force_set_state";
  /**  SolrCore name. */
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
  public static final String CLUSTER_STATE = "/clusterstate.json";
  public static final String CLUSTER_PROPS = "/clusterprops.json";
  public static final String COLLECTION_PROPS_ZKNODE = "collectionprops.json";
  public static final String REJOIN_AT_HEAD_PROP = "rejoinAtHead";
  public static final String SOLR_SECURITY_CONF_PATH = "/security.json";
  public static final String SOLR_AUTOSCALING_CONF_PATH = "/autoscaling.json";
  public static final String SOLR_AUTOSCALING_EVENTS_PATH = "/autoscaling/events";
  public static final String SOLR_AUTOSCALING_TRIGGER_STATE_PATH = "/autoscaling/triggerState";
  public static final String SOLR_AUTOSCALING_NODE_ADDED_PATH = "/autoscaling/nodeAdded";
  public static final String SOLR_AUTOSCALING_NODE_LOST_PATH = "/autoscaling/nodeLost";

  public static final String REPLICATION_FACTOR = "replicationFactor";
  public static final String MAX_SHARDS_PER_NODE = "maxShardsPerNode";
  public static final String AUTO_ADD_REPLICAS = "autoAddReplicas";
  public static final String MAX_CORES_PER_NODE = "maxCoresPerNode";
  public static final String PULL_REPLICAS = "pullReplicas";
  public static final String NRT_REPLICAS = "nrtReplicas";
  public static final String TLOG_REPLICAS = "tlogReplicas";
  public static final String READ_ONLY = "readOnly";

  public static final String ROLES = "/roles.json";

  public static final String CONFIGS_ZKNODE = "/configs";
  public final static String CONFIGNAME_PROP="configName";

  public static final String LEGACY_CLOUD = "legacyCloud";

  /**
   * @deprecated use {@link org.apache.solr.common.params.CollectionAdminParams#DEFAULTS} instead.
   */
  @Deprecated
  public static final String COLLECTION_DEF = "collectionDefaults";

  public static final String URL_SCHEME = "urlScheme";
  
  public static final String REPLICA_TYPE = "type";

  /** A view of the current state of all collections; combines all the different state sources into a single view. */
  protected volatile ClusterState clusterState;

  private static final int GET_LEADER_RETRY_INTERVAL_MS = 50;
  private static final int GET_LEADER_RETRY_DEFAULT_TIMEOUT = Integer.parseInt(System.getProperty("zkReaderGetLeaderRetryTimeoutMs", "4000"));;

  public static final String LEADER_ELECT_ZKNODE = "leader_elect";

  public static final String SHARD_LEADERS_ZKNODE = "leaders";
  public static final String ELECTION_NODE = "election";

  /** Collections tracked in the legacy (shared) state format, reflects the contents of clusterstate.json. */
  private Map<String, ClusterState.CollectionRef> legacyCollectionStates = emptyMap();

  /** Last seen ZK version of clusterstate.json. */
  private int legacyClusterStateVersion = 0;

  /** Collections with format2 state.json, "interesting" and actively watched. */
  private final ConcurrentHashMap<String, DocCollection> watchedCollectionStates = new ConcurrentHashMap<>();

  /** Collections with format2 state.json, not "interesting" and not actively watched. */
  private final ConcurrentHashMap<String, LazyCollectionRef> lazyCollectionStates = new ConcurrentHashMap<>();

  /** Collection properties being actively watched */
  private final ConcurrentHashMap<String, Map<String, String>> watchedCollectionProps = new ConcurrentHashMap<>();

  private volatile SortedSet<String> liveNodes = emptySortedSet();

  private volatile Map<String, Object> clusterProperties = Collections.emptyMap();

  private final ZkConfigManager configManager;

  private ConfigData securityData;

  private final Runnable securityNodeListener;

  private ConcurrentHashMap<String, CollectionWatch<CollectionStateWatcher>> collectionWatches = new ConcurrentHashMap<>();

  private ConcurrentHashMap<String, CollectionWatch<CollectionPropsWatcher>> collectionPropsWatches = new ConcurrentHashMap<>();

  private Set<CloudCollectionsListener> cloudCollectionsListeners = ConcurrentHashMap.newKeySet();

  private final ExecutorService notifications = ExecutorUtil.newMDCAwareCachedThreadPool("watches");

  private Set<LiveNodesListener> liveNodesListeners = ConcurrentHashMap.newKeySet();
  
  /** Used to submit notifications to Collection Properties watchers in order **/
  private final ExecutorService collectionPropsNotifications = ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrjNamedThreadFactory("collectionPropsNotifications"));

  private static final long LAZY_CACHE_TIME = TimeUnit.NANOSECONDS.convert(STATE_UPDATE_DELAY, TimeUnit.MILLISECONDS);

  /**
   * Get current {@link AutoScalingConfig}.
   * @return current configuration from <code>autoscaling.json</code>. NOTE:
   * this data is retrieved from ZK on each call.
   */
  public AutoScalingConfig getAutoScalingConfig() throws KeeperException, InterruptedException {
    return getAutoScalingConfig(null);
  }

  /**
   * Get current {@link AutoScalingConfig}.
   * @param watcher optional {@link Watcher} to set on a znode to watch for config changes.
   * @return current configuration from <code>autoscaling.json</code>. NOTE:
   * this data is retrieved from ZK on each call.
   */
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws KeeperException, InterruptedException {
    Stat stat = new Stat();

    Map<String, Object> map = new HashMap<>();
    try {
      byte[] bytes = zkClient.getData(SOLR_AUTOSCALING_CONF_PATH, watcher, stat, true);
      if (bytes != null && bytes.length > 0) {
        map = (Map<String, Object>) fromJSON(bytes);
      }
    } catch (KeeperException.NoNodeException e) {
      // ignore
    }
    map.put(AutoScalingParams.ZK_VERSION, stat.getVersion());
    return new AutoScalingConfig(map);
  }

  private static class CollectionWatch <T> {

    int coreRefCount = 0;
    Set<T> stateWatchers = ConcurrentHashMap.newKeySet();

    public boolean canBeRemoved() {
      return coreRefCount + stateWatchers.size() == 0;
    }

  }

  public static final Set<String> KNOWN_CLUSTER_PROPS = unmodifiableSet(new HashSet<>(asList(
      LEGACY_CLOUD,
      URL_SCHEME,
      AUTO_ADD_REPLICAS,
      CoreAdminParams.BACKUP_LOCATION,
      MAX_CORES_PER_NODE,
      CollectionAdminParams.DEFAULTS)));

  /**
   * Returns config set name for collection.
   *
   * @param collection to return config set name for
   */
  public String readConfigName(String collection) {

    String configName = null;

    String path = COLLECTIONS_ZKNODE + "/" + collection;
    log.debug("Loading collection config from: [{}]", path);

    try {
      byte[] data = zkClient.getData(path, null, null, true);

      if (data != null) {
        ZkNodeProps props = ZkNodeProps.load(data);
        configName = props.getStr(CONFIGNAME_PROP);
      }

      if (configName != null) {
        String configPath = CONFIGS_ZKNODE + "/" + configName;
        if (!zkClient.exists(configPath, true)) {
          log.error("Specified config=[{}] does not exist in ZooKeeper at location=[{}]", configName, configPath);
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "Specified config does not exist in ZooKeeper: " + configName);
        } else {
          log.debug("path=[{}] [{}]=[{}] specified config exists in ZooKeeper", configPath, CONFIGNAME_PROP, configName);
        }
      } else {
        throw new ZooKeeperException(ErrorCode.INVALID_STATE, "No config data found at path: " + path);
      }
    } catch (KeeperException| InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error loading config name for collection " + collection, e);
    }

    return configName;
  }


  private final SolrZkClient zkClient;
  
  private final boolean closeClient;

  private volatile boolean closed = false;
  
  private Set<CountDownLatch> waitLatches = ConcurrentHashMap.newKeySet();

  public ZkStateReader(SolrZkClient zkClient) {
    this(zkClient, null);
  }

  public ZkStateReader(SolrZkClient zkClient, Runnable securityNodeListener) {
    this.zkClient = zkClient;
    this.configManager = new ZkConfigManager(zkClient);
    this.closeClient = false;
    this.securityNodeListener = securityNodeListener;
    assert ObjectReleaseTracker.track(this);
  }


  public ZkStateReader(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    this.zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
        // on reconnect, reload cloud info
        new OnReconnect() {
          @Override
          public void command() {
            try {
              ZkStateReader.this.createClusterStateWatchersAndUpdate();
            } catch (KeeperException e) {
              log.error("A ZK error has occurred", e);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.error("Interrupted", e);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "Interrupted", e);
            }
          }
        });
    this.configManager = new ZkConfigManager(zkClient);
    this.closeClient = true;
    this.securityNodeListener = null;
    
    assert ObjectReleaseTracker.track(this);
  }

  public ZkConfigManager getConfigManager() {
    return configManager;
  }

  /**
   * Forcibly refresh cluster state from ZK. Do this only to avoid race conditions because it's expensive.
   *
   * It is cheaper to call {@link #forceUpdateCollection(String)} on a single collection if you must.
   * 
   * @lucene.internal
   */
  public void forciblyRefreshAllClusterStateSlow() throws KeeperException, InterruptedException {
    synchronized (getUpdateLock()) {
      if (clusterState == null) {
        // Never initialized, just run normal initialization.
        createClusterStateWatchersAndUpdate();
        return;
      }
      // No need to set watchers because we should already have watchers registered for everything.
      refreshCollectionList(null);
      refreshLiveNodes(null);
      refreshLegacyClusterState(null);
      // Need a copy so we don't delete from what we're iterating over.
      Collection<String> safeCopy = new ArrayList<>(watchedCollectionStates.keySet());
      Set<String> updatedCollections = new HashSet<>();
      for (String coll : safeCopy) {
        DocCollection newState = fetchCollectionState(coll, null);
        if (updateWatchedCollection(coll, newState)) {
          updatedCollections.add(coll);
        }
      }
      constructState(updatedCollections);
    }
  }

  /**
   * Forcibly refresh a collection's internal state from ZK. Try to avoid having to resort to this when
   * a better design is possible.
   */
  //TODO shouldn't we call ZooKeeper.sync() at the right places to prevent reading a stale value?  We do so for aliases.
  public void forceUpdateCollection(String collection) throws KeeperException, InterruptedException {

    synchronized (getUpdateLock()) {
      if (clusterState == null) {
        log.warn("ClusterState watchers have not been initialized");
        return;
      }

      ClusterState.CollectionRef ref = clusterState.getCollectionRef(collection);
      if (ref == null || legacyCollectionStates.containsKey(collection)) {
        // We either don't know anything about this collection (maybe it's new?) or it's legacy.
        // First update the legacy cluster state.
        log.debug("Checking legacy cluster state for collection {}", collection);
        refreshLegacyClusterState(null);
        if (!legacyCollectionStates.containsKey(collection)) {
          // No dice, see if a new collection just got created.
          LazyCollectionRef tryLazyCollection = new LazyCollectionRef(collection);
          if (tryLazyCollection.get() != null) {
            // What do you know, it exists!
            log.debug("Adding lazily-loaded reference for collection {}", collection);
            lazyCollectionStates.putIfAbsent(collection, tryLazyCollection);
            constructState(Collections.singleton(collection));
          }
        }
      } else if (ref.isLazilyLoaded()) {
        log.debug("Refreshing lazily-loaded state for collection {}", collection);
        if (ref.get() != null) {
          return;
        }
        // Edge case: if there's no external collection, try refreshing legacy cluster state in case it's there.
        refreshLegacyClusterState(null);
      } else if (watchedCollectionStates.containsKey(collection)) {
        // Exists as a watched collection, force a refresh.
        log.debug("Forcing refresh of watched collection state for {}", collection);
        DocCollection newState = fetchCollectionState(collection, null);
        if (updateWatchedCollection(collection, newState)) {
          constructState(Collections.singleton(collection));
        }
      } else {
        log.error("Collection {} is not lazy or watched!", collection);
      }
    }

  }

  /** Refresh the set of live nodes. */
  public void updateLiveNodes() throws KeeperException, InterruptedException {
    refreshLiveNodes(null);
  }

  public Integer compareStateVersions(String coll, int version) {
    DocCollection collection = clusterState.getCollectionOrNull(coll);
    if (collection == null) return null;
    if (collection.getZNodeVersion() < version) {
      log.debug("Server older than client {}<{}", collection.getZNodeVersion(), version);
      DocCollection nu = getCollectionLive(this, coll);
      if (nu == null) return -1 ;
      if (nu.getZNodeVersion() > collection.getZNodeVersion()) {
        if (updateWatchedCollection(coll, nu)) {
          synchronized (getUpdateLock()) {
            constructState(Collections.singleton(coll));
          }
        }
        collection = nu;
      }
    }
    
    if (collection.getZNodeVersion() == version) {
      return null;
    }
    
    log.debug("Wrong version from client [{}]!=[{}]", version, collection.getZNodeVersion());
    
    return collection.getZNodeVersion();
  }
  
  public synchronized void createClusterStateWatchersAndUpdate() throws KeeperException,
      InterruptedException {
    // We need to fetch the current cluster state and the set of live nodes

    log.debug("Updating cluster state from ZooKeeper... ");

    // Sanity check ZK structure.
    if (!zkClient.exists(CLUSTER_STATE, true)) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "Cannot connect to cluster at " + zkClient.getZkServerAddress() + ": cluster not found/not ready");
    }

    // on reconnect of SolrZkClient force refresh and re-add watches.
    loadClusterProperties();
    refreshLiveNodes(new LiveNodeWatcher());
    refreshLegacyClusterState(new LegacyClusterStateWatcher());
    refreshStateFormat2Collections();
    refreshCollectionList(new CollectionsChildWatcher());
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

    collectionPropsWatches.forEach((k,v) -> {
      new PropsWatcher(k).refreshAndWatch(true);
    });
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
              synchronized (ZkStateReader.this.getUpdateLock()) {
                log.debug("Updating [{}] ... ", SOLR_SECURITY_CONF_PATH);

                // remake watch
                final Watcher thisWatch = this;
                final Stat stat = new Stat();
                final byte[] data = getZkClient().getData(SOLR_SECURITY_CONF_PATH, thisWatch, stat, true);
                try {
                  callback.call(new Pair<>(data, stat));
                } catch (Exception e) {
                  log.error("Error running collections node listener", e);
                }
              }
            } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException e) {
              log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
            } catch (KeeperException e) {
              log.error("A ZK error has occurred", e);
              throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.warn("Interrupted", e);
            }
          }

        }, true);
  }

  /**
   * Construct the total state view from all sources.
   * Must hold {@link #getUpdateLock()} before calling this.
   *
   * @param changedCollections collections that have changed since the last call,
   *                           and that should fire notifications
   */
  private void constructState(Set<String> changedCollections) {

    Set<String> liveNodes = this.liveNodes; // volatile read

    // Legacy clusterstate is authoritative, for backwards compatibility.
    // To move a collection's state to format2, first create the new state2 format node, then remove legacy entry.
    Map<String, ClusterState.CollectionRef> result = new LinkedHashMap<>(legacyCollectionStates);

    // Add state format2 collections, but don't override legacy collection states.
    for (Map.Entry<String, DocCollection> entry : watchedCollectionStates.entrySet()) {
      result.putIfAbsent(entry.getKey(), new ClusterState.CollectionRef(entry.getValue()));
    }

    // Finally, add any lazy collections that aren't already accounted for.
    for (Map.Entry<String, LazyCollectionRef> entry : lazyCollectionStates.entrySet()) {
      result.putIfAbsent(entry.getKey(), entry.getValue());
    }

    this.clusterState = new ClusterState(liveNodes, result, legacyClusterStateVersion);

    log.debug("clusterStateSet: legacy [{}] interesting [{}] watched [{}] lazy [{}] total [{}]",
        legacyCollectionStates.keySet().size(),
        collectionWatches.keySet().size(),
        watchedCollectionStates.keySet().size(),
        lazyCollectionStates.keySet().size(),
        clusterState.getCollectionStates().size());

    if (log.isTraceEnabled()) {
      log.trace("clusterStateSet: legacy [{}] interesting [{}] watched [{}] lazy [{}] total [{}]",
          legacyCollectionStates.keySet(),
          collectionWatches.keySet(),
          watchedCollectionStates.keySet(),
          lazyCollectionStates.keySet(),
          clusterState.getCollectionStates());
    }

    notifyCloudCollectionsListeners();

    for (String collection : changedCollections) {
      notifyStateWatchers(liveNodes, collection, clusterState.getCollectionOrNull(collection));
    }

  }

  /**
   * Refresh legacy (shared) clusterstate.json
   */
  private void refreshLegacyClusterState(Watcher watcher) throws KeeperException, InterruptedException {
    try {
      final Stat stat = new Stat();
      final byte[] data = zkClient.getData(CLUSTER_STATE, watcher, stat, true);
      final ClusterState loadedData = ClusterState.load(stat.getVersion(), data, emptySet(), CLUSTER_STATE);
      synchronized (getUpdateLock()) {
        if (this.legacyClusterStateVersion >= stat.getVersion()) {
          // Nothing to do, someone else updated same or newer.
          return;
        }
        Set<String> updatedCollections = new HashSet<>();
        for (String coll : this.collectionWatches.keySet()) {
          ClusterState.CollectionRef ref = this.legacyCollectionStates.get(coll);
          // legacy collections are always in-memory
          DocCollection oldState = ref == null ? null : ref.get();
          ClusterState.CollectionRef newRef = loadedData.getCollectionStates().get(coll);
          DocCollection newState = newRef == null ? null : newRef.get();
          if (newState == null) {
            // check that we haven't just migrated
            newState = watchedCollectionStates.get(coll);
          }
          if (!Objects.equals(oldState, newState)) {
            updatedCollections.add(coll);
          }
        }
        this.legacyCollectionStates = loadedData.getCollectionStates();
        this.legacyClusterStateVersion = stat.getVersion();
        constructState(updatedCollections);
      }
    } catch (KeeperException.NoNodeException e) {
      // Ignore missing legacy clusterstate.json.
      synchronized (getUpdateLock()) {
        this.legacyCollectionStates = emptyMap();
        this.legacyClusterStateVersion = 0;
        constructState(Collections.emptySet());
      }
    }
  }

  /**
   * Refresh state format2 collections.
   */
  private void refreshStateFormat2Collections() {
    for (String coll : collectionWatches.keySet()) {
      new StateWatcher(coll).refreshAndWatch();
    }
  }

  // We don't get a Stat or track versions on getChildren() calls, so force linearization.
  private final Object refreshCollectionListLock = new Object();

  /**
   * Search for any lazy-loadable state format2 collections.
   *
   * A stateFormat=1 collection which is not interesting to us can also
   * be put into the {@link #lazyCollectionStates} map here. But that is okay
   * because {@link #constructState(Set)} will give priority to collections in the
   * shared collection state over this map.
   * In fact this is a clever way to avoid doing a ZK exists check on
   * the /collections/collection_name/state.json znode
   * Such an exists check is done in {@link ClusterState#hasCollection(String)} and
   * {@link ClusterState#getCollectionsMap()} methods
   * have a safeguard against exposing wrong collection names to the users
   */
  private void refreshCollectionList(Watcher watcher) throws KeeperException, InterruptedException {
    synchronized (refreshCollectionListLock) {
      List<String> children = null;
      try {
        children = zkClient.getChildren(COLLECTIONS_ZKNODE, watcher, true);
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
  }

  // We don't get a Stat or track versions on getChildren() calls, so force linearization.
  private final Object refreshCollectionsSetLock = new Object();
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
    listener.onChange(Collections.emptySet(), lastFetchedCollectionSet.get());
  }

  private void notifyCloudCollectionsListeners() {
    notifyCloudCollectionsListeners(false);
  }

  private void notifyCloudCollectionsListeners(boolean notifyIfSame) {
    synchronized (refreshCollectionsSetLock) {
      final Set<String> newCollections = getCurrentCollections();
      final Set<String> oldCollections = lastFetchedCollectionSet.getAndSet(newCollections);
      if (!newCollections.equals(oldCollections) || notifyIfSame) {
        cloudCollectionsListeners.forEach(listener -> listener.onChange(oldCollections, newCollections));
      }
    }
  }

  private Set<String> getCurrentCollections() {
    Set<String> collections = new HashSet<>();
    collections.addAll(legacyCollectionStates.keySet());
    collections.addAll(watchedCollectionStates.keySet());
    collections.addAll(lazyCollectionStates.keySet());
    return collections;
  }

  private class LazyCollectionRef extends ClusterState.CollectionRef {
    private final String collName;
    private long lastUpdateTime;
    private DocCollection cachedDocCollection;

    public LazyCollectionRef(String collName) {
      super(null);
      this.collName = collName;
      this.lastUpdateTime = -1;
    }

    @Override
    public synchronized DocCollection get(boolean allowCached) {
      gets.incrementAndGet();
      if (!allowCached || lastUpdateTime < 0 || System.nanoTime() - lastUpdateTime > LAZY_CACHE_TIME) {
        boolean shouldFetch = true;
        if (cachedDocCollection != null) {
          Stat exists = null;
          try {
            exists = zkClient.exists(getCollectionPath(collName), null, true);
          } catch (Exception e) {}
          if (exists != null && exists.getVersion() == cachedDocCollection.getZNodeVersion()) {
            shouldFetch = false;
          }
        }
        if (shouldFetch) {
          cachedDocCollection = getCollectionLive(ZkStateReader.this, collName);
          lastUpdateTime = System.nanoTime();
        }
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

  // We don't get a Stat or track versions on getChildren() calls, so force linearization.
  private final Object refreshLiveNodesLock = new Object();
  // Ensures that only the latest getChildren fetch gets applied.
  private final AtomicReference<SortedSet<String>> lastFetchedLiveNodes = new AtomicReference<>();

  /**
   * Refresh live_nodes.
   */
  private void refreshLiveNodes(Watcher watcher) throws KeeperException, InterruptedException {
    synchronized (refreshLiveNodesLock) {
      SortedSet<String> newLiveNodes;
      try {
        List<String> nodeList = zkClient.getChildren(LIVE_NODES_ZKNODE, watcher, true);
        newLiveNodes = new TreeSet<>(nodeList);
      } catch (KeeperException.NoNodeException e) {
        newLiveNodes = emptySortedSet();
      }
      lastFetchedLiveNodes.set(newLiveNodes);
    }

    // Can't lock getUpdateLock() until we release the other, it would cause deadlock.
    SortedSet<String> oldLiveNodes, newLiveNodes;
    synchronized (getUpdateLock()) {
      newLiveNodes = lastFetchedLiveNodes.getAndSet(null);
      if (newLiveNodes == null) {
        // Someone else won the race to apply the last update, just exit.
        return;
      }

      oldLiveNodes = this.liveNodes;
      this.liveNodes = newLiveNodes;
      if (clusterState != null) {
        clusterState.setLiveNodes(newLiveNodes);
      }
    }
    if (oldLiveNodes.size() != newLiveNodes.size()) {
      log.info("Updated live nodes from ZooKeeper... ({}) -> ({})", oldLiveNodes.size(), newLiveNodes.size());
    }
    if (log.isDebugEnabled()) {
      log.debug("Updated live nodes from ZooKeeper... {} -> {}", oldLiveNodes, newLiveNodes);
    }
    if (!oldLiveNodes.equals(newLiveNodes)) { // fire listeners
      liveNodesListeners.forEach(listener -> {
        if (listener.onChange(new TreeSet<>(oldLiveNodes), new TreeSet<>(newLiveNodes))) {
          removeLiveNodesListener(listener);
        }
      });
    }
  }

  public void registerLiveNodesListener(LiveNodesListener listener) {
    // fire it once with current live nodes
    if (listener.onChange(new TreeSet<>(getClusterState().getLiveNodes()), new TreeSet<>(getClusterState().getLiveNodes()))) {
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
  
  public Object getUpdateLock() {
    return this;
  }

  public void close() {
    this.closed  = true;
    
    notifications.shutdownNow();
    
    waitLatches.parallelStream().forEach(c -> { c.countDown(); });
    
    ExecutorUtil.shutdownAndAwaitTermination(notifications);
    ExecutorUtil.shutdownAndAwaitTermination(collectionPropsNotifications);
    if (closeClient) {
      zkClient.close();
    }
    assert ObjectReleaseTracker.release(this);
  }
  
  public String getLeaderUrl(String collection, String shard, int timeout) throws InterruptedException {
    ZkCoreNodeProps props = new ZkCoreNodeProps(getLeaderRetry(collection, shard, timeout));
    return props.getCoreUrl();
  }
  
  public Replica getLeader(Set<String> liveNodes, DocCollection docCollection, String shard) {
    Replica replica = docCollection != null ? docCollection.getLeader(shard) : null;
    if (replica != null && liveNodes.contains(replica.getNodeName())) {
      return replica;
    }
    return null;
  }
  public Replica getLeader(String collection, String shard) {
    if (clusterState != null) {
      DocCollection docCollection = clusterState.getCollectionOrNull(collection);
      Replica replica = docCollection != null ? docCollection.getLeader(shard) : null;
      if (replica != null && getClusterState().liveNodesContain(replica.getNodeName())) {
        return replica;
      }
    }
    return null;
  }

  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(String collection, String shard) throws InterruptedException {
    return getLeaderRetry(collection, shard, GET_LEADER_RETRY_DEFAULT_TIMEOUT);
  }

  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(String collection, String shard, int timeout) throws InterruptedException {

    AtomicReference<Replica> leader = new AtomicReference<>();
    try {
      waitForState(collection, timeout, TimeUnit.MILLISECONDS, (n, c) -> {
        if (c == null)
          return false;
        Replica l = getLeader(n, c, shard);
        if (l != null) {
          leader.set(l);
          return true;
        }
        return false;
      });
    } catch (TimeoutException | InterruptedException e) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "No registered leader was found after waiting for "
          + timeout + "ms " + ", collection: " + collection + " slice: " + shard + " saw state=" + clusterState.getCollectionOrNull(collection)
          + " with live_nodes=" + clusterState.getLiveNodes());
    }
    return leader.get();
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
        + LEADER_ELECT_ZKNODE  + (shardId != null ? ("/" + shardId + "/" + ELECTION_NODE)
        : "");
  }


  public List<ZkCoreNodeProps> getReplicaProps(String collection, String shardId, String thisCoreNodeName) {
    return getReplicaProps(collection, shardId, thisCoreNodeName, null);
  }
  
  public List<ZkCoreNodeProps> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
      Replica.State mustMatchStateFilter) {
    return getReplicaProps(collection, shardId, thisCoreNodeName, mustMatchStateFilter, null);
  }
  
  public List<ZkCoreNodeProps> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
      Replica.State mustMatchStateFilter, Replica.State mustNotMatchStateFilter) {
    //TODO: We don't need all these getReplicaProps method overloading. Also, it's odd that the default is to return replicas of type TLOG and NRT only
    return getReplicaProps(collection, shardId, thisCoreNodeName, mustMatchStateFilter, null, EnumSet.of(Replica.Type.TLOG,  Replica.Type.NRT));
  }
  
  public List<ZkCoreNodeProps> getReplicaProps(String collection, String shardId, String thisCoreNodeName,
      Replica.State mustMatchStateFilter, Replica.State mustNotMatchStateFilter, final EnumSet<Replica.Type> acceptReplicaType) {
    assert thisCoreNodeName != null;
    ClusterState clusterState = this.clusterState;
    if (clusterState == null) {
      return null;
    }
    final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
    if (docCollection == null || docCollection.getSlicesMap() == null) {
      throw new ZooKeeperException(ErrorCode.BAD_REQUEST,
          "Could not find collection in zk: " + collection);
    }
    
    Map<String,Slice> slices = docCollection.getSlicesMap();
    Slice replicas = slices.get(shardId);
    if (replicas == null) {
      throw new ZooKeeperException(ErrorCode.BAD_REQUEST, "Could not find shardId in zk: " + shardId);
    }
    
    Map<String,Replica> shardMap = replicas.getReplicasMap();
    List<ZkCoreNodeProps> nodes = new ArrayList<>(shardMap.size());
    for (Entry<String,Replica> entry : shardMap.entrySet().stream().filter((e)->acceptReplicaType.contains(e.getValue().getType())).collect(Collectors.toList())) {
      ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(entry.getValue());
      
      String coreNodeName = entry.getValue().getName();
      
      if (clusterState.liveNodesContain(nodeProps.getNodeName()) && !coreNodeName.equals(thisCoreNodeName)) {
        if (mustMatchStateFilter == null || mustMatchStateFilter == Replica.State.getState(nodeProps.getState())) {
          if (mustNotMatchStateFilter == null || mustNotMatchStateFilter != Replica.State.getState(nodeProps.getState())) {
            nodes.add(nodeProps);
          }
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
   *
   * N.B. Cluster properties are updated via ZK watchers, and so may not necessarily
   * be completely up-to-date.  If you need to get the latest version, then use a
   * {@link ClusterProperties} instance.
   *
   * @param key           the property to read
   * @param defaultValue  a default value to use if no such property exists
   * @param <T>           the type of the property
   * @return the cluster property, or a default if the property is not set
   */
  @SuppressWarnings("unchecked")
  public <T> T getClusterProperty(String key, T defaultValue) {
    T value = (T) Utils.getObjectByPath( clusterProperties, false, key);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**Same as the above but allows a full json path as a list of parts
   *
   * @param keyPath path to the property example ["collectionDefauls", "numShards"]
   * @param defaultValue a default value to use if no such property exists
   * @return the cluster property, or a default if the property is not set
   */
  public <T> T getClusterProperty(List<String> keyPath, T defaultValue) {
    T value = (T) Utils.getObjectByPath( clusterProperties, false, keyPath);
    if (value == null)
      return defaultValue;
    return value;
  }

  /**
   * Get all cluster properties for this cluster
   *
   * N.B. Cluster properties are updated via ZK watchers, and so may not necessarily
   * be completely up-to-date.  If you need to get the latest version, then use a
   * {@link ClusterProperties} instance.
   *
   * @return a Map of cluster properties
   */
  public Map<String, Object> getClusterProperties() {
    return Collections.unmodifiableMap(clusterProperties);
  }

  private final Watcher clusterPropertiesWatcher = event -> {
    // session events are not change events, and do not remove the watcher
    if (Watcher.Event.EventType.None.equals(event.getType())) {
      return;
    }
    loadClusterProperties();
  };

  @SuppressWarnings("unchecked")
  private void loadClusterProperties() {
    try {
      while (true) {
        try {
          byte[] data = zkClient.getData(ZkStateReader.CLUSTER_PROPS, clusterPropertiesWatcher, new Stat(), true);
          this.clusterProperties = ClusterProperties.convertCollectionDefaultsToNestedFormat((Map<String, Object>) Utils.fromJSON(data));
          log.debug("Loaded cluster properties: {}", this.clusterProperties);
          return;
        } catch (KeeperException.NoNodeException e) {
          this.clusterProperties = Collections.emptyMap();
          log.debug("Loaded empty cluster properties");
          // set an exists watch, and if the node has been created since the last call,
          // read the data again
          if (zkClient.exists(ZkStateReader.CLUSTER_PROPS, clusterPropertiesWatcher, true) == null)
            return;
        }
      }
    } catch (KeeperException | InterruptedException e) {
      log.error("Error reading cluster properties from zookeeper", SolrZkClient.checkInterrupted(e));
    }
  }

  /**
   * Get collection properties for a given collection. If the collection is watched, simply return it from the cache,
   * otherwise fetch it directly from zookeeper.
   */
  public Map<String, String> getCollectionProperties(final String collection) {
    Map<String, String> properties = watchedCollectionProps.get(collection);
    if (properties == null) {
      try {
        properties = fetchCollectionProperties(collection, null);
        // Not storing the value in watchedCollectionProps, because it can gat stale, since we have no watcher set.
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error reading collection properties", SolrZkClient.checkInterrupted(e));
      }
    }

    return properties;
  }

  static String getCollectionPropsPath(final String collection) {
    return COLLECTIONS_ZKNODE + '/' + collection + '/' + COLLECTION_PROPS_ZKNODE;
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> fetchCollectionProperties(String collection, Watcher watcher) throws KeeperException, InterruptedException {
    final String znodePath = getCollectionPropsPath(collection);
    while (true) {
      try {
        byte[] data = zkClient.getData(znodePath, watcher, null, true);
        return (Map<String, String>) Utils.fromJSON(data);
      } catch (ClassCastException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to parse collection properties for collection " + collection, e);
      } catch (KeeperException.NoNodeException e) {
        if (watcher != null) {
          // Leave an exists watch in place in case a collectionprops.json is created later.
          Stat exists = zkClient.exists(znodePath, watcher, true);
          if (exists != null) {
            // Rare race condition, we tried to fetch the data and couldn't find it, then we found it exists.
            // Loop and try again.
            continue;
          }
        }
        return Collections.emptyMap();
      }
    }
  }

  /**
   * Returns the content of /security.json from ZooKeeper as a Map
   * If the files doesn't exist, it returns null.
   */
  public ConfigData getSecurityProps(boolean getFresh) {
    if (!getFresh) {
      if (securityData == null) return new ConfigData(EMPTY_MAP,-1);
      return new ConfigData(securityData.data, securityData.version);
    }
    try {
      Stat stat = new Stat();
      if(getZkClient().exists(SOLR_SECURITY_CONF_PATH, true)) {
        final byte[] data = getZkClient().getData(ZkStateReader.SOLR_SECURITY_CONF_PATH, null, stat, true);
        return data != null && data.length > 0 ?
            new ConfigData((Map<String, Object>) Utils.fromJSON(data), stat.getVersion()) :
            null;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR,"Error reading security properties", e) ;
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR,"Error reading security properties", e) ;
    }
    return null;
  }

  /**
   * Returns the baseURL corresponding to a given node's nodeName --
   * NOTE: does not (currently) imply that the nodeName (or resulting
   * baseURL) exists in the cluster.
   * @lucene.experimental
   */
  public String getBaseUrlForNodeName(final String nodeName) {
    return Utils.getBaseUrlForNodeName(nodeName, getClusterProperty(URL_SCHEME, "http"));
  }

  /** Watches a single collection's format2 state.json. */
  class StateWatcher implements Watcher {
    private final String coll;

    StateWatcher(String coll) {
      this.coll = coll;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }

      if (!collectionWatches.containsKey(coll)) {
        // This collection is no longer interesting, stop watching.
        log.debug("Uninteresting collection {}", coll);
        return;
      }

      Set<String> liveNodes = ZkStateReader.this.liveNodes;
      log.info("A cluster state change: [{}] for collection [{}] has occurred - updating... (live nodes size: [{}])",
              event, coll, liveNodes.size());

      refreshAndWatch();

    }

    /**
     * Refresh collection state from ZK and leave a watch for future changes.
     * As a side effect, updates {@link #clusterState} and {@link #watchedCollectionStates}
     * with the results of the refresh.
     */
    public void refreshAndWatch() {
      try {
        DocCollection newState = fetchCollectionState(coll, this);
        updateWatchedCollection(coll, newState);
        synchronized (getUpdateLock()) {
          constructState(Collections.singleton(coll));
        }

      } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("Unwatched collection: [{}]", coll, e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Unwatched collection: [{}]", coll, e);
      }
    }
  }

  /** Watches the legacy clusterstate.json. */
  class LegacyClusterStateWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      int liveNodesSize = ZkStateReader.this.clusterState == null ? 0 : ZkStateReader.this.clusterState.getLiveNodes().size();
      log.debug("A cluster state change: [{}], has occurred - updating... (live nodes size: [{}])", event, liveNodesSize);
      refreshAndWatch();
    }

    /** Must hold {@link #getUpdateLock()} before calling this method. */
    public void refreshAndWatch() {
      try {
        refreshLegacyClusterState(this);
      } catch (KeeperException.NoNodeException e) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
                "Cannot connect to cluster at " + zkClient.getZkServerAddress() + ": cluster not found/not ready");
      } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("A ZK error has occurred", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }
  }

  /** Watches collection properties */
  class PropsWatcher implements Watcher {
    private final String coll;

    PropsWatcher(String coll) {
      this.coll = coll;
    }

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }

      if (!collectionPropsWatches.containsKey(coll)) {
        // No one can be notified of the change, we can ignore it and "unset" the watch
        log.debug("Ignoring property change for collection {}", coll);
        return;
      }

      log.info("A collection property change: [{}] for collection [{}] has occurred - updating...",
          event, coll);

      refreshAndWatch(true);
    }

    /**
     * Refresh collection properties from ZK and leave a watch for future changes. Updates the properties in
     * watchedCollectionProps with the results of the refresh. Optionally notifies watchers
     */
    void refreshAndWatch(boolean notifyWatchers) {
      try {
        synchronized (coll) { // We only have one PropsWatcher instance per collection, so it's fine to sync on coll
          Map<String, String> properties = fetchCollectionProperties(coll, this);
          watchedCollectionProps.put(coll, properties);
          /*
           * Note that if two events were fired close to each other and the second one arrived first, we would read the collectionprops.json
           * twice for the same data and notify watchers (in case of notifyWatchers==true) twice for the same data, however it's guaranteed
           * that after processing both events, watchedCollectionProps will have the latest data written to ZooKeeper and that the watchers
           * won't be called with out of order data
           * 
           */
          if (notifyWatchers) {
            notifyPropsWatchers(coll, properties);
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

  /** Watches /collections children . */
  class CollectionsChildWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      if (ZkStateReader.this.closed) {
        return;
      }
      
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      log.debug("A collections change: [{}], has occurred - updating...", event);
      refreshAndWatch();
      synchronized (getUpdateLock()) {
        constructState(Collections.emptySet());
      }
    }

    /** Must hold {@link #getUpdateLock()} before calling this method. */
    public void refreshAndWatch() {
      try {
        refreshCollectionList(this);
      } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("A ZK error has occurred", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }
  }

  /** Watches the live_nodes and syncs changes. */
  class LiveNodeWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      log.debug("A live node change: [{}], has occurred - updating... (live nodes size: [{}])", event, liveNodes.size());
      refreshAndWatch();
    }

    public void refreshAndWatch() {
      try {
        refreshLiveNodes(this);
      } catch (KeeperException.SessionExpiredException | KeeperException.ConnectionLossException e) {
        log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK: [{}]", e.getMessage());
      } catch (KeeperException e) {
        log.error("A ZK error has occurred", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "A ZK error has occurred", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("Interrupted", e);
      }
    }
  }

  public static DocCollection getCollectionLive(ZkStateReader zkStateReader, String coll) {
    try {
      return zkStateReader.fetchCollectionState(coll, null);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Could not load collection from ZK: " + coll, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.BAD_REQUEST, "Could not load collection from ZK: " + coll, e);
    }
  }

  private DocCollection fetchCollectionState(String coll, Watcher watcher) throws KeeperException, InterruptedException {
    String collectionPath = getCollectionPath(coll);
    while (true) {
      try {
        Stat stat = new Stat();
        byte[] data = zkClient.getData(collectionPath, watcher, stat, true);
        ClusterState state = ClusterState.load(stat.getVersion(), data,
            Collections.<String>emptySet(), collectionPath);
        ClusterState.CollectionRef collectionRef = state.getCollectionStates().get(coll);
        return collectionRef == null ? null : collectionRef.get();
      } catch (KeeperException.NoNodeException e) {
        if (watcher != null) {
          // Leave an exists watch in place in case a state.json is created later.
          Stat exists = zkClient.exists(collectionPath, watcher, true);
          if (exists != null) {
            // Rare race condition, we tried to fetch the data and couldn't find it, then we found it exists.
            // Loop and try again.
            continue;
          }
        }
        return null;
      }
    }
  }

  public static String getCollectionPathRoot(String coll) {
    return COLLECTIONS_ZKNODE+"/"+coll;
  }

  public static String getCollectionPath(String coll) {
    return getCollectionPathRoot(coll) + "/state.json";
  }

  /**
   * Notify this reader that a local Core is a member of a collection, and so that collection
   * state should be watched.
   *
   * Not a public API.  This method should only be called from ZkController.
   *
   * The number of cores per-collection is tracked, and adding multiple cores from the same
   * collection does not increase the number of watches.
   *
   * @param collection the collection that the core is a member of
   *
   * @see ZkStateReader#unregisterCore(String)
   */
  public void registerCore(String collection) {
    AtomicBoolean reconstructState = new AtomicBoolean(false);
    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) {
        reconstructState.set(true);
        v = new CollectionWatch<>();
      }
      v.coreRefCount++;
      return v;
    });
    if (reconstructState.get()) {
      new StateWatcher(collection).refreshAndWatch();
    }
  }

  /**
   * Notify this reader that a local core that is a member of a collection has been closed.
   *
   * Not a public API.  This method should only be called from ZkController.
   *
   * If no cores are registered for a collection, and there are no {@link CollectionStateWatcher}s
   * for that collection either, the collection watch will be removed.
   *
   * @param collection the collection that the core belongs to
   */
  public void unregisterCore(String collection) {
    AtomicBoolean reconstructState = new AtomicBoolean(false);
    collectionWatches.compute(collection, (k, v) -> {
      if (v == null)
        return null;
      if (v.coreRefCount > 0)
        v.coreRefCount--;
      if (v.canBeRemoved()) {
        watchedCollectionStates.remove(collection);
        lazyCollectionStates.put(collection, new LazyCollectionRef(collection));
        reconstructState.set(true);
        return null;
      }
      return v;
    });
    if (reconstructState.get()) {
      synchronized (getUpdateLock()) {
        constructState(Collections.emptySet());
      }
    }
  }

  /**
   * Register a CollectionStateWatcher to be called when the state of a collection changes
   */
  public void registerCollectionStateWatcher(String collection, CollectionStateWatcher stateWatcher) {
    AtomicBoolean watchSet = new AtomicBoolean(false);
    collectionWatches.compute(collection, (k, v) -> {
      if (v == null) {
        v = new CollectionWatch<>();
        watchSet.set(true);
      }
      v.stateWatchers.add(stateWatcher);
      return v;
    });

    if (watchSet.get()) {
      new StateWatcher(collection).refreshAndWatch();
    }

    DocCollection state = clusterState.getCollectionOrNull(collection);
    if (stateWatcher.onStateChanged(liveNodes, state) == true) {
      removeCollectionStateWatcher(collection, stateWatcher);
    }
  }

  /**
   * Block until a CollectionStatePredicate returns true, or the wait times out
   *
   * Note that the predicate may be called again even after it has returned true, so
   * implementors should avoid changing state within the predicate call itself.
   *
   * @param collection the collection to watch
   * @param wait       how long to wait
   * @param unit       the units of the wait parameter
   * @param predicate  the predicate to call on state changes
   * @throws InterruptedException on interrupt
   * @throws TimeoutException on timeout
   */
  public void waitForState(final String collection, long wait, TimeUnit unit, CollectionStatePredicate predicate)
      throws InterruptedException, TimeoutException {
    
    if (closed) {
      throw new AlreadyClosedException();
    }
    
    final CountDownLatch latch = new CountDownLatch(1);
    waitLatches.add(latch);
    AtomicReference<DocCollection> docCollection = new AtomicReference<>();
    CollectionStateWatcher watcher = (n, c) -> {
      docCollection.set(c);
      boolean matches = predicate.matches(n, c);
      if (matches)
        latch.countDown();
      
      return matches;
    };
    registerCollectionStateWatcher(collection, watcher);

    try {
      // wait for the watcher predicate to return true, or time out
      if (!latch.await(wait, unit))
        throw new TimeoutException("Timeout waiting to see state for collection=" + collection + " :" + docCollection.get());

    }
    finally {
      removeCollectionStateWatcher(collection, watcher);
      waitLatches.remove(latch);
    }
  }

  /**
   * Block until a LiveNodesStatePredicate returns true, or the wait times out
   *
   * Note that the predicate may be called again even after it has returned true, so
   * implementors should avoid changing state within the predicate call itself.
   *
   * @param wait       how long to wait
   * @param unit       the units of the wait parameter
   * @param predicate  the predicate to call on state changes
   * @throws InterruptedException on interrupt
   * @throws TimeoutException on timeout
   */
  public void waitForLiveNodes(long wait, TimeUnit unit, LiveNodesPredicate predicate)
      throws InterruptedException, TimeoutException {
    
    if (closed) {
      throw new AlreadyClosedException();
    }
    
    final CountDownLatch latch = new CountDownLatch(1);
    waitLatches.add(latch);

    
    LiveNodesListener listener = (o, n) -> {
      boolean matches = predicate.matches(o, n);
      if (matches)
        latch.countDown();
      return matches;
    };
    
    registerLiveNodesListener(listener);

    try {
      // wait for the watcher predicate to return true, or time out
      if (!latch.await(wait, unit))
        throw new TimeoutException("Timeout waiting for live nodes, currently they are: " + getClusterState().getLiveNodes());

    }
    finally {
      removeLiveNodesListener(listener);
      waitLatches.remove(latch);
    }
  }

  
  /**
   * Remove a watcher from a collection's watch list.
   *
   * This allows Zookeeper watches to be removed if there is no interest in the
   * collection.
   *
   * @param collection the collection
   * @param watcher    the watcher
   */
  public void removeCollectionStateWatcher(String collection, CollectionStateWatcher watcher) {
    AtomicBoolean reconstructState = new AtomicBoolean(false);
    collectionWatches.compute(collection, (k, v) -> {
      if (v == null)
        return null;
      v.stateWatchers.remove(watcher);
      if (v.canBeRemoved()) {
        watchedCollectionStates.remove(collection);
        lazyCollectionStates.put(collection, new LazyCollectionRef(collection));
        reconstructState.set(true);
        return null;
      }
      return v;
    });
    if (reconstructState.get()) {
      synchronized (getUpdateLock()) {
        constructState(Collections.emptySet());
      }
    }
  }

  /* package-private for testing */
  Set<CollectionStateWatcher> getStateWatchers(String collection) {
    final Set<CollectionStateWatcher> watchers = new HashSet<>();
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

    if (newState == null) {
      log.debug("Removing cached collection state for [{}]", coll);
      watchedCollectionStates.remove(coll);
      return true;
    }

    boolean updated = false;
    // CAS update loop
    while (true) {
      if (!collectionWatches.containsKey(coll)) {
        break;
      }
      DocCollection oldState = watchedCollectionStates.get(coll);
      if (oldState == null) {
        if (watchedCollectionStates.putIfAbsent(coll, newState) == null) {
          log.debug("Add data for [{}] ver [{}]", coll, newState.getZNodeVersion());
          updated = true;
          break;
        }
      } else {
        if (oldState.getZNodeVersion() >= newState.getZNodeVersion()) {
          // no change to state, but we might have been triggered by the addition of a
          // state watcher, so run notifications
          updated = true;
          break;
        }
        if (watchedCollectionStates.replace(coll, oldState, newState)) {
          log.debug("Updating data for [{}] from [{}] to [{}]", coll, oldState.getZNodeVersion(), newState.getZNodeVersion());
          updated = true;
          break;
        }
      }
    }

    // Resolve race with unregisterCore.
    if (!collectionWatches.containsKey(coll)) {
      watchedCollectionStates.remove(coll);
      log.debug("Removing uninteresting collection [{}]", coll);
    }

    return updated;
  }

  public void registerCollectionPropsWatcher(final String collection, CollectionPropsWatcher propsWatcher) {
    AtomicBoolean watchSet = new AtomicBoolean(false);
    collectionPropsWatches.compute(collection, (k, v) -> {
      if (v == null) {
        v = new CollectionWatch<>();
        watchSet.set(true);
      }
      v.stateWatchers.add(propsWatcher);
      return v;
    });

    if (watchSet.get()) {
      new PropsWatcher(collection).refreshAndWatch(false);
    }
  }

  public void removeCollectionPropsWatcher(String collection, CollectionPropsWatcher watcher) {
    collectionPropsWatches.compute(collection, (k, v) -> {
      if (v == null)
        return null;
      v.stateWatchers.remove(watcher);
      if (v.canBeRemoved()) {
        watchedCollectionProps.remove(collection);
        return null;
      }
      return v;
    });
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

  private void notifyStateWatchers(Set<String> liveNodes, String collection, DocCollection collectionState) {
    if (this.closed) {
      return;
    }
    try {
      notifications.submit(new Notification(liveNodes, collection, collectionState));
    }
    catch (RejectedExecutionException e) {
      if (closed == false) {
        log.error("Couldn't run collection notifications for {}", collection, e);
      }
    }
  }

  private class Notification implements Runnable {

    final Set<String> liveNodes;
    final String collection;
    final DocCollection collectionState;

    private Notification(Set<String> liveNodes, String collection, DocCollection collectionState) {
      this.liveNodes = liveNodes;
      this.collection = collection;
      this.collectionState = collectionState;
    }

    @Override
    public void run() {
      List<CollectionStateWatcher> watchers = new ArrayList<>();
      collectionWatches.compute(collection, (k, v) -> {
        if (v == null)
          return null;
        watchers.addAll(v.stateWatchers);
        return v;
      });
      for (CollectionStateWatcher watcher : watchers) {
        try {
          if (watcher.onStateChanged(liveNodes, collectionState)) {
            removeCollectionStateWatcher(collection, watcher);
          }
        } catch (Exception exception) {
          log.warn("Error on calling watcher", exception);
        }
      }
    }

  }

  //
  //  Aliases related
  //

  /** Access to the {@link Aliases}. */
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
    synchronized (getUpdateLock()) {
      constructState(Collections.emptySet());
      zkClient.exists(ALIASES, watcher, true);
    }
    aliasesManager.update();
  }

  /**
   * A class to manage the aliases instance, including watching for changes.
   * There should only ever be one instance of this class
   * per instance of ZkStateReader. Normally it will not be useful to create a new instance since
   * this watcher automatically re-registers itself every time it is updated.
   */
  public class AliasesManager implements Watcher  { // the holder is a Zk watcher
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
      for (int triesLeft = 30; triesLeft > 0; triesLeft--) {
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
            log.debug(e.toString(), e);
            log.warn("Couldn't save aliases due to race with another modification; will update and retry until timeout");
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
      collectionPropsNotifications.submit(new PropsNotification(collection, properties));
    } catch (RejectedExecutionException e) {
      if (!closed) {
        log.error("Couldn't run collection properties notifications for {}", collection, e);
      }
    }
  }

  private class PropsNotification implements Runnable {

    final String collection;
    final Map<String, String> collectionProperties;

    private PropsNotification(String collection, Map<String, String> collectionProperties) {
      this.collection = collection;
      this.collectionProperties = collectionProperties;
    }

    @Override
    public void run() {
      List<CollectionPropsWatcher> watchers = new ArrayList<>();
      collectionPropsWatches.compute(collection, (k, v) -> {
        if (v == null)
          return null;
        watchers.addAll(v.stateWatchers);
        return v;
      });
      for (CollectionPropsWatcher watcher : watchers) {
        if (watcher.onStateChanged(collectionProperties)) {
          removeCollectionPropsWatcher(collection, watcher);
        }
      }
    }

  }

}
