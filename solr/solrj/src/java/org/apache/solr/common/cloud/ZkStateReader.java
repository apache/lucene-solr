package org.apache.solr.common.cloud;

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
 * Unless required byOCP applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.Closeable;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.Callable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
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
import static java.util.Collections.unmodifiableSet;
import static org.apache.solr.common.util.Utils.fromJSON;

public class ZkStateReader implements Closeable {
  private static Logger log = LoggerFactory.getLogger(ZkStateReader.class);
  
  public static final String BASE_URL_PROP = "base_url";
  public static final String NODE_NAME_PROP = "node_name";
  public static final String CORE_NODE_NAME_PROP = "core_node_name";
  public static final String ROLES_PROP = "roles";
  public static final String STATE_PROP = "state";
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
  public static final String PROPERTY_PROP = "property";
  public static final String PROPERTY_VALUE_PROP = "property.value";
  public static final String MAX_AT_ONCE_PROP = "maxAtOnce";
  public static final String MAX_WAIT_SECONDS_PROP = "maxWaitSeconds";
  public static final String COLLECTIONS_ZKNODE = "/collections";
  public static final String LIVE_NODES_ZKNODE = "/live_nodes";
  public static final String ALIASES = "/aliases.json";
  public static final String CLUSTER_STATE = "/clusterstate.json";
  public static final String CLUSTER_PROPS = "/clusterprops.json";
  public static final String REJOIN_AT_HEAD_PROP = "rejoinAtHead";
  public static final String SOLR_SECURITY_CONF_PATH = "/security.json";

  public static final String REPLICATION_FACTOR = "replicationFactor";
  public static final String MAX_SHARDS_PER_NODE = "maxShardsPerNode";
  public static final String AUTO_ADD_REPLICAS = "autoAddReplicas";

  public static final String ROLES = "/roles.json";

  public static final String CONFIGS_ZKNODE = "/configs";
  public final static String CONFIGNAME_PROP="configName";

  public static final String LEGACY_CLOUD = "legacyCloud";

  public static final String URL_SCHEME = "urlScheme";

  /** A view of the current state of all collections; combines all the different state sources into a single view. */
  protected volatile ClusterState clusterState;

  private static final int GET_LEADER_RETRY_INTERVAL_MS = 50;
  private static final int GET_LEADER_RETRY_DEFAULT_TIMEOUT = 4000;

  public static final String LEADER_ELECT_ZKNODE = "leader_elect";

  public static final String SHARD_LEADERS_ZKNODE = "leaders";
  public static final String ELECTION_NODE = "election";

  /** Collections we actively care about, and will try to keep watch on. */
  private final Set<String> interestingCollections = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  /** Collections tracked in the legacy (shared) state format, reflects the contents of clusterstate.json. */
  private Map<String, ClusterState.CollectionRef> legacyCollectionStates = emptyMap();
  /** Last seen ZK version of clusterstate.json. */
  private int legacyClusterStateVersion = 0;

  /** Collections with format2 state.json, "interesting" and actively watched. */
  private final ConcurrentHashMap<String, DocCollection> watchedCollectionStates = new ConcurrentHashMap<String, DocCollection>();

  /** Collections with format2 state.json, not "interesting" and not actively watched. */
  private volatile Map<String, ClusterState.CollectionRef> lazyCollectionStates = new HashMap<>();

  private volatile Set<String> liveNodes = emptySet();

  private final ZkConfigManager configManager;

  private ConfigData securityData;

  private final Runnable securityNodeListener;

  public static final Set<String> KNOWN_CLUSTER_PROPS = unmodifiableSet(new HashSet<>(asList(
      LEGACY_CLOUD,
      URL_SCHEME,
      AUTO_ADD_REPLICAS)));

  /**
   * Returns config set name for collection.
   *
   * @param collection to return config set name for
   */
  public String readConfigName(String collection) {

    String configName = null;

    String path = COLLECTIONS_ZKNODE + "/" + collection;
    if (log.isInfoEnabled()) {
      log.info("Load collection config from:" + path);
    }

    try {
      byte[] data = zkClient.getData(path, null, null, true);

      if(data != null) {
        ZkNodeProps props = ZkNodeProps.load(data);
        configName = props.getStr(CONFIGNAME_PROP);
      }

      if (configName != null) {
        if (!zkClient.exists(CONFIGS_ZKNODE + "/" + configName, true)) {
          log.error("Specified config does not exist in ZooKeeper:" + configName);
          throw new ZooKeeperException(ErrorCode.SERVER_ERROR,
              "Specified config does not exist in ZooKeeper:" + configName);
        } else if (log.isInfoEnabled()) {
          log.info("path={} {}={} specified config exists in ZooKeeper",
              new Object[] {path, CONFIGNAME_PROP, configName});
        }
      } else {
        throw new ZooKeeperException(ErrorCode.INVALID_STATE, "No config data found at path: " + path);
      }
    }
    catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error loading config name for collection " + collection, e);
    }
    catch (InterruptedException e) {
      Thread.interrupted();
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error loading config name for collection " + collection, e);
    }

    return configName;
  }


  private static class ZKTF implements ThreadFactory {
    private static ThreadGroup tg = new ThreadGroup("ZkStateReader");
    @Override
    public Thread newThread(Runnable r) {
      Thread td = new Thread(tg, r);
      td.setDaemon(true);
      return td;
    }
  }

  private final SolrZkClient zkClient;
  
  private final boolean closeClient;

  private volatile Aliases aliases = new Aliases();

  private volatile boolean closed = false;

  public ZkStateReader(SolrZkClient zkClient) {
    this(zkClient, null);
  }

  public ZkStateReader(SolrZkClient zkClient, Runnable securityNodeListener) {
    this.zkClient = zkClient;
    this.configManager = new ZkConfigManager(zkClient);
    this.closeClient = false;
    this.securityNodeListener = securityNodeListener;
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
              log.error("", e);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                  "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.error("", e);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                  "", e);
            }
          }
        });
    this.configManager = new ZkConfigManager(zkClient);
    this.closeClient = true;
    this.securityNodeListener = null;
  }

  public ZkConfigManager getConfigManager() {
    return configManager;
  }

  /**
   * Forcibly refresh cluster state from ZK. Do this only to avoid race conditions because it's expensive.
   */
  public void updateClusterState() throws KeeperException, InterruptedException {
    synchronized (getUpdateLock()) {
      if (clusterState == null) {
        // Never initialized, just run normal initialization.
        createClusterStateWatchersAndUpdate();
        return;
      }
      // No need to set watchers because we should already have watchers registered for everything.
      refreshLegacyClusterState(null);
      for (String coll : watchedCollectionStates.keySet()) {
        DocCollection newState = fetchCollectionState(coll, null);
        updateWatchedCollection(coll, newState);
      }
      refreshLazyFormat2Collections(true);
      refreshLiveNodes(null);
      constructState();
    }
  }

  /** Refresh the set of live nodes. */
  public void updateLiveNodes() throws KeeperException, InterruptedException {
    refreshLiveNodes(null);
  }
  
  public Aliases getAliases() {
    return aliases;
  }

  public Integer compareStateVersions(String coll, int version) {
    DocCollection collection = clusterState.getCollectionOrNull(coll);
    if (collection == null) return null;
    if (collection.getZNodeVersion() < version) {
      log.debug("server older than client {}<{}", collection.getZNodeVersion(), version);
      DocCollection nu = getCollectionLive(this, coll);
      if (nu == null) return -1 ;
      if (nu.getZNodeVersion() > collection.getZNodeVersion()) {
        updateWatchedCollection(coll, nu);
        collection = nu;
      }
    }
    
    if (collection.getZNodeVersion() == version) {
      return null;
    }
    
    log.debug("wrong version from client {}!={} ", version, collection.getZNodeVersion());
    
    return collection.getZNodeVersion();
  }
  
  public synchronized void createClusterStateWatchersAndUpdate() throws KeeperException,
      InterruptedException {
    // We need to fetch the current cluster state and the set of live nodes

    log.info("Updating cluster state from ZooKeeper... ");

    // Sanity check ZK structure.
    if (!zkClient.exists(CLUSTER_STATE, true)) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
              "Cannot connect to cluster at " + zkClient.getZkServerAddress() + ": cluster not found/not ready");
    }

    // on reconnect of SolrZkClient force refresh and re-add watches.
    refreshLegacyClusterState(new LegacyClusterStateWatcher());
    refreshStateFormat2Collections();
    refreshLazyFormat2Collections(true);
    refreshLiveNodes(new LiveNodeWatcher());

    synchronized (ZkStateReader.this.getUpdateLock()) {
      constructState();

      zkClient.exists(ALIASES,
          new Watcher() {
            
            @Override
            public void process(WatchedEvent event) {
              // session events are not change events,
              // and do not remove the watcher
              if (EventType.None.equals(event.getType())) {
                return;
              }
              try {
                synchronized (ZkStateReader.this.getUpdateLock()) {
                  log.info("Updating aliases... ");

                  // remake watch
                  final Watcher thisWatch = this;
                  Stat stat = new Stat();
                  byte[] data = zkClient.getData(ALIASES, thisWatch, stat ,
                      true);

                  Aliases aliases = ClusterState.load(data);

                  ZkStateReader.this.aliases = aliases;
                }
              } catch (KeeperException e) {
                if (e.code() == KeeperException.Code.SESSIONEXPIRED
                    || e.code() == KeeperException.Code.CONNECTIONLOSS) {
                  log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
                  return;
                }
                log.error("", e);
                throw new ZooKeeperException(
                    SolrException.ErrorCode.SERVER_ERROR, "", e);
              } catch (InterruptedException e) {
                // Restore the interrupted status
                Thread.currentThread().interrupt();
                log.warn("", e);
                return;
              }
            }
            
          }, true);
    }
    updateAliases();

    if (securityNodeListener != null) {
      addSecuritynodeWatcher(SOLR_SECURITY_CONF_PATH, new Callable<Pair<byte[], Stat>>() {
        @Override
        public void call(Pair<byte[], Stat> pair) {
          ConfigData cd = new ConfigData();
          cd.data = pair.getKey() == null || pair.getKey().length == 0 ? EMPTY_MAP : Utils.getDeepCopy((Map) fromJSON(pair.getKey()), 4, false);
          cd.version = pair.getValue() == null ? -1 : pair.getValue().getVersion();
          securityData = cd;
          securityNodeListener.run();
        }
      });
    }
  }

  //todo remove when we move to java 8
  static public class Pair<K, V> implements Serializable {
    private K key;

    public K getKey() {
      return key;
    }

    private V value;

    public V getValue() {
      return value;
    }

    public Pair(K key, V value) {
      this.key = key;
      this.value = value;
    }
  }


  private void addSecuritynodeWatcher(final String path, final Callable<Pair<byte[], Stat>> callback)
      throws KeeperException, InterruptedException {
    zkClient.exists(SOLR_SECURITY_CONF_PATH,
        new Watcher() {

          @Override
          public void process(WatchedEvent event) {
            // session events are not change events,
            // and do not remove the watcher
            if (EventType.None.equals(event.getType())) {
              return;
            }
            try {
              synchronized (ZkStateReader.this.getUpdateLock()) {
                log.info("Updating {} ... ", path);

                // remake watch
                final Watcher thisWatch = this;
                Stat stat = new Stat();
                byte[] data = getZkClient().getData(path, thisWatch, stat, true);
                try {
                  callback.call(new Pair<>(data, stat));
                } catch (Exception e) {
                  if (e instanceof KeeperException) throw (KeeperException) e;
                  if (e instanceof InterruptedException) throw (InterruptedException) e;
                  log.error("Error running collections node listener", e);
                }
              }
            } catch (KeeperException e) {
              if (e.code() == KeeperException.Code.SESSIONEXPIRED
                  || e.code() == KeeperException.Code.CONNECTIONLOSS) {
                log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
                return;
              }
              log.error("", e);
              throw new ZooKeeperException(
                  ErrorCode.SERVER_ERROR, "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.warn("", e);
              return;
            }
          }

        }, true);
  }

  /**
   * Construct the total state view from all sources.
   * Must hold {@link #getUpdateLock()} before calling this.
   */
  private void constructState() {
    // Legacy clusterstate is authoritative, for backwards compatibility.
    // To move a collection's state to format2, first create the new state2 format node, then remove legacy entry.
    Map<String, ClusterState.CollectionRef> result = new LinkedHashMap<>(legacyCollectionStates);

    // Are there any interesting collections that disappeared from the legacy cluster state?
    for (String coll : interestingCollections) {
      if (!result.containsKey(coll) && !watchedCollectionStates.containsKey(coll)) {
        new StateWatcher(coll).refreshAndWatch(true);
      }
    }
  
    // Add state format2 collections, but don't override legacy collection states.
    for (Map.Entry<String, DocCollection> entry : watchedCollectionStates.entrySet()) {
      if (!result.containsKey(entry.getKey())) {
        result.put(entry.getKey(), new ClusterState.CollectionRef(entry.getValue()));
      }
    }

    // Finally, add any lazy collections that aren't already accounted for.
    for (Map.Entry<String, ClusterState.CollectionRef> entry : lazyCollectionStates.entrySet()) {
      if (!result.containsKey(entry.getKey())) {
        result.put(entry.getKey(), entry.getValue());
      }
    }

    this.clusterState = new ClusterState(liveNodes, result, legacyClusterStateVersion);
  }

  /**
   * Refresh legacy (shared) clusterstate.json
   */
  private void refreshLegacyClusterState(Watcher watcher)
      throws KeeperException, InterruptedException {
    try {
      Stat stat = new Stat();
      byte[] data = zkClient.getData(CLUSTER_STATE, watcher, stat, true);
      ClusterState loadedData = ClusterState.load(stat.getVersion(), data, Collections.<String>emptySet(), CLUSTER_STATE);
      synchronized (getUpdateLock()) {
        this.legacyCollectionStates = loadedData.getCollectionStates();
        this.legacyClusterStateVersion = stat.getVersion();
      }
    } catch (KeeperException.NoNodeException e) {
      // Ignore missing legacy clusterstate.json.
      synchronized (getUpdateLock()) {
        this.legacyCollectionStates = emptyMap();
        this.legacyClusterStateVersion = 0;
      }
    }
  }

  /**
   * Refresh state format2 collections.
   */
  private void refreshStateFormat2Collections() {
    // It's okay if no format2 state.json exists, if one did not previous exist.
    for (String coll : interestingCollections) {
      new StateWatcher(coll).refreshAndWatch(watchedCollectionStates.containsKey(coll));
    }
  }

  /**
   * Search for any lazy-loadable state format2 collections.
   */
  private void refreshLazyFormat2Collections(boolean fullRefresh) throws KeeperException, InterruptedException {
    List<String> children = null;
    try {
      children = zkClient.getChildren(COLLECTIONS_ZKNODE, null, true);
    } catch (KeeperException.NoNodeException e) {
      log.warn("Error fetching collection names");
      // fall through
    }
    if (children == null || children.isEmpty()) {
      synchronized (getUpdateLock()) {
        this.lazyCollectionStates = new HashMap<>();
      }
      return;
    }

    Map<String, ClusterState.CollectionRef> result = new HashMap<>();
    for (String collName : children) {
      if (interestingCollections.contains(collName)) {
        // We will create an eager collection for any interesting collections.
        continue;
      }

      if (!fullRefresh) {
        // Try to use an already-created lazy collection if it's not a full refresh.
        ClusterState.CollectionRef existing = lazyCollectionStates.get(collName);
        if (existing != null) {
          result.put(collName, existing);
          continue;
        }
      }

      ClusterState.CollectionRef lazyCollectionState = tryMakeLazyCollectionStateFormat2(collName);
      if (lazyCollectionState != null) {
        result.put(collName, lazyCollectionState);
      }
    }

    synchronized (getUpdateLock()) {
      this.lazyCollectionStates = result;
    }
  }

  private ClusterState.CollectionRef tryMakeLazyCollectionStateFormat2(final String collName) {
    boolean exists = false;
    try {
      exists = zkClient.exists(getCollectionPath(collName), true);
    } catch (Exception e) {
      log.warn("Error reading collections nodes", e);
    }
    if (!exists) {
      return null;
    }

    // if it is not collection, then just create a reference which can fetch
    // the collection object just in time from ZK
    return new ClusterState.CollectionRef(null) {
      @Override
      public DocCollection get() {
        return getCollectionLive(ZkStateReader.this, collName);
      }

      @Override
      public boolean isLazilyLoaded() {
        return true;
      }

      @Override
      public String toString() {
        return "lazy DocCollection(" + collName + ")";
      }
    };
  }

  /**
   * Refresh live_nodes.
   */
  private void refreshLiveNodes(Watcher watcher) throws KeeperException, InterruptedException {
    Set<String> newLiveNodes;
    try {
      List<String> nodeList = zkClient.getChildren(LIVE_NODES_ZKNODE, watcher, true);
      log.debug("Updating live nodes from ZooKeeper... ({})", nodeList.size());
      newLiveNodes = new HashSet<>(nodeList);
    } catch (KeeperException.NoNodeException e) {
      newLiveNodes = emptySet();
    }
    synchronized (getUpdateLock()) {
      this.liveNodes = newLiveNodes;
      if (clusterState != null) {
        clusterState.setLiveNodes(newLiveNodes);
      }
    }
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
    if (closeClient) {
      zkClient.close();
    }
  }
  
  public String getLeaderUrl(String collection, String shard, int timeout)
      throws InterruptedException, KeeperException {
    ZkCoreNodeProps props = new ZkCoreNodeProps(getLeaderRetry(collection,
        shard, timeout));
    return props.getCoreUrl();
  }

  public Replica getLeader(String collection, String shard) throws InterruptedException {
    if (clusterState != null) {
      Replica replica = clusterState.getLeader(collection, shard);
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
    long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);
    while (true) {
      Replica leader = getLeader(collection, shard);
      if (leader != null) return leader;
      if (System.nanoTime() >= timeoutAt || closed) break;
      Thread.sleep(GET_LEADER_RETRY_INTERVAL_MS);
    }
    throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "No registered leader was found after waiting for "
        + timeout + "ms " + ", collection: " + collection + " slice: " + shard);
  }

  /**
   * Get path where shard leader properties live in zookeeper.
   */
  public static String getShardLeadersPath(String collection, String shardId) {
    return COLLECTIONS_ZKNODE + "/" + collection + "/"
        + SHARD_LEADERS_ZKNODE + (shardId != null ? ("/" + shardId)
        : "");
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
    assert thisCoreNodeName != null;
    ClusterState clusterState = this.clusterState;
    if (clusterState == null) {
      return null;
    }
    Map<String,Slice> slices = clusterState.getSlicesMap(collection);
    if (slices == null) {
      throw new ZooKeeperException(ErrorCode.BAD_REQUEST,
          "Could not find collection in zk: " + collection + " "
              + clusterState.getCollections());
    }
    
    Slice replicas = slices.get(shardId);
    if (replicas == null) {
      throw new ZooKeeperException(ErrorCode.BAD_REQUEST, "Could not find shardId in zk: " + shardId);
    }
    
    Map<String,Replica> shardMap = replicas.getReplicasMap();
    List<ZkCoreNodeProps> nodes = new ArrayList<>(shardMap.size());
    for (Entry<String,Replica> entry : shardMap.entrySet()) {
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

  public void updateAliases() throws KeeperException, InterruptedException {
    byte[] data = zkClient.getData(ALIASES, null, null, true);

    Aliases aliases = ClusterState.load(data);

    ZkStateReader.this.aliases = aliases;
  }
  public Map getClusterProps(){
    Map result = null;
    try {
      if(getZkClient().exists(ZkStateReader.CLUSTER_PROPS, true)){
        result = (Map) Utils.fromJSON(getZkClient().getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true)) ;
      } else {
        result= new LinkedHashMap();
      }
      return result;
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR,"Error reading cluster properties",e) ;
    }
  }

  /**
   * This method sets a cluster property.
   *
   * @param propertyName  The property name to be set.
   * @param propertyValue The value of the property.
   */
  public void setClusterProperty(String propertyName, String propertyValue) {
    if (!KNOWN_CLUSTER_PROPS.contains(propertyName)) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Not a known cluster property " + propertyName);
    }

    for (; ; ) {
      Stat s = new Stat();
      try {
        if (getZkClient().exists(CLUSTER_PROPS, true)) {
          int v = 0;
          Map properties = (Map) Utils.fromJSON(getZkClient().getData(CLUSTER_PROPS, null, s, true));
          if (propertyValue == null) {
            //Don't update ZK unless absolutely necessary.
            if (properties.get(propertyName) != null) {
              properties.remove(propertyName);
              getZkClient().setData(CLUSTER_PROPS, Utils.toJSON(properties), s.getVersion(), true);
            }
          } else {
            //Don't update ZK unless absolutely necessary.
            if (!propertyValue.equals(properties.get(propertyName))) {
              properties.put(propertyName, propertyValue);
              getZkClient().setData(CLUSTER_PROPS, Utils.toJSON(properties), s.getVersion(), true);
            }
          }
        } else {
          Map properties = new LinkedHashMap();
          properties.put(propertyName, propertyValue);
          getZkClient().create(CLUSTER_PROPS, Utils.toJSON(properties), CreateMode.PERSISTENT, true);
        }
      } catch (KeeperException.BadVersionException bve) {
        log.warn("Race condition while trying to set a new cluster prop on current version " + s.getVersion());
        //race condition
        continue;
      } catch (KeeperException.NodeExistsException nee) {
        log.warn("Race condition while trying to set a new cluster prop on current version " + s.getVersion());
        //race condition
        continue;
      } catch (Exception ex) {
        log.error("Error updating path " + CLUSTER_PROPS, ex);
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error updating cluster property " + propertyName, ex);
      }
      break;
    }
  }



  /**
   * Returns the content of /security.json from ZooKeeper as a Map
   * If the files doesn't exist, it returns null.
   */
  public ConfigData getSecurityProps(boolean getFresh) {
    if (!getFresh) {
      if (securityData == null) return new ConfigData(EMPTY_MAP, -1);
      return new ConfigData(securityData.data, securityData.version);
    }
    try {
      Stat stat = new Stat();
      if(getZkClient().exists(SOLR_SECURITY_CONF_PATH, true)) {
        byte[] data = getZkClient()
            .getData(ZkStateReader.SOLR_SECURITY_CONF_PATH, null, stat, true);
        return data != null && data.length > 0 ?
            new ConfigData((Map<String, Object>) Utils.fromJSON(data), stat.getVersion()) :
            null;
      }
    } catch (KeeperException | InterruptedException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR,"Error reading security properties",e) ;
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
    final int _offset = nodeName.indexOf("_");
    if (_offset < 0) {
      throw new IllegalArgumentException("nodeName does not contain expected '_' seperator: " + nodeName);
    }
    final String hostAndPort = nodeName.substring(0,_offset);
    try {
      final String path = URLDecoder.decode(nodeName.substring(1+_offset), "UTF-8");
      String urlScheme = (String) getClusterProps().get(URL_SCHEME);
      if(urlScheme == null) {
        urlScheme = "http";
      }
      return urlScheme + "://" + hostAndPort + (path.isEmpty() ? "" : ("/" + path));
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("JVM Does not seem to support UTF-8", e);
    }
  }

  /** Watches a single collection's format2 state.json. */
  class StateWatcher implements Watcher {
    private final String coll;

    StateWatcher(String coll) {
      this.coll = coll;
    }

    @Override
    public void process(WatchedEvent event) {
      if (!interestingCollections.contains(coll)) {
        // This collection is no longer interesting, stop watching.
        log.info("Uninteresting collection {}", coll);
        return;
      }

      // session events are not change events,
      // and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }

      log.info("A cluster state change: {} for collection {} has occurred - updating... (live nodes size: {})",
              (event), coll, ZkStateReader.this.clusterState == null ? 0
                      : ZkStateReader.this.clusterState.getLiveNodes().size());

      refreshAndWatch(true);
      synchronized (getUpdateLock()) {
        constructState();
      }
    }

    /**
     * Refresh collection state from ZK and leave a watch for future changes.
     * As a side effect, updates {@link #clusterState} and {@link #watchedCollectionStates}
     * with the results of the refresh.
     *
     * @param expectExists if true, error if no state node exists
     */
    public void refreshAndWatch(boolean expectExists) {
      try {
        DocCollection newState = fetchCollectionState(coll, this);
        updateWatchedCollection(coll, newState);
      } catch (KeeperException.NoNodeException e) {
        if (expectExists) {
          log.warn("State node vanished for collection: " + coll, e);
        }
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.SESSIONEXPIRED
                || e.code() == KeeperException.Code.CONNECTIONLOSS) {
          log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
          return;
        }
        log.error("Unwatched collection: " + coll, e);
        throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "", e);

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        log.error("Unwatched collection :" + coll, e);
      }
    }
  }

  /** Watches the legacy clusterstate.json. */
  class LegacyClusterStateWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events,
      // and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      log.info("A cluster state change: {}, has occurred - updating... (live nodes size: {})", (event), ZkStateReader.this.clusterState == null ? 0 : ZkStateReader.this.clusterState.getLiveNodes().size());
      refreshAndWatch();
      synchronized (getUpdateLock()) {
        constructState();
      }
    }

    /** Must hold {@link #getUpdateLock()} before calling this method. */
    public void refreshAndWatch() {
      try {
        refreshLegacyClusterState(this);
        // Changes to clusterstate.json signal global state changes.
        // TODO: get rid of clusterstate.json as a signaling mechanism.
        refreshLazyFormat2Collections(false);
      } catch (KeeperException.NoNodeException e) {
        throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE,
                "Cannot connect to cluster at " + zkClient.getZkServerAddress() + ": cluster not found/not ready");
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.SESSIONEXPIRED
                || e.code() == KeeperException.Code.CONNECTIONLOSS) {
          log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
          return;
        }
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("", e);
      }
    }
  }

  /** Watches the live_nodes and syncs changes. */
  class LiveNodeWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      // session events are not change events,
      // and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      log.info("A live node change: {}, has occurred - updating... (live nodes size: {})", (event), liveNodes.size());
      refreshAndWatch();
    }

    /** Must hold {@link #getUpdateLock()} before calling this method. */
    public void refreshAndWatch() {
      try {
        refreshLiveNodes(this);
      } catch (KeeperException e) {
        if (e.code() == KeeperException.Code.SESSIONEXPIRED
            || e.code() == KeeperException.Code.CONNECTIONLOSS) {
          log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
          return;
        }
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("", e);
      }
    }
  }

  public static DocCollection getCollectionLive(ZkStateReader zkStateReader,
      String coll) {
    try {
      return zkStateReader.fetchCollectionState(coll, null);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Could not load collection from ZK:" + coll, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Could not load collection from ZK:" + coll, e);
    }
  }

  private DocCollection fetchCollectionState(String coll, Watcher watcher) throws KeeperException, InterruptedException {
    String collectionPath = getCollectionPath(coll);
    try {
      Stat stat = new Stat();
      byte[] data = zkClient.getData(collectionPath, watcher, stat, true);
      ClusterState state = ClusterState.load(stat.getVersion(), data,
              Collections.<String>emptySet(), collectionPath);
      ClusterState.CollectionRef collectionRef = state.getCollectionStates().get(coll);
      return collectionRef == null ? null : collectionRef.get();
    } catch (KeeperException.NoNodeException e) {
      return null;
    }
  }

  public static String getCollectionPath(String coll) {
    return COLLECTIONS_ZKNODE+"/"+coll + "/state.json";
  }

  public void addCollectionWatch(String coll) throws KeeperException, InterruptedException {
    if (interestingCollections.add(coll)) {
      log.info("addZkWatch {}", coll);
      new StateWatcher(coll).refreshAndWatch(false);
      synchronized (getUpdateLock()) {
        constructState();
      }
    }
  }

  private void updateWatchedCollection(String coll, DocCollection newState) {
    if (newState == null) {
      log.info("Deleting data for {}", coll);
      watchedCollectionStates.remove(coll);
      return;
    }

    log.info("Updating data for {} to ver {} ", coll, newState.getZNodeVersion());
    // CAS update loop
    while (true) {
      DocCollection oldState = watchedCollectionStates.get(coll);
      if (oldState == null) {
        if (watchedCollectionStates.putIfAbsent(coll, newState) == null) {
          break;
        }
      } else {
        if (oldState.getZNodeVersion() >= newState.getZNodeVersion()) {
          // Nothing to do, someone else updated same or newer.
          return;
        }
        if (watchedCollectionStates.replace(coll, oldState, newState)) {
          break;
        }
      }
    }
  }
  
  /** This is not a public API. Only used by ZkController */
  public void removeZKWatch(String coll) {
    interestingCollections.remove(coll);
    watchedCollectionStates.remove(coll);
    ClusterState.CollectionRef lazyCollectionStateFormat2 = tryMakeLazyCollectionStateFormat2(coll);
    synchronized (getUpdateLock()) {
      if (lazyCollectionStateFormat2 != null) {
        this.lazyCollectionStates.put(coll, lazyCollectionStateFormat2);
      }
      constructState();
    }
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
}
