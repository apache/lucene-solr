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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.ByteUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.noggit.CharArr;
import org.noggit.JSONParser;
import org.noggit.JSONWriter;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ZkStateReader implements Closeable {
  private static Logger log = LoggerFactory.getLogger(ZkStateReader.class);
  
  public static final String BASE_URL_PROP = "base_url";
  public static final String NODE_NAME_PROP = "node_name";
  public static final String CORE_NODE_NAME_PROP = "core_node_name";
  public static final String ROLES_PROP = "roles";
  public static final String STATE_PROP = "state";
  public static final String CORE_NAME_PROP = "core";
  public static final String COLLECTION_PROP = "collection";
  public static final String SHARD_ID_PROP = "shard";
  public static final String REPLICA_PROP = "replica";
  public static final String SHARD_RANGE_PROP = "shard_range";
  public static final String SHARD_STATE_PROP = "shard_state";
  public static final String SHARD_PARENT_PROP = "shard_parent";
  public static final String NUM_SHARDS_PROP = "numShards";
  public static final String LEADER_PROP = "leader";
  public static final String PROPERTY_PROP = "property";
  public static final String PROPERTY_VALUE_PROP = "property.value";
  
  public static final String COLLECTIONS_ZKNODE = "/collections";
  public static final String LIVE_NODES_ZKNODE = "/live_nodes";
  public static final String ALIASES = "/aliases.json";
  public static final String CLUSTER_STATE = "/clusterstate.json";
  public static final String CLUSTER_PROPS = "/clusterprops.json";

  public static final String REPLICATION_FACTOR = "replicationFactor";
  public static final String MAX_SHARDS_PER_NODE = "maxShardsPerNode";
  public static final String AUTO_ADD_REPLICAS = "autoAddReplicas";

  public static final String ROLES = "/roles.json";

  public static final String RECOVERING = "recovering";
  public static final String RECOVERY_FAILED = "recovery_failed";
  public static final String ACTIVE = "active";
  public static final String DOWN = "down";
  public static final String SYNC = "sync";

  public static final String CONFIGS_ZKNODE = "/configs";
  public final static String CONFIGNAME_PROP="configName";

  public static final String LEGACY_CLOUD = "legacyCloud";

  public static final String URL_SCHEME = "urlScheme";
  
  protected volatile ClusterState clusterState;

  private static final long SOLRCLOUD_UPDATE_DELAY = Long.parseLong(System.getProperty("solrcloud.update.delay", "5000"));

  public static final String LEADER_ELECT_ZKNODE = "/leader_elect";

  public static final String SHARD_LEADERS_ZKNODE = "leaders";

  private final Set<String> watchedCollections = new HashSet<String>();

  /**These are collections which are actively watched by this  instance .
   *
   */
  private Map<String , DocCollection> watchedCollectionStates = new ConcurrentHashMap<String, DocCollection>();


  //
  // convenience methods... should these go somewhere else?
  //
  public static byte[] toJSON(Object o) {
    CharArr out = new CharArr();
    new JSONWriter(out, 2).write(o); // indentation by default
    return toUTF8(out);
  }

  public static byte[] toUTF8(CharArr out) {
    byte[] arr = new byte[out.size() << 2]; // is 4x the real worst-case upper-bound?
    int nBytes = ByteUtils.UTF16toUTF8(out, 0, out.size(), arr, 0);
    return Arrays.copyOf(arr, nBytes);
  }

  public static Object fromJSON(byte[] utf8) {
    // convert directly from bytes to chars
    // and parse directly from that instead of going through
    // intermediate strings or readers
    CharArr chars = new CharArr();
    ByteUtils.UTF8toUTF16(utf8, 0, utf8.length, chars);
    JSONParser parser = new JSONParser(chars.getArray(), chars.getStart(), chars.length());
    try {
      return ObjectBuilder.getVal(parser);
    } catch (IOException e) {
      throw new RuntimeException(e); // should never happen w/o using real IO
    }
  }
  
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
  private ScheduledExecutorService updateCloudExecutor = Executors.newScheduledThreadPool(1, new ZKTF());

  private boolean clusterStateUpdateScheduled;

  private SolrZkClient zkClient;
  
  private boolean closeClient = false;

  private ZkCmdExecutor cmdExecutor;

  private volatile Aliases aliases = new Aliases();

  private volatile boolean closed = false;

  public ZkStateReader(SolrZkClient zkClient) {
    this.zkClient = zkClient;
    initZkCmdExecutor(zkClient.getZkClientTimeout());
  }

  public ZkStateReader(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) throws InterruptedException, TimeoutException, IOException {
    closeClient = true;
    initZkCmdExecutor(zkClientTimeout);
    zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
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
  }
  
  private void initZkCmdExecutor(int zkClientTimeout) {
    // we must retry at least as long as the session timeout
    cmdExecutor = new ZkCmdExecutor(zkClientTimeout);
  }
  
  // load and publish a new CollectionInfo
  public void updateClusterState(boolean immediate) throws KeeperException, InterruptedException {
    updateClusterState(immediate, false);
  }
  
  // load and publish a new CollectionInfo
  public void updateLiveNodes() throws KeeperException, InterruptedException {
    updateClusterState(true, true);
  }
  
  public Aliases getAliases() {
    return aliases;
  }

  public Boolean checkValid(String coll, int version) {
    DocCollection collection = clusterState.getCollectionOrNull(coll);
    if (collection == null) return null;
    if (collection.getZNodeVersion() < version) {
      log.debug("server older than client {}<{}", collection.getZNodeVersion(), version);
      DocCollection nu = getCollectionLive(this, coll);
      if (nu.getZNodeVersion() > collection.getZNodeVersion()) {
        updateWatchedCollection(nu);
        collection = nu;
      }
    }
    
    if (collection.getZNodeVersion() == version) {
      return Boolean.TRUE;
    }
    
    log.debug("wrong version from client {}!={} ", version, collection.getZNodeVersion());
    
    return Boolean.FALSE;
  }
  
  public synchronized void createClusterStateWatchersAndUpdate() throws KeeperException,
      InterruptedException {
    // We need to fetch the current cluster state and the set of live nodes
    
    synchronized (getUpdateLock()) {
      cmdExecutor.ensureExists(CLUSTER_STATE, zkClient);
      cmdExecutor.ensureExists(ALIASES, zkClient);
      
      log.info("Updating cluster state from ZooKeeper... ");
      
      zkClient.exists(CLUSTER_STATE, new Watcher() {
        
        @Override
        public void process(WatchedEvent event) {
          // session events are not change events,
          // and do not remove the watcher
          if (EventType.None.equals(event.getType())) {
            return;
          }
          log.info("A cluster state change: {}, has occurred - updating... (live nodes size: {})", (event) , ZkStateReader.this.clusterState == null ? 0 : ZkStateReader.this.clusterState.getLiveNodes().size());
          try {
            
            // delayed approach
            // ZkStateReader.this.updateClusterState(false, false);
            synchronized (ZkStateReader.this.getUpdateLock()) {
              // remake watch
              final Watcher thisWatch = this;
              Set<String> ln = ZkStateReader.this.clusterState.getLiveNodes();
              // update volatile
              ZkStateReader.this.clusterState = constructState(ln, thisWatch);
            }
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
            return;
          }
        }
        
      }, true);
    }
   
    
    synchronized (ZkStateReader.this.getUpdateLock()) {
      List<String> liveNodes = zkClient.getChildren(LIVE_NODES_ZKNODE,
          new Watcher() {
            
            @Override
            public void process(WatchedEvent event) {
              // session events are not change events,
              // and do not remove the watcher
              if (EventType.None.equals(event.getType())) {
                return;
              }
              try {
                // delayed approach
                // ZkStateReader.this.updateClusterState(false, true);
                synchronized (ZkStateReader.this.getUpdateLock()) {
                  List<String> liveNodes = zkClient.getChildren(
                      LIVE_NODES_ZKNODE, this, true);
                  log.debug("Updating live nodes... ({})", liveNodes.size());
                  Set<String> liveNodesSet = new HashSet<>();
                  liveNodesSet.addAll(liveNodes);

                  ClusterState clusterState =  ZkStateReader.this.clusterState;

                  clusterState.setLiveNodes(liveNodesSet);
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
    
      Set<String> liveNodeSet = new HashSet<>();
      liveNodeSet.addAll(liveNodes);
      this.clusterState = constructState(liveNodeSet, null);

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
    //on reconnect of SolrZkClient re-add watchers for the watched external collections
    synchronized (this) {
      for (String watchedCollection : watchedCollections) {
        addZkWatch(watchedCollection);
      }
    }
  }

  private ClusterState constructState(Set<String> ln, Watcher watcher)
      throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    byte[] data = zkClient.getData(CLUSTER_STATE, watcher, stat, true);
    ClusterState loadedData = ClusterState.load(stat.getVersion(), data, ln,
        CLUSTER_STATE);
    Map<String,ClusterState.CollectionRef> result = new LinkedHashMap<>();
    result.putAll(loadedData.getCollectionStates());// first load all
                                                    // collections in
                                                    // clusterstate.json
    for (String s : getIndividualColls()) {
      DocCollection watched = watchedCollectionStates.get(s);
      if (watched != null) {
        // if it is a watched collection, add too
        result.put(s, new ClusterState.CollectionRef(watched));
      } else {
        // if it is not collection, then just create a reference which can fetch 
        // the collection object just in time from ZK
        final String collName = s;
        result.put(s, new ClusterState.CollectionRef(null) {
          @Override
          public DocCollection get() {
            return getCollectionLive(ZkStateReader.this, collName);
          }
        });
      }
    }
    return new ClusterState(ln, result, stat.getVersion());
  }


  private Set<String> getIndividualColls() throws KeeperException, InterruptedException {
    List<String> children = null;
    try {
      children = zkClient.getChildren(COLLECTIONS_ZKNODE, null, true);
    } catch (KeeperException.NoNodeException e) {
      log.warn("Error fetching collection names");
      
      return new HashSet<>();
    }
    if (children == null || children.isEmpty()) return new HashSet<>();
    HashSet<String> result = new HashSet<>(children.size());
    
    for (String c : children) {
      try {
        if (zkClient.exists(getCollectionPath(c), true)) {
          result.add(c);
        }
      } catch (Exception e) {
        log.warn("Error reading collections nodes", e);
      }
    }
    return result;
  }

  // load and publish a new CollectionInfo
  private synchronized void updateClusterState(boolean immediate,
      final boolean onlyLiveNodes) throws KeeperException,
      InterruptedException {
    // build immutable CloudInfo
    
    if (immediate) {
      ClusterState clusterState;
      synchronized (getUpdateLock()) {
        List<String> liveNodes = zkClient.getChildren(LIVE_NODES_ZKNODE, null,
            true);
        Set<String> liveNodesSet = new HashSet<>();
        liveNodesSet.addAll(liveNodes);
        
        if (!onlyLiveNodes) {
          log.debug("Updating cloud state from ZooKeeper... ");
          
          clusterState = constructState(liveNodesSet,null);
        } else {
          log.debug("Updating live nodes from ZooKeeper... ({})", liveNodesSet.size());
          clusterState = this.clusterState;
          clusterState.setLiveNodes(liveNodesSet);
        }
        this.clusterState = clusterState;
      }
      synchronized (ZkStateReader.this) {
        for (String watchedCollection : watchedCollections) {
          updateWatchedCollection(getCollectionLive(this, watchedCollection));
        }
      }

    } else {
      if (clusterStateUpdateScheduled) {
        log.debug("Cloud state update for ZooKeeper already scheduled");
        return;
      }
      log.debug("Scheduling cloud state update from ZooKeeper...");
      clusterStateUpdateScheduled = true;
      updateCloudExecutor.schedule(new Runnable() {
        
        @Override
        public void run() {
          log.debug("Updating cluster state from ZooKeeper...");
          synchronized (getUpdateLock()) {
            clusterStateUpdateScheduled = false;
            ClusterState clusterState;
            try {
              List<String> liveNodes = zkClient.getChildren(LIVE_NODES_ZKNODE,
                  null, true);
              Set<String> liveNodesSet = new HashSet<>();
              liveNodesSet.addAll(liveNodes);
              
              if (!onlyLiveNodes) {
                log.debug("Updating cloud state from ZooKeeper... ");

                clusterState = constructState(liveNodesSet,null);
              } else {
                log.debug("Updating live nodes from ZooKeeper... ");
                clusterState = ZkStateReader.this.clusterState;
                clusterState.setLiveNodes(liveNodesSet);
              }
              
              ZkStateReader.this.clusterState = clusterState;
              
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
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } 
            // update volatile
            ZkStateReader.this.clusterState = clusterState;

            synchronized (ZkStateReader.this) {
              for (String watchedCollection : watchedCollections) {
                updateWatchedCollection(getCollectionLive(ZkStateReader.this, watchedCollection));
              }
            }
          }
        }
      }, SOLRCLOUD_UPDATE_DELAY, TimeUnit.MILLISECONDS);
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
  
  abstract class RunnableWatcher implements Runnable {
    Watcher watcher;
    public RunnableWatcher(Watcher watcher){
      this.watcher = watcher;
    }

  }
  
  public String getLeaderUrl(String collection, String shard, int timeout)
      throws InterruptedException, KeeperException {
    ZkCoreNodeProps props = new ZkCoreNodeProps(getLeaderRetry(collection,
        shard, timeout));
    return props.getCoreUrl();
  }
  
  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(String collection, String shard) throws InterruptedException {
    return getLeaderRetry(collection, shard, 4000);
  }

  /**
   * Get shard leader properties, with retry if none exist.
   */
  public Replica getLeaderRetry(String collection, String shard, int timeout) throws InterruptedException {
    long timeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);
    while (System.nanoTime() < timeoutAt && !closed) {
      if (clusterState != null) {    
        Replica replica = clusterState.getLeader(collection, shard);
        if (replica != null && getClusterState().liveNodesContain(replica.getNodeName())) {
          return replica;
        }
      }
      Thread.sleep(50);
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

  public List<ZkCoreNodeProps> getReplicaProps(String collection,
      String shardId, String thisCoreNodeName) {
    return getReplicaProps(collection, shardId, thisCoreNodeName, null);
  }
  
  public List<ZkCoreNodeProps> getReplicaProps(String collection,
      String shardId, String thisCoreNodeName, String mustMatchStateFilter) {
    return getReplicaProps(collection, shardId, thisCoreNodeName, mustMatchStateFilter, null);
  }
  
  public List<ZkCoreNodeProps> getReplicaProps(String collection,
      String shardId, String thisCoreNodeName, String mustMatchStateFilter, String mustNotMatchStateFilter) {
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
        if (mustMatchStateFilter == null || mustMatchStateFilter.equals(nodeProps.getState())) {
          if (mustNotMatchStateFilter == null || !mustNotMatchStateFilter.equals(nodeProps.getState())) {
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
        result = (Map) ZkStateReader.fromJSON(getZkClient().getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true)) ;
      } else {
        result= new LinkedHashMap();
      }
      return result;
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR,"Error reading cluster properties",e) ;
    }
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

  public static DocCollection getCollectionLive(ZkStateReader zkStateReader,
      String coll) {
    String collectionPath = getCollectionPath(coll);
    try {
      if (!zkStateReader.getZkClient().exists(collectionPath, true)) return null;
      Stat stat = new Stat();
      byte[] data = zkStateReader.getZkClient().getData(collectionPath, null, stat, true);
      ClusterState state = ClusterState.load(stat.getVersion(), data,
          Collections.<String> emptySet(), collectionPath);
      ClusterState.CollectionRef collectionRef = state.getCollectionStates().get(coll);
      return collectionRef == null ? null : collectionRef.get();
    } catch (KeeperException.NoNodeException e) {
      log.warn("No node available : " + collectionPath, e);
      return null;
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Could not load collection from ZK:" + coll, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Could not load collection from ZK:" + coll, e);
    }
  }

  public static String getCollectionPath(String coll) {
    return COLLECTIONS_ZKNODE+"/"+coll + "/state.json";
  }

  public void addCollectionWatch(String coll) throws KeeperException, InterruptedException {
    synchronized (this) {
      if (watchedCollections.contains(coll)) return;
      else {
        watchedCollections.add(coll);
      }
      addZkWatch(coll);
    }
  }

  private void addZkWatch(final String coll) throws KeeperException,
      InterruptedException {
    log.info("addZkWatch {}", coll);
    final String fullpath = getCollectionPath(coll);
    synchronized (getUpdateLock()) {
      
      cmdExecutor.ensureExists(fullpath, zkClient);
      log.info("Updating collection state at {} from ZooKeeper... ", fullpath);
      
      Watcher watcher = new Watcher() {
        
        @Override
        public void process(WatchedEvent event) {
          // session events are not change events,
          // and do not remove the watcher
          if (EventType.None.equals(event.getType())) {
            return;
          }
          log.info("A cluster state change: {}, has occurred - updating... ",
              (event), ZkStateReader.this.clusterState == null ? 0
                  : ZkStateReader.this.clusterState.getLiveNodes().size());
          try {
            
            // delayed approach
            // ZkStateReader.this.updateClusterState(false, false);
            synchronized (ZkStateReader.this.getUpdateLock()) {
              if (!watchedCollections.contains(coll)) {
                log.info("Unwatched collection {}", coll);
                return;
              }
              // remake watch
              final Watcher thisWatch = this;
              Stat stat = new Stat();
              byte[] data = zkClient.getData(fullpath, thisWatch, stat, true);
              
              if (data == null || data.length == 0) {
                log.warn("No value set for collection state : {}", coll);
                return;
                
              }
              ClusterState clusterState = ClusterState.load(stat.getVersion(),
                  data, Collections.<String> emptySet(), fullpath);
              // update volatile
              
              DocCollection newState = clusterState.getCollectionStates()
                  .get(coll).get();
              updateWatchedCollection(newState);
              
            }
          } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.SESSIONEXPIRED
                || e.code() == KeeperException.Code.CONNECTIONLOSS) {
              log.warn("ZooKeeper watch triggered, but Solr cannot talk to ZK");
              return;
            }
            log.error("Unwatched collection :" + coll, e);
            throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "", e);
            
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Unwatched collection :" + coll, e);
            return;
          }
        }
        
      };
      zkClient.exists(fullpath, watcher, true);
    }
    updateWatchedCollection(getCollectionLive(this, coll));
  }
  
  private void updateWatchedCollection(DocCollection newState) {
    watchedCollectionStates.put(newState.getName(), newState);
    log.info("Updating data for {} to ver {} ", newState.getName(),
        newState.getZNodeVersion());
    
    this.clusterState = clusterState.copyWith(Collections.singletonMap(
        newState.getName(), newState));
  }
  
  /** This is not a public API. Only used by ZkController */
  public void removeZKWatch(final String coll) {
    synchronized (this) {
      watchedCollections.remove(coll);
      try {
        updateClusterState(true);
      } catch (KeeperException e) {
        log.error("Error updating state",e);
      } catch (InterruptedException e) {
        log.error("Error updating state",e);
        Thread.currentThread().interrupt();
      }
    }
  }

}
