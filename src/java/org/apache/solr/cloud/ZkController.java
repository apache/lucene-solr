package org.apache.solr.cloud;

/**
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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.cloud.SolrZkClient.OnReconnect;
import org.apache.solr.common.SolrException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle ZooKeeper interactions.
 * 
 * notes: loads everything on init, creates what's not there - further updates
 * are prompted with Watches.
 * 
 * TODO: exceptions during shutdown on attempts to update cloud state
 * 
 */
public final class ZkController {

  private static Logger log = LoggerFactory.getLogger(ZkController.class);

  static final String NEWL = System.getProperty("line.separator");

  private static final long CLOUD_UPDATE_DELAY = Long.parseLong(System.getProperty("CLOUD_UPDATE_DELAY", "5000"));

  private final static Pattern URL_POST = Pattern.compile("https?://(.*)");
  private final static Pattern URL_PREFIX = Pattern.compile("(https?://).*");

  // package private for tests
  static final String SHARDS_ZKNODE = "/shards";
  static final String CONFIGS_ZKNODE = "/configs";
  static final String COLLECTIONS_ZKNODE = "/collections";
  static final String NODES_ZKNODE = "/live_nodes";

  public static final String URL_PROP = "url";
  public static final String NODE_NAME = "node_name";

  private SolrZkClient zkClient;

  private volatile CloudState cloudState;

  private String zkServerAddress;

  private String localHostPort;
  private String localHostContext;
  private String localHostName;
  private String localHost;

  private String hostName;
  
  private ScheduledExecutorService updateCloudExecutor = Executors.newScheduledThreadPool(1);

  private boolean cloudStateUpdateScheduled;

  private boolean readonly;  // temporary hack to enable reuse in SolrJ client

  /**
   * @param zkServerAddress ZooKeeper server host address
   * @param zkClientTimeout
   * @param zkClientConnectTimeout
   * @param localHost
   * @param locaHostPort
   * @param localHostContext
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws IOException
   */
  public ZkController(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout, String localHost, String locaHostPort,
      String localHostContext) throws InterruptedException,
      TimeoutException, IOException {
    this.zkServerAddress = zkServerAddress;
    this.localHostPort = locaHostPort;
    this.localHostContext = localHostContext;
    this.localHost = localHost;
    this.readonly = localHostPort==null;
    cloudState = new CloudState(new HashSet<String>(0), new HashMap<String,Map<String,Slice>>(0));
    zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout, zkClientConnectTimeout,
        // on reconnect, reload cloud info
        new OnReconnect() {

          public void command() {
            try {
              // nocommit: recreate watches ????
              createEphemeralLiveNode();
              updateCloudState(false);
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
            } catch (IOException e) {
              log.error("", e);
              throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                  "", e);
            }

          }
        });
    
    init();
  }

  /**
   * @param shardId
   * @param collection
   * @throws IOException
   * @throws InterruptedException 
   * @throws KeeperException 
   */
  private void addZkShardsNode(String shardId, String collection) throws IOException, InterruptedException, KeeperException {

    String shardsZkPath = COLLECTIONS_ZKNODE + "/" + collection + SHARDS_ZKNODE + "/" + shardId;
    
    try {
      
      // shards node
      if (!zkClient.exists(shardsZkPath)) {
        if (log.isInfoEnabled()) {
          log.info("creating zk shards node:" + shardsZkPath);
        }
        // makes shards zkNode if it doesn't exist
        zkClient.makePath(shardsZkPath, CreateMode.PERSISTENT, null);
        
        // nocommit - scrutinize
        // ping that there is a new shardId
        zkClient.setData(COLLECTIONS_ZKNODE, (byte[])null);

      }
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
    }

  }

  /**
   * Closes the underlying ZooKeeper client.
   */
  public void close() {
    try {
      zkClient.close();
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    }
  }

  /**
   * @param collection
   * @param fileName
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean configFileExists(String collection, String fileName)
      throws KeeperException, InterruptedException {
    Stat stat = zkClient.exists(CONFIGS_ZKNODE + "/" + collection + "/" + fileName, null);
    return stat != null;
  }

  /**
   * @return information about the cluster from ZooKeeper
   */
  public CloudState getCloudState() {
    return cloudState;
  }

  /**
   * @param zkConfigName
   * @param fileName
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public byte[] getConfigFileData(String zkConfigName, String fileName)
      throws KeeperException, InterruptedException {
    String zkPath = CONFIGS_ZKNODE + "/" + zkConfigName + "/" + fileName;
    byte[] bytes = zkClient.getData(zkPath, null, null);
    if (bytes == null) {
      log.error("Config file contains no data:" + zkPath);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Config file contains no data:" + zkPath);
    }
    
    return bytes;
  }

  // TODO: consider how this is done
  private String getHostAddress() throws IOException {

    if (localHost == null) {
      localHost = "http://" + InetAddress.getLocalHost().getHostName();
    } else {
      Matcher m = URL_PREFIX.matcher(localHost);
      if (m.matches()) {
        String prefix = m.group(1);
        localHost = prefix + localHost;
      } else {
        localHost = "http://" + localHost;
      }
    }

    return localHost;
  }
  
  public String getHostName() {
    return hostName;
  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }

  /**
   * @return
   */
  public String getZkServerAddress() {
    return zkServerAddress;
  }

  private void init() {

    try {
      localHostName = getHostAddress();
      Matcher m = URL_POST.matcher(localHostName);

      if (m.matches()) {
        hostName = m.group(1);
      } else {
        log.error("Unrecognized host:" + localHostName);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "Unrecognized host:" + localHostName);
      }
      
      // makes nodes zkNode
      try {
        zkClient.makePath(NODES_ZKNODE);
      } catch (KeeperException e) {
        // its okay if another beats us creating the node
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        }
      }
      
      createEphemeralLiveNode();
      setUpCollectionsNode();
      
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

  private void createEphemeralLiveNode() throws KeeperException,
      InterruptedException {
    String nodeName = getNodeName();
    String nodePath = NODES_ZKNODE + "/" + nodeName;
    log.info("Register node as live in ZooKeeper:" + nodePath);
    Watcher liveNodeWatcher = new Watcher() {

      public void process(WatchedEvent event) {
        try {
          log.info("Updating live nodes:" + zkClient);
          try {
            updateLiveNodes();
          } finally {
            // remake watch
            zkClient.getChildren(event.getPath(), this);
          }
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
        } catch (IOException e) {
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        }
        
      }
      
    };
    try {
      if (!readonly) {
        boolean nodeDeleted = true;
        try {
          // we attempt a delete in the case of a quick server bounce -
          // if there was not a graceful shutdown, the node may exist
          // until expiration timeout - so a node won't be created here because
          // it exists, but eventually the node will be removed. So delete
          // in case it exists and create a new node.
          zkClient.delete(nodePath, -1);
        } catch (KeeperException.NoNodeException e) {
          // fine if there is nothing to delete
          nodeDeleted = false;
        }
        if (nodeDeleted) {
          log
              .info("Found a previous node that still exists while trying to register a new live node "
                  + nodePath + " - removing existing node to create another.");
        }
        zkClient.makePath(nodePath, CreateMode.EPHEMERAL);
      }
    } catch (KeeperException e) {
      // its okay if the node already exists
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
      System.out.println("NODE ALREADY EXISTS");
    }
    zkClient.getChildren(NODES_ZKNODE, liveNodeWatcher);
  }
  
  public String getNodeName() {
    return hostName + ":" + localHostPort + "_"+ localHostContext;
  }

  // load and publish a new CollectionInfo
  public void updateCloudState(boolean immediate) throws KeeperException, InterruptedException,
      IOException {
    updateCloudState(immediate, false);
  }
  
  // load and publish a new CollectionInfo
  private void updateLiveNodes() throws KeeperException, InterruptedException,
      IOException {
    updateCloudState(true, true);
  }
  
  // load and publish a new CollectionInfo
  private synchronized void updateCloudState(boolean immediate, final boolean onlyLiveNodes) throws KeeperException, InterruptedException,
      IOException {

    // TODO: - incremental update rather than reread everything
    
    // build immutable CloudInfo
    
    if(immediate) {
      if(!onlyLiveNodes) {
        log.info("Updating cloud state from ZooKeeper... ");
      } else {
        log.info("Updating live nodes from ZooKeeper... ");
      }
      CloudState cloudState;
      cloudState = CloudState.buildCloudState(zkClient, this.cloudState, onlyLiveNodes);
      // update volatile
      this.cloudState = cloudState;
    } else {
      if(cloudStateUpdateScheduled) {
        log.info("Cloud state update for ZooKeeper already scheduled");
        return;
      }
      log.info("Scheduling cloud state update from ZooKeeper...");
      cloudStateUpdateScheduled = true;
      updateCloudExecutor.schedule(new Runnable() {
        
        public void run() {
          log.info("Updating cloud state from ZooKeeper...");
          synchronized (ZkController.this) {
            cloudStateUpdateScheduled = false;
            CloudState cloudState;
            try {
              cloudState = CloudState.buildCloudState(zkClient,
                  ZkController.this.cloudState, onlyLiveNodes);
            } catch (KeeperException e) {
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (IOException e) {
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            }
            // update volatile
            ZkController.this.cloudState = cloudState;
          }
        }
      }, CLOUD_UPDATE_DELAY, TimeUnit.MILLISECONDS);
    }

  }

  /**
   * @param path
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean pathExists(String path) throws KeeperException,
      InterruptedException {
    return zkClient.exists(path);
  }

  /**
   * @param collection
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   * @throws IOException 
   */
  public String readConfigName(String collection) throws KeeperException,
      InterruptedException, IOException {

    String configName = null;

    String path = COLLECTIONS_ZKNODE + "/" + collection;
    if (log.isInfoEnabled()) {
      log.info("Load collection config from:" + path);
    }
    byte[] data = zkClient.getData(path, null, null);
    ZkNodeProps props = new ZkNodeProps();
    
    if(data != null) {
      props.load(data);
      configName = props.get("configName");
    }
    
    if (configName != null && !zkClient.exists(CONFIGS_ZKNODE + "/" + configName)) {
      log.error("Specified config does not exist in ZooKeeper:" + configName);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Specified config does not exist in ZooKeeper:" + configName);
    }

    return configName;
  }

  /**
   * Register shard with ZooKeeper.
   * 
   * @param coreName
   * @param cloudDesc
   * @param forcePropsUpdate update solr.xml core props even if the shard is already registered
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void register(String coreName, CloudDescriptor cloudDesc, boolean forcePropsUpdate) throws IOException,
      KeeperException, InterruptedException {
    String shardUrl = localHostName + ":" + localHostPort + "/" + localHostContext
        + "/" + coreName;
    
    String collection = cloudDesc.getCollectionName();
    
    String shardsZkPath = COLLECTIONS_ZKNODE + "/" + collection + SHARDS_ZKNODE + "/" + cloudDesc.getShardId();

    boolean shardZkNodeAlreadyExists = zkClient.exists(shardsZkPath);
    
    if(shardZkNodeAlreadyExists && !forcePropsUpdate) {
      return;
    }
    
    if (log.isInfoEnabled()) {
      log.info("Register shard - core:" + coreName + " address:"
          + shardUrl);
    }

    ZkNodeProps props = new ZkNodeProps();
    props.put(URL_PROP, shardUrl);
    
    props.put(NODE_NAME, getNodeName());

    byte[] bytes = props.store();
    
    String shardZkNodeName = getNodeName() + "_" + coreName;

    if(shardZkNodeAlreadyExists && forcePropsUpdate) {
      zkClient.setData(shardsZkPath + "/" + shardZkNodeName, bytes);
      // tell everyone to update cloud info
      zkClient.setData(COLLECTIONS_ZKNODE, (byte[])null);
    } else {
      addZkShardsNode(cloudDesc.getShardId(), collection);
      try {
        zkClient.create(shardsZkPath + "/" + shardZkNodeName, bytes,
            CreateMode.PERSISTENT);
        // tell everyone to update cloud info
        zkClient.setData(COLLECTIONS_ZKNODE, (byte[])null);
      } catch (KeeperException e) {
        // its okay if the node already exists
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          throw e;
        }
        // for some reason the shard already exists, though it didn't when we
        // started registration - just return
        return;
      }
    }

  }

  /**
   * @param coreName
   * @param cloudDesc
   */
  public void unregister(String coreName, CloudDescriptor cloudDesc) {
    // TODO : perhaps mark the core down in zk?
  }

  /**
   * @param dir
   * @param zkPath
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void uploadToZK(File dir, String zkPath) throws IOException, KeeperException, InterruptedException {
    File[] files = dir.listFiles();
    for(File file : files) {
      if (!file.getName().startsWith(".")) {
        if (!file.isDirectory()) {
          zkClient.setData(zkPath + "/" + file.getName(), file);
        } else {
          uploadToZK(file, zkPath + "/" + file.getName());
        }
      }
    }
  }
  
  /**
   * @param dir
   * @param configName
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void uploadConfigDir(File dir, String configName) throws IOException, KeeperException, InterruptedException {
    uploadToZK(dir, ZkController.CONFIGS_ZKNODE + "/" + configName);
  }

  // convenience for testing
  void printLayoutToStdOut() throws KeeperException, InterruptedException {
    zkClient.printLayoutToStdOut();
  }

  private void setUpCollectionsNode() throws KeeperException, InterruptedException {
    try {
      if (!zkClient.exists(COLLECTIONS_ZKNODE) && !readonly) {
        if (log.isInfoEnabled()) {
          log.info("creating zk collections node:" + COLLECTIONS_ZKNODE);
        }
        // makes collections zkNode if it doesn't exist
        zkClient.makePath(COLLECTIONS_ZKNODE, CreateMode.PERSISTENT, null);
      }
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      }
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    }
    
    log.info("Start watching collections zk node for changes");
    zkClient.getChildren(COLLECTIONS_ZKNODE, new Watcher(){

      public void process(WatchedEvent event) {
          try {
            log.info("Detected a new or removed collection");
            synchronized (ZkController.this) {
              addShardZkNodeWatches();
              updateCloudState(false);
            }
            // re-watch
            zkClient.getChildren(event.getPath(), this);
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
          } catch (IOException e) {
            log.error("", e);
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          }

      }});
    
    zkClient.exists(COLLECTIONS_ZKNODE, new Watcher(){

      public void process(WatchedEvent event) {
        if(event.getType() !=  EventType.NodeDataChanged) {
          return;
        }
        log.info("Notified of CloudState change");
        try {
          synchronized (ZkController.this) {
            addShardZkNodeWatches();
            updateCloudState(false);
          }
          zkClient.exists(COLLECTIONS_ZKNODE, this);
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
        } catch (IOException e) {
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        }
        
      }});
  }
  
  public void addShardZkNodeWatches() throws KeeperException, InterruptedException {
    CloudState cloudState = getCloudState();
    Set<String> knownCollections = cloudState.getCollections();
    
    List<String> collections = zkClient.getChildren(COLLECTIONS_ZKNODE, null);
    for(final String collection : collections) {
      if(!knownCollections.contains(collection)) {
        Watcher watcher = new Watcher() {
          public void process(WatchedEvent event) {
            log.info("Detected changed ShardId in collection:" + collection);
            try {
              addShardsWatches(collection);
              updateCloudState(false);
            } catch (KeeperException e) {
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (InterruptedException e) {
              // Restore the interrupted status
              Thread.currentThread().interrupt();
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            } catch (IOException e) {
              log.error("", e);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR, "", e);
            }
          }
        };
        boolean madeWatch = true;

        for (int i = 0; i < 5; i++) {
          try {
            zkClient.getChildren(COLLECTIONS_ZKNODE + "/" + collection
                + SHARDS_ZKNODE, watcher);
          } catch (KeeperException.NoNodeException e) {
            // most likely, the collections node has been created, but not the
            // shards node yet -- pause and try again
            madeWatch = false;
            if (i == 4) {
              // nocommit : 
//              throw new ZooKeeperException(
//                  SolrException.ErrorCode.SERVER_ERROR,
//                  "Could not set shards zknode watch, because the zknode does not exist");
            break;
            }
            Thread.sleep(50);
          }
          if(madeWatch) {
            break;
          }
        }
      }
    }
  }
  
  public void addShardsWatches(final String collection) throws KeeperException,
      InterruptedException {
    if (zkClient.exists(COLLECTIONS_ZKNODE + "/" + collection + SHARDS_ZKNODE)) {
      List<String> shardIds = zkClient.getChildren(COLLECTIONS_ZKNODE + "/"
          + collection + SHARDS_ZKNODE, null);
      CloudState cloudState = getCloudState();
      Set<String> knownShardIds;
      Map<String,Slice> slices = cloudState.getSlices(collection);
      if (slices != null) {
        knownShardIds = slices.keySet();
      } else {
        knownShardIds = new HashSet<String>(0);
      }
      for (final String shardId : shardIds) {
        if (!knownShardIds.contains(shardId)) {
          zkClient.getChildren(COLLECTIONS_ZKNODE + "/" + collection
              + SHARDS_ZKNODE + "/" + shardId, new Watcher() {

            public void process(WatchedEvent event) {
              log.info("Detected a shard change under ShardId:" + shardId + " in collection:" + collection);
              try {
                updateCloudState(false);
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
              } catch (IOException e) {
                log.error("", e);
                throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                    "", e);
              }
            }
          });
        }
      }
    }
  }
  
  public void addShardsWatches() throws KeeperException, InterruptedException {
    List<String> collections = zkClient.getChildren(COLLECTIONS_ZKNODE, null);
    for (final String collection : collections) {
      addShardsWatches(collection);
    }
  }

  public void createCollectionZkNode(String collection) throws KeeperException, InterruptedException, IOException {
    log.info("Check for collection zkNode:" + collection);
    String collectionPath = COLLECTIONS_ZKNODE + "/" + collection;
    
    try {
      if(!zkClient.exists(collectionPath)) {
        log.info("Creating collection in ZooKeeper:" + collection);
        try {
          ZkNodeProps props = new ZkNodeProps();
          // if we are bootstrapping a collection, default the config for
          // a new collection to the collection we are bootstrapping
          if(System.getProperty("bootstrap_confdir") != null) {
            String confName = System.getProperty("bootstrap_confname", "configuration1");
            log.info("Setting config for collection:" + collection + " to " + confName);
            props.put("configName",  confName);
          } else {
            // check for configName
            log.info("Looking for collection configName");
            int retry = 1;
            for (; retry < 6; retry++) {
              if (zkClient.exists(COLLECTIONS_ZKNODE + "/" + collection)) {
                ZkNodeProps collectionProps = new ZkNodeProps();
                collectionProps.load(zkClient.getData(COLLECTIONS_ZKNODE + "/"
                    + collection, null, null));
                if (collectionProps.containsKey("configName")) {
                  break;
                }
              }
              log.info("Could not find collection configName - pausing for 2 seconds and trying again - try: " + retry);
              Thread.sleep(2000);
            }
            if (retry == 6) {
              log.error("Could not find conigName for collection " + collection);
              throw new ZooKeeperException(
                  SolrException.ErrorCode.SERVER_ERROR,
                  "Could not find conigName for collection " + collection);
            }
          }
          
          zkClient.makePath(COLLECTIONS_ZKNODE + "/" + collection, props.store(), CreateMode.PERSISTENT, null, true);
         
          // ping that there is a new collection
          zkClient.setData(COLLECTIONS_ZKNODE, (byte[])null);
        } catch (KeeperException e) {
          // its okay if the node already exists
          if (e.code() != KeeperException.Code.NODEEXISTS) {
            throw e;
          }
        }
      } else {
        log.info("Collection zkNode exists");
      }
      
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
    }
    
  }

}
