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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Collection;
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

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.cloud.SolrZkClient.OnReconnect;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

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
  static final String NODES_ZKNODE = "/nodes";

  public static final String URL_PROP = "url";
  public static final String ROLE_PROP = "role";
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


  /**
   * @param zkServerAddress ZooKeeper server host address
   * @param zkClientTimeout
   * @param localHost
   * @param locaHostPort
   * @param localHostContext
   * @param coreContainer
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws IOException
   */
  public ZkController(String zkServerAddress, int zkClientTimeout, String localHost, String locaHostPort,
      String localHostContext, final CoreContainer coreContainer) throws InterruptedException,
      TimeoutException, IOException {
    this.zkServerAddress = zkServerAddress;
    this.localHostPort = locaHostPort;
    this.localHostContext = localHostContext;
    this.localHost = localHost;
    cloudState = new CloudState(new HashSet<String>(0), new HashMap<String,Map<String,Slice>>(0));
    zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout,
        // on reconnect, reload cloud info
        new OnReconnect() {

          public void command() {
            try {

              createEphemeralNode();
              // register cores in case any new cores came online will zk was down
              
              // coreContainer may currently be null in tests, so don't re-register
              if(coreContainer != null) {
                Collection<SolrCore> cores = coreContainer.getCores();
                for(SolrCore core : cores) {
                  register(core, false);
                }
              }
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
    
    boolean newShardId = false;
    
    try {
      
      // shards node
      if (!zkClient.exists(shardsZkPath)) {
        if (log.isInfoEnabled()) {
          log.info("creating zk shards node:" + shardsZkPath);
        }
        // makes shards zkNode if it doesn't exist
        zkClient.makePath(shardsZkPath, CreateMode.PERSISTENT, null);
        
        newShardId = true;

      }
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
    }
    
    if(newShardId) {
      // nocommit - scrutinize
      // ping that there is a new shardId
      zkClient.setData(COLLECTIONS_ZKNODE, (byte[])null);
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
   * Load SolrConfig from ZooKeeper.
   * 
   * TODO: consider *many* cores firing up at once and loading the same files
   * from ZooKeeper
   * 
   * @param resourceLoader
   * @param solrConfigFileName
   * @return
   * @throws IOException
   * @throws ParserConfigurationException
   * @throws SAXException
   * @throws InterruptedException
   * @throws KeeperException
   */
  public SolrConfig getConfig(String zkConfigName, String solrConfigFileName,
      SolrResourceLoader resourceLoader) throws IOException,
      ParserConfigurationException, SAXException, KeeperException,
      InterruptedException {
    byte[] config = zkClient.getData(CONFIGS_ZKNODE + "/" + zkConfigName + "/"
        + solrConfigFileName, null, null);
    InputStream is = new ByteArrayInputStream(config);
    SolrConfig cfg = solrConfigFileName == null ? new SolrConfig(
        resourceLoader, SolrConfig.DEFAULT_CONF_FILE, is) : new SolrConfig(
        resourceLoader, solrConfigFileName, is);

    return cfg;
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
    return zkClient.getData(CONFIGS_ZKNODE + "/" + zkConfigName, null, null);
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

  /**
   * Load IndexSchema from ZooKeeper.
   * 
   * TODO: consider *many* cores firing up at once and loading the same files
   * from ZooKeeper
   * 
   * @param resourceLoader
   * @param schemaName
   * @param config
   * @return
   * @throws InterruptedException
   * @throws KeeperException
   */
  public IndexSchema getSchema(String zkConfigName, String schemaName,
      SolrConfig config, SolrResourceLoader resourceLoader)
      throws KeeperException, InterruptedException {
    byte[] configBytes = zkClient.getData(CONFIGS_ZKNODE + "/" + zkConfigName
        + "/" + schemaName, null, null);
    InputStream is = new ByteArrayInputStream(configBytes);
    IndexSchema schema = new IndexSchema(config, schemaName, is);
    return schema;
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
      
      // makes nodes node
      try {
        // TODO: for now, no watch - if a node goes down or comes up, its going to change
        // shards info anyway and cause a state update - this could change if we do incremental
        // state update
        zkClient.makePath(NODES_ZKNODE);
      } catch (KeeperException e) {
        // its okay if another beats us creating the node
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          log.error("", e);
          throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
              "", e);
        }
      }
      createEphemeralNode();
      
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

  private void createEphemeralNode() throws KeeperException,
      InterruptedException {
    String nodeName = getNodeName();
    String nodePath = NODES_ZKNODE + "/" + nodeName;
    log.info("Register node as live in ZooKeeper:" + nodePath);
    try {
      zkClient.makePath(nodePath, CreateMode.EPHEMERAL);
    } catch (KeeperException e) {
      // its okay if the node already exists
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
    }
  }
  
  public String getNodeName() {
    return hostName + ":" + localHostPort + "_"+ localHostContext;
  }

  // load and publish a new CollectionInfo
  public synchronized void updateCloudState(boolean immediate) throws KeeperException, InterruptedException,
      IOException {

    // TODO: - incremental update rather than reread everything
    
    // build immutable CloudInfo
    
    if(immediate) {
      log.info("Updating cloud state from ZooKeeper... :" + zkClient.keeper);
      CloudState cloudState;
      cloudState = CloudState.buildCloudState(zkClient);
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
              cloudState = CloudState.buildCloudState(zkClient);
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
   */
  public String readConfigName(String collection) throws KeeperException,
      InterruptedException {
    // nocommit: load all config at once or organize differently (Properties?)
    String configName = null;

    String path = COLLECTIONS_ZKNODE + "/" + collection;
    if (log.isInfoEnabled()) {
      log.info("Load collection config from:" + path);
    }
    List<String> children;
    try {
      children = zkClient.getChildren(path, null);
    } catch (KeeperException.NoNodeException e) {
      log.error(
          "Config name to use for collection:"
              + collection + " could not be located", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Config name to use for collection:"
              + collection + " could not be located", e);
    }
    for (String node : children) {
      // nocommit: do we actually want to handle settings in the node name?
      if (node.startsWith("config=")) {
        configName = node.substring(node.indexOf("=") + 1);
        if (log.isInfoEnabled()) {
          log.info("Using collection config:" + configName);
        }
        // nocommmit : bail or read more?
      }
    }
    
    if (configName != null && !zkClient.exists(CONFIGS_ZKNODE + "/" + configName)) {
      log.error("Specified config does not exist in ZooKeeper:" + configName);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "Specified config does not exist in ZooKeeper:" + configName);
    }

    return configName;
  }

  /**
   * Register shard. A SolrCore calls this on startup to register with
   * ZooKeeper.
   * 
   * @param core SolrCore to register as a shard
   * @param forcePropsUpdate update solr.xml core props even if the shard is already registered
   * 
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void register(SolrCore core, boolean forcePropsUpdate) throws IOException,
      KeeperException, InterruptedException {
    String coreName = core.getCoreDescriptor().getName();
    String shardUrl = localHostName + ":" + localHostPort + "/" + localHostContext
        + "/" + coreName;
    
    CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
    String collection = cloudDesc.getCollectionName();
    
    String shardsZkPath = COLLECTIONS_ZKNODE + "/" + collection + SHARDS_ZKNODE + "/" + cloudDesc.getShardId();

    boolean shardZkNodeAlreadyExists = zkClient.exists(shardsZkPath);
    
    if(shardZkNodeAlreadyExists && !forcePropsUpdate) {
      return;
    }
    
    if (log.isInfoEnabled()) {
      log.info("Register shard - core:" + core.getName() + " address:"
          + shardUrl);
    }

    ZkNodeProps props = new ZkNodeProps();
    props.put(URL_PROP, shardUrl);

    props.put(ROLE_PROP, cloudDesc.getRole());
    
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
   * @param core
   */
  public void unregister(SolrCore core) {
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
      if (!zkClient.exists(COLLECTIONS_ZKNODE)) {
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
            if(i == 4) {
              // nocommit:
              // no shards yet, just bail
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

  public void createCollectionZkNode(String collection) throws KeeperException, InterruptedException {
    String collectionPath = COLLECTIONS_ZKNODE + "/" + collection;
    
    boolean newCollection = false;
    
    try {
      if(!zkClient.exists(collectionPath)) {
        log.info("Creating collection in ZooKeeper:" + collection);
        try {
          zkClient.makePath(collectionPath, CreateMode.PERSISTENT, null);
          String confName = readConfigName(collection);
          if(confName == null && System.getProperty("bootstrap_confdir") != null) {
            log.info("Setting config for collection:" + collection + " to " + confName);
            confName = System.getProperty("bootstrap_confname", "configuration1");
            zkClient.makePath(COLLECTIONS_ZKNODE + "/" + collection + "/config=" + confName);
          }
          newCollection = true;
        } catch (KeeperException e) {
          // its okay if another beats us creating the node
          if (e.code() != KeeperException.Code.NODEEXISTS) {
            throw e;
          }
        }
      }
      
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        throw e;
      }
    }
    
    if(newCollection) {
      // nocommit - scrutinize
      // ping that there is a new collection
      zkClient.setData(COLLECTIONS_ZKNODE, (byte[])null);
    }
    
  }

}
