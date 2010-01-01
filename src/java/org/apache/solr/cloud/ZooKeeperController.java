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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
 * TODO: handle ZooKeeper goes down / failures, Solr still runs
 */
public final class ZooKeeperController {
  static final String NEWL = System.getProperty("line.separator");
  
  private static final String COLLECTIONS_ZKNODE = "/collections/";

  static final String NODE_ZKPREFIX = "/node";

  private static final String SHARDS_ZKNODE = "/shards";

  static final String PROPS_DESC = "NodeDesc";




  private static final String CONFIGS_ZKNODE = "/configs/";


  // nocommit - explore handling shard changes
  // watches the shards zkNode
  static class ShardsWatcher implements Watcher {

    private ZooKeeperController controller;

    public ShardsWatcher(ZooKeeperController controller) {
      this.controller = controller;
    }

    public void process(WatchedEvent event) {
      // nocommit : this will be called too often as shards register themselves
      System.out.println("shards changed");

      try {
        // refresh watcher
        controller.getKeeperConnection().exists(event.getPath(), this);

        // TODO: need to load whole state?
        controller.loadCollectionInfo();

      } catch (KeeperException e) {
        log.error("", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "ZooKeeper Exception", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
      } catch (IOException e) {
        log.error("", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "IOException", e);
      }

    }

  }

  final ShardsWatcher SHARD_WATCHER = new ShardsWatcher(this);

  private final static Pattern URL_POST = Pattern.compile("https?://(.*)");

  private final static Pattern URL_PREFIX = Pattern.compile("(https?://).*");

  private static Logger log = LoggerFactory
      .getLogger(ZooKeeperController.class);

  private SolrZkClient zkClient;

  SolrZkClient getKeeperConnection() {
    return zkClient;
  }

  private String collectionName;

  private volatile CollectionInfo collectionInfo;

  private String shardsZkPath;

  private String zkServerAddress;

  private String hostPort;

  private String hostContext;

  private String configName;

  private String zooKeeperHostName;

  /**
   * 
   * @param zkServerAddress ZooKeeper server host address
   * @param collection
   * @param hostUrl
   * @param hostPort
   * @param hostContext
   * @param zkClientTimeout
   */
  public ZooKeeperController(String zkServerAddress, String collection,
      String hostUrl, String hostPort, String hostContext, int zkClientTimeout) {

    this.collectionName = collection;
    this.zkServerAddress = zkServerAddress;
    this.hostPort = hostPort;
    this.hostContext = hostContext;
    zkClient = new SolrZkClient(zkServerAddress, zkClientTimeout);

    shardsZkPath = COLLECTIONS_ZKNODE + collectionName + SHARDS_ZKNODE;

    init();
  }

  private void init() {

    try {
      zkClient.connect();



      zooKeeperHostName = getHostAddress();
      Matcher m = URL_POST.matcher(zooKeeperHostName);
      if (m.matches()) {
        String hostName = m.group(1);

        // register host
        zkClient.makePath(hostName);
      } else {
        // nocommit
        throw new IllegalStateException("Bad host:" + zooKeeperHostName);
      }

      // build layout if not exists
      buildZkLayoutZkNodes();
      
      configName = readConfigName(collectionName);

      // load the state of the cloud
      loadCollectionInfo();

    } catch (IOException e) {
      log.error("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Can't create ZooKeeperController", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      log.error("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Timeout waiting for ZooKeeper connection", e);
    } catch (KeeperException e) {
      log.error("KeeperException", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "", e);
    }

  }

  public boolean configFileExists(String fileName) throws KeeperException,
      InterruptedException {
    return configFileExists(configName, fileName);
  }

  /**
   * nocommit: adds nodes if they don't exist, eg /shards/ node. consider race
   * conditions
   */
  private void buildZkLayoutZkNodes() throws IOException {
    try {
      // shards node
      if (!exists(shardsZkPath)) {
        if (log.isInfoEnabled()) {
          log.info("creating zk shards node:" + shardsZkPath);
        }
        // makes shards zkNode if it doesn't exist
        zkClient.makePath(shardsZkPath, CreateMode.PERSISTENT, SHARD_WATCHER);
      }
    } catch (KeeperException e) {
      // its okay if another beats us creating the node
      if (e.code() != KeeperException.Code.NODEEXISTS) {
        log.error("ZooKeeper Exception", e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "ZooKeeper Exception", e);
      }
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
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
    }
  }

  /**
   * @return information about the current collection from ZooKeeper
   */
  public CollectionInfo getCollectionInfo() {
    return collectionInfo;
  }

  /**
   * @return
   */
  public String getZkServerAddress() {
    return zkServerAddress;
  }

  // load and publish a new CollectionInfo
  private void loadCollectionInfo() throws KeeperException, InterruptedException, IOException {
    // build immutable CollectionInfo
    
      CollectionInfo collectionInfo = new CollectionInfo(zkClient, shardsZkPath);
      // update volatile
      this.collectionInfo = collectionInfo;
  }

  /**
   * @return name of configuration zkNode to use
   */
  public String getConfigName() {
    return configName;
  }

  // nocommit - testing
  public String getSearchNodes() {
    StringBuilder nodeString = new StringBuilder();
    boolean first = true;
    List<String> nodes;

    nodes = collectionInfo.getSearchShards();
    // nocommit
    System.out.println("there are " + nodes.size() + " node(s)");
    for (String node : nodes) {
      nodeString.append(node);
      if (first) {
        first = false;
      } else {
        nodeString.append(',');
      }
    }
    return nodeString.toString();
  }

  /**
   * Register shard. A SolrCore calls this on startup to register with
   * ZooKeeper.
   * 
   * @param core
   */
  public void registerShard(SolrCore core) {
    String coreName = core.getCoreDescriptor().getName();
    String shardUrl = zooKeeperHostName + ":" + hostPort + "/" + hostContext
        + "/" + coreName;

    // nocommit:
    if (log.isInfoEnabled()) {
      log.info("Register shard - core:" + core.getName() + " address:"
          + shardUrl);
    }

    try {
      // create node
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      // nocommit: could do xml
      Properties props = new Properties();
      props.put(CollectionInfo.URL_PROP, shardUrl);

      String shardList = core.getCoreDescriptor().getShardList();

      props.put(CollectionInfo.SHARD_LIST_PROP, shardList == null ? "" : shardList);
      props.store(baos, PROPS_DESC);

      zkClient.create(shardsZkPath + NODE_ZKPREFIX, baos
          .toByteArray(), CreateMode.EPHEMERAL_SEQUENTIAL, SHARD_WATCHER);

    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.error("ZooKeeper Exception", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "ZooKeeper Exception", e);
    }

  }

  // nocommit: fooling around
  private String getHostAddress() throws IOException {
    String host = null;

    if (host == null) {
      host = "http://" + InetAddress.getLocalHost().getHostName();
    } else {
      Matcher m = URL_PREFIX.matcher(host);
      if (m.matches()) {
        String prefix = m.group(1);
        host = prefix + host;
      }
    }
    if (log.isInfoEnabled()) {
      log.info("Register host with ZooKeeper:" + host);
    }

    return host;
  }

  /**
   * Check if path exists in ZooKeeper.
   * 
   * @param path ZooKeeper path
   * @return true if path exists in ZooKeeper
   * @throws InterruptedException
   * @throws KeeperException
   */
  public boolean exists(String path) throws KeeperException,
      InterruptedException {
    Object exists = zkClient.exists(path, null);

    return exists != null;
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
    byte[] config = zkClient.getData(CONFIGS_ZKNODE + zkConfigName
        + "/" + solrConfigFileName, null, null);
    InputStream is = new ByteArrayInputStream(config);
    SolrConfig cfg = solrConfigFileName == null ? new SolrConfig(
        resourceLoader, SolrConfig.DEFAULT_CONF_FILE, is) : new SolrConfig(
        resourceLoader, solrConfigFileName, is);

    return cfg;
  }

  public byte[] getConfigFileData(String zkConfigName, String fileName)
      throws KeeperException, InterruptedException {
    return zkClient.getData(CONFIGS_ZKNODE + zkConfigName, null, null);
  }

  // /**
  // * Get data at zkNode path/fileName.
  // *
  // * @param path to zkNode
  // * @param fileName name of zkNode
  // * @return data at path/file
  // * @throws InterruptedException
  // * @throws KeeperException
  // */
  // public byte[] getFile(String path, String fileName) throws KeeperException,
  // InterruptedException {
  // byte[] bytes = null;
  // String configPath = path + "/" + fileName;
  //
  // if (log.isInfoEnabled()) {
  // log.info("Reading " + fileName + " from zookeeper at " + configPath);
  // }
  // bytes = keeperConnection.getData(configPath, null, null);
  //
  // return bytes;
  // }

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
    byte[] configBytes = zkClient.getData(CONFIGS_ZKNODE + zkConfigName
        + "/" + schemaName, null, null);
    InputStream is = new ByteArrayInputStream(configBytes);
    IndexSchema schema = new IndexSchema(config, schemaName, is);
    return schema;
  }

  public String readConfigName(String collection) throws KeeperException,
      InterruptedException {
    // nocommit: load all config at once or organize differently (Properties?)
    String configName = null;

    String path = COLLECTIONS_ZKNODE + collection;
    if (log.isInfoEnabled()) {
      log.info("Load collection config from:" + path);
    }
    List<String> children;
    try {
      children = zkClient.getChildren(path, null);
    } catch(KeeperException.NoNodeException e) {
      log.error("Could not find config name to use for collection:" + collection, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Timeout waiting for ZooKeeper connection", e);
    }
    for (String node : children) {
      // nocommit
      System.out.println("check child:" + node);
      // nocommit: do we actually want to handle settings in the node name?
      if (node.startsWith("config=")) {
        configName = node.substring(node.indexOf("=") + 1);
        if (log.isInfoEnabled()) {
          log.info("Using collection config:" + configName);
        }
      }
    }

    if (configName == null) {
      throw new IllegalStateException("no config specified for collection:"
          + collection);
    }

    return configName;
  }

  /**
   * Read info on the available Shards and Nodes.
   * 
   * @param path to the shards zkNode
   * @return Map from shard name to a {@link ShardInfoList}
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   */
  public Map<String,ShardInfoList> readShardInfo(String path)
      throws KeeperException, InterruptedException, IOException {
    // for now, just reparse everything
    HashMap<String,ShardInfoList> shardNameToShardList = new HashMap<String,ShardInfoList>();

    if (!exists(path)) {
      throw new IllegalStateException("Cannot find zk node that should exist:"
          + path);
    }
    List<String> nodes = zkClient.getChildren(path, null);

    for (String zkNodeName : nodes) {
      byte[] data = zkClient.getData(path + "/" + zkNodeName, null,
          null);

      Properties props = new Properties();
      props.load(new ByteArrayInputStream(data));

      String url = (String) props.get(CollectionInfo.URL_PROP);
      String shardNameList = (String) props.get(CollectionInfo.SHARD_LIST_PROP);
      String[] shardsNames = shardNameList.split(",");
      for (String shardName : shardsNames) {
        ShardInfoList sList = shardNameToShardList.get(shardName);
        List<ShardInfo> shardList;
        if (sList == null) {
          shardList = new ArrayList<ShardInfo>(1);
        } else {
          List<ShardInfo> oldShards = sList.getShards();
          shardList = new ArrayList<ShardInfo>(oldShards.size() + 1);
          shardList.addAll(oldShards);
        }

        ShardInfo shard = new ShardInfo(url);
        shardList.add(shard);
        ShardInfoList list = new ShardInfoList(shardList);

        shardNameToShardList.put(shardName, list);
      }

    }

    return Collections.unmodifiableMap(shardNameToShardList);
  }

  public boolean configFileExists(String configName, String fileName)
      throws KeeperException, InterruptedException {
    Stat stat = zkClient.exists(CONFIGS_ZKNODE + configName, null);
    return stat != null;
  }

}
