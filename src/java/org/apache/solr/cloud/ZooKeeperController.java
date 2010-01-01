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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handle ZooKeeper interactions.
 * 
 * notes: loads everything on init, creates what's not there - further updates
 * are prompted with Watches.
 * 
 * TODO: handle ZooKeeper goes down / failures, Solr still runs
 */
public final class ZooKeeperController {
  private static final String COLLECTIONS_ZKNODE = "/collections/";

  static final String NODE_ZKPREFIX = "/node";

  private static final String SHARDS_ZKNODE = "/shards";

  static final String PROPS_DESC = "NodeDesc";

  static final String SHARD_LIST_PROP = "shard_list";

  static final String URL_PROP = "url";

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
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "ZooKeeper Exception", e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
      }

    }

  }

  final ShardsWatcher SHARD_WATCHER = new ShardsWatcher(this);

  private final static Pattern URL_POST = Pattern.compile("https?://(.*)");

  private final static Pattern URL_PREFIX = Pattern.compile("(https?://).*");

  private static Logger log = LoggerFactory
      .getLogger(ZooKeeperController.class);


  private ZooKeeperConnection keeperConnection;

  ZooKeeperConnection getKeeperConnection() {
    return keeperConnection;
  }

  private ZooKeeperReader zkReader;

  private String collectionName;

  private volatile CollectionInfo collectionInfo;

  private String shardsZkPath;

  private ZooKeeperWriter zkWriter;

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
    keeperConnection = new ZooKeeperConnection(zkServerAddress, zkClientTimeout);
 
    shardsZkPath = COLLECTIONS_ZKNODE + collectionName + SHARDS_ZKNODE;

    init();
  }
  
  private void init() {

    try {
      keeperConnection.connect();
      
      // nocommit : consider losing these and having everything on ZooKeeperConnection
      zkReader = new ZooKeeperReader(keeperConnection);
      zkWriter = new ZooKeeperWriter(keeperConnection);

      configName = zkReader.readConfigName(collectionName);

      zooKeeperHostName = getHostAddress();
      Matcher m = URL_POST.matcher(zooKeeperHostName);
      if (m.matches()) {
        String hostName = m.group(1);

        // register host
        zkWriter.makePath(hostName);
      } else {
        // nocommit
        throw new IllegalStateException("Bad host:" + zooKeeperHostName);
      }

      // build layout if not exists
      buildZkLayoutZkNodes();

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
  
  public boolean configFileExists(String fileName) throws KeeperException, InterruptedException {
    return zkReader.configFileExists(configName, fileName);
  }

  /**
   * nocommit: adds nodes if they don't exist, eg /shards/ node. consider race
   * conditions
   */
  private void buildZkLayoutZkNodes() throws IOException {
    try {
      // shards node
      if (!zkReader.exists(shardsZkPath)) {
        if (log.isInfoEnabled()) {
          log.info("creating zk shards node:" + shardsZkPath);
        }
        // makes shards zkNode if it doesn't exist
        zkWriter.makePath(shardsZkPath, CreateMode.PERSISTENT, SHARD_WATCHER);
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
      keeperConnection.close();
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
   * @return an object that encapsulates most of the ZooKeeper read util operations.
   */
  public ZooKeeperReader getZkReader() {
    return zkReader;
  }

  /**
   * @return an object that encapsulates most of the ZooKeeper write util operations.
   */
  public ZooKeeperWriter getZkWriter() {
    return zkWriter;
  }
  

  /**
   * @return
   */
  public String getZooKeeperHost() {
    return zkServerAddress;
  }

  // load and publish a new CollectionInfo
  private void loadCollectionInfo() {
    // build immutable CollectionInfo
    boolean updateCollectionInfo = false;
    Map<String,ShardInfoList> shardNameToShardList = null;
    try {
      shardNameToShardList = zkReader.readShardInfo(shardsZkPath);
      updateCollectionInfo = true;
    } catch (KeeperException e) {
      // nocommit: its okay if we cannot access ZK - just log
      // and continue
      log.error("", e);
    } catch (IOException e) {
      log.error("", e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
    }

    if(updateCollectionInfo) {
      CollectionInfo collectionInfo = new CollectionInfo(shardNameToShardList);
      // update volatile
      this.collectionInfo = collectionInfo;
    }
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
    String shardUrl = zooKeeperHostName + ":" + hostPort + "/" + hostContext + "/"
        + coreName;

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
      props.put(URL_PROP, shardUrl);
      
      String shardList = core.getCoreDescriptor().getShardList();
      
      props.put(SHARD_LIST_PROP, shardList == null ? "" : shardList);
      props.store(baos, PROPS_DESC);

      zkWriter.makeEphemeralSeqPath(shardsZkPath + NODE_ZKPREFIX, baos
          .toByteArray(), SHARD_WATCHER);

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
}
