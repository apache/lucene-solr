package org.apache.solr.cloud;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 *
 */
public class ZooKeeperReader {
  static final String NEWL = System.getProperty("line.separator");

  private static Logger log = LoggerFactory.getLogger(ZooKeeperReader.class);

  private static final String SHARD_LIST_PROP = "shard_list";

  private static final String URL_PROP = "url";

  private static final String CONFIGS_ZKNODE = "/configs/";

  private static final String COLLECTIONS_ZKNODE = "/collections/";

  private ZooKeeperConnection keeperConnection;

  private boolean closeKeeper;

 
  /**
   * @param keeperConnection
   */
  ZooKeeperReader(ZooKeeperConnection keeperConnection) {
    this.keeperConnection = keeperConnection;
  }

  /**
   * 
   * For testing. For regular use see {@link #ZooKeeperReader(ZooKeeper)}.
   * 
   * @param zooKeeperHost
   * @param zkClientTimeout
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  ZooKeeperReader(String zooKeeperHost, int zkClientTimeout)
      throws IOException, InterruptedException, TimeoutException {
    closeKeeper = true;
 
    keeperConnection = new ZooKeeperConnection(zooKeeperHost, zkClientTimeout);
    keeperConnection.connect();
  }

  /**
   * Check if path exists in ZooKeeper.
   * 
   * @param path ZooKeeper path
   * @return true if path exists in ZooKeeper
   */
  public boolean exists(String path) {
    Object exists = null;
    try {
      exists = keeperConnection.exists(path, null);
    } catch (KeeperException e) {
      log.error("ZooKeeper Exception", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "ZooKeeper Exception", e);
    } catch (InterruptedException e) {
      // nocommit: handle
    }
    return exists != null;
  }

  public void close() throws InterruptedException {
    if (closeKeeper) {
      keeperConnection.close();
    }
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
    byte[] config = getFile(CONFIGS_ZKNODE + zkConfigName, solrConfigFileName);
    InputStream is = new ByteArrayInputStream(config);
    SolrConfig cfg = solrConfigFileName == null ? new SolrConfig(
        resourceLoader, SolrConfig.DEFAULT_CONF_FILE, is) : new SolrConfig(
        resourceLoader, solrConfigFileName, is);

    return cfg;
  }
  
  public byte[] getConfigFileData(String zkConfigName, String fileName) throws KeeperException, InterruptedException {
    return keeperConnection.getData(CONFIGS_ZKNODE + zkConfigName, null, null);
  }

  /**
   * Get data at zkNode path/fileName.
   * 
   * @param path to zkNode
   * @param fileName name of zkNode
   * @return data at path/file
   * @throws InterruptedException
   * @throws KeeperException
   */
  public byte[] getFile(String path, String fileName) throws KeeperException,
      InterruptedException {
    byte[] bytes = null;
    String configPath = path + "/" + fileName;

    if (log.isInfoEnabled()) {
      log.info("Reading " + fileName + " from zookeeper at " + configPath);
    }
    bytes = keeperConnection.getData(configPath, null, null);

    return bytes;
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
    byte[] configBytes = getFile(CONFIGS_ZKNODE + zkConfigName, schemaName);
    InputStream is = new ByteArrayInputStream(configBytes);
    IndexSchema schema = new IndexSchema(config, schemaName, is);
    return schema;
  }

  /**
   * Fills string with printout of current ZooKeeper layout.
   * 
   * @param path
   * @param indent
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void printLayout(String path, int indent, StringBuilder string)
      throws KeeperException, InterruptedException {
    byte[] data = keeperConnection.getData(path, null, null);
    List<String> children = keeperConnection.getChildren(path, null);
    StringBuilder dent = new StringBuilder();
    for (int i = 0; i < indent; i++) {
      dent.append(" ");
    }
    string.append(dent + path + " (" + children.size() + ")" + NEWL);
    if (data != null) {
      try {
        String dataString = new String(data, "UTF-8");
        if (!path.endsWith(".txt") && !path.endsWith(".xml")) {
          string.append(dent + "DATA:\n" + dent + "    "
              + dataString.replaceAll("\n", "\n" + dent + "    ") + NEWL);
        } else {
          string.append(dent + "DATA: ...supressed..." + NEWL);
        }
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
    }

    for (String child : children) {
      if (!child.equals("quota")) {
        printLayout(path + (path.equals("/") ? "" : "/") + child, indent + 1,
            string);
      }
    }

  }

  /**
   * Prints current ZooKeeper layout to stdout.
   * 
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void printLayoutToStdOut() throws KeeperException,
      InterruptedException {
    StringBuilder sb = new StringBuilder();
    printLayout("/", 0, sb);
    System.out.println(sb.toString());
  }

  public String readConfigName(String collection) throws KeeperException,
      InterruptedException {
    // nocommit: load all config at once or organize differently (Properties?)
    String configName = null;

    String path = COLLECTIONS_ZKNODE + collection;
    if (log.isInfoEnabled()) {
      log.info("Load collection config from:" + path);
    }
    List<String> children = keeperConnection.getChildren(path, null);
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
    List<String> nodes = keeperConnection.getChildren(path, null);

    for (String zkNodeName : nodes) {
      byte[] data = keeperConnection.getData(path + "/" + zkNodeName, null, null);

      Properties props = new Properties();
      props.load(new ByteArrayInputStream(data));

      String url = (String) props.get(URL_PROP);
      String shardNameList = (String) props.get(SHARD_LIST_PROP);
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

  /**
   * Get Stat for path.
   * 
   * @param path
   * @return Stat for path or null if it doesn't exist
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Stat stat(String path) throws KeeperException, InterruptedException {
    return keeperConnection.exists(path, null);
  }

  public boolean configFileExists(String configName, String fileName) throws KeeperException, InterruptedException {
    Stat stat = keeperConnection.exists(CONFIGS_ZKNODE + configName, null);
    return stat != null;
  }

}
