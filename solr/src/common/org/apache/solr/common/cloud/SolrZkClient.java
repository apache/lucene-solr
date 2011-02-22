package org.apache.solr.common.cloud;

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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkClientConnectionStrategy.ZkUpdate;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * All Solr ZooKeeper interactions should go through this class rather than
 * ZooKeeper. This class handles synchronous connects and reconnections.
 *
 */
public class SolrZkClient {
  static final String NEWL = System.getProperty("line.separator");

  static final int DEFAULT_CLIENT_CONNECT_TIMEOUT = 30000;

  private static final Logger log = LoggerFactory
      .getLogger(SolrZkClient.class);

  private ConnectionManager connManager;

  private volatile SolrZooKeeper keeper;
  
  /**
   * @param zkServerAddress
   * @param zkClientTimeout
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws IOException
   */
  public SolrZkClient(String zkServerAddress, int zkClientTimeout) throws InterruptedException, TimeoutException, IOException {
    this(zkServerAddress, zkClientTimeout, new DefaultConnectionStrategy(), null);
  }
  
  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout, OnReconnect onReonnect) throws InterruptedException, TimeoutException, IOException {
    this(zkServerAddress, zkClientTimeout, new DefaultConnectionStrategy(), onReonnect, zkClientConnectTimeout);
  }

  /**
   * @param zkServerAddress
   * @param zkClientTimeout
   * @param strat
   * @param onReconnect
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws IOException
   */
  public SolrZkClient(String zkServerAddress, int zkClientTimeout,
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect) throws InterruptedException,
      TimeoutException, IOException {
    this(zkServerAddress, zkClientTimeout, strat, onReconnect, DEFAULT_CLIENT_CONNECT_TIMEOUT);
  }

  /**
   * @param zkServerAddress
   * @param zkClientTimeout
   * @param strat
   * @param onReconnect
   * @param clientConnectTimeout
   * @throws InterruptedException
   * @throws TimeoutException
   * @throws IOException
   */
  public SolrZkClient(String zkServerAddress, int zkClientTimeout,
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect, int clientConnectTimeout) throws InterruptedException,
      TimeoutException, IOException {
    connManager = new ConnectionManager("ZooKeeperConnection Watcher:"
        + zkServerAddress, this, zkServerAddress, zkClientTimeout, strat, onReconnect);
    strat.connect(zkServerAddress, zkClientTimeout, connManager,
        new ZkUpdate() {
          @Override
          public void update(SolrZooKeeper zooKeeper) {
            SolrZooKeeper oldKeeper = keeper;
            keeper = zooKeeper;
            if (oldKeeper != null) {
              try {
                oldKeeper.close();
              } catch (InterruptedException e) {
                // Restore the interrupted status
                Thread.currentThread().interrupt();
                log.error("", e);
                throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                    "", e);
              }
            }
          }
        });
    connManager.waitForConnected(clientConnectTimeout);
  }

  /**
   * @return true if client is connected
   */
  public boolean isConnected() {
    return keeper != null && keeper.getState() == ZooKeeper.States.CONNECTED;
  }
  
  /**
   * @param path
   * @param version
   * @throws InterruptedException
   * @throws KeeperException
   */
  public void delete(final String path, int version)
      throws InterruptedException, KeeperException {
    keeper.delete(path, version);
  }

  /**
   * Return the stat of the node of the given path. Return null if no such a
   * node exists.
   * <p>
   * If the watch is non-null and the call is successful (no exception is thrown),
   * a watch will be left on the node with the given path. The watch will be
   * triggered by a successful operation that creates/delete the node or sets
   * the data on the node.
   *
   * @param path the node path
   * @param watcher explicit watcher
   * @return the stat of the node of the given path; return null if no such a
   *         node exists.
   * @throws KeeperException If the server signals an error
   * @throws InterruptedException If the server transaction is interrupted.
   * @throws IllegalArgumentException if an invalid path is specified
   */
  public Stat exists(final String path, Watcher watcher)
      throws KeeperException, InterruptedException {
    return keeper.exists(path, watcher);
  }
  
  /**
   * @param path
   * @return true if path exists
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean exists(final String path)
      throws KeeperException, InterruptedException {
    return keeper.exists(path, null) != null;
  }

  /**
   * @param path
   * @param data
   * @param acl
   * @param createMode
   * @return path of created node
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String create(final String path, byte data[], List<ACL> acl,
      CreateMode createMode) throws KeeperException, InterruptedException {
    return keeper.create(path, data, acl, createMode);
  }

  /**
   * @param path
   * @param watcher
   * @return children of the node at the path
   * @throws KeeperException
   * @throws InterruptedException
   */
  public List<String> getChildren(final String path, Watcher watcher)
      throws KeeperException, InterruptedException {
    return keeper.getChildren(path, watcher);
  }

  /**
   * @param path
   * @param watcher
   * @param stat
   * @return node's data
   * @throws KeeperException
   * @throws InterruptedException
   */
  public byte[] getData(final String path, Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException {
    return keeper.getData(path, watcher, stat);
  }

  /**
   * @param path
   * @param data
   * @param version
   * @return node's state
   * @throws KeeperException
   * @throws InterruptedException
   */
  public Stat setData(final String path, byte data[], int version)
      throws KeeperException, InterruptedException {
    return keeper.setData(path, data, version);
  }
  
  /**
   * 
   * @param path
   * @param data
   * @param createMode
   * @return path of created node
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String create(String path, byte[] data, CreateMode createMode) throws KeeperException, InterruptedException {

    String zkPath = keeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);

    return zkPath;
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * @param path
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String path) throws KeeperException,
      InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT);
  }
  
  public void makePath(String path, CreateMode createMode) throws KeeperException,
      InterruptedException {
    makePath(path, null, createMode);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * @param path
   * @param data to set on the last zkNode
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String path, byte[] data) throws KeeperException,
      InterruptedException {
    makePath(path, data, CreateMode.PERSISTENT);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * @param path
   * @param data to set on the last zkNode
   * @param createMode
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String path, byte[] data, CreateMode createMode)
      throws KeeperException, InterruptedException {
    makePath(path, data, createMode, null);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * @param path
   * @param data to set on the last zkNode
   * @param createMode
   * @param watcher
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher) throws KeeperException, InterruptedException {
    makePath(path, data, createMode, watcher, false);
  }
  
  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * @param path
   * @param data to set on the last zkNode
   * @param createMode
   * @param watcher
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher, boolean failOnExists) throws KeeperException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info("makePath: " + path);
    }
    
    if (path.startsWith("/")) {
      path = path.substring(1, path.length());
    }
    String[] paths = path.split("/");
    StringBuilder sbPath = new StringBuilder();
    for (int i = 0; i < paths.length; i++) {
      byte[] bytes = null;
      String pathPiece = paths[i];
      sbPath.append("/" + pathPiece);
      String currentPath = sbPath.toString();
      Object exists = exists(currentPath, watcher);
      if (exists == null || ((i == paths.length -1) && failOnExists)) {
        CreateMode mode = CreateMode.PERSISTENT;
        if (i == paths.length - 1) {
          mode = createMode;
          bytes = data;
        }
        keeper.create(currentPath, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        if(i == paths.length -1) {
          // set new watch
          exists(currentPath, watcher);
        }
      } else if (i == paths.length - 1) {
        // TODO: version ? for now, don't worry about race
        setData(currentPath, data, -1);
        // set new watch
        exists(currentPath, watcher);
      }
    }
  }

  /**
   * @param zkPath
   * @param createMode
   * @param watcher
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String zkPath, CreateMode createMode, Watcher watcher)
      throws KeeperException, InterruptedException {
    makePath(zkPath, null, createMode, watcher);
  }

  /**
   * Write data to ZooKeeper.
   * 
   * @param path
   * @param data
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void setData(String path, byte[] data) throws KeeperException,
      InterruptedException {

    makePath(path);

    Object exists = exists(path, null);
    if (exists != null) {
      setData(path, data, -1);
    } else {
      create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  /**
   * Write file to ZooKeeper - default system encoding used.
   * 
   * @param path path to upload file to e.g. /solr/conf/solrconfig.xml
   * @param file path to file to be uploaded
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void setData(String path, File file) throws IOException,
      KeeperException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info("Write to ZooKeepeer " + file.getAbsolutePath() + " to " + path);
    }

    String data = FileUtils.readFileToString(file);
    setData(path, data.getBytes("UTF-8"));
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
    byte[] data = getData(path, null, null);
    List<String> children = getChildren(path, null);
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
        // can't happen - UTF-8
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

  /**
   * @throws InterruptedException
   */
  public void close() throws InterruptedException {
    keeper.close();
  }

  /**
   * Allows package private classes to update volatile ZooKeeper.
   * 
   * @param keeper
   * @throws InterruptedException 
   */
  void updateKeeper(SolrZooKeeper keeper) throws InterruptedException {
   SolrZooKeeper oldKeeper = this.keeper;
   this.keeper = keeper;
   if (oldKeeper != null) {
     oldKeeper.close();
   }
  }
  
  public SolrZooKeeper getSolrZooKeeper() {
    return keeper;
  }

}
