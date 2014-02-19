package org.apache.solr.common.cloud;

/*
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
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkClientConnectionStrategy.ZkUpdate;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
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
  // These should *only* be used for debugging or monitoring purposes
  public static final AtomicLong numOpens = new AtomicLong();
  public static final AtomicLong numCloses = new AtomicLong();
  
  static final String NEWL = System.getProperty("line.separator");

  static final int DEFAULT_CLIENT_CONNECT_TIMEOUT = 30000;

  private static final Logger log = LoggerFactory
      .getLogger(SolrZkClient.class);

  private ConnectionManager connManager;

  private volatile SolrZooKeeper keeper;
  
  private ZkCmdExecutor zkCmdExecutor;

  private volatile boolean isClosed = false;
  private ZkClientConnectionStrategy zkClientConnectionStrategy;
  private int zkClientTimeout;
  
  public int getZkClientTimeout() {
    return zkClientTimeout;
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout) {
    this(zkServerAddress, zkClientTimeout, new DefaultConnectionStrategy(), null);
  }
  
  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout, OnReconnect onReonnect) {
    this(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, new DefaultConnectionStrategy(), onReonnect);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout,
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect) {
    this(zkServerAddress, zkClientTimeout, DEFAULT_CLIENT_CONNECT_TIMEOUT, strat, onReconnect);
  }
  
  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout,
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect) {
    this(zkServerAddress, zkClientTimeout, clientConnectTimeout, strat, onReconnect, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout, 
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect, BeforeReconnect beforeReconnect) {
    this.zkClientConnectionStrategy = strat;
    this.zkClientTimeout = zkClientTimeout;
    // we must retry at least as long as the session timeout
    zkCmdExecutor = new ZkCmdExecutor(zkClientTimeout);
    connManager = new ConnectionManager("ZooKeeperConnection Watcher:"
        + zkServerAddress, this, zkServerAddress, strat, onReconnect, beforeReconnect);
    try {
      strat.connect(zkServerAddress, zkClientTimeout, connManager,
          new ZkUpdate() {
            @Override
            public void update(SolrZooKeeper zooKeeper) {
              SolrZooKeeper oldKeeper = keeper;
              keeper = zooKeeper;
              try {
                closeKeeper(oldKeeper);
              } finally {
                if (isClosed) {
                  // we may have been closed
                  closeKeeper(SolrZkClient.this.keeper);
                }
              }
            }
          });
    } catch (Exception e) {
      connManager.close();
      if (keeper != null) {
        try {
          keeper.close();
        } catch (InterruptedException e1) {
          Thread.currentThread().interrupt();
        }
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    
    try {
      connManager.waitForConnected(clientConnectTimeout);
    } catch (Exception e) {
      connManager.close();
      try {
        keeper.close();
      } catch (InterruptedException e1) {
        Thread.currentThread().interrupt();
      }
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    numOpens.incrementAndGet();
  }

  public ConnectionManager getConnectionManager() {
    return connManager;
  }
  
  public ZkClientConnectionStrategy getZkClientConnectionStrategy() {
    return zkClientConnectionStrategy;
  }

  /**
   * Returns true if client is connected
   */
  public boolean isConnected() {
    return keeper != null && keeper.getState() == ZooKeeper.States.CONNECTED;
  }
  
  public void delete(final String path, final int version, boolean retryOnConnLoss)
      throws InterruptedException, KeeperException {
    if (retryOnConnLoss) {
      zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public Stat execute() throws KeeperException, InterruptedException {
          keeper.delete(path, version);
          return null;
        }
      });
    } else {
      keeper.delete(path, version);
    }
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
  public Stat exists(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (retryOnConnLoss) {
      return zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public Stat execute() throws KeeperException, InterruptedException {
          return keeper.exists(path, watcher);
        }
      });
    } else {
      return keeper.exists(path, watcher);
    }
  }
  
  /**
   * Returns true if path exists
   */
  public Boolean exists(final String path, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (retryOnConnLoss) {
      return zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public Boolean execute() throws KeeperException, InterruptedException {
          return keeper.exists(path, null) != null;
        }
      });
    } else {
      return keeper.exists(path, null) != null;
    }
  }

  /**
   * Returns path of created node
   */
  public String create(final String path, final byte data[], final List<ACL> acl,
      final CreateMode createMode, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    if (retryOnConnLoss) {
      return zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public String execute() throws KeeperException, InterruptedException {
          return keeper.create(path, data, acl, createMode);
        }
      });
    } else {
      return keeper.create(path, data, acl, createMode);
    }
  }

  /**
   * Returns children of the node at the path
   */
  public List<String> getChildren(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (retryOnConnLoss) {
      return zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public List<String> execute() throws KeeperException, InterruptedException {
          return keeper.getChildren(path, watcher);
        }
      });
    } else {
      return keeper.getChildren(path, watcher);
    }
  }

  /**
   * Returns node's data
   */
  public byte[] getData(final String path, final Watcher watcher, final Stat stat, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (retryOnConnLoss) {
      return zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public byte[] execute() throws KeeperException, InterruptedException {
          return keeper.getData(path, watcher, stat);
        }
      });
    } else {
      return keeper.getData(path, watcher, stat);
    }
  }

  /**
   * Returns node's state
   */
  public Stat setData(final String path, final byte data[], final int version, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (retryOnConnLoss) {
      return zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public Stat execute() throws KeeperException, InterruptedException {
          return keeper.setData(path, data, version);
        }
      });
    } else {
      return keeper.setData(path, data, version);
    }
  }
  
  /**
   * Returns path of created node
   */
  public String create(final String path, final byte[] data,
      final CreateMode createMode, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    if (retryOnConnLoss) {
      return zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public String execute() throws KeeperException, InterruptedException {
          return keeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
              createMode);
        }
      });
    } else {
      return keeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
    }
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   */
  public void makePath(String path, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT, retryOnConnLoss);
  }
  
  public void makePath(String path, boolean failOnExists, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT, null, failOnExists, retryOnConnLoss);
  }
  
  public void makePath(String path, File file, boolean failOnExists, boolean retryOnConnLoss)
      throws IOException, KeeperException, InterruptedException {
    makePath(path, FileUtils.readFileToByteArray(file),
        CreateMode.PERSISTENT, null, failOnExists, retryOnConnLoss);
  }
  
  public void makePath(String path, File file, boolean retryOnConnLoss) throws IOException,
      KeeperException, InterruptedException {
    makePath(path, FileUtils.readFileToByteArray(file), retryOnConnLoss);
  }
  
  public void makePath(String path, CreateMode createMode, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, null, createMode, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, data, CreateMode.PERSISTENT, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, CreateMode createMode, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(path, data, createMode, null, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    makePath(path, data, createMode, watcher, true, retryOnConnLoss);
  }
  


  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * Note: retryOnConnLoss is only respected for the final node - nodes
   * before that are always retried on connection loss.
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher, boolean failOnExists, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info("makePath: " + path);
    }
    boolean retry = true;
    
    if (path.startsWith("/")) {
      path = path.substring(1, path.length());
    }
    String[] paths = path.split("/");
    StringBuilder sbPath = new StringBuilder();
    for (int i = 0; i < paths.length; i++) {
      byte[] bytes = null;
      String pathPiece = paths[i];
      sbPath.append("/" + pathPiece);
      final String currentPath = sbPath.toString();
      Object exists = exists(currentPath, watcher, retryOnConnLoss);
      if (exists == null || ((i == paths.length -1) && failOnExists)) {
        CreateMode mode = CreateMode.PERSISTENT;
        if (i == paths.length - 1) {
          mode = createMode;
          bytes = data;
          if (!retryOnConnLoss) retry = false;
        }
        try {
          if (retry) {
            final CreateMode finalMode = mode;
            final byte[] finalBytes = bytes;
            zkCmdExecutor.retryOperation(new ZkOperation() {
              @Override
              public Object execute() throws KeeperException, InterruptedException {
                keeper.create(currentPath, finalBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, finalMode);
                return null;
              }
            });
          } else {
            keeper.create(currentPath, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
          }
        } catch (NodeExistsException e) {
          
          if (!failOnExists) {
            // TODO: version ? for now, don't worry about race
            setData(currentPath, data, -1, retryOnConnLoss);
            // set new watch
            exists(currentPath, watcher, retryOnConnLoss);
            return;
          }
          
          // ignore unless it's the last node in the path
          if (i == paths.length - 1) {
            throw e;
          }
        }
        if(i == paths.length -1) {
          // set new watch
          exists(currentPath, watcher, retryOnConnLoss);
        }
      } else if (i == paths.length - 1) {
        // TODO: version ? for now, don't worry about race
        setData(currentPath, data, -1, retryOnConnLoss);
        // set new watch
        exists(currentPath, watcher, retryOnConnLoss);
      }
    }
  }

  public void makePath(String zkPath, CreateMode createMode, Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(zkPath, null, createMode, watcher, retryOnConnLoss);
  }

  /**
   * Write data to ZooKeeper.
   */
  public Stat setData(String path, byte[] data, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    return setData(path, data, -1, retryOnConnLoss);
  }

  /**
   * Write file to ZooKeeper - default system encoding used.
   * 
   * @param path path to upload file to e.g. /solr/conf/solrconfig.xml
   * @param file path to file to be uploaded
   */
  public Stat setData(String path, File file, boolean retryOnConnLoss) throws IOException,
      KeeperException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info("Write to ZooKeepeer " + file.getAbsolutePath() + " to " + path);
    }

    byte[] data = FileUtils.readFileToByteArray(file);
    return setData(path, data, retryOnConnLoss);
  }

  /**
   * Fills string with printout of current ZooKeeper layout.
   */
  public void printLayout(String path, int indent, StringBuilder string)
      throws KeeperException, InterruptedException {
    byte[] data = getData(path, null, null, true);
    List<String> children = getChildren(path, null, true);
    StringBuilder dent = new StringBuilder();
    for (int i = 0; i < indent; i++) {
      dent.append(" ");
    }
    string.append(dent + path + " (" + children.size() + ")" + NEWL);
    if (data != null) {
      try {
        String dataString = new String(data, "UTF-8");
        if ((!path.endsWith(".txt") && !path.endsWith(".xml")) || path.endsWith(ZkStateReader.CLUSTER_STATE)) {
          if (path.endsWith(".xml")) {
            // this is the cluster state in xml format - lets pretty print
            dataString = prettyPrint(dataString);
          }
          
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
        try {
          printLayout(path + (path.equals("/") ? "" : "/") + child, indent + 1,
              string);
        } catch (NoNodeException e) {
          // must have gone away
        }
      }
    }

  }

  /**
   * Prints current ZooKeeper layout to stdout.
   */
  public void printLayoutToStdOut() throws KeeperException,
      InterruptedException {
    StringBuilder sb = new StringBuilder();
    printLayout("/", 0, sb);
    System.out.println(sb.toString());
  }
  
  public static String prettyPrint(String input, int indent) {
    try {
      Source xmlInput = new StreamSource(new StringReader(input));
      StringWriter stringWriter = new StringWriter();
      StreamResult xmlOutput = new StreamResult(stringWriter);
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      transformerFactory.setAttribute("indent-number", indent);
      Transformer transformer = transformerFactory.newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.transform(xmlInput, xmlOutput);
      return xmlOutput.getWriter().toString();
    } catch (Exception e) {
      throw new RuntimeException("Problem pretty printing XML", e);
    }
  }
  
  private static String prettyPrint(String input) {
    return prettyPrint(input, 2);
  }

  public void close() {
    if (isClosed) return; // it's okay if we over close - same as solrcore
    isClosed = true;
    try {
      closeKeeper(keeper);
    } finally {
      connManager.close();
    }
    numCloses.incrementAndGet();
  }

  public boolean isClosed() {
    return isClosed;
  }

  /**
   * Allows package private classes to update volatile ZooKeeper.
   */
  void updateKeeper(SolrZooKeeper keeper) throws InterruptedException {
   SolrZooKeeper oldKeeper = this.keeper;
   this.keeper = keeper;
   if (oldKeeper != null) {
     oldKeeper.close();
   }
   // we might have been closed already
   if (isClosed) this.keeper.close();
  }
  
  public SolrZooKeeper getSolrZooKeeper() {
    return keeper;
  }
  
  private void closeKeeper(SolrZooKeeper keeper) {
    if (keeper != null) {
      try {
        keeper.close();
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "",
            e);
      }
    }
  }

  // yeah, it's recursive :(
  public void clean(String path) throws InterruptedException, KeeperException {
    List<String> children;
    try {
      children = getChildren(path, null, true);
    } catch (NoNodeException r) {
      return;
    }
    for (String string : children) {
      // we can't clean the built-in zookeeper node
      if (path.equals("/") && string.equals("zookeeper")) continue;
      if (path.equals("/")) {
        clean(path + string);
      } else {
        clean(path + "/" + string);
      }
    }
    try {
      if (!path.equals("/")) {
        delete(path, -1, true);
      }
    } catch (NoNodeException r) {
      return;
    }
  }

}
