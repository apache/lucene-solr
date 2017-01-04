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

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ZkClientConnectionStrategy.ZkUpdate;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
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
public class SolrZkClient implements Closeable {

  static final String NEWL = System.getProperty("line.separator");

  static final int DEFAULT_CLIENT_CONNECT_TIMEOUT = 30000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private ConnectionManager connManager;

  private volatile SolrZooKeeper keeper;

  private ZkCmdExecutor zkCmdExecutor;

  private final ExecutorService zkCallbackExecutor = ExecutorUtil.newMDCAwareCachedThreadPool(new SolrjNamedThreadFactory("zkCallback"));

  private volatile boolean isClosed = false;
  private ZkClientConnectionStrategy zkClientConnectionStrategy;
  private int zkClientTimeout;
  private ZkACLProvider zkACLProvider;
  private String zkServerAddress;

  public int getZkClientTimeout() {
    return zkClientTimeout;
  }

  // expert: for tests
  public SolrZkClient() {

  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout) {
    this(zkServerAddress, zkClientTimeout, new DefaultConnectionStrategy(), null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    this(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, new DefaultConnectionStrategy(), null);
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
    this(zkServerAddress, zkClientTimeout, clientConnectTimeout, strat, onReconnect, null, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout,
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect, BeforeReconnect beforeReconnect) {
    this(zkServerAddress, zkClientTimeout, clientConnectTimeout, strat, onReconnect, beforeReconnect, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout,
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect, BeforeReconnect beforeReconnect, ZkACLProvider zkACLProvider) {
    this.zkServerAddress = zkServerAddress;
    
    if (strat == null) {
      strat = new DefaultConnectionStrategy();
    }
    this.zkClientConnectionStrategy = strat;

    if (!strat.hasZkCredentialsToAddAutomatically()) {
      ZkCredentialsProvider zkCredentialsToAddAutomatically = createZkCredentialsToAddAutomatically();
      strat.setZkCredentialsToAddAutomatically(zkCredentialsToAddAutomatically);
    }

    this.zkClientTimeout = zkClientTimeout;
    // we must retry at least as long as the session timeout
    zkCmdExecutor = new ZkCmdExecutor(zkClientTimeout);
    connManager = new ConnectionManager("ZooKeeperConnection Watcher:"
        + zkServerAddress, this, zkServerAddress, strat, onReconnect, beforeReconnect);

    try {
      strat.connect(zkServerAddress, zkClientTimeout, wrapWatcher(connManager),
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
    assert ObjectReleaseTracker.track(this);
    if (zkACLProvider == null) {
      this.zkACLProvider = createZkACLProvider();
    } else {
      this.zkACLProvider = zkACLProvider;
    }
  }

  public ConnectionManager getConnectionManager() {
    return connManager;
  }

  public ZkClientConnectionStrategy getZkClientConnectionStrategy() {
    return zkClientConnectionStrategy;
  }

  public static final String ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkCredentialsProvider";
  protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
    String zkCredentialsProviderClassName = System.getProperty(ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkCredentialsProviderClassName)) {
      try {
        log.info("Using ZkCredentialsProvider: " + zkCredentialsProviderClassName);
        return (ZkCredentialsProvider)Class.forName(zkCredentialsProviderClassName).getConstructor().newInstance();
      } catch (Throwable t) {
        // just ignore - go default
        log.warn("VM param zkCredentialsProvider does not point to a class implementing ZkCredentialsProvider and with a non-arg constructor", t);
      }
    }
    log.debug("Using default ZkCredentialsProvider");
    return new DefaultZkCredentialsProvider();
  }

  public static final String ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkACLProvider";
  protected ZkACLProvider createZkACLProvider() {
    String zkACLProviderClassName = System.getProperty(ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkACLProviderClassName)) {
      try {
        log.info("Using ZkACLProvider: " + zkACLProviderClassName);
        return (ZkACLProvider)Class.forName(zkACLProviderClassName).getConstructor().newInstance();
      } catch (Throwable t) {
        // just ignore - go default
        log.warn("VM param zkACLProvider does not point to a class implementing ZkACLProvider and with a non-arg constructor", t);
      }
    }
    log.debug("Using default ZkACLProvider");
    return new DefaultZkACLProvider();
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
   * Wraps the watcher so that it doesn't fire off ZK's event queue. In order to guarantee that a watch object will
   * only be triggered once for a given notification, users need to wrap their watcher using this method before
   * calling {@link #exists(String, org.apache.zookeeper.Watcher, boolean)} or
   * {@link #getData(String, org.apache.zookeeper.Watcher, org.apache.zookeeper.data.Stat, boolean)}.
   */
  public Watcher wrapWatcher(final Watcher watcher) {
    if (watcher == null || watcher instanceof SolrZkWatcher) return watcher;

    return new SolrZkWatcher() {
      @Override
      public void process(final WatchedEvent event) {
        log.debug("Submitting job to respond to event " + event);
        try {
          zkCallbackExecutor.submit(() -> watcher.process(event));
        } catch (RejectedExecutionException e) {
          // If not a graceful shutdown
          if (!isClosed()) {
            throw e;
          }
        }
      }
    };
  }

  private interface SolrZkWatcher extends Watcher {
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
          return keeper.exists(path, wrapWatcher(watcher));
        }
      });
    } else {
      return keeper.exists(path, wrapWatcher(watcher));
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
   * Returns children of the node at the path
   */
  public List<String> getChildren(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (retryOnConnLoss) {
      return zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public List<String> execute() throws KeeperException, InterruptedException {
          return keeper.getChildren(path, wrapWatcher(watcher));
        }
      });
    } else {
      return keeper.getChildren(path, wrapWatcher(watcher));
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
          return keeper.getData(path, wrapWatcher(watcher), stat);
        }
      });
    } else {
      return keeper.getData(path, wrapWatcher(watcher), stat);
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
          return keeper.create(path, data, zkACLProvider.getACLsToAdd(path),
              createMode);
        }
      });
    } else {
      List<ACL> acls = zkACLProvider.getACLsToAdd(path);
      return keeper.create(path, data, acls, createMode);
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
    makePath(path, null, CreateMode.PERSISTENT, null, failOnExists, retryOnConnLoss, 0);
  }

  public void makePath(String path, File file, boolean failOnExists, boolean retryOnConnLoss)
      throws IOException, KeeperException, InterruptedException {
    makePath(path, FileUtils.readFileToByteArray(file),
        CreateMode.PERSISTENT, null, failOnExists, retryOnConnLoss, 0);
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
    makePath(path, data, createMode, watcher, true, retryOnConnLoss, 0);
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
      Watcher watcher, boolean failOnExists, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    makePath(path, data, createMode, watcher, failOnExists, retryOnConnLoss, 0);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * skipPathParts will force the call to fail if the first skipPathParts do not exist already.
   *
   * Note: retryOnConnLoss is only respected for the final node - nodes
   * before that are always retried on connection loss.
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher, boolean failOnExists, boolean retryOnConnLoss, int skipPathParts) throws KeeperException, InterruptedException {
    log.debug("makePath: {}", path);
    boolean retry = true;

    if (path.startsWith("/")) {
      path = path.substring(1, path.length());
    }
    String[] paths = path.split("/");
    StringBuilder sbPath = new StringBuilder();
    for (int i = 0; i < paths.length; i++) {
      String pathPiece = paths[i];
      sbPath.append("/" + pathPiece);
      if (i < skipPathParts) {
        continue;
      }
      byte[] bytes = null;
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
                keeper.create(currentPath, finalBytes, zkACLProvider.getACLsToAdd(currentPath), finalMode);
                return null;
              }
            });
          } else {
            keeper.create(currentPath, bytes, zkACLProvider.getACLsToAdd(currentPath), mode);
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
    log.debug("Write to ZooKeeper: {} to {}", file.getAbsolutePath(), path);
    byte[] data = FileUtils.readFileToByteArray(file);
    return setData(path, data, retryOnConnLoss);
  }

  public List<OpResult> multi(final Iterable<Op> ops, boolean retryOnConnLoss) throws InterruptedException, KeeperException  {
    if (retryOnConnLoss) {
      return zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public List<OpResult> execute() throws KeeperException, InterruptedException {
          return keeper.multi(ops);
        }
      });
    } else {
      return keeper.multi(ops);
    }
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
      String dataString = new String(data, StandardCharsets.UTF_8);
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
      closeCallbackExecutor();
    }
    assert ObjectReleaseTracker.release(this);
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

  private void closeCallbackExecutor() {
    try {
      ExecutorUtil.shutdownAndAwaitTermination(zkCallbackExecutor);
    } catch (Exception e) {
      SolrException.log(log, e);
    }
  }


  /**
   * Validates if zkHost contains a chroot. See http://zookeeper.apache.org/doc/r3.2.2/zookeeperProgrammers.html#ch_zkSessions
   */
  public static boolean containsChroot(String zkHost) {
    return zkHost.contains("/");
  }

  /**
   * Check to see if a Throwable is an InterruptedException, and if it is, set the thread interrupt flag
   * @param e the Throwable
   * @return the Throwable
   */
  public static Throwable checkInterrupted(Throwable e) {
    if (e instanceof InterruptedException)
      Thread.interrupted();
    return e;
  }

  /**
   * @return the address of the zookeeper cluster
   */
  public String getZkServerAddress() {
    return zkServerAddress;
  }

  public ZkACLProvider getZkACLProvider() {
    return zkACLProvider;
  }

  /**
   * Set the ACL on a single node in ZooKeeper. This will replace all existing ACL on that node.
   *
   * @param path path to set ACL on e.g. /solr/conf/solrconfig.xml
   * @param acls a list of {@link ACL}s to be applied
   * @param retryOnConnLoss true if the command should be retried on connection loss
   */
  public Stat setACL(String path, List<ACL> acls, boolean retryOnConnLoss) throws InterruptedException, KeeperException  {
    if (retryOnConnLoss) {
      return zkCmdExecutor.retryOperation(new ZkOperation() {
        @Override
        public Stat execute() throws KeeperException, InterruptedException {
          return keeper.setACL(path, acls, -1);
        }
      });
    } else {
      return keeper.setACL(path, acls, -1);
    }
  }

  /**
   * Update all ACLs for a zk tree based on our configured {@link ZkACLProvider}.
   * @param root the root node to recursively update
   */
  public void updateACLs(final String root) throws KeeperException, InterruptedException {
    ZkMaintenanceUtils.traverseZkTree(this, root, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, path -> {
      try {
        setACL(path, getZkACLProvider().getACLsToAdd(path), true);
        log.debug("Updated ACL on {}", path);
      } catch (NoNodeException e) {
        // If a node was deleted, don't bother trying to set ACLs on it.
        return;
      }
    });
  }

  // Some pass-throughs to allow less code disruption to other classes that use SolrZkClient.
  public void clean(String path) throws InterruptedException, KeeperException {
    ZkMaintenanceUtils.clean(this, path);
  }

  public void upConfig(Path confPath, String confName) throws IOException {
    ZkMaintenanceUtils.upConfig(this, confPath, confName);
  }

  public String listZnode(String path, Boolean recurse) throws KeeperException, InterruptedException, SolrServerException {
    return ZkMaintenanceUtils.listZnode(this, path, recurse);
  }

  public void downConfig(String confName, Path confPath) throws IOException {
    ZkMaintenanceUtils.downConfig(this, confName, confPath);
  }

  public void zkTransfer(String src, Boolean srcIsZk, 
                         String dst, Boolean dstIsZk,
                         Boolean recurse) throws SolrServerException, KeeperException, InterruptedException, IOException {
    ZkMaintenanceUtils.zkTransfer(this, src, srcIsZk, dst, dstIsZk, recurse);
  }

  public void moveZnode(String src, String dst) throws SolrServerException, KeeperException, InterruptedException {
    ZkMaintenanceUtils.moveZnode(this, src, dst);
  }

  public void uploadToZK(final Path rootPath, final String zkPath,
                         final Pattern filenameExclusions) throws IOException {
    ZkMaintenanceUtils.uploadToZK(this, rootPath, zkPath, filenameExclusions);
  }
  public void downloadFromZK(String zkPath, Path dir) throws IOException {
    ZkMaintenanceUtils.downloadFromZK(this, zkPath, dir);
  }
}
