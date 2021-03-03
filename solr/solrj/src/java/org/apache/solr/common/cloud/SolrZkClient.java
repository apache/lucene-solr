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

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ConnectionManager.IsClosed;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 *
 * All Solr ZooKeeper interactions should go through this class rather than
 * ZooKeeper. This class handles synchronous connects and reconnections.
 *
 */
public class SolrZkClient implements Closeable {
  private static final int MAX_BYTES_FOR_ZK_LAYOUT_DATA_SHOW = 750;

  static final String NEWL = System.getProperty("line.separator");

  static final int DEFAULT_CLIENT_CONNECT_TIMEOUT = 5000;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final String INDENT = "  ";
  private final int zkClientConnectTimeout;
  private CloseTracker closeTracker;

  private final ConnectionManager connManager;

  private ZkCmdExecutor zkCmdExecutor;

  protected final ExecutorService zkCallbackExecutor = ParWork.getExecutorService(Integer.MAX_VALUE, false, false);
     // ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("zkCallback"));
  protected final ExecutorService zkConnManagerCallbackExecutor = ParWork.getExecutorService(Integer.MAX_VALUE, false, false);
     // ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("zkConnectionManagerCallback"));

  private volatile boolean isClosed = false;

  private volatile int zkClientTimeout;
  private volatile ZkACLProvider zkACLProvider;
  private volatile String zkServerAddress;
  private volatile IsClosed higherLevelIsClosed;
  private volatile boolean started;

  public int getZkClientTimeout() {
    return zkClientTimeout;
  }

  public int getZkClientConnectTimeout() {
    return zkClientConnectTimeout;
  }

  // expert: for tests
  public SolrZkClient() {
    assert (closeTracker = new CloseTracker()) != null;
    zkClientConnectTimeout = 0;
    connManager = new ConnectionManager("ZooKeeperConnection Watcher:"
        + zkServerAddress, this, zkServerAddress, zkClientTimeout);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout) {
    this(zkServerAddress, zkClientTimeout, DEFAULT_CLIENT_CONNECT_TIMEOUT);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    this(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout,  int zkClientConnectTimeout, final OnReconnect onReconnect) {
    this(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, onReconnect, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout, final OnReconnect onReconnect, BeforeReconnect beforeReconnect) {
    this(zkServerAddress, zkClientTimeout, clientConnectTimeout, onReconnect, beforeReconnect, null, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout, final OnReconnect onReconnect, BeforeReconnect beforeReconnect, ZkACLProvider zkACLProvider, IsClosed higherLevelIsClosed) {
    assert ObjectReleaseTracker.track(this);
    log.info("Creating new zkclient instance timeout={} connectTimeout={}", zkClientTimeout, clientConnectTimeout);
    if (log.isDebugEnabled()) log.debug("Creating new {} instance {}", SolrZkClient.class.getSimpleName(), this);
    assert (closeTracker = new CloseTracker()) != null;
    this.zkServerAddress = zkServerAddress;
    this.higherLevelIsClosed = higherLevelIsClosed;

    this.zkClientTimeout = zkClientTimeout;
    this.zkClientConnectTimeout = clientConnectTimeout;

    if (zkACLProvider == null) {
      this.zkACLProvider = createZkACLProvider();
    } else {
      this.zkACLProvider = zkACLProvider;
    }

    zkCmdExecutor = new ZkCmdExecutor(this, 3, new IsClosed() {

      @Override
      public boolean isClosed() {
        try {
          if (higherLevelIsClosed != null) {
            return SolrZkClient.this.higherLevelIsClosed.isClosed() || isClosed;
          } else {
            return  isClosed;
          }
        } catch (NullPointerException e) {
          return isClosed;
        }
      }
    });

    connManager = new ConnectionManager("ZooKeeperConnection Watcher:"
        + zkServerAddress, this, zkServerAddress, zkClientTimeout, onReconnect, beforeReconnect);

    ZkCredentialsProvider zkCredentialsToAddAutomatically = createZkCredentialsToAddAutomatically();
    if (zkCredentialsToAddAutomatically != null) {
      connManager.setZkCredentialsToAddAutomatically(zkCredentialsToAddAutomatically);
    }
  }

  public SolrZkClient start() {
    if (started) {
      throw new IllegalStateException("Already started");
    }
    started = true;
    if (log.isDebugEnabled()) log.debug("Starting {} instance {}", SolrZkClient.class.getSimpleName(), this);
    try {
      connManager.start();
      connManager.waitForConnected(this.zkClientConnectTimeout);
    } catch (TimeoutException e) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
    }
    return this;
  }

  public void setOnReconnect(OnReconnect onReconnect) {
    this.connManager.setOnReconnect(onReconnect);
  }

  public ConnectionManager getConnectionManager() {
    return connManager;
  }

  public static final String ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkCredentialsProvider";
  protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
    String zkCredentialsProviderClassName = System.getProperty(ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkCredentialsProviderClassName)) {
      try {
        if (log.isDebugEnabled()) log.debug("Using ZkCredentialsProvider: {}", zkCredentialsProviderClassName);
        return (ZkCredentialsProvider)Class.forName(zkCredentialsProviderClassName).getConstructor().newInstance();
      } catch (Throwable t) {
        // just ignore - go default
        log.warn("VM param zkCredentialsProvider does not point to a class implementing ZkCredentialsProvider and with a non-arg constructor", t);
      }
    }
    if (log.isDebugEnabled()) log.debug("Using default ZkCredentialsProvider");
    return new DefaultZkCredentialsProvider();
  }

  public static final String ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkACLProvider";
  protected ZkACLProvider createZkACLProvider() {
    String zkACLProviderClassName = System.getProperty(ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkACLProviderClassName)) {
      try {
        if (log.isDebugEnabled()) log.debug("Using ZkACLProvider: {}", zkACLProviderClassName);
        return (ZkACLProvider)Class.forName(zkACLProviderClassName).getConstructor().newInstance();
      } catch (Throwable t) {
        // just ignore - go default
        log.warn("VM param zkACLProvider does not point to a class implementing ZkACLProvider and with a non-arg constructor", t);
      }
    }
    if (log.isDebugEnabled()) log.debug("Using default ZkACLProvider");
    return new DefaultZkACLProvider();
  }

  /**
   * Returns true if client is connected
   */
  public boolean isConnected() {

    return connManager.getKeeper() != null && connManager.getKeeper().getState().isConnected();
  }

  public boolean isAlive() {

    return connManager.getKeeper() != null && connManager.getKeeper().getState().isAlive();
  }

  public void delete(final String path, final int version) throws KeeperException, InterruptedException {
    delete(path, version, true);
  }

  public void delete(final String path, final int version, boolean retryOnConnLoss)
      throws InterruptedException, KeeperException {

    if (retryOnConnLoss) {
      ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> {
        connManager.getKeeper().delete(path, version);
        return null;
      });
    } else {
      connManager.getKeeper().delete(path, version);
    }
  }

  public void deleteAsync(final String path, final int version)
      throws InterruptedException, KeeperException {
    connManager.getKeeper().delete(path, version, (rc, path1, ctx) -> {
      if (rc != 0) {
        log.error("got zk error deleting path {} {}", path1, rc);
        KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
        log.error("Exception deleting znode path=" + path1, e);
      }
    }, "");
  }

  /**
   * Wraps the watcher so that it doesn't fire off ZK's event queue. In order to guarantee that a watch object will
   * only be triggered once for a given notification, users need to wrap their watcher using this method before
   * calling {@link #exists(String, org.apache.zookeeper.Watcher, boolean)} or
   * {@link #getData(String, org.apache.zookeeper.Watcher, org.apache.zookeeper.data.Stat, boolean)}.
   */
  public Watcher wrapWatcher(final Watcher watcher) {
    if (watcher == null || watcher instanceof ProcessWatchWithExecutor) return watcher;

    return new ProcessWatchWithExecutor(watcher, this);
  }

  public Stat exists(final String path, final Watcher watcher) throws KeeperException, InterruptedException {
    return exists(path, watcher, true);
  }

  public Stat exists(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    return exists(path, watcher, retryOnConnLoss, true);
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
  public Stat exists(final String path, final Watcher watcher, boolean retryOnConnLoss, boolean retryOnSessionExpiration)
      throws KeeperException, InterruptedException {
    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().exists(path, watcher == null ? null : wrapWatcher(watcher)), retryOnSessionExpiration);
    } else {
      return connManager.getKeeper().exists(path, watcher == null ? null : wrapWatcher(watcher));
    }
  }

  /**
   * Returns true if path exists
   */
  public Boolean exists(final String path, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss) {
      Stat existsStat = ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().exists(path, null));
      if (log.isDebugEnabled()) log.debug("exists state return is {} {}", path, existsStat);
      return existsStat != null;
    } else {
      Stat existsStat = connManager.getKeeper().exists(path, null);
      if (log.isDebugEnabled()) log.debug("exists state return is {} {}", path, existsStat);
      return existsStat != null;
    }
  }

  public Boolean exists(final String path) throws KeeperException, InterruptedException {
    return this.exists(path, true);
  }

      /**
       * Returns children of the node at the path
       */
  public List<String> getChildren(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().getChildren(path, watcher == null ? null : wrapWatcher(watcher)));
    } else {
      return connManager.getKeeper().getChildren(path, watcher == null ? null : wrapWatcher(watcher));
    }
  }

  public List<String> getChildren(final String path, final Watcher watcher, Stat stat, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    return getChildren(path, watcher, stat, retryOnConnLoss, true);
  }

  public List<String> getChildren(final String path, final Watcher watcher, Stat stat, boolean retryOnConnLoss,  boolean retrySessionExpiration)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().getChildren(path, watcher == null ? null : wrapWatcher(watcher), stat), retrySessionExpiration);
    } else {
      return connManager.getKeeper().getChildren(path, watcher == null ? null : wrapWatcher(watcher));
    }
  }

  public byte[] getData(final String path, final Watcher watcher, final Stat stat) throws KeeperException, InterruptedException {
    return getData(path, watcher, stat, true);
  }

  public byte[] getData(final String path, final Watcher watcher, final Stat stat, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    return getData(path, watcher, stat, retryOnConnLoss, true);
  }

      /**
       * Returns node's data
       */
  public byte[] getData(final String path, final Watcher watcher, final Stat stat, boolean retryOnConnLoss, boolean retryOnSessionExpiration)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss && zkCmdExecutor != null) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().getData(path, watcher == null ? null : wrapWatcher(watcher), stat), retryOnSessionExpiration);
    } else {
      return connManager.getKeeper().getData(path, watcher == null ? null : wrapWatcher(watcher), stat);
    }
  }

  public Stat setData(final String path, final byte data[], final int version, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    return setData(path, data, version, retryOnConnLoss, true);
  }

  /**
   * Returns node's state
   */
  public Stat setData(final String path, final byte data[], final int version, boolean retryOnConnLoss, boolean retryOnSessionExpiration)
      throws KeeperException, InterruptedException {

    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, new SetData(connManager.getKeeper(), path, data, version), retryOnSessionExpiration);
    } else {
      return connManager.getKeeper().setData(path, data, version);
    }
  }

  public void atomicUpdate(String path, Function<byte[], byte[]> editor) throws KeeperException, InterruptedException {
   atomicUpdate(path, (stat, bytes) -> editor.apply(bytes));
  }

  public void atomicUpdate(String path, BiFunction<Stat , byte[], byte[]> editor) throws KeeperException, InterruptedException {
    for (; ; ) {
      byte[] modified = null;
      byte[] zkData = null;
      Stat s = new Stat();
      try {
        if (exists(path)) {
          zkData = getData(path, null, s);
          modified = editor.apply(s, zkData);
          if (modified == null) {
            //no change , no need to persist
            return;
          }
          setData(path, modified, s.getVersion(), true);
          break;
        } else {
          modified = editor.apply(s,null);
          if (modified == null) {
            //no change , no need to persist
            return;
          }
          create(path, modified, CreateMode.PERSISTENT, true);
          break;
        }
      } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
        continue;
      }
    }


  }


  public void enableCloseLock() {
    if (closeTracker != null) {
      closeTracker.enableCloseLock();
    }
  }

  public void disableCloseLock() {
    if (closeTracker != null) {
      closeTracker.disableCloseLock();
    }
  }

  public String create(final String path, final byte[] data, final CreateMode createMode, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    return create(path, data, createMode, retryOnConnLoss, retryOnConnLoss);
  }

  /**
   * Returns path of created node
   */
  public String create(final String path, final byte[] data, final CreateMode createMode, boolean retryOnConnLoss, boolean retryOnSessionExp) throws KeeperException, InterruptedException {
    List<ACL> acls = zkACLProvider.getACLsToAdd(path);

    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().create(path, data, acls, createMode), retryOnSessionExp);
    } else {
      return connManager.getKeeper().create(path, data, acls, createMode);
    }
  }

  public String create(final String path, final File file, final CreateMode createMode, boolean retryOnConnLoss) throws KeeperException, InterruptedException, IOException {
    byte[] data = FileUtils.readFileToByteArray(file);
    return create(path, data, createMode, retryOnConnLoss);
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

    if (log.isDebugEnabled()) log.debug("makePath: {}", path);

    boolean retry = true;
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    String[] paths = path.split("/");
    StringBuilder sbPath = new StringBuilder();
    for (int i = 0; i < paths.length; i++) {
      String pathPiece = paths[i];
      sbPath.append("/").append(pathPiece);
      if (i < skipPathParts) {
        continue;
      }
      byte[] bytes = null;
      final String currentPath = sbPath.toString();

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
          ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> {
            connManager.getKeeper().create(currentPath, finalBytes, zkACLProvider.getACLsToAdd(currentPath), finalMode);
            return null;
          });
        } else {
          connManager.getKeeper().create(currentPath, bytes, zkACLProvider.getACLsToAdd(currentPath), mode);
        }
      } catch (NoAuthException e) {
        // in auth cases, we may not have permission for an earlier part of a path, which is fine
        if (i == paths.length - 1 || !exists(currentPath, retryOnConnLoss)) {

          throw e;
        }
      } catch (NodeExistsException e) {

        if (!failOnExists && i == paths.length - 1) {
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

    }
  }

  public void makePath(String zkPath, CreateMode createMode, Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(zkPath, null, createMode, watcher, retryOnConnLoss);
  }

  public void mkDirs(String path, byte[] bytes) throws KeeperException {
    Map<String,byte[]> dataMap = new HashMap<String,byte[]>(1);
    dataMap.put(path, bytes);
    mkdirs(dataMap);
  }

  public void mkdirs(String... paths) throws KeeperException {
    Map<String,byte[]> dataMap = new HashMap<String,byte[]>(paths.length);
    for (String path : paths) {
      dataMap.put(path, null);
    }
    mkdirs(dataMap);
  }

  public void mkdirs(Map<String,byte[]> dataMap) throws KeeperException {
    mkDirs(dataMap, Collections.emptyMap(), 0);
  }

  public void mkdirs(Map<String,byte[]> dataMap, int pathsAlreadyCreated) throws KeeperException {
    mkDirs(dataMap, Collections.emptyMap(), pathsAlreadyCreated);
  }

  public void mkdirs(Map<String,byte[]> dataMap, Map<String,CreateMode> createModeMap) throws KeeperException {
    mkDirs(dataMap, createModeMap, 0);
  }

  public void mkDirs(Map<String,byte[]> dataMap, Map<String,CreateMode> createModeMap, int pathsAlreadyCreated) throws KeeperException {
    Set<String> paths = dataMap.keySet();

    if (log.isDebugEnabled()) {
      log.debug("mkDirs(String paths={}) - start", paths);
    }
    Set<String> madePaths = new HashSet<>(paths.size() * 3);
    List<String> pathsToMake = new ArrayList<>(paths.size() * 3);

    for (String fullpath : paths) {
      if (!fullpath.startsWith("/")) throw new IllegalArgumentException("Paths must start with /, " + fullpath);
      StringBuilder sb = new StringBuilder();
      if (log.isDebugEnabled()) {
        log.debug("path {}", fullpath);
      }
      String[] subpaths = fullpath.split("/");
      int cnt = 0;
      for (String subpath : subpaths) {
        if (subpath.equals("")) continue;
        cnt++;
        if (subpath.length() == 0) continue;
        if (log.isDebugEnabled()) {
          log.debug("subpath {}", subpath);
        }
        sb.append("/" + subpath.replaceAll("\\/", ""));
        if (cnt > pathsAlreadyCreated) {
          pathsToMake.add(sb.toString());
        }
      }
    }

    List<String> nodeAlreadyExistsPaths = new LinkedList<>();

    CountDownLatch latch = new CountDownLatch(pathsToMake.size());
    int[] code = new int[1];
    String[] path = new String[1];
    boolean[]  failed = new boolean[1];
    boolean[] nodata = new boolean[1];
    for (String makePath : pathsToMake) {
      path[0] = null;
      nodata[0] = false;
      code[0] = 0;
      if (!makePath.startsWith("/")) makePath = "/" + makePath;

      byte[] data = dataMap.get(makePath);

      CreateMode createMode = createModeMap.getOrDefault(makePath, CreateMode.PERSISTENT);

      if (!madePaths.add(makePath)) {
        if (log.isDebugEnabled()) log.debug("skipping already made {}", makePath + " data: " + (data == null ? "none" : data.length + "b"));
        // already made
        latch.countDown();
        continue;
      }
      if (log.isDebugEnabled()) log.debug("makepath {}", makePath + " data: " + (data == null ? "none" : data.length + "b"));

      assert getZkACLProvider() != null;

      connManager.getKeeper().create(makePath, data, getZkACLProvider().getACLsToAdd(makePath), createMode,
          new MkDirsCallback(nodeAlreadyExistsPaths, path, code, failed, nodata, data, latch), "");
    }


    boolean success = false;
    try {
      success = latch.await(15, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      log.error("mkDirs(String=" + paths + ")", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    // MRM TODO:, still haackey, do fails right
    if (code[0] != 0) {
      KeeperException e = KeeperException.create(KeeperException.Code.get(code[0]), path[0]);
      throw e;
//      if (e instanceof NodeExistsException && (nodata[0])) {
//        // okay
//        log.warn("Node aready exists", e);
//        //printLayout();
//        throw e;
//      } else {
//        log.error("Could not create start cluster zk nodes", e);
//        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not create start cluster zk nodes", e);
//      }
    }

    if (!success) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Timeout waiting for operations to complete");
    }
    
    // we optimistically tried to create all the paths in dataMap, but that fails for existing znodes
    // so send updates to those paths instead (if any)
    if (!nodeAlreadyExistsPaths.isEmpty()) {
      updateExistingPaths(nodeAlreadyExistsPaths, dataMap);
    }
    
    if (log.isDebugEnabled()) {
      log.debug("mkDirs(String) - end");
    }
  }

  public Map<String,byte[]> getData(List<String> paths) {

    Map<String,byte[]> dataMap = Collections.synchronizedSortedMap(new TreeMap<>());
    CountDownLatch latch = new CountDownLatch(paths.size());

    for (String path : paths) {

      connManager.getKeeper().getData(path, false, (rc, path1, ctx, data, stat) -> {
        if (rc != 0) {
          final KeeperException.Code keCode = KeeperException.Code.get(rc);
          if (keCode == KeeperException.Code.NONODE) {
            if (log.isDebugEnabled()) log.debug("No node found for {}", path1);
          }
        } else {
          dataMap.put(path1, data);
        }
        latch.countDown();
      }, null);
    }

    boolean success;
    try {
      success = latch.await(15, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      log.error("mkDirs(String=" + paths + ")", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    if (!success) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Timeout waiting for operations to complete");
    }

    return dataMap;
  }

  public void delete(Collection<String> paths, boolean wait) throws KeeperException {
    if (log.isDebugEnabled()) log.debug("delete paths {} wait={}", paths, wait);
    if (paths.size() == 0) {
      return;
    }
    CountDownLatch latch = null;
    if (wait) {
      latch = new CountDownLatch(paths.size());
    }
    KeeperException[] ke = new KeeperException[1];
    for (String path : paths) {
      if (log.isDebugEnabled()) log.debug("process path={} connManager={}", path, connManager);
  

      CountDownLatch finalLatch = latch;

      connManager.getKeeper().delete(path, -1, (rc, path1, ctx) -> {
        try {
          // MRM TODO:
          if (log.isDebugEnabled()) {
            log.debug("async delete resp rc={}, path1={}, ctx={}", rc, path1, ctx);
          }
          if (rc != 0) {
            log.error("got zk error deleting paths {}", rc);
            KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
            if (e instanceof NoNodeException) {
              if (log.isDebugEnabled()) log.debug("Problem removing zk node {}", path1);
            } else {
              ke[0] = e;
            }
          }
        } finally {
          if (wait) {
            finalLatch.countDown();
          }
        }
      }, null);

    }

    if (log.isDebugEnabled()) {
      log.debug("done with all paths, see if wait ... wait={}", wait);
    }
    if (wait) {
      TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      boolean success = false;
      while (!timeout.hasTimedOut() && !isClosed) {
        if (!connManager.getKeeper().getState().isConnected()) {
          try {
            connManager.waitForConnected(30000);
          } catch (TimeoutException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
          } catch (InterruptedException e) {
            ParWork.propagateInterrupt(e);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
          }
        }

        if (ke[0] != null) {
          throw ke[0];
        }
        try {
          success = latch.await(2, TimeUnit.SECONDS);
          if (log.isDebugEnabled()) log.debug("done waiting on latch, success={}", success);
          if (success) {
            break;
          }
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
          log.error("", e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }
    }

    if (ke[0] != null) {
      throw ke[0];
    }
    if (log.isDebugEnabled()) {
      log.debug("done with delete {} {}", paths, wait);
    }
  }

  // Calls setData for a list of existing paths in parallel
  private void updateExistingPaths(List<String> pathsToUpdate, Map<String,byte[]> dataMap) throws KeeperException {
    final KeeperException[] keeperExceptions = new KeeperException[1];
    pathsToUpdate.parallelStream().forEach(new PathConsumer(dataMap, keeperExceptions));
    if (keeperExceptions[0] != null) {
      throw keeperExceptions[0];
    }
  }

  public void data(String path, byte[] data) throws KeeperException {
    try {

      connManager.getKeeper().setData(path, data, -1);
    } catch (InterruptedException e) {
      ParWork.propagateInterrupt(e);
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
    }
  }

  public void mkdirs(String znode, File file) throws KeeperException {
    try {
      mkDirs(znode, FileUtils.readFileToByteArray(file));
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
    }
  }

  public void mkdir(String path) throws KeeperException, InterruptedException {
    mkdir(path, null);
  }

  public void mkdir(String path, byte[] data) throws KeeperException, InterruptedException {
    mkdir(path, data, CreateMode.PERSISTENT);
  }

  public String mkdir(String path, byte[] data, CreateMode createMode) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) log.debug("mkdir path={}", path);
    boolean retryOnConnLoss = true; // MRM TODO:
    if (retryOnConnLoss) {
      ZkCmdExecutor.retryOperation(zkCmdExecutor, new CreateZkOperation(path, data, createMode));
    } else {
      String createdPath;
      try {
    
        createdPath = connManager.getKeeper().create(path, data, getZkACLProvider().getACLsToAdd(path), createMode);
      } catch (IllegalArgumentException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, path, e);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
      }
      return createdPath;
    }
    return null;
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
    if (log.isDebugEnabled()) {
      log.debug("Write to ZooKeeper: {} to {}", file.getAbsolutePath(), path);
    }
    byte[] data = FileUtils.readFileToByteArray(file);
    return setData(path, data, retryOnConnLoss);
  }

  public List<OpResult> multi(final Iterable<Op> ops, boolean retryOnConnLoss) throws InterruptedException, KeeperException  {
     return multi(ops, retryOnConnLoss, true);
  }

  public List<OpResult> multi(final Iterable<Op> ops, boolean retryOnConnLoss, boolean retryOnSessionExp) throws InterruptedException, KeeperException  {
  
    if (retryOnConnLoss) {
      return ZkCmdExecutor.retryOperation(zkCmdExecutor, () -> connManager.getKeeper().multi(ops), retryOnSessionExp);
    } else {
      return connManager.getKeeper().multi(ops);
    }
  }

  public void printLayout(String path, int indent, int maxBytesBeforeSuppress, StringBuilder output) {
    try {
      printLayout(path, "", indent, maxBytesBeforeSuppress, output);
    } catch (Exception e) {
      log.error("Exception printing layout", e);
    }
  }

  public void printLayout(String path, int indent, StringBuilder output) {
    try {
      printLayout(path, "", indent, MAX_BYTES_FOR_ZK_LAYOUT_DATA_SHOW, output);
    } catch (Exception e) {
      log.error("Exception printing layout", e);
    }
  }

  /**
   * Fills string with printout of current ZooKeeper layout.
   */
  public void printLayout(String path, String node, int indent, int maxBytesBeforeSuppress, StringBuilder output) {
    try {
      //log.info("path={} node={} indext={}", path, node, indent);

      //    if (node != null && node.length() > 0) {
      //      path = path;
      //    }

      List<String> children = null;
      if (!path.trim().equals("/")) {
        byte[] data = EMPTY_BYTES;
        Stat stat = new Stat();

        try {
          data = getData(path, null, stat, true);
          children = getChildren(path, null, true);
          Collections.sort(children);
        } catch (Exception e1) {
          if (e1 instanceof KeeperException.NoNodeException) {
            // things change ...
            children = Collections.emptyList();
          } else {
            ParWork.propagateInterrupt(e1, true);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Problem with path='" + path + "'", e1);
          }
        }
        StringBuilder dent = new StringBuilder();
        for (int i = 0; i < indent; i++) {
          dent.append(INDENT);
        }
        String childrenString;
        if (children.size() > 0) {
          childrenString = "c=" + children.size() + ",";
        } else {
          childrenString = "";
        }
        output.append(dent.toString()).append(children.size() == 0 ? node : "+" + node).append(" [").append(childrenString).append("v=").append ((stat == null ? "?" : stat.getVersion()) + "]");
        StringBuilder dataBuilder = new StringBuilder();
        String dataString;
        if (data != null && data.length > 0) {
//          if (path.endsWith(".json")) {
//            dataString = Utils.fromJSON(data).toString();
//          } else {
            dataString = new String(data, StandardCharsets.UTF_8);
        //  }
          int lines;
          if (maxBytesBeforeSuppress != MAX_BYTES_FOR_ZK_LAYOUT_DATA_SHOW) {
            lines = 0;
          } else {
            lines = dataString.split("\\r\\n|\\r|\\n").length;
          }

          if ((stat != null && stat.getDataLength() < maxBytesBeforeSuppress && lines < 4) || path.endsWith("state.json") || path
              .endsWith("security.json") || (path.endsWith("solrconfig.xml") && Boolean.getBoolean("solr.tests.printsolrconfig")) || path.endsWith("_statupdates") || path.contains("/terms/")) {
            //        if (path.endsWith(".xml")) {
            //          // this is the cluster state in xml format - lets pretty print
            //          dataString = prettyPrint(path, dataString);
            //        }
            dataString = dataString.replaceAll("\\n", "\n" + dent.toString() + INDENT);

            dataBuilder.append(" (" + (stat != null ? stat.getDataLength() : "?") + "b) : " + (lines > 1 ? "\n" + dent.toString() + INDENT : "") + dataString.trim()).append(NEWL);
          } else {
            dataBuilder.append(" (" + (stat != null ? stat.getDataLength() : "?") + "b) : ...supressed...").append(NEWL);
          }
        } else {
          output.append(NEWL);
        }
        output.append(dataBuilder);
        indent += 2;
      } else {
        output.append("/");
      }
      if (children == null) {
        try {
          children = getChildren(path, null, true);
        } catch (KeeperException | InterruptedException e1) {
          if (e1 instanceof KeeperException.NoNodeException) {
            // things change ...
            children = Collections.emptyList();
          } else {
            ParWork.propagateInterrupt(e1, true);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Problem with path='" + path + "'", e1);
          }
        }
      }
      if (children != null) {
        for (String child : children) {
          if (!child.equals("quota") && !child.equals("/zookeeper")) {
            printLayout(path.equals("/") ? "/" + child : path + "/" + child, child, indent, maxBytesBeforeSuppress, output);
          }
        }
      }
    } catch (Exception e) {
      log.error("Exception printing layout", e);
    }
  }

  public void printLayout() {
    StringBuilder sb = new StringBuilder(1024);
    printLayout("/",0, sb);
    log.warn("\n\n_____________________________________________________________________\n\n\nZOOKEEPER LAYOUT:\n\n" + sb.toString() + "\n\n_____________________________________________________________________\n\n");
  }

  public void printLayoutToStream(PrintStream out) {
    StringBuilder sb = new StringBuilder(1024);
    printLayout("/", 0, sb);
    out.println(sb.toString());
  }

  public void printLayoutToStream(PrintStream out, String path) {
    StringBuilder sb = new StringBuilder(1024);
    printLayout(path, 0, sb);
    out.println(sb.toString());
  }

  public void printLayoutToStream(PrintStream out, int maxBytesBeforeSuppress) {
    StringBuilder sb = new StringBuilder(1024);
    printLayout("/", 0, maxBytesBeforeSuppress, sb);
    out.println(sb.toString());
  }

  public void printLayoutToStream(PrintStream out, String path, int maxBytesBeforeSuppress) {
    StringBuilder sb = new StringBuilder(1024);
    printLayout(path, 0, maxBytesBeforeSuppress, sb);
    out.println(sb.toString());
  }

  public void printLayoutToFile(Path file) {
    StringBuilder sb = new StringBuilder(1024);
    printLayout("/",0, sb);
    try {
      Files.writeString(file, sb.toString(), StandardOpenOption.CREATE);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public static String prettyPrint(String path, String dataString, int indent) {
    try {
      Source xmlInput = new StreamSource(new StringReader(dataString));
      try (StringWriter stringWriter = new StringWriter()) {
        StreamResult xmlOutput = new StreamResult(stringWriter);
        try (Writer writer = xmlOutput.getWriter()) {
          return writer.toString();
        }
      } finally {
        IOUtils.closeQuietly(((StreamSource) xmlInput).getInputStream());
      }
    } catch (Exception e) {
      log.error("prettyPrint(path={}, dataString={})", dataString, indent, e);
      ParWork.propagateInterrupt(e);
      return "XML Parsing Failure";
    }
  }

  private static String prettyPrint(String path, String input) {
    String returnString = prettyPrint(path, input, 2);
    return returnString;
  }
  public void close() {
    if (log.isDebugEnabled()) log.debug("Closing {} instance {}", SolrZkClient.class.getSimpleName(), this);

    isClosed = true;
    connManager.close();

    assert closeTracker != null ? closeTracker.close() : true;
    assert ObjectReleaseTracker.release(this);
  }

  public boolean isClosed() {
    return isClosed;
  }

  public SolrZooKeeper getSolrZooKeeper() {

    if (connManager.getKeeper() == null) {
      throw new AlreadyClosedException("No ZooKeeper object");
    }
    return (SolrZooKeeper) connManager.getKeeper();
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
      Thread.currentThread().interrupt();
    return e;
  }

  /**
   * @return the address of the zookeeper cluster
   */
  public String getZkServerAddress() {
    return zkServerAddress;
  }

  /**
   * Gets the raw config node /zookeeper/config as returned by server. Response may look like
   * <pre>
   * server.1=localhost:2780:2783:participant;localhost:2791
   * server.2=localhost:2781:2784:participant;localhost:2792
   * server.3=localhost:2782:2785:participant;localhost:2793
   * version=400000003
   * </pre>
   * @return Multi line string representing the config. For standalone ZK this will return empty string
   */
  public String getConfig() {
    try {
      Stat stat = new Stat();

      connManager.getKeeper().sync(ZooDefs.CONFIG_NODE, null, null);
      byte[] data = connManager.getKeeper().getConfig(false, stat);
      if (data == null || data.length == 0) {
        return "";
      }
      return new String(data, StandardCharsets.UTF_8);
    } catch (KeeperException|InterruptedException ex) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to get config from zookeeper", ex);
    }
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
  
      return connManager.getKeeper().setACL(path, acls, -1);
  }

  public void setHigherLevelIsClosed(IsClosed isClosed) {
    this.higherLevelIsClosed = isClosed;
  }

  public IsClosed getHigherLevelIsClosed() {
    return this.higherLevelIsClosed;
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

  public void cleanChildren(String path) throws InterruptedException, KeeperException {
    ZkMaintenanceUtils.cleanChildren(this, path);
  }

  public void clean(String path, Predicate<String> nodeFilter) throws InterruptedException, KeeperException {
    if (log.isDebugEnabled()) log.debug("clean path {}" + path);
    ZkMaintenanceUtils.clean(this, path, nodeFilter);
  }

  public void upConfig(Path confPath, String confName) throws IOException, KeeperException {
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
                         final Pattern filenameExclusions) throws IOException, KeeperException {
    ZkMaintenanceUtils.uploadToZK(this, rootPath, zkPath, filenameExclusions);
  }
  public void downloadFromZK(String zkPath, Path dir) throws IOException {
    ZkMaintenanceUtils.downloadFromZK(this, zkPath, dir);
  }

  public Op createPathOp(String path) {
    return createPathOp(path, null);
  }

  public Op createPathOp(String path, byte[] data) {
    return Op.create(path, data, getZkACLProvider().getACLsToAdd(path), CreateMode.PERSISTENT);
  }

  public void setAclProvider(ZkACLProvider zkACLProvider) {
    this.zkACLProvider = zkACLProvider;
  }

  public void setIsClosed(IsClosed isClosed) {
    this.higherLevelIsClosed = isClosed;
  }

  public void setDisconnectListener(ConnectionManager.DisconnectListener dl) {
    this.connManager.setDisconnectListener(dl);
  }

  public void addWatch(String basePath, Watcher watcher, AddWatchMode mode) throws KeeperException, InterruptedException {
    getSolrZooKeeper().addWatch(basePath, watcher == null ? null : wrapWatcher(watcher), mode);
  }

  public void addWatch(String basePath, Watcher watcher, AddWatchMode mode, AsyncCallback.VoidCallback cb, Object ctx) {
    getSolrZooKeeper().addWatch(basePath, watcher == null ? null : wrapWatcher(watcher), mode, cb, ctx);
  }

  public void removeWatches(String path, Watcher watcher, Watcher.WatcherType watcherType, boolean local, AsyncCallback.VoidCallback cb, Object ctx) {
    getSolrZooKeeper().removeWatches(path, watcher, watcherType, local, cb, ctx);
  }

  public void removeWatches(String path, Watcher watcher, Watcher.WatcherType watcherType, boolean local) throws KeeperException, InterruptedException {
    getSolrZooKeeper().removeWatches(path, watcher, watcherType, local);
  }

  public long getSessionId() {
    return getSolrZooKeeper().getSessionId();
  }

  /**
   * Watcher wrapper that ensures that heavy implementations of process do not interfere with our ability
   * to react to other watches, but also ensures that two wrappers containing equal watches are considered
   * equal (and thus we won't accumulate multiple wrappers of the same watch).
   */
  private final static class ProcessWatchWithExecutor implements Watcher { // see below for why final.
    private final Watcher watcher;
    SolrZkClient solrZkClient;

    ProcessWatchWithExecutor(Watcher watcher, SolrZkClient solrZkClient) {
      this.solrZkClient = solrZkClient;
      if (watcher == null) {
        throw new IllegalArgumentException("Watcher must not be null");
      }
      this.watcher = watcher;
    }

    @Override
    public void process(final WatchedEvent event) {
      try {
        if (watcher instanceof ConnectionManager) {
          solrZkClient.zkConnManagerCallbackExecutor.submit(() -> watcher.process(event));
        } else {
          if (event.getType() != Event.EventType.None) {
            solrZkClient.zkCallbackExecutor.submit(new ParWork.SolrFutureTask("ZkSolrEventThread", () -> {
              watcher.process(event);
              return null;
            }));
          }
        }
      } catch (RejectedExecutionException e) {
        if (log.isDebugEnabled()) log.debug("Will not process zookeeper update after close");
      }
        // If not a graceful shutdown
//        if (!isClosed()) {
//          throw e;
//        }
    }

    // These overrides of hashcode/equals ensure that we don't store the same exact watch
    // multiple times in org.apache.zookeeper.ZooKeeper.ZKWatchManager.dataWatches
    // (a Map<String<Set<Watch>>). This class is marked final to avoid oddball
    // cases with sub-classes, if you need different behavior, find a new class or make
    // sure you account for the case where two diff sub-classes with different behavior
    // for process(WatchEvent) and have been created with the same watch object.
    @Override
    public int hashCode() {
      return watcher.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ProcessWatchWithExecutor) {
        return this.watcher.equals(((ProcessWatchWithExecutor) obj).watcher);
      }
      return false;
    }
  }

  private static class SetData implements ZkOperation {
    private final ZooKeeper keeper;
    private final String path;
    private final byte[] data;
    private final int version;

    public SetData(ZooKeeper keeper, String path, byte[] data, int version) {
      this.keeper = keeper;
      this.path = path;
      this.data = data;
      this.version = version;
    }

    @Override
    public Object execute() throws KeeperException, InterruptedException {
      return keeper.setData(path, data, version);
    }
  }

  private static class MkDirsCallback implements AsyncCallback.Create2Callback {
    private final List<String> nodeAlreadyExistsPaths;
    private final String[] path;
    private final int[] code;
    private final boolean[] failed;
    private final boolean[] nodata;
    private final byte[] data;
    private final CountDownLatch latch;

    public MkDirsCallback(List<String> nodeAlreadyExistsPaths, String[] path, int[] code, boolean[] failed, boolean[] nodata, byte[] data, CountDownLatch latch) {
      this.nodeAlreadyExistsPaths = nodeAlreadyExistsPaths;
      this.path = path;
      this.code = code;
      this.failed = failed;
      this.nodata = nodata;
      this.data = data;
      this.latch = latch;
    }

    @Override
    public void processResult(int rc, String zkpath, Object ctx, String name, Stat stat) {
      if (rc != 0) {
        final KeeperException.Code keCode = KeeperException.Code.get(rc);
        if (keCode == KeeperException.Code.NODEEXISTS) {
          nodeAlreadyExistsPaths.add(zkpath);
        } else {
          log.warn("create znode {} failed due to: {}", zkpath, keCode);
          if (path[0] == null) {
            // capture the first error for reporting back
            code[0] = rc;
            failed[0] = true;
            path[0] = "" + zkpath;
            nodata[0] = data == null;
          }
        }
      } else {
        log.debug("Created znode at path: {}", zkpath);
      }

      latch.countDown();
    }
  }

  private class PathConsumer implements Consumer<String> {
    private final Map<String,byte[]> dataMap;
    private final KeeperException[] keeperExceptions;

    public PathConsumer(Map<String,byte[]> dataMap, KeeperException[] keeperExceptions) {
      this.dataMap = dataMap;
      this.keeperExceptions = keeperExceptions;
    }

    @Override
    public void accept(String p) {
      try {
        setData(p, dataMap.get(p), -1, true);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        log.error("Failed to set data for {}", p, e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } catch (KeeperException ke) {
        log.error("Failed to set data for {}", p, ke);
        keeperExceptions[0] = ke;
      }
    }
  }

  private class CreateZkOperation implements ZkOperation {
    private final String path;
    private final byte[] data;
    private final CreateMode createMode;

    public CreateZkOperation(String path, byte[] data, CreateMode createMode) {
      this.path = path;
      this.data = data;
      this.createMode = createMode;
    }

    @Override
    public Object execute() throws KeeperException {
      String createdPath;
      try {
    
        createdPath = connManager.getKeeper().create(path, data, getZkACLProvider().getACLsToAdd(path), createMode);
      } catch (IllegalArgumentException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, path, e);
      } catch (InterruptedException e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
      }
      return createdPath;
    }
  }
}
