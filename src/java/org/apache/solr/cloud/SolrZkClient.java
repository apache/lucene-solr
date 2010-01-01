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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrZkClient {
  static final String NEWL = System.getProperty("line.separator");
  
  private static final int CONNECT_TIMEOUT = 5000;

  protected static final Logger log = LoggerFactory
      .getLogger(SolrZkClient.class);

  private String zkHost;
  private int zkClientTimeout;
  
  boolean connected = false;
  
  private CountdownWatcher cw = new CountdownWatcher("ZooKeeperConnection Watcher");

  private volatile ZooKeeper keeper;

  public SolrZkClient(String zkHost, int zkClientTimeout) {
    this.zkHost = zkHost;
    this.zkClientTimeout = zkClientTimeout;
  }

  public void connect() throws InterruptedException, TimeoutException,
      IOException {
    if(connected) {
      return;
    }
    // nocommit
    log.info("Connecting to ZooKeeper...");
    
    keeper = new ZooKeeper(zkHost, zkClientTimeout, cw);
    cw.waitForConnected(CONNECT_TIMEOUT);

    // nocommit
    log.info("Connected");
  }
  
  public boolean connected() {
    return keeper != null && keeper.getState() == ZooKeeper.States.CONNECTED;
  }

  class CountdownWatcher implements Watcher {

    private final String name;
    private CountDownLatch clientConnected;
    private KeeperState state;
    private boolean connected;

    public CountdownWatcher(String name) {
      this.name = name;
      reset();
    }

    private synchronized void reset() {
      clientConnected = new CountDownLatch(1);
      state = KeeperState.Disconnected;
      connected = false;
    }

    public synchronized void process(WatchedEvent event) {
      if (log.isInfoEnabled()) {
        log.info("Watcher " + name + " got event " + event);
      }

      state = event.getState();
      if (state == KeeperState.SyncConnected) {
        connected = true;
        clientConnected.countDown();
      } else if (state == KeeperState.Expired) {
        connected = false;
        log.info("Attempting to reconnect to ZooKeeper...");
        boolean connected = true;

        // nocommit : close old ZooKeeper client?

        try {
          connect();
        } catch (InterruptedException e) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
          connected = false;
        } catch (TimeoutException e) {
          connected = false;
        } catch (IOException e) {
          // nocommit
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        log.info("Connected:" + connected);
        // nocommit: start reconnect attempts
      } else if (state == KeeperState.Disconnected) {
        connected = false;
        // nocommit: start reconnect attempts
      } else {
        connected = false;
      }
      notifyAll();
    }

    public synchronized boolean isConnected() {
      return connected;
    }

    public synchronized KeeperState state() {
      return state;
    }

    public synchronized void waitForConnected(long timeout)
        throws InterruptedException, TimeoutException {
      long expire = System.currentTimeMillis() + timeout;
      long left = timeout;
      while (!connected && left > 0) {
        wait(left);
        left = expire - System.currentTimeMillis();
      }
      if (!connected) {
        throw new TimeoutException("Did not connect");
      }
    }

    public synchronized void waitForDisconnected(long timeout)
        throws InterruptedException, TimeoutException {
      long expire = System.currentTimeMillis() + timeout;
      long left = timeout;
      while (connected && left > 0) {
        wait(left);
        left = expire - System.currentTimeMillis();
      }
      if (connected) {
        throw new TimeoutException("Did not disconnect");
      }
    }
  }

  public Stat exists(final String path, Watcher watcher)
      throws KeeperException, InterruptedException {
    return keeper.exists(path, watcher);
  }

  public String create(final String path, byte data[], List<ACL> acl,
      CreateMode createMode) throws KeeperException, InterruptedException {
    return keeper.create(path, data, acl, createMode);
  }

  public List<String> getChildren(final String path, Watcher watcher)
      throws KeeperException, InterruptedException {
    return keeper.getChildren(path, watcher);
  }

  public byte[] getData(final String path, Watcher watcher, Stat stat)
      throws KeeperException, InterruptedException {
    return keeper.getData(path, watcher, stat);
  }

  public Stat setData(final String path, byte data[], int version)
      throws KeeperException, InterruptedException {
    return keeper.setData(path, data, version);
  }
  
  /**
   * 
   * @param path
   * @param data
   * @param watcher
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String create(String path, byte[] data, CreateMode createMode,
      Watcher watcher) throws KeeperException, InterruptedException {

    String zkPath = keeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
    // nocommit : race issue on keeper switch
    exists(zkPath, watcher);
    
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
      if (exists == null) {
        CreateMode mode = CreateMode.PERSISTENT;
        if (i == paths.length - 1) {
          mode = createMode;
          bytes = data;
        }
        create(currentPath, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        // set new watch
        exists(currentPath, watcher);
      } else if (i == paths.length - 1) {
        // nocommit: version ?
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
  public void write(String path, byte[] data) throws KeeperException,
      InterruptedException {

    makePath(path);

    Object exists = exists(path, null);
    if (exists != null) {
      setData(path, data, -1);
    } else {
      create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
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
  public void write(String path, File file) throws IOException,
      KeeperException, InterruptedException {
    if(log.isInfoEnabled()) {
      log.info("Write to ZooKeepeer " + file.getAbsolutePath() + " to " + path);
    }

    String data = FileUtils.readFileToString(file);
    write(path, data.getBytes());
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
}
