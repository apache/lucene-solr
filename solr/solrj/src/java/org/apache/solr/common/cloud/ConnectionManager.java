package org.apache.solr.common.cloud;

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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.apache.solr.common.SolrException;
import org.apache.zookeeper.SolrZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConnectionManager implements Watcher {
  protected static final Logger log = LoggerFactory
      .getLogger(ConnectionManager.class);

  private final String name;
  private CountDownLatch clientConnected;
  private KeeperState state;
  private boolean connected;

  private final ZkClientConnectionStrategy connectionStrategy;

  private String zkServerAddress;

  private int zkClientTimeout;

  private SolrZkClient client;

  private OnReconnect onReconnect;

  private volatile boolean isClosed = false;

  public ConnectionManager(String name, SolrZkClient client, String zkServerAddress, int zkClientTimeout, ZkClientConnectionStrategy strat, OnReconnect onConnect) {
    this.name = name;
    this.client = client;
    this.connectionStrategy = strat;
    this.zkServerAddress = zkServerAddress;
    this.zkClientTimeout = zkClientTimeout;
    this.onReconnect = onConnect;
    reset();
  }

  private synchronized void reset() {
    clientConnected = new CountDownLatch(1);
    state = KeeperState.Disconnected;
    connected = false;
  }

  public synchronized void process(WatchedEvent event) {
    if (log.isInfoEnabled()) {
      log.info("Watcher " + this + " name:" + name + " got event " + event
          + " path:" + event.getPath() + " type:" + event.getType());
    }
    
    if (isClosed) {
      return;
    }

    state = event.getState();
    if (state == KeeperState.SyncConnected) {
      connected = true;
      clientConnected.countDown();
    } else if (state == KeeperState.Expired) {
      connected = false;
      log.info("Attempting to reconnect to recover relationship with ZooKeeper...");

      try {
        connectionStrategy.reconnect(zkServerAddress, zkClientTimeout, this,
            new ZkClientConnectionStrategy.ZkUpdate() {
              @Override
              public void update(SolrZooKeeper keeper) {
                // if keeper does not replace oldKeeper we must be sure to close it
                synchronized (connectionStrategy) {
                  try {
                    waitForConnected(SolrZkClient.DEFAULT_CLIENT_CONNECT_TIMEOUT);
                  } catch (InterruptedException e1) {
                    closeKeeper(keeper);
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Giving up on connecting - we were interrupted", e1);
                  } catch (Exception e1) {
                    closeKeeper(keeper);
                    throw new RuntimeException(e1);
                  }
                  
                  try {
                    client.updateKeeper(keeper);
                  } catch (InterruptedException e) {
                    closeKeeper(keeper);
                    Thread.currentThread().interrupt();
                    // we must have been asked to stop
                    throw new RuntimeException(e);
                  } catch(Throwable t) {
                    closeKeeper(keeper);
                    throw new RuntimeException(t);
                  }
      
                  if (onReconnect != null) {
                    onReconnect.command();
                  }
                  synchronized (ConnectionManager.this) {
                    ConnectionManager.this.connected = true;
                  }
                }
                
              }

            });
      } catch (Exception e) {
        SolrException.log(log, "", e);
      }
      log.info("Connected:" + connected);
    } else if (state == KeeperState.Disconnected) {
      connected = false;
    } else {
      connected = false;
    }
    notifyAll();
  }

  public synchronized boolean isConnected() {
    return !isClosed && connected;
  }
  
  // we use a volatile rather than sync
  // to avoid deadlock on shutdown
  public void close() {
    this.isClosed = true;
  }

  public synchronized KeeperState state() {
    return state;
  }

  public synchronized void waitForConnected(long waitForConnection)
      throws InterruptedException, TimeoutException {
    long expire = System.currentTimeMillis() + waitForConnection;
    long left = 1;
    while (!connected && left > 0) {
      if (isClosed) {
        break;
      }
      wait(500);
      left = expire - System.currentTimeMillis();
    }
    if (!connected) {
      throw new TimeoutException("Could not connect to ZooKeeper " + zkServerAddress + " within " + waitForConnection + " ms");
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

  private void closeKeeper(SolrZooKeeper keeper) {
    try {
      keeper.close();
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    }
  }
}
