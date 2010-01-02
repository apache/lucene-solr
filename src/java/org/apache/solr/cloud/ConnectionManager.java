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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
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

  private ZkClientConnectionStrategy connectionStrategy;

  private String zkServerAddress;

  private int zkClientTimeout;

  public ConnectionManager(String name, String zkServerAddress, int zkClientTimeout, ZkClientConnectionStrategy strat) {
    this.name = name;
    this.connectionStrategy = strat;
    this.zkServerAddress = zkServerAddress;
    this.zkClientTimeout = zkClientTimeout;
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
        connectionStrategy.reconnect(zkServerAddress, zkClientTimeout, this);
      } catch (IOException e) {
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

  public synchronized ZooKeeper waitForConnected(long waitForConnection)
      throws InterruptedException, TimeoutException, IOException {
    ZooKeeper keeper = new ZooKeeper(zkServerAddress, zkClientTimeout, this);
    long expire = System.currentTimeMillis() + waitForConnection;
    long left = waitForConnection;
    while (!connected && left > 0) {
      wait(left);
      left = expire - System.currentTimeMillis();
    }
    if (!connected) {
      throw new TimeoutException("Could not connect to ZooKeeper within " + waitForConnection + " ms");
    }
    return keeper;
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
