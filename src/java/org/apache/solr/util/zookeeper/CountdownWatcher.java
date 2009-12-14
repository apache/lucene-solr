/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.util.zookeeper;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountdownWatcher implements Watcher {
  protected static final Logger LOG = LoggerFactory
      .getLogger(CountdownWatcher.class);

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
    LOG.info("Watcher " + name + " got event " + event);

    state = event.getState();
    if (state == KeeperState.SyncConnected) {
      connected = true;
      clientConnected.countDown();
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
