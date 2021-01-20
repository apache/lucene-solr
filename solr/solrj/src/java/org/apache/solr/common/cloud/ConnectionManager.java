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

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.common.SolrException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.Watcher.Event.KeeperState.AuthFailed;
import static org.apache.zookeeper.Watcher.Event.KeeperState.Disconnected;
import static org.apache.zookeeper.Watcher.Event.KeeperState.Expired;

public class ConnectionManager implements Watcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String name;

  private volatile boolean connected = false;

  private final ZkClientConnectionStrategy connectionStrategy;

  private final String zkServerAddress;

  private final SolrZkClient client;

  private final OnReconnect onReconnect;
  private final BeforeReconnect beforeReconnect;

  private volatile boolean isClosed = false;

  // Track the likely expired state
  private static class LikelyExpiredState {
    private static LikelyExpiredState NOT_EXPIRED = new LikelyExpiredState(StateType.NOT_EXPIRED, 0);
    private static LikelyExpiredState EXPIRED = new LikelyExpiredState(StateType.EXPIRED, 0);

    public enum StateType {
      NOT_EXPIRED,    // definitely not expired
      EXPIRED,        // definitely expired
      TRACKING_TIME   // not sure, tracking time of last disconnect
    }

    private StateType stateType;
    private long lastDisconnectTime;
    public LikelyExpiredState(StateType stateType, long lastDisconnectTime) {
      this.stateType = stateType;
      this.lastDisconnectTime = lastDisconnectTime;
    }

    public boolean isLikelyExpired(long timeToExpire) {
      return stateType == StateType.EXPIRED
        || ( stateType == StateType.TRACKING_TIME && (System.nanoTime() - lastDisconnectTime >  TimeUnit.NANOSECONDS.convert(timeToExpire, TimeUnit.MILLISECONDS)));
    }
  }

  public static abstract class IsClosed {
    public abstract boolean isClosed();
  }

  private volatile LikelyExpiredState likelyExpiredState = LikelyExpiredState.EXPIRED;

  private IsClosed isClosedCheck;

  public ConnectionManager(String name, SolrZkClient client, String zkServerAddress, ZkClientConnectionStrategy strat, OnReconnect onConnect, BeforeReconnect beforeReconnect, IsClosed isClosed) {
    this.name = name;
    this.client = client;
    this.connectionStrategy = strat;
    this.zkServerAddress = zkServerAddress;
    this.onReconnect = onConnect;
    this.beforeReconnect = beforeReconnect;
    this.isClosedCheck = isClosed;
  }

  private synchronized void connected() {
    connected = true;
    likelyExpiredState = LikelyExpiredState.NOT_EXPIRED;
    notifyAll();
  }

  private synchronized void disconnected() {
    connected = false;
    // record the time we expired unless we are already likely expired
    if (!likelyExpiredState.isLikelyExpired(0)) {
      likelyExpiredState = new LikelyExpiredState(LikelyExpiredState.StateType.TRACKING_TIME, System.nanoTime());
    }
    notifyAll();
  }

  @Override
  public void process(WatchedEvent event) {
    if (event.getState() == AuthFailed || event.getState() == Disconnected || event.getState() == Expired) {
      log.warn("Watcher {} name: {} got event {} path: {} type: {}", this, name, event, event.getPath(), event.getType());
    } else {
      if (log.isDebugEnabled()) {
        log.debug("Watcher {} name: {} got event {} path: {} type: {}", this, name, event, event.getPath(), event.getType());
      }
    }

    if (isClosed()) {
      log.debug("Client->ZooKeeper status change trigger but we are already closed");
      return;
    }

    KeeperState state = event.getState();

    if (state == KeeperState.SyncConnected) {
      log.info("zkClient has connected");
      connected();
      connectionStrategy.connected();
    } else if (state == Expired) {
      if (isClosed()) {
        return;
      }
      // we don't call disconnected here, because we know we are expired
      connected = false;
      likelyExpiredState = LikelyExpiredState.EXPIRED;

      log.warn("Our previous ZooKeeper session was expired. Attempting to reconnect to recover relationship with ZooKeeper...");

      if (beforeReconnect != null) {
        try {
          beforeReconnect.command();
        } catch (Exception e) {
          log.warn("Exception running beforeReconnect command", e);
        }
      }

      do {
        // This loop will break if a valid connection is made. If a connection is not made then it will repeat and
        // try again to create a new connection.
        try {
          connectionStrategy.reconnect(zkServerAddress,
              client.getZkClientTimeout(), this,
              new ZkClientConnectionStrategy.ZkUpdate() {
                @Override
                public void update(SolrZooKeeper keeper) {
                  try {
                    waitForConnected(Long.MAX_VALUE);

                    try {
                      client.updateKeeper(keeper);
                    } catch (InterruptedException e) {
                      closeKeeper(keeper);
                      Thread.currentThread().interrupt();
                      // we must have been asked to stop
                      throw new RuntimeException(e);
                    }

                    if (onReconnect != null) {
                      onReconnect.command();
                    }

                  } catch (Exception e1) {
                    // if there was a problem creating the new SolrZooKeeper
                    // or if we cannot run our reconnect command, close the keeper
                    // our retry loop will try to create one again
                    closeKeeper(keeper);
                    throw new RuntimeException(e1);
                  }
                }
              });

          break;

        } catch (Exception e) {
          SolrException.log(log, "", e);
          log.info("Could not connect due to error, sleeping for 1s and trying again");
          waitSleep(1000);
        }

      } while (!isClosed());
      log.info("zkClient Connected: {}", connected);
    } else if (state == KeeperState.Disconnected) {
      log.warn("zkClient has disconnected");
      disconnected();
      connectionStrategy.disconnected();
    } else if (state == KeeperState.AuthFailed) {
      log.warn("zkClient received AuthFailed");
    }
  }

  public synchronized boolean isConnectedAndNotClosed() {
    return !isClosed() && connected;
  }

  public synchronized boolean isConnected() {
    return connected;
  }

  // we use a volatile rather than sync
  // to avoid possible deadlock on shutdown
  public void close() {
    this.isClosed = true;
    this.likelyExpiredState = LikelyExpiredState.EXPIRED;
  }

  private boolean isClosed() {
    return isClosed || isClosedCheck.isClosed();
  }

  public boolean isLikelyExpired() {
    return isClosed() || likelyExpiredState.isLikelyExpired((long) (client.getZkClientTimeout() * 0.90));
  }

  public synchronized void waitSleep(long waitFor) {
    try {
      wait(waitFor);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public synchronized void waitForConnected(long waitForConnection)
      throws TimeoutException {
    log.info("Waiting for client to connect to ZooKeeper");
    long expire = System.nanoTime() + TimeUnit.NANOSECONDS.convert(waitForConnection, TimeUnit.MILLISECONDS);
    long left = 1;
    while (!connected && left > 0) {
      if (isClosed()) {
        break;
      }
      try {
        wait(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      left = expire - System.nanoTime();
    }
    if (!connected) {
      throw new TimeoutException("Could not connect to ZooKeeper " + zkServerAddress + " within " + waitForConnection + " ms");
    }
    log.info("Client is connected to ZooKeeper");
  }

  public synchronized void waitForDisconnected(long timeout)
      throws InterruptedException, TimeoutException {
    long expire = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS);
    long left = timeout;
    while (connected && left > 0) {
      wait(left);
      left = expire - System.nanoTime();
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
