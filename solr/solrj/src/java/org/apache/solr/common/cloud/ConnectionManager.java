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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.zookeeper.Watcher.Event.KeeperState.AuthFailed;
import static org.apache.zookeeper.Watcher.Event.KeeperState.Disconnected;
import static org.apache.zookeeper.Watcher.Event.KeeperState.Expired;

public class ConnectionManager implements Watcher, Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String name;
  private final int zkTimeout;

  private volatile boolean connected = false;

  private final String zkServerAddress;

  private final SolrZkClient client;

  private volatile SolrZooKeeper keeper;

  private volatile OnReconnect onReconnect;
  private volatile BeforeReconnect beforeReconnect;

  private volatile boolean isClosed = false;

  private volatile CountDownLatch connectedLatch = new CountDownLatch(1);
  private volatile CountDownLatch disconnectedLatch = new CountDownLatch(1);
  private volatile DisconnectListener disconnectListener;
  private volatile int lastConnectedState = 0;

  private volatile ZkCredentialsProvider zkCredentialsToAddAutomatically;
  private volatile boolean zkCredentialsToAddAutomaticallyUsed;

//  private Set<ZkClientConnectionStrategy.DisconnectedListener> disconnectedListeners = ConcurrentHashMap.newKeySet();
//  private Set<ZkClientConnectionStrategy.ConnectedListener> connectedListeners = ConcurrentHashMap.newKeySet();

  public void setOnReconnect(OnReconnect onReconnect) {
    this.onReconnect = onReconnect;
  }

  public void setBeforeReconnect(BeforeReconnect beforeReconnect) {
    this.beforeReconnect = beforeReconnect;
  }

  public ZooKeeper getKeeper() {
    return keeper;
  }

  public void setZkCredentialsToAddAutomatically(ZkCredentialsProvider zkCredentialsToAddAutomatically) {
    this.zkCredentialsToAddAutomatically = zkCredentialsToAddAutomatically;
  }

  public ZkCredentialsProvider getZkCredentialsToAddAutomatically() {
    return this.zkCredentialsToAddAutomatically;
  }

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

  public ConnectionManager(String name, SolrZkClient client, String zkServerAddress, int zkTimeout, OnReconnect onConnect, BeforeReconnect beforeReconnect) {
    this.name = name;
    this.client = client;
    this.zkServerAddress = zkServerAddress;
    this.onReconnect = onConnect;
    this.beforeReconnect = beforeReconnect;
    this.zkTimeout = zkTimeout;
    assert ObjectReleaseTracker.track(this);
  }

  private void connected() {
    if (lastConnectedState == 1) {
      disconnected();
    }

    connected = true;
    likelyExpiredState = LikelyExpiredState.NOT_EXPIRED;
    disconnectedLatch = new CountDownLatch(1);
    lastConnectedState = 1;
    log.info("Connected, notify any wait");
    connectedLatch.countDown();
//    for (ConnectedListener listener : connectedListeners) {
//      try {
//        listener.connected();
//      } catch (Exception e) {
//        ParWork.propegateInterrupt(e);
//      }
//    }

  }

  private void disconnected() {
    connected = false;
    // record the time we expired unless we are already likely expired
    if (!likelyExpiredState.isLikelyExpired(0)) {
      likelyExpiredState = new LikelyExpiredState(LikelyExpiredState.StateType.TRACKING_TIME, System.nanoTime());
    }
    disconnectedLatch.countDown();
    connectedLatch = new CountDownLatch(1);

    try {
      disconnectListener.disconnected();
    } catch (NullPointerException e) {
      // okay
    } catch (Exception e) {
      ParWork.propegateInterrupt(e);
      log.warn("Exception firing disonnectListener");
    }
    lastConnectedState = 0;
  }

  public void start() throws IOException {
    updatezk();
  }

  private synchronized void updatezk() throws IOException {
    if (keeper != null) {
      keeper.close();
    }
    SolrZooKeeper zk = createSolrZooKeeper(zkServerAddress, zkTimeout, this);
    keeper = zk;
  }

  @Override
  public synchronized void process(WatchedEvent event) {
    if (event.getState() == AuthFailed || event.getState() == Disconnected || event.getState() == Expired) {
      log.warn("Watcher {} name: {} got event {} path: {} type: {}", this, name, event, event.getPath(), event.getType());
    } else {
      if (log.isInfoEnabled()) {
        log.info("Watcher {} name: {} got event {} path: {} type: {}", this, name, event, event.getPath(), event.getType());
      }
    }

//    if (isClosed()) {
//      log.debug("Client->ZooKeeper status change trigger but we are already closed");
//      return;
//    }

    KeeperState state = event.getState();

    if (state == KeeperState.SyncConnected) {
      log.info("zkClient has connected");
      connected();
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
        }  catch (Exception e) {
          ParWork.propegateInterrupt("Exception running beforeReconnect command", e);
          if (e instanceof  InterruptedException || e instanceof AlreadyClosedException) {
            return;
          }
        }
      }

      if (keeper != null) {
        // if there was a problem creating the new SolrZooKeeper
        // or if we cannot run our reconnect command, close the keeper
        // our retry loop will try to create one again
        try {
          keeper. close();
        } catch (Exception e) {
          ParWork.propegateInterrupt("Exception closing keeper after hitting exception", e);
          if (e instanceof  InterruptedException || e instanceof AlreadyClosedException) {
            return;
          }
        }
      }

      do {
        // This loop will break if a valid connection is made. If a connection is not made then it will repeat and
        // try again to create a new connection.
        log.info("Running reconnect strategy");
        try {
          updatezk();
          try {
            waitForConnected(1000);

            if (onReconnect != null) {
              try {
                onReconnect.command();
              } catch (Exception e) {
                SolrException exp = new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
                ParWork.propegateInterrupt("$ZkClientConnectionStrategy.ZkUpdate.update(SolrZooKeeper=" + keeper + ")", e);
                if (e instanceof InterruptedException || e instanceof AlreadyClosedException) {
                  return;
                }
                throw exp;
              }
            }
          } catch (InterruptedException | AlreadyClosedException e) {
            ParWork.propegateInterrupt(e);
            return;
          } catch (Exception e1) {
            log.error("Exception updating zk instance", e1);
            SolrException exp = new SolrException(SolrException.ErrorCode.SERVER_ERROR, e1);
            throw exp;
          }

          if (log.isDebugEnabled()) {
            log.debug("$ZkClientConnectionStrategy.ZkUpdate.update(SolrZooKeeper) - end");
          }
        } catch (AlreadyClosedException e) {
          return;
        } catch (Exception e) {
          SolrException.log(log, "", e);
          log.info("Could not connect due to error, trying again ..");
          ParWork.close(keeper);
          break;
        }

      } while (!isClosed() && !client.isClosed());

      log.info("zkClient Connected: {}", connected);
    } else if (state == KeeperState.Disconnected) {
      log.info("zkClient has disconnected");
      disconnected();
    } else if (state == KeeperState.Closed) {
      log.info("zkClient state == closed");
      //disconnected();
      //connectionStrategy.disconnected();
    } else if (state == KeeperState.AuthFailed) {
      log.warn("zkClient received AuthFailed");
    }
  }

  public boolean isConnectedAndNotClosed() {
    return !isClosed() && connected;
  }

  public boolean isConnected() {
    return connected;
  }

  // we use a volatile rather than sync
  // to avoid possible deadlock on shutdown
  public synchronized void close() {
    log.info("Close called on ZK ConnectionManager");
    this.isClosed = true;
    this.likelyExpiredState = LikelyExpiredState.EXPIRED;

    keeper.close();
//
//    try {
//      waitForDisconnected(5000);
//    } catch (InterruptedException e) {
//      ParWork.propegateInterrupt(e);
//    } catch (TimeoutException e) {
//      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, e);
//    }
    assert ObjectReleaseTracker.release(this);
  }

  private boolean isClosed() {
    return client.isClosed() || isClosed;
  }

  public boolean isLikelyExpired() {
    return isClosed() || likelyExpiredState.isLikelyExpired((long) (client.getZkClientTimeout() * 0.90));
  }

  public void waitForConnected(long waitForConnection)
          throws TimeoutException, InterruptedException {
    log.info("Waiting for client to connect to ZooKeeper");

    if (client.isConnected()) return;
    boolean success = connectedLatch.await(waitForConnection, TimeUnit.MILLISECONDS);
    if (client.isConnected()) return;
    if (!success) {
      throw new TimeoutException("Timeout waiting to connect to ZooKeeper " + zkServerAddress + " " + waitForConnection + "ms");
    }

    log.info("Client is connected to ZooKeeper");
  }

  public void waitForDisconnected(long waitForDisconnected)
      throws InterruptedException, TimeoutException {

    if (!client.isConnected() || lastConnectedState != 1) return;
    boolean success = disconnectedLatch.await(1000, TimeUnit.MILLISECONDS);
    if (!success) {
      throw new TimeoutException("Timeout waiting to disconnect from ZooKeeper");
    }

  }

  public interface DisconnectListener {
    void disconnected();
  }

  public void setDisconnectListener(DisconnectListener dl) {
    this.disconnectListener = dl;

  }

  protected SolrZooKeeper createSolrZooKeeper(final String serverAddress, final int zkClientTimeout,
                                              final Watcher watcher) throws IOException {
    SolrZooKeeper result = new SolrZooKeeper(serverAddress, zkClientTimeout, watcher);

    if (zkCredentialsToAddAutomatically != null) {
      for (ZkCredentialsProvider.ZkCredentials zkCredentials : zkCredentialsToAddAutomatically.getCredentials()) {
        result.addAuthInfo(zkCredentials.getScheme(), zkCredentials.getAuth());
      }
    }

    return result;
  }


  public interface DisconnectedListener {
    void disconnected();
  }

  public interface ConnectedListener {
    void connected();
  }


}
