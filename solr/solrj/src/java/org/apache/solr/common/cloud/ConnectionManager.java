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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
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
  private final int zkSessionTimeout;

  private volatile boolean connected = false;

  private final String zkServerAddress;

  private final SolrZkClient client;

  private volatile SolrZooKeeper keeper;

  private volatile OnReconnect onReconnect;
  private volatile BeforeReconnect beforeReconnect;

  private volatile boolean isClosed = false;

  private final ReentrantLock keeperLock = new ReentrantLock(true);

  private volatile CountDownLatch connectedLatch = new CountDownLatch(1);
  private volatile CountDownLatch disconnectedLatch = new CountDownLatch(1);
  private volatile DisconnectListener disconnectListener;
  private volatile int lastConnectedState = 0;

  private volatile ZkCredentialsProvider zkCredentialsToAddAutomatically;
  private volatile boolean zkCredentialsToAddAutomaticallyUsed;

  private ReentrantLock ourLock = new ReentrantLock(true);
  private volatile boolean expired;


  //  private Set<ZkClientConnectionStrategy.DisconnectedListener> disconnectedListeners = ConcurrentHashMap.newKeySet();
//  private Set<ZkClientConnectionStrategy.ConnectedListener> connectedListeners = ConcurrentHashMap.newKeySet();

  public void setOnReconnect(OnReconnect onReconnect) {
    this.onReconnect = onReconnect;
  }

  public void setBeforeReconnect(BeforeReconnect beforeReconnect) {
    this.beforeReconnect = beforeReconnect;
  }

  public ZooKeeper getKeeper() {
    if (isClosed) {
      throw new AlreadyClosedException(this + " SolrZkClient is not currently connected state=" + keeper.getState());
    }

    SolrZooKeeper rKeeper = keeper;
    return rKeeper;
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

  public ConnectionManager(String name, SolrZkClient client, String zkServerAddress, int zkSessionTimeout, OnReconnect onConnect, BeforeReconnect beforeReconnect) {
    this.name = name;
    this.client = client;
    this.zkServerAddress = zkServerAddress;
    this.onReconnect = onConnect;
    this.beforeReconnect = beforeReconnect;
    this.zkSessionTimeout = zkSessionTimeout;
    assert ObjectReleaseTracker.track(this);
  }

  private void connected() {
    ourLock.lock();
    try {
      connected = true;
      disconnectedLatch = new CountDownLatch(1);
      lastConnectedState = 1;
      if (log.isDebugEnabled()) log.debug("Connected, notify any wait");
      connectedLatch.countDown();
      //    for (ConnectedListener listener : connectedListeners) {
      //      try {
      //        listener.connected();
      //      } catch (Exception e) {
      //        ParWork.propegateInterrupt(e);
      //      }
      //    }
    } finally {
      ourLock.unlock();
    }

  }

  private void disconnected() {
    ourLock.lock();
    try {
      connected = false;
      // record the time we expired unless we are already likely expired
      disconnectedLatch.countDown();
      connectedLatch = new CountDownLatch(1);

      try {
        if (disconnectListener != null) {
          disconnectListener.disconnected();
        }
      } catch (NullPointerException e) {
        // okay
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        log.warn("Exception firing disonnectListener");
      }
      lastConnectedState = 0;
    } finally {
      ourLock.unlock();
    }
  }

  public void start() throws IOException {
    updatezk();
  }

  private void updatezk() throws IOException {
    keeperLock.lock();
    try {
      if (keeper != null) {
        ParWork.close(keeper);
        if (beforeReconnect != null && expired) {
          try {
            beforeReconnect.command();
          } catch (Exception e) {
            log.error("Exception running beforeReconnect command", e);
            if (e instanceof InterruptedException || e instanceof AlreadyClosedException) {
              return;
            }
          }
        }
      }
      SolrZooKeeper zk = createSolrZooKeeper(zkServerAddress, zkSessionTimeout, this);
      keeper = zk;
    } finally {
      keeperLock.unlock();
    }
  }

  @Override
  public void process(WatchedEvent event) {
    ourLock.lock();
    try {
      if (event.getState() == AuthFailed || event.getState() == Disconnected || event.getState() == Expired) {
        log.warn("Watcher {} name: {} got event {} path: {} type: {}", this, name, event, event.getPath(), event.getType());
      } else {
        if (log.isInfoEnabled()) {
          log.info("Watcher {} name: {} got event {} path: {} type: {}", this, name, event, event.getPath(), event.getType());
        }
      }


      KeeperState state = event.getState();

      if (state == KeeperState.SyncConnected) {
        expired = false;
        connected = true;
        if (isClosed()) return;
        if (log.isDebugEnabled()) log.debug("zkClient has connected");
        // nocommit - maybe use root shared
        client.zkConnManagerCallbackExecutor.execute(() -> {
          connected();
        });

      } else if (state == Expired) {
        connected = false;
        expired = true;

        log.warn("Our previous ZooKeeper session was expired. Attempting to reconnect to recover relationship with ZooKeeper...");

        client.zkConnManagerCallbackExecutor.execute(() -> {
          disconnected();
        });
        reconnect();
      } else if (state == KeeperState.Disconnected) {
        log.info("zkClient has disconnected");
//        client.zkConnManagerCallbackExecutor.execute(() -> {
//
//        });
      } else if (state == KeeperState.AuthFailed) {
        log.warn("zkClient received AuthFailed");
      } else if (state == KeeperState.Closed) {
        log.info("zkClient session is closed");
        if (!isClosed) {
          close();
        }
      }
    } finally {
      ourLock.unlock();
    }
  }

  private void reconnect() {
    ActionThrottle throttle = new ActionThrottle("ConnectionManager", 1000);
    do {
      if (isClosed()) return;
      // This loop will break if a valid connection is made. If a connection is not made then it will repeat and
      // try again to create a new connection.
      throttle.minimumWaitBetweenActions();
      throttle.markAttemptingAction();
      log.info("Running reconnect strategy");
      try {
        updatezk();
        try {
          waitForConnected(30000);
          if (onReconnect != null) {
            client.zkConnManagerCallbackExecutor.execute(() -> {
              try {
                onReconnect.command();
              } catch (Exception e) {
                SolrException exp = new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
                ParWork.propagateInterrupt("$ZkClientConnectionStrategy.ZkUpdate.update(SolrZooKeeper=" + keeper + ")", e);
                if (e instanceof InterruptedException || e instanceof AlreadyClosedException) {
                  return;
                }
                throw exp;
              }
            });
          }
        } catch (InterruptedException | AlreadyClosedException e) {
          ParWork.propagateInterrupt(e);
          return;
        } catch (Exception e1) {
          log.error("Exception updating zk instance", e1);
          SolrException exp = new SolrException(SolrException.ErrorCode.SERVER_ERROR, e1);
          throw exp;
        }

      } catch (AlreadyClosedException e) {
        log.info("Ran into AlreadyClosedException on reconnect");
        return;
      } catch (Exception e) {
        SolrException.log(log, "", e);
        log.info("Could not connect due to error, trying again ..");
        ParWork.close(keeper);
        break;
      }

    } while (!isClosed());

    log.info("zkClient Connected: {}", connected);
  }

  public boolean isConnectedAndNotClosed() {
    return !isClosed() && connected;
  }

  public boolean isConnected() {
    SolrZooKeeper fkeeper = keeper;
    return fkeeper != null & fkeeper.getState().isConnected();
  }

  public void close() {
    if (log.isDebugEnabled()) log.debug("Close called on ZK ConnectionManager");
    this.isClosed = true;

    client.zkCallbackExecutor.shutdown();

    ExecutorUtil.awaitTermination(client.zkCallbackExecutor);
    SolrZooKeeper fkeeper = keeper;
    if (fkeeper != null) {
      fkeeper.register(new NullWatcher());
      fkeeper.close();
    }
    client.zkConnManagerCallbackExecutor.shutdown();
    ExecutorUtil.awaitTermination(client.zkConnManagerCallbackExecutor);

    assert ObjectReleaseTracker.release(this);
  }

  private boolean isClosed() {
    return client.isClosed() || isClosed;
  }

  public void waitForConnected(long waitForConnection)
          throws TimeoutException, InterruptedException {
    if (log.isDebugEnabled()) log.debug("Waiting for client to connect to ZooKeeper");
    SolrZooKeeper fkeeper = keeper;
    if (fkeeper != null && fkeeper.getState().isConnected()) return;
    TimeOut timeout = new TimeOut(waitForConnection, TimeUnit.MILLISECONDS, TimeSource.NANO_TIME);
    while (!timeout.hasTimedOut()  && !isClosed()) {
      fkeeper = keeper;
      if (fkeeper != null && fkeeper.getState().isConnected()) return;
      boolean success = connectedLatch.await(500, TimeUnit.MILLISECONDS);
      if (success) return;
      fkeeper = keeper;
      if (fkeeper != null && fkeeper.getState().isConnected()) return;
    }
    if (isClosed()) {
      throw new AlreadyClosedException();
    }
    if (timeout.hasTimedOut()) {
      throw new TimeoutException("Timeout waiting to connect to ZooKeeper "
              + zkServerAddress + " " + waitForConnection + "ms");
    }

    if (log.isDebugEnabled()) log.debug("Client is connected to ZooKeeper");
  }

//  public void waitForDisconnected(long waitForDisconnected)
//      throws InterruptedException, TimeoutException {
//
//    if (!client.isConnected() || lastConnectedState != 1) return;
//    boolean success = disconnectedLatch.await(1000, TimeUnit.MILLISECONDS);
//    if (!success) {
//      throw new TimeoutException("Timeout waiting to disconnect from ZooKeeper");
//    }
//
//  }

  public interface DisconnectListener {
    void disconnected();
  }

  public void setDisconnectListener(DisconnectListener dl) {
    this.disconnectListener = dl;
  }

  protected SolrZooKeeper createSolrZooKeeper(final String serverAddress, final int zkSessionTimeout,
                                              final Watcher watcher) throws IOException {
    SolrZooKeeper result = new SolrZooKeeper(serverAddress, zkSessionTimeout, watcher);

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

  private static class NullWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {

    }
  }
}
