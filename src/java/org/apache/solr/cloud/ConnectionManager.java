package org.apache.solr.cloud;

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

  private SolrZkClient client;

  public ConnectionManager(String name, SolrZkClient client, String zkServerAddress, int zkClientTimeout, ZkClientConnectionStrategy strat) {
    this.name = name;
    this.client = client;
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
        connectionStrategy.reconnect(zkServerAddress, zkClientTimeout, this, new ZkClientConnectionStrategy.ZkUpdate() {
          @Override
          public void update(ZooKeeper keeper) {
           client.updateKeeper(keeper);
          }
        });
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
