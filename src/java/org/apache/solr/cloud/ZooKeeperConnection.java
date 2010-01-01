package org.apache.solr.cloud;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperConnection {
  private static final int CONNECT_TIMEOUT = 5000;

  protected static final Logger log = LoggerFactory
      .getLogger(ZooKeeperConnection.class);

  private String zooKeeperHost;

  private int zkClientTimeout;
  
  boolean connected = false;
  
  private CountdownWatcher cw = new CountdownWatcher("ZooKeeperConnection Watcher");

  private volatile ZooKeeper keeper;

  public ZooKeeperConnection(String zooKeeperHost, int zkClientTimeout) {
    this.zooKeeperHost = zooKeeperHost;
    this.zkClientTimeout = zkClientTimeout;
  }

  public void connect() throws InterruptedException, TimeoutException,
      IOException {
    // nocommit
    log.info("Connecting to ZooKeeper...");
    
    keeper = new ZooKeeper(zooKeeperHost, zkClientTimeout, cw);
    cw.waitForConnected(CONNECT_TIMEOUT);

    // nocommit
    log.info("Connected");
  }
  
  public boolean connected() {
    return keeper.getState() == ZooKeeper.States.CONNECTED;
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

  public void close() throws InterruptedException {
    keeper.close();
  }
}
