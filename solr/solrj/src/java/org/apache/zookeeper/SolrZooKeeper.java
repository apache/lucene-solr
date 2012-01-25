package org.apache.zookeeper;

import java.io.IOException;
import java.nio.channels.SocketChannel;

// we use this class to expose nasty stuff for tests
public class SolrZooKeeper extends ZooKeeper {

  public SolrZooKeeper(String connectString, int sessionTimeout,
      Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
  }
  
  public ClientCnxn getConnection() {
    return cnxn;
  }
  
  /**
   * Cause this ZooKeeper object to stop receiving from the ZooKeeperServer
   * for the given number of milliseconds.
   * @param ms the number of milliseconds to pause.
   */
  public void pauseCnxn(final long ms) {
    new Thread() {
      public void run() {
        synchronized (cnxn) {
          try {
            try {
              ((SocketChannel) cnxn.sendThread.sockKey.channel()).socket()
                  .close();
            } catch (Exception e) {

            }
            Thread.sleep(ms);
          } catch (InterruptedException e) {}
        }
      }
    }.start();
  }

}
