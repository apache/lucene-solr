package org.apache.solr.cloud;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;

public class ZooKeeperTestServer {
  private static final int PORT = 2181;

  protected ZKServerMain zkServer = new ZKServerMain();

  private String zkDir;

  class ZKServerMain extends ZooKeeperServerMain {
    public void shutdown() {
      super.shutdown();
    }
  }

  public ZooKeeperTestServer(String zkDir) {
    this.zkDir = zkDir;
  }

  public void run() throws InterruptedException {
    // we don't call super.setUp
    Thread zooThread = new Thread() {
      @Override
      public void run() {
        ServerConfig config = new ServerConfig() {
          {
            this.clientPort = PORT;
            this.dataDir = zkDir;;
            this.dataLogDir = zkDir;
          }
        };

        try {
          zkServer.runFromConfig(config);
        } catch (Throwable e) {
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    };

    zooThread.setDaemon(true);
    zooThread.start();
    Thread.sleep(500); // pause for ZooKeeper to start
  }

  public void shutdown() {
    zkServer.shutdown();
  }
}
