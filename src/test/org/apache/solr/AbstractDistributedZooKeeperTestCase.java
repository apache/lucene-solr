package org.apache.solr;

import java.io.File;
import java.io.IOException;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.TestHarness;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;

public abstract class AbstractDistributedZooKeeperTestCase extends BaseDistributedSearchTestCase {
  protected ZooKeeperServerMain zkServer = new ZooKeeperServerMain();

  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());
  
  @Override
  public void setUp() throws Exception {
    // we don't call super.setUp
    log.info("####SETUP_START " + getName());
    
    System.setProperty("zkHost", AbstractZooKeeperTestCase.ZOO_KEEPER_HOST);
    Thread zooThread = new Thread() {
      @Override
      public void run() {
        ServerConfig config = new ServerConfig() {
          {
            this.clientPort = 2181;
            this.dataDir = tmpDir.getAbsolutePath() + File.separator
                + "zookeeper/server1/data";
            this.dataLogDir = this.dataDir;
          }
        };

        try {
          zkServer.runFromConfig(config);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

      }
    };
    zooThread.setDaemon(true);
    zooThread.start();
    Thread.sleep(500); // pause for ZooKeeper to start
    AbstractZooKeeperTestCase.buildZooKeeper(getSolrConfigFile(), getSchemaFile());

    dataDir = tmpDir;
    dataDir.mkdirs();

    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    CoreContainer.Initializer init = new CoreContainer.Initializer() {
      {
        this.dataDir = super.dataDir;
      }
    };

    h = new TestHarness("", init);
    lrf = h.getRequestFactory("standard", 0, 20, "version", "2.2");

    log.info("####SETUP_END " + getName());

    testDir = new File(System.getProperty("java.io.tmpdir")
            + System.getProperty("file.separator")
            + getClass().getName() + "-" + System.currentTimeMillis());
    testDir.mkdirs();
    postSetUp();
  }

  public void tearDown() throws Exception {
    super.tearDown();
  }
}
