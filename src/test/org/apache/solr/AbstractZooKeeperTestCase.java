package org.apache.solr;

import java.io.File;

import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.TestHarness;
import org.apache.solr.util.ZooPut;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base test class for ZooKeeper tests.
 */
public abstract class AbstractZooKeeperTestCase extends AbstractSolrTestCase {
  public static final String ZOO_KEEPER_HOST = "localhost:2181/solr";
  protected static Logger log = LoggerFactory.getLogger(AbstractZooKeeperTestCase.class);
  
  class ZKServerMain extends ZooKeeperServerMain {
	  public void shutdown() {
		  super.shutdown();
	  }
  }
  protected ZKServerMain zkServer = new ZKServerMain();

  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());

  public AbstractZooKeeperTestCase() {

  }

  public String getSchemaFile() {
    return "schema.xml";
  }

  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }

  public void setUp() throws Exception {
    // we don't call super.setUp
    System.setProperty("zkHost", ZOO_KEEPER_HOST);
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
          log.info("ZOOKEEPER EXIT");
        } catch (Throwable e) {          
          e.printStackTrace();
          throw new RuntimeException(e);
        }
      }
    };
    
    zooThread.setDaemon(true);
    zooThread.start();
    Thread.sleep(500); // pause for ZooKeeper to start

    buildZooKeeper(getSolrConfigFile(), getSchemaFile());

    log.info("####SETUP_START " + getName());
    dataDir = tmpDir;
    dataDir.mkdirs();

    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");

    CoreContainer.Initializer init = new CoreContainer.Initializer() {
      {
        this.dataDir = AbstractZooKeeperTestCase.this.dataDir.getAbsolutePath();
      }
    };

    h = new TestHarness("", init);
    lrf = h.getRequestFactory("standard", 0, 20, "version", "2.2");

    log.info("####SETUP_END " + getName());
  }

  public static void buildZooKeeper(String config, String schema) throws Exception {
    ZooPut zooPut = new ZooPut(ZOO_KEEPER_HOST.substring(0, ZOO_KEEPER_HOST.indexOf('/')));
    zooPut.makePath("/solr");
    zooPut.close();
    
    zooPut = new ZooPut(ZOO_KEEPER_HOST);
    
    zooPut.makePath("/collections/collection1/config=collection1");
    
    putConfig(zooPut, config);
    putConfig(zooPut, schema);
    putConfig(zooPut, "stopwords.txt");
    putConfig(zooPut, "protwords.txt");
    putConfig(zooPut, "mapping-ISOLatin1Accent.txt");
    putConfig(zooPut, "old_synonyms.txt");
    zooPut.close();
  }
  
  private static void putConfig(ZooPut zooPut, String name) throws Exception {
    zooPut.putFile("/configs/collection1/" + name, new File("solr" + File.separator
        + "conf" + File.separator + name));
  }

  public void tearDown() throws Exception {
	zkServer.shutdown();
    super.tearDown();
  }
}
