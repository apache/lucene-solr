package org.apache.solr.cloud;

import java.io.File;

import junit.framework.TestCase;

public class ZkSolrClientTest extends TestCase {
  protected File tmpDir = new File(System.getProperty("java.io.tmpdir")
      + System.getProperty("file.separator") + getClass().getName() + "-"
      + System.currentTimeMillis());
  
  public void testBasic() throws Exception {
    String zkDir = tmpDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    ZkTestServer server = null;
    SolrZkClient zkClient = null;
    try {
      server = new ZkTestServer(zkDir);
      server.run();

      AbstractZkTestCase.makeSolrZkNode();

      zkClient = new SolrZkClient(AbstractZkTestCase.ZOO_KEEPER_ADDRESS,
          AbstractZkTestCase.TIMEOUT);
      String shardsPath = "/collections/collection1/shards";
      zkClient.makePath(shardsPath);

      zkClient.makePath("collections/collection1/config=collection1");
      
      zkClient.dissconect();
      
      zkClient.makePath("collections/collection1/config=collection2");

    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
      if (server != null) {
        server.shutdown();
      }
    }
  }
}
