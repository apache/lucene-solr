package org.apache.solr.cloud;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.CoreDescriptor;
import org.junit.BeforeClass;
import org.junit.Test;

public class OverseerTest extends SolrTestCaseJ4 {

  static final int TIMEOUT = 10000;
  private static final boolean DEBUG = false;

  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  @Test
  public void testShardAssignment() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);

    ZkController zkController = null;
    SolrZkClient zkClient = null;
    try {
      server.run();
      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);

      zkController = new ZkController(server.getZkAddress(), TIMEOUT, 10000,
          "localhost", "8983", "solr", 3, new CurrentCoreDescriptorProvider() {

            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });

      System.setProperty("bootstrap_confdir", getFile("solr/conf")
          .getAbsolutePath());

      CloudDescriptor collection1Desc = new CloudDescriptor();
      collection1Desc.setCollectionName("collection1");

      CloudDescriptor collection2Desc = new CloudDescriptor();
      collection2Desc.setCollectionName("collection2");

      //create collection specs
      Map<String,String> props = new HashMap<String,String>();
   
      props.put("num_shards", "3");
      ZkNodeProps zkProps = new ZkNodeProps(props);
      zkClient.setData("/collections/collection1", zkProps.store());
      props.put("num_shards", "1");
      zkProps = new ZkNodeProps(props);
      zkClient.setData("/collections/collection2", zkProps.store());
      ZkNodeProps z = new ZkNodeProps(props);
      
      CoreDescriptor desc = new CoreDescriptor(null, "core1", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard1 = zkController.register("core1", desc);
      collection1Desc.setShardId(null);
      desc = new CoreDescriptor(null, "core2", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard2 = zkController.register("core2", desc);
      collection1Desc.setShardId(null);
      desc = new CoreDescriptor(null, "core3", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard3 = zkController.register("core3", desc);
      collection1Desc.setShardId(null);
      desc = new CoreDescriptor(null, "core4", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard4 = zkController.register("core4", desc);
      collection1Desc.setShardId(null);
      desc = new CoreDescriptor(null, "core5", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard5 = zkController.register("core5", desc);
      collection1Desc.setShardId(null);
      desc = new CoreDescriptor(null, "core6", "");
      desc.setCloudDescriptor(collection1Desc);
      String shard6 = zkController.register("core6", desc);
      collection1Desc.setShardId(null);

      assertEquals("shard1", shard1);
      assertEquals("shard2", shard2);
      assertEquals("shard3", shard3);
      assertEquals("shard1", shard4);
      assertEquals("shard2", shard5);
      assertEquals("shard3", shard6);

    } finally {
      if (DEBUG) {
        if (zkController != null) {
          zkController.printLayoutToStdOut();
        }
      }
      if (zkClient != null) {
        zkClient.close();
      }
      if (zkController != null) {
        zkController.close();
      }
      server.shutdown();
    }

  }
}
