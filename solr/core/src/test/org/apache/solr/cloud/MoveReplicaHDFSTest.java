package org.apache.solr.cloud;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 *
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class, // hdfs currently leaks thread(s)
    MoveReplicaHDFSTest.ForkJoinThreadsFilter.class
})
public class MoveReplicaHDFSTest extends MoveReplicaTest {

  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setupClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());

    ZkConfigManager configManager = new ZkConfigManager(zkClient());
    configManager.uploadConfigDir(configset("cloud-hdfs"), "conf1");

    System.setProperty("solr.hdfs.home", HdfsTestUtil.getDataDir(dfsCluster, "data"));
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    cluster.shutdown(); // need to close before the MiniDFSCluster
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
  }


  public static class ForkJoinThreadsFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
      String name = t.getName();
      if (name.startsWith("ForkJoinPool.commonPool")) {
        return true;
      }
      return false;
    }
  }

}
