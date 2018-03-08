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
package org.apache.solr.core;

import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.handler.SnapShooter;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.store.hdfs.HdfsLocalityReporter;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.apache.solr.util.MockCoreContainer.MockCoreDescriptor;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class HdfsDirectoryFactoryTest extends SolrTestCaseJ4 {
  
  private static MiniDFSCluster dfsCluster;
  
  @BeforeClass
  public static void setupClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath(), false);
  }
  
  @AfterClass
  public static void teardownClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    System.clearProperty("solr.hdfs.home");
    System.clearProperty(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB);
    dfsCluster = null;
  }

  @Test
  public void testInitArgsOrSysPropConfig() throws Exception {
    
    HdfsDirectoryFactory hdfsFactory = new HdfsDirectoryFactory();
    
    // test sys prop config
    
    System.setProperty("solr.hdfs.home", HdfsTestUtil.getURI(dfsCluster) + "/solr1");
    hdfsFactory.init(new NamedList<>());
    String dataHome = hdfsFactory.getDataHome(new MockCoreDescriptor());

    assertTrue(dataHome.endsWith("/solr1/mock/data"));
    
    System.clearProperty("solr.hdfs.home");
    
    // test init args config
    
    NamedList<Object> nl = new NamedList<>();
    nl.add("solr.hdfs.home", HdfsTestUtil.getURI(dfsCluster) + "/solr2");
    hdfsFactory.init(nl);
    dataHome = hdfsFactory.getDataHome(new MockCoreDescriptor());

    assertTrue(dataHome.endsWith("/solr2/mock/data"));
    
    // test sys prop and init args config - init args wins
    
    System.setProperty("solr.hdfs.home", HdfsTestUtil.getURI(dfsCluster) + "/solr1");
    hdfsFactory.init(nl);
    dataHome = hdfsFactory.getDataHome(new MockCoreDescriptor());

    assertTrue(dataHome.endsWith("/solr2/mock/data"));
    
    System.clearProperty("solr.hdfs.home");
    
    
    // set conf dir by sys prop
    
    Path confDir = createTempDir();
    
    System.setProperty(HdfsDirectoryFactory.CONFIG_DIRECTORY, confDir.toString());
    
    Directory dir = hdfsFactory.create(HdfsTestUtil.getURI(dfsCluster) + "/solr", NoLockFactory.INSTANCE, DirContext.DEFAULT);
    try {
      assertEquals(confDir.toString(), hdfsFactory.getConfDir());
    } finally {
      dir.close();
    }
    
    // check bool and int getConf impls
    nl = new NamedList<>();
    nl.add(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB, 4);
    System.setProperty(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB, "3");
    nl.add(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, true);
    System.setProperty(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, "false");
    
    hdfsFactory.init(nl);
    
    assertEquals(4, hdfsFactory.getConfig(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB, 0));
    assertEquals(true, hdfsFactory.getConfig(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, false));
    
    nl = new NamedList<>();
    hdfsFactory.init(nl);
    System.setProperty(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, "true");
    
    assertEquals(3, hdfsFactory.getConfig(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB, 0));
    assertEquals(true, hdfsFactory.getConfig(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, false));
    
    System.clearProperty(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB);
    System.clearProperty(HdfsDirectoryFactory.BLOCKCACHE_ENABLED);
    
    assertEquals(0, hdfsFactory.getConfig(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB, 0));
    assertEquals(false, hdfsFactory.getConfig(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, false));
    
    hdfsFactory.close();
  }

  @Test
  public void testCleanupOldIndexDirectories() throws Exception {

    try (HdfsDirectoryFactory hdfsFactory = new HdfsDirectoryFactory()) {

      System.setProperty("solr.hdfs.home", HdfsTestUtil.getURI(dfsCluster) + "/solr1");
      hdfsFactory.init(new NamedList<>());
      String dataHome = hdfsFactory.getDataHome(new MockCoreDescriptor());
      assertTrue(dataHome.endsWith("/solr1/mock/data"));
      System.clearProperty("solr.hdfs.home");

      FileSystem hdfs = dfsCluster.getFileSystem();

      org.apache.hadoop.fs.Path dataHomePath = new org.apache.hadoop.fs.Path(dataHome);
      org.apache.hadoop.fs.Path currentIndexDirPath = new org.apache.hadoop.fs.Path(dataHomePath, "index");
      assertTrue(!hdfs.isDirectory(currentIndexDirPath));
      hdfs.mkdirs(currentIndexDirPath);
      assertTrue(hdfs.isDirectory(currentIndexDirPath));

      String timestamp1 = new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date());
      org.apache.hadoop.fs.Path oldIndexDirPath = new org.apache.hadoop.fs.Path(dataHomePath, "index." + timestamp1);
      assertTrue(!hdfs.isDirectory(oldIndexDirPath));
      hdfs.mkdirs(oldIndexDirPath);
      assertTrue(hdfs.isDirectory(oldIndexDirPath));

      hdfsFactory.cleanupOldIndexDirectories(dataHomePath.toString(), currentIndexDirPath.toString(), false);

      assertTrue(hdfs.isDirectory(currentIndexDirPath));
      assertTrue(!hdfs.isDirectory(oldIndexDirPath));
    }
  }
  
  @Test
  public void testLocalityReporter() throws Exception {
    Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
    conf.set("dfs.permissions.enabled", "false");

    Random r = random();
    HdfsDirectoryFactory factory = new HdfsDirectoryFactory();
    SolrMetricManager metricManager = new SolrMetricManager();
    String registry = TestUtil.randomSimpleString(r, 2, 10);
    String scope = TestUtil.randomSimpleString(r,2, 10);
    Map<String,String> props = new HashMap<String,String>();
    props.put(HdfsDirectoryFactory.HDFS_HOME, HdfsTestUtil.getURI(dfsCluster) + "/solr");
    props.put(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, "false");
    props.put(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_ENABLE, "false");
    props.put(HdfsDirectoryFactory.LOCALITYMETRICS_ENABLED, "true");
    factory.init(new NamedList<>(props));
    factory.initializeMetrics(metricManager, registry, scope);

    // get the metrics map for the locality bean
    MetricsMap metrics = (MetricsMap)metricManager.registry(registry).getMetrics().get("OTHER." + scope + ".hdfsLocality");
    // We haven't done anything, so there should be no data
    Map<String,Object> statistics = metrics.getValue();
    assertEquals("Saw bytes that were not written: " + statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_TOTAL), 0l,
        statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_TOTAL));
    assertEquals(
        "Counted bytes as local when none written: " + statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_RATIO), 0,
        statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_RATIO));
    
    // create a directory and a file
    String path = HdfsTestUtil.getURI(dfsCluster) + "/solr3/";
    Directory dir = factory.create(path, NoLockFactory.INSTANCE, DirContext.DEFAULT);
    try(IndexOutput writer = dir.createOutput("output", null)) {
      writer.writeLong(42l);
    }
    
    final long long_bytes = Long.SIZE / Byte.SIZE;
    
    // no locality because hostname not set
    factory.setHost("bogus");
    statistics = metrics.getValue();
    assertEquals("Wrong number of total bytes counted: " + statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_TOTAL),
        long_bytes, statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_TOTAL));
    assertEquals("Wrong number of total blocks counted: " + statistics.get(HdfsLocalityReporter.LOCALITY_BLOCKS_TOTAL),
        1, statistics.get(HdfsLocalityReporter.LOCALITY_BLOCKS_TOTAL));
    assertEquals(
        "Counted block as local when bad hostname set: " + statistics.get(HdfsLocalityReporter.LOCALITY_BLOCKS_LOCAL),
        0, statistics.get(HdfsLocalityReporter.LOCALITY_BLOCKS_LOCAL));
        
    // set hostname and check again
    factory.setHost("127.0.0.1");
    statistics = metrics.getValue();
    assertEquals(
        "Did not count block as local after setting hostname: "
            + statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_LOCAL),
        long_bytes, statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_LOCAL));
        
    factory.close();
  }
}
