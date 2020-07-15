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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrIgnoredThreadsFilter;
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
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
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
    try {
      HdfsTestUtil.teardownClass(dfsCluster);
    } finally {
      dfsCluster = null;
      System.clearProperty(HdfsDirectoryFactory.HDFS_HOME);
      System.clearProperty(HdfsDirectoryFactory.CONFIG_DIRECTORY);
      System.clearProperty(HdfsDirectoryFactory.BLOCKCACHE_ENABLED);
      System.clearProperty(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB);
      System.clearProperty(HdfsDirectoryFactory.LOCALITYMETRICS_ENABLED);
    }
  }

  @Test
  @SuppressWarnings({"try"})
  public void testInitArgsOrSysPropConfig() throws Exception {
    try(HdfsDirectoryFactory hdfsFactory = new HdfsDirectoryFactory()) {
      // test sys prop config
      System.setProperty(HdfsDirectoryFactory.HDFS_HOME, HdfsTestUtil.getURI(dfsCluster) + "/solr1");
      hdfsFactory.init(new NamedList<>());
      String dataHome = hdfsFactory.getDataHome(new MockCoreDescriptor());

      assertTrue(dataHome.endsWith("/solr1/mock/data"));

      System.clearProperty(HdfsDirectoryFactory.HDFS_HOME);

      // test init args config
      NamedList<Object> nl = new NamedList<>();
      nl.add(HdfsDirectoryFactory.HDFS_HOME, HdfsTestUtil.getURI(dfsCluster) + "/solr2");
      hdfsFactory.init(nl);
      dataHome = hdfsFactory.getDataHome(new MockCoreDescriptor());

      assertTrue(dataHome.endsWith("/solr2/mock/data"));

      // test sys prop and init args config - init args wins
      System.setProperty(HdfsDirectoryFactory.HDFS_HOME, HdfsTestUtil.getURI(dfsCluster) + "/solr1");
      hdfsFactory.init(nl);
      dataHome = hdfsFactory.getDataHome(new MockCoreDescriptor());

      assertTrue(dataHome.endsWith("/solr2/mock/data"));

      System.clearProperty(HdfsDirectoryFactory.HDFS_HOME);

      // set conf dir by sys prop
      Path confDir = createTempDir();

      System.setProperty(HdfsDirectoryFactory.CONFIG_DIRECTORY, confDir.toString());

      try (Directory dir = hdfsFactory
          .create(HdfsTestUtil.getURI(dfsCluster) + "/solr", NoLockFactory.INSTANCE, DirContext.DEFAULT)) {
        assertEquals(confDir.toString(), hdfsFactory.getConfDir());
      }

      // check bool and int getConf impls
      nl = new NamedList<>();
      nl.add(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB, 4);
      System.setProperty(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB, "3");
      nl.add(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, true);
      System.setProperty(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, "false");

      hdfsFactory.init(nl);

      assertEquals(4, hdfsFactory.getConfig(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB, 0));
      assertTrue(hdfsFactory.getConfig(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, false));

      nl = new NamedList<>();
      hdfsFactory.init(nl);
      System.setProperty(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, "true");

      assertEquals(3, hdfsFactory.getConfig(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB, 0));
      assertTrue(hdfsFactory.getConfig(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, false));

      System.clearProperty(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB);
      System.clearProperty(HdfsDirectoryFactory.BLOCKCACHE_ENABLED);

      assertEquals(0, hdfsFactory.getConfig(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_MAXMERGESIZEMB, 0));
      assertFalse(hdfsFactory.getConfig(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, false));
    }
  }

  @Test
  public void testCleanupOldIndexDirectories() throws Exception {
    try (HdfsDirectoryFactory hdfsFactory = new HdfsDirectoryFactory()) {
      System.setProperty(HdfsDirectoryFactory.HDFS_HOME, HdfsTestUtil.getURI(dfsCluster) + "/solr1");
      hdfsFactory.init(new NamedList<>());
      String dataHome = hdfsFactory.getDataHome(new MockCoreDescriptor());
      assertTrue(dataHome.endsWith("/solr1/mock/data"));
      System.clearProperty(HdfsDirectoryFactory.HDFS_HOME);

      try(FileSystem hdfs = FileSystem.get(HdfsTestUtil.getClientConfiguration(dfsCluster))) {
        org.apache.hadoop.fs.Path dataHomePath = new org.apache.hadoop.fs.Path(dataHome);
        org.apache.hadoop.fs.Path currentIndexDirPath = new org.apache.hadoop.fs.Path(dataHomePath, "index");
        assertFalse(checkHdfsDirectory(hdfs,currentIndexDirPath));
        hdfs.mkdirs(currentIndexDirPath);
        assertTrue(checkHdfsDirectory(hdfs, currentIndexDirPath));

        String timestamp1 = new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date());
        org.apache.hadoop.fs.Path oldIndexDirPath = new org.apache.hadoop.fs.Path(dataHomePath, "index." + timestamp1);
        assertFalse(checkHdfsDirectory(hdfs,oldIndexDirPath));
        hdfs.mkdirs(oldIndexDirPath);
        assertTrue(checkHdfsDirectory(hdfs, oldIndexDirPath));

        hdfsFactory.cleanupOldIndexDirectories(dataHomePath.toString(), currentIndexDirPath.toString(), false);

        assertTrue(checkHdfsDirectory(hdfs, currentIndexDirPath));
        assertFalse(checkHdfsDirectory(hdfs, oldIndexDirPath));
      }
    }
  }

  private boolean checkHdfsDirectory(FileSystem hdfs, org.apache.hadoop.fs.Path path) throws IOException {
    try {
      return hdfs.getFileStatus(path).isDirectory();
    } catch (FileNotFoundException e) {
      return false;
    }
  }
  
  @Test
  public void testLocalityReporter() throws Exception {
    Random r = random();
    try(HdfsDirectoryFactory factory = new HdfsDirectoryFactory()) {
      SolrMetricManager metricManager = new SolrMetricManager();
      String registry = TestUtil.randomSimpleString(r, 2, 10);
      String scope = TestUtil.randomSimpleString(r, 2, 10);
      Map<String, String> props = new HashMap<>();
      props.put(HdfsDirectoryFactory.HDFS_HOME, HdfsTestUtil.getURI(dfsCluster) + "/solr");
      props.put(HdfsDirectoryFactory.BLOCKCACHE_ENABLED, "false");
      props.put(HdfsDirectoryFactory.NRTCACHINGDIRECTORY_ENABLE, "false");
      props.put(HdfsDirectoryFactory.LOCALITYMETRICS_ENABLED, "true");
      factory.init(new NamedList<>(props));
      factory.initializeMetrics(metricManager, registry, "foo", scope);

      // get the metrics map for the locality bean
      MetricsMap metrics = (MetricsMap) ((SolrMetricManager.GaugeWrapper) metricManager.registry(registry).getMetrics().get("OTHER." + scope + ".hdfsLocality")).getGauge();
      // We haven't done anything, so there should be no data
      Map<String, Object> statistics = metrics.getValue();
      assertEquals("Saw bytes that were not written: " + statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_TOTAL), 0L,
          statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_TOTAL));
      assertEquals(
          "Counted bytes as local when none written: " + statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_RATIO), 0,
          statistics.get(HdfsLocalityReporter.LOCALITY_BYTES_RATIO));

      // create a directory and a file
      String path = HdfsTestUtil.getURI(dfsCluster) + "/solr3/";
      try (Directory dir = factory.create(path, NoLockFactory.INSTANCE, DirContext.DEFAULT)) {
        try (IndexOutput writer = dir.createOutput("output", null)) {
          writer.writeLong(42L);
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
      }
    }
  }

  @Test
  public void testIsAbsolute() throws Exception {
    try(HdfsDirectoryFactory hdfsFactory = new HdfsDirectoryFactory()) {
      String relativePath = Strings.repeat(
          RandomStrings.randomAsciiAlphanumOfLength(random(), random().nextInt(10) + 1) + '/',
          random().nextInt(5) + 1);
      assertFalse(hdfsFactory.isAbsolute(relativePath));
      assertFalse(hdfsFactory.isAbsolute("/" + relativePath));

      for(String rootPrefix : Arrays.asList("file://", "hdfs://", "s3a://", "foo://")) {
        assertTrue(hdfsFactory.isAbsolute(rootPrefix + relativePath));
      }
    }
  }
}
