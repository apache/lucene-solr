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
package org.apache.solr.cloud.hdfs;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.regex.Pattern;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.MiniDFSNNTopology;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.util.HdfsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsTestUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String LOGICAL_HOSTNAME = "ha-nn-uri-%d";

  private static final boolean HA_TESTING_ENABLED = false; // SOLR-XXX

  private static Map<MiniDFSCluster,Timer> timers = new ConcurrentHashMap<>();

  private static FSDataOutputStream badTlogOutStream;

  private static FileSystem badTlogOutStreamFs;

  public static MiniDFSCluster setupClass(String dir) throws Exception {
    return setupClass(dir, true, true);
  }

  public static MiniDFSCluster setupClass(String dir, boolean haTesting) throws Exception {
    return setupClass(dir, haTesting, true);
  }

  /**
   * Checks that commons-lang3 FastDateFormat works with configured locale
   */
  @SuppressForbidden(reason="Call FastDateFormat.format same way Hadoop calls it")
  private static void checkFastDateFormat() {
    try {
      FastDateFormat.getInstance().format(System.currentTimeMillis());
    } catch (ArrayIndexOutOfBoundsException e) {
      LuceneTestCase.assumeNoException("commons-lang3 FastDateFormat doesn't work with " +
          Locale.getDefault().toLanguageTag(), e);
    }
  }

  /**
   * Hadoop fails to generate locale agnostic ids - Checks that generated string matches
   */
  private static void checkGeneratedIdMatches() {
    // This is basically how Namenode generates fsimage ids and checks that the fsimage filename matches
    LuceneTestCase.assumeTrue("Check that generated id matches regex",
        Pattern.matches("(\\d+)", String.format(Locale.getDefault(),"%019d", 0)));
  }

  public static MiniDFSCluster setupClass(String dir, boolean safeModeTesting, boolean haTesting) throws Exception {
    LuceneTestCase.assumeFalse("HDFS tests were disabled by -Dtests.disableHdfs",
      Boolean.parseBoolean(System.getProperty("tests.disableHdfs", "false")));

    checkFastDateFormat();
    checkGeneratedIdMatches();

    if (!HA_TESTING_ENABLED) haTesting = false;

    int dataNodes = Integer.getInteger("tests.hdfs.numdatanodes", 2);

    Configuration conf = new Configuration();
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions.enabled", "false");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hdfs.minidfs.basedir", dir + File.separator + "hdfsBaseDir");
    conf.set("dfs.namenode.name.dir", dir + File.separator + "nameNodeNameDir");
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    System.setProperty("test.build.data", dir + File.separator + "hdfs" + File.separator + "build");
    System.setProperty("test.cache.data", dir + File.separator + "hdfs" + File.separator + "cache");
    System.setProperty("solr.lock.type", DirectoryFactory.LOCK_TYPE_HDFS);

    System.setProperty("solr.hdfs.blockcache.global", Boolean.toString(LuceneTestCase.random().nextBoolean()));

    final MiniDFSCluster dfsCluster;

    if (!haTesting) {
      dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(dataNodes).format(true).build();

      System.setProperty("solr.hdfs.home", getDataDir(dfsCluster, "solr_hdfs_home"));
    } else {
      dfsCluster = new MiniDFSCluster.Builder(conf)
          .nnTopology(MiniDFSNNTopology.simpleHATopology()).numDataNodes(dataNodes)
          .build();

      Configuration haConfig = getClientConfiguration(dfsCluster);

      HdfsUtil.TEST_CONF = haConfig;
      System.setProperty("solr.hdfs.home", getDataDir(dfsCluster, "solr_hdfs_home"));
    }

    dfsCluster.waitActive();

    if (haTesting) dfsCluster.transitionToActive(0);

    int rndMode = LuceneTestCase.random().nextInt(3);
    if (safeModeTesting && rndMode == 1) {
      NameNodeAdapter.enterSafeMode(dfsCluster.getNameNode(), false);

      int rnd = LuceneTestCase.random().nextInt(10000);
      Timer timer = new Timer();
      timers.put(dfsCluster, timer);
      timer.schedule(new TimerTask() {

        @Override
        public void run() {
          NameNodeAdapter.leaveSafeMode(dfsCluster.getNameNode());
        }
      }, rnd);

    } else if (haTesting && rndMode == 2) {
      int rnd = LuceneTestCase.random().nextInt(30000);
      Timer timer = new Timer();
      timers.put(dfsCluster, timer);
      timer.schedule(new TimerTask() {

        @Override
        public void run() {
          // TODO: randomly transition to standby
//          try {
//            dfsCluster.transitionToStandby(0);
//            dfsCluster.transitionToActive(1);
//          } catch (IOException e) {
//            throw new RuntimeException();
//          }

        }
      }, rnd);
    }  else {
      // TODO: we could do much better at testing this
      // force a lease recovery by creating a tlog file and not closing it
      URI uri = dfsCluster.getURI();
      Path hdfsDirPath = new Path(uri.toString() + "/solr/collection1/core_node1/data/tlog/tlog.0000000000000000000");
      // tran log already being created testing
      badTlogOutStreamFs = FileSystem.get(hdfsDirPath.toUri(), conf);
      badTlogOutStream = badTlogOutStreamFs.create(hdfsDirPath);
    }

    SolrTestCaseJ4.useFactory("org.apache.solr.core.HdfsDirectoryFactory");

    return dfsCluster;
  }

  public static Configuration getClientConfiguration(MiniDFSCluster dfsCluster) {
    if (dfsCluster.getNameNodeInfos().length > 1) {
      Configuration conf = new Configuration();
      HATestUtil.setFailoverConfigurations(dfsCluster, conf);
      return conf;
    } else {
      return new Configuration();
    }
  }

  public static void teardownClass(MiniDFSCluster dfsCluster) throws Exception {

    if (badTlogOutStream != null) {
      IOUtils.closeQuietly(badTlogOutStream);
    }

    if (badTlogOutStreamFs != null) {
      IOUtils.closeQuietly(badTlogOutStreamFs);
    }

    SolrTestCaseJ4.resetFactory();

    try {
      if (dfsCluster != null) {
        Timer timer = timers.remove(dfsCluster);
        if (timer != null) {
          timer.cancel();
        }
        try {
          dfsCluster.shutdown();
        } catch (Error e) {
          // Added in SOLR-7134
          // Rarely, this can fail to either a NullPointerException
          // or a class not found exception. The later may fixable
          // by adding test dependencies.
          log.warn("Exception shutting down dfsCluster", e);
        }
      }
    } finally {
      System.clearProperty("solr.lock.type");
      System.clearProperty("test.build.data");
      System.clearProperty("test.cache.data");
      System.clearProperty("solr.hdfs.home");
      System.clearProperty("solr.hdfs.blockcache.global");
    }
  }

  public static String getDataDir(MiniDFSCluster dfsCluster, String dataDir)
      throws IOException {
    if (dataDir == null) {
      return null;
    }
    String dir =  "/"
        + new File(dataDir).toString().replaceAll(":", "_")
            .replaceAll("/", "_").replaceAll(" ", "_");

    return getURI(dfsCluster) + dir;
  }

  public static String getURI(MiniDFSCluster dfsCluster) {
    if (dfsCluster.getNameNodeInfos().length > 1) {
      String logicalName = String.format(Locale.ENGLISH, LOGICAL_HOSTNAME, dfsCluster.getInstanceId()); // NOTE: hdfs uses default locale
      return "hdfs://" + logicalName;
    } else {
      URI uri = dfsCluster.getURI(0);
      return uri.toString() ;
    }
  }

  /**
   * By default in JDK9+, the ForkJoinWorkerThreadFactory does not give SecurityManager permissions
   * to threads that are created. This works around that with a custom thread factory.
   * See SOLR-9515 and HDFS-14251
   * Used in org.apache.hadoop.hdfs.server.datanode.fsdataset.impl.BlockPoolSlice
   */
  public static class HDFSForkJoinThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
      ForkJoinWorkerThread worker = new SecurityManagerWorkerThread(pool);
      worker.setName("solr-hdfs-threadpool-" + worker.getPoolIndex());
      return worker;
    }
  }

  private static class SecurityManagerWorkerThread extends ForkJoinWorkerThread {
    SecurityManagerWorkerThread(ForkJoinPool pool) {
      super(pool);
    }
  }
}
