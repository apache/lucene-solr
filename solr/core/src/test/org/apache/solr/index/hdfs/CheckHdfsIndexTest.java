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
package org.apache.solr.index.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.index.BaseTestCheckIndex;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
@SolrTestCaseJ4.SuppressSSL
// commented out on: 24-Dec-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
public class CheckHdfsIndexTest extends AbstractFullDistribZkTestBase {
  private static MiniDFSCluster dfsCluster;
  private static Path path;

  private BaseTestCheckIndex testCheckIndex;
  private Directory directory;

  public CheckHdfsIndexTest() {
    super();
    sliceCount = 1;
    fixShardCount(1);

    testCheckIndex = new BaseTestCheckIndex();
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
    path = new Path(HdfsTestUtil.getURI(dfsCluster) + "/solr/");
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    try {
      HdfsTestUtil.teardownClass(dfsCluster);
    } finally {
      dfsCluster = null;
      path = null;
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
    directory = new HdfsDirectory(path, conf);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      if (null != directory) {
        directory.close();
      }
    } finally {
      try(FileSystem fs = FileSystem.get(HdfsTestUtil.getClientConfiguration(dfsCluster))) {
        fs.delete(path, true);
      } finally {
        super.tearDown();
      }
    }
  }

  @Override
  protected String getDataDir(String dataDir) throws IOException {
    return HdfsTestUtil.getDataDir(dfsCluster, dataDir);
  }

  @Test
  public void doTest() throws Exception {
    waitForRecoveriesToFinish(false);

    indexr(id, 1);
    commit();

    waitForRecoveriesToFinish(false);

    String[] args;
    {
      SolrClient client = clients.get(0);
      NamedList<Object> response = client.query(new SolrQuery().setRequestHandler("/admin/system")).getResponse();
      @SuppressWarnings({"unchecked"})
      NamedList<Object> coreInfo = (NamedList<Object>) response.get("core");
      @SuppressWarnings({"unchecked"})
      String indexDir = ((NamedList<Object>) coreInfo.get("directory")).get("data") + "/index";

      args = new String[] {indexDir};
    }

    assertEquals("CheckHdfsIndex return status", 0, CheckHdfsIndex.doMain(args));
  }

  @Test
  public void testDeletedDocs() throws IOException {
    testCheckIndex.testDeletedDocs(directory);
  }

  @Test
  public void testChecksumsOnly() throws IOException {
    testCheckIndex.testChecksumsOnly(directory);
  }

  @Test
  public void testChecksumsOnlyVerbose() throws IOException {
    testCheckIndex.testChecksumsOnlyVerbose(directory);
  }

  @Test
  @Ignore("We explicitly use a NoLockFactory, so this test doesn't make sense.")
  public void testObtainsLock() throws IOException {
    testCheckIndex.testObtainsLock(directory);
  }
}
