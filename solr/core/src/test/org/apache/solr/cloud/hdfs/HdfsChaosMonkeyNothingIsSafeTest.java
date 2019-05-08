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

import java.io.IOException;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.cloud.ChaosMonkeyNothingIsSafeTest;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@Slow
@Nightly
@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class HdfsChaosMonkeyNothingIsSafeTest extends ChaosMonkeyNothingIsSafeTest {
  private static MiniDFSCluster dfsCluster;
  
  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("solr.hdfs.blockcache.global", "true"); // always use global cache, this test can create a lot of directories
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
  }
  
  @AfterClass
  public static void teardownClass() throws Exception {
    try {
      HdfsTestUtil.teardownClass(dfsCluster);
    } finally {
      dfsCluster = null;
      System.clearProperty("solr.hdfs.blockcache.global");
    }
  }
  
  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();
    
    // super class may hard code directory
    useFactory("org.apache.solr.core.HdfsDirectoryFactory");
  }
  
  @Override
  protected String getDataDir(String dataDir) throws IOException {
    return HdfsTestUtil.getDataDir(dfsCluster, dataDir);
  }
}
