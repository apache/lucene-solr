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
import org.apache.solr.cloud.BasicDistributedZkTest;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@Slow
@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class HdfsNNFailoverTest extends BasicDistributedZkTest {
  private static final String COLLECTION = "collection";
  private static MiniDFSCluster dfsCluster;
  
  @BeforeClass
  public static void setupClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath(), false, true);
  }
  
  @AfterClass
  public static void teardownClass() throws Exception {
    try {
      HdfsTestUtil.teardownClass(dfsCluster);
    } finally {
      dfsCluster = null;
    }
  }
  
  @Override
  protected String getDataDir(String dataDir) throws IOException {
    return HdfsTestUtil.getDataDir(dfsCluster, dataDir);
  }
  
  public HdfsNNFailoverTest() {
    super();
    sliceCount = 1;
    fixShardCount(TEST_NIGHTLY ? 7 : random().nextInt(2) + 1);
  }

  protected String getSolrXml() {
    return "solr.xml";
  }

  @Test
  public void test() throws Exception {
    createCollection(COLLECTION, "conf1", 1, 1, 1);
    
    waitForRecoveriesToFinish(COLLECTION, false);
    
    // TODO:  SOLR-7360 Enable HDFS NameNode failover testing. 
//    dfsCluster.transitionToStandby(0);
//    dfsCluster.transitionToActive(1);
  }

}
