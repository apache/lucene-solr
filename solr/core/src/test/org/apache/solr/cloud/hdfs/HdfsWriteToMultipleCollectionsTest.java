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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.BasicDistributedZkTest;
import org.apache.solr.cloud.StoppableIndexingThread;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.HdfsDirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blockcache.BlockCache;
import org.apache.solr.store.blockcache.BlockDirectory;
import org.apache.solr.store.blockcache.BlockDirectoryCache;
import org.apache.solr.store.blockcache.Cache;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.apache.solr.util.RefCounted;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@Nightly
@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class HdfsWriteToMultipleCollectionsTest extends BasicDistributedZkTest {
  private static final String ACOLLECTION = "acollection";
  private static MiniDFSCluster dfsCluster;
  
  @BeforeClass
  public static void setupClass() throws Exception {
    schemaString = "schema15.xml";      // we need a string id
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
  }
  
  @AfterClass
  public static void teardownClass() throws Exception {
    try {
      HdfsTestUtil.teardownClass(dfsCluster);
    } finally {
      dfsCluster = null;
      schemaString = null;
    }
  }
  
  @Override
  protected String getDataDir(String dataDir) throws IOException {
    return HdfsTestUtil.getDataDir(dfsCluster, dataDir);
  }
  
  public HdfsWriteToMultipleCollectionsTest() {
    super();
    sliceCount = 1;
    fixShardCount(3);
  }
  
  protected String getSolrXml() {
    return "solr.xml";
  }

  @Test
  public void test() throws Exception {
    int docCount = random().nextInt(1313) + 1;
    int cnt = random().nextInt(4) + 1;
    for (int i = 0; i < cnt; i++) {
      createCollection(ACOLLECTION + i, "conf1", 2, 2, 9);
    }
    for (int i = 0; i < cnt; i++) {
      waitForRecoveriesToFinish(ACOLLECTION + i, false);
    }
    List<CloudSolrClient> cloudClients = new ArrayList<>();
    List<StoppableIndexingThread> threads = new ArrayList<>();
    for (int i = 0; i < cnt; i++) {
      CloudSolrClient client = getCloudSolrClient(zkServer.getZkAddress());
      client.setDefaultCollection(ACOLLECTION + i);
      cloudClients.add(client);
      StoppableIndexingThread indexThread = new StoppableIndexingThread(null, client, "1", true, docCount, 1, true);
      threads.add(indexThread);
      indexThread.start();
    }
    
    int addCnt = 0;
    for (StoppableIndexingThread thread : threads) {
      thread.join();
      addCnt += thread.getNumAdds() - thread.getNumDeletes();
    }
   
    long collectionsCount = 0;
    for (CloudSolrClient client : cloudClients) {
      client.commit();
      collectionsCount += client.query(new SolrQuery("*:*")).getResults().getNumFound();
    }

    IOUtils.close(cloudClients);

    assertEquals(addCnt, collectionsCount);
    
    BlockCache lastBlockCache = null;
    // assert that we are using the block directory and that write and read caching are being used
    for (JettySolrRunner jetty : jettys) {
      CoreContainer cores = jetty.getCoreContainer();
      Collection<SolrCore> solrCores = cores.getCores();
      for (SolrCore core : solrCores) {
        if (core.getCoreDescriptor().getCloudDescriptor().getCollectionName()
            .startsWith(ACOLLECTION)) {
          DirectoryFactory factory = core.getDirectoryFactory();
          assertTrue("Found: " + core.getDirectoryFactory().getClass().getName(), factory instanceof HdfsDirectoryFactory);
          Directory dir = factory.get(core.getDataDir(), null, null);
          try {
            long dataDirSize = factory.size(dir);
            Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
            FileSystem fileSystem = FileSystem.newInstance(
                new Path(core.getDataDir()).toUri(), conf);
            long size = fileSystem.getContentSummary(
                new Path(core.getDataDir())).getLength();
            assertEquals(size, dataDirSize);
          } finally {
            core.getDirectoryFactory().release(dir);
          }
          
          RefCounted<IndexWriter> iwRef = core.getUpdateHandler()
              .getSolrCoreState().getIndexWriter(core);
          try {
            IndexWriter iw = iwRef.get();
            NRTCachingDirectory directory = (NRTCachingDirectory) iw.getDirectory();
            BlockDirectory blockDirectory = (BlockDirectory) directory
                .getDelegate();
            assertTrue(blockDirectory.isBlockCacheReadEnabled());
            // see SOLR-6424
            assertFalse(blockDirectory.isBlockCacheWriteEnabled());
            Cache cache = blockDirectory.getCache();
            // we know it's a BlockDirectoryCache, but future proof
            assertTrue(cache instanceof BlockDirectoryCache);
            BlockCache blockCache = ((BlockDirectoryCache) cache)
                .getBlockCache();
            if (lastBlockCache != null) {
              if (Boolean.getBoolean("solr.hdfs.blockcache.global")) {
                assertEquals(lastBlockCache, blockCache);
              } else {
                assertNotSame(lastBlockCache, blockCache);
              }
            }
            lastBlockCache = blockCache;
          } finally {
            iwRef.decref();
          }
        }
      }
    }
  }
}
