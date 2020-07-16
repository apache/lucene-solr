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
package org.apache.solr.store.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
@Nightly
public class HdfsLockFactoryTest extends SolrTestCaseJ4 {
  
  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    try {
      HdfsTestUtil.teardownClass(dfsCluster);
    } finally {
      dfsCluster = null;
    }
  }
  
  @Test
  public void testBasic() throws IOException {
    String uri = HdfsTestUtil.getURI(dfsCluster);
    Path lockPath = new Path(uri, "/basedir/lock");
    Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
    HdfsDirectory dir = new HdfsDirectory(lockPath, conf);
    
    try (Lock lock = dir.obtainLock("testlock")) {
      assert lock != null;
      try (Lock lock2 = dir.obtainLock("testlock")) {
        assert lock2 != null;
        fail("Locking should fail");
      } catch (LockObtainFailedException lofe) {
        // pass
      }
    }
    // now repeat after close()
    try (Lock lock = dir.obtainLock("testlock")) {
      assert lock != null;
      try (Lock lock2 = dir.obtainLock("testlock")) {
        assert lock2 != null;
        fail("Locking should fail");
      } catch (LockObtainFailedException lofe) {
        // pass
      }
    }
    dir.close();
  }
}
