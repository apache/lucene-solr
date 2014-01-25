package org.apache.solr.store.hdfs;

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

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.store.Lock;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;

@ThreadLeakScope(Scope.NONE) // hdfs client currently leaks thread (HADOOP-9049)
public class HdfsLockFactoryTest extends SolrTestCaseJ4 {
  
  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    createTempDir();
    dfsCluster = HdfsTestUtil.setupClass(TEMP_DIR.getAbsolutePath()
        + File.separator + HdfsLockFactoryTest.class.getName() + "_hdfsdir-"
        + System.currentTimeMillis());
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @Test
  public void testBasic() throws IOException {
    URI uri = dfsCluster.getURI();
    Path lockPath = new Path(uri.toString(), "/basedir/lock");
    HdfsLockFactory lockFactory = new HdfsLockFactory(lockPath, new Configuration());
    Lock lock = lockFactory.makeLock("testlock");
    boolean success = lock.obtain();
    assertTrue("We could not get the lock when it should be available", success);
    success = lock.obtain();
    assertFalse("We got the lock but it should be unavailble", success);
    lock.close();
    success = lock.obtain();
    assertTrue("We could not get the lock when it should be available", success);
    success = lock.obtain();
    assertFalse("We got the lock but it should be unavailble", success);
  }
  

}