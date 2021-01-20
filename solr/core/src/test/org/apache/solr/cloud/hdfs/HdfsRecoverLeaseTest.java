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
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.apache.solr.util.FSHDFSUtils;
import org.apache.solr.util.FSHDFSUtils.CallerInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class HdfsRecoverLeaseTest extends SolrTestCaseJ4 {
  
  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath(), false);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    try {
      HdfsTestUtil.teardownClass(dfsCluster);
    } finally {
      dfsCluster = null;
    }
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
    long startRecoverLeaseSuccessCount = FSHDFSUtils.RECOVER_LEASE_SUCCESS_COUNT.get();
    
    URI uri = dfsCluster.getURI();
    Path path = new Path(uri);
    Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
    FileSystem fs1 = FileSystem.get(path.toUri(), conf);
    Path testFile = new Path(uri.toString() + "/testfile");
    FSDataOutputStream out = fs1.create(testFile);
    
    out.write(5);
    out.hflush();
    out.close();

    FSHDFSUtils.recoverFileLease(fs1, testFile, conf, new CallerInfo() {
      
      @Override
      public boolean isCallerClosed() {
        return false;
      }
    });
    assertEquals(0, FSHDFSUtils.RECOVER_LEASE_SUCCESS_COUNT.get() - startRecoverLeaseSuccessCount);
    
    fs1.close();

    
    FileSystem fs2 = FileSystem.get(path.toUri(), conf);
    Path testFile2 = new Path(uri.toString() + "/testfile2");
    FSDataOutputStream out2 = fs2.create(testFile2);
    
    if (random().nextBoolean()) {
      int cnt = random().nextInt(100);
      for (int i = 0; i < cnt; i++) {
        out2.write(random().nextInt(20000));
      }
      out2.hflush();
    }

    
    // closing the fs will close the file it seems
    // fs2.close();
    
    FileSystem fs3 = FileSystem.get(path.toUri(), conf);

    FSHDFSUtils.recoverFileLease(fs3, testFile2, conf, new CallerInfo() {
      
      @Override
      public boolean isCallerClosed() {
        return false;
      }
    });
    assertEquals(1, FSHDFSUtils.RECOVER_LEASE_SUCCESS_COUNT.get() - startRecoverLeaseSuccessCount);
    
    fs3.close();
    fs2.close();
  }
  
  @Test
  public void testMultiThreaded() throws Exception {
    long startRecoverLeaseSuccessCount = FSHDFSUtils.RECOVER_LEASE_SUCCESS_COUNT.get();
    
    final URI uri = dfsCluster.getURI();
    final Path path = new Path(uri);
    final Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
    
    // n threads create files
    class WriterThread extends Thread {
      private FileSystem fs;
      private int id;
      
      public WriterThread(int id) {
        this.id = id;
        try {
          fs = FileSystem.get(path.toUri(), conf);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      
      @Override
      public void run() {
        Path testFile = new Path(uri.toString() + "/file-" + id);
        FSDataOutputStream out;
        try {
          out = fs.create(testFile);
          
          if (random().nextBoolean()) {
            int cnt = random().nextInt(100);
            for (int i = 0; i < cnt; i++) {
              out.write(random().nextInt(20000));
            }
            out.hflush();
          }
        } catch (IOException e) {
          throw new RuntimeException();
        }
      }
      
      public void close() throws IOException {
        fs.close();
      }
      
      public int getFileId() {
        return id;
      }
    }
    
    class RecoverThread extends Thread {
      private FileSystem fs;
      private int id;
      
      public RecoverThread(int id) {
        this.id = id;
        try {
          fs = FileSystem.get(path.toUri(), conf);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      
      @Override
      public void run() {
        Path testFile = new Path(uri.toString() + "/file-" + id);
        try {
          FSHDFSUtils.recoverFileLease(fs, testFile, conf, new CallerInfo() {
            
            @Override
            public boolean isCallerClosed() {
              return false;
            }
          });
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
      
      public void close() throws IOException {
        fs.close();
      }
    }
    
    Set<WriterThread> writerThreads = new HashSet<WriterThread>();
    Set<RecoverThread> recoverThreads = new HashSet<RecoverThread>();
    
    int threadCount = 3;
    for (int i = 0; i < threadCount; i++) {
      WriterThread wt = new WriterThread(i);
      writerThreads.add(wt);
      wt.run();
    }
    
    for (WriterThread wt : writerThreads) {
      wt.join();
    }
    
    Thread.sleep(2000);
    
    for (WriterThread wt : writerThreads) {
      RecoverThread rt = new RecoverThread(wt.getFileId());
      recoverThreads.add(rt);
      rt.run();
    }
    
    for (WriterThread wt : writerThreads) {
      wt.close();
    }
    
    for (RecoverThread rt : recoverThreads) {
      rt.close();
    }

    assertEquals(threadCount, FSHDFSUtils.RECOVER_LEASE_SUCCESS_COUNT.get() - startRecoverLeaseSuccessCount);
    
  }

}
