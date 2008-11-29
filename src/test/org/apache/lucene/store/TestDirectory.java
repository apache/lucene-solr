package org.apache.lucene.store;

/**
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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

import java.io.File;

public class TestDirectory extends LuceneTestCase {

  public void testDetectClose() throws Throwable {
    Directory dir = new RAMDirectory();
    dir.close();
    try {
      dir.createOutput("test");
      fail("did not hit expected exception");
    } catch (AlreadyClosedException ace) {
    }

    dir = FSDirectory.getDirectory(System.getProperty("tempDir"));
    dir.close();
    try {
      dir.createOutput("test");
      fail("did not hit expected exception");
    } catch (AlreadyClosedException ace) {
    }
  }


  // Test that different instances of FSDirectory can coexist on the same
  // path, can read, write, and lock files.
  public void testDirectInstantiation() throws Exception {
    File path = new File(System.getProperty("tempDir"));

    int sz = 3;
    Directory[] dirs = new Directory[sz];

    dirs[0] = new FSDirectory(path, null);
    dirs[1] = new NIOFSDirectory(path, null);
    dirs[2] = new MMapDirectory(path, null);

    for (int i=0; i<sz; i++) {
      Directory dir = dirs[i];
      dir.ensureOpen();
      String fname = "foo." + i;
      String lockname = "foo" + i + ".lck";
      IndexOutput out = dir.createOutput(fname);
      out.writeByte((byte)i);
      out.close();

      for (int j=0; j<sz; j++) {
        Directory d2 = dirs[j];
        d2.ensureOpen();
        assertTrue(d2.fileExists(fname));
        assertEquals(1, d2.fileLength(fname));

        // don't test read on MMapDirectory, since it can't really be
        // closed and will cause a failure to delete the file.
        if (d2 instanceof MMapDirectory) continue;
        
        IndexInput input = d2.openInput(fname);
        assertEquals((byte)i, input.readByte());
        input.close();
      }

      // delete with a different dir
      dirs[(i+1)%sz].deleteFile(fname);

      for (int j=0; j<sz; j++) {
        Directory d2 = dirs[j];
        assertFalse(d2.fileExists(fname));
      }

      Lock lock = dir.makeLock(lockname);
      assertTrue(lock.obtain());

      for (int j=0; j<sz; j++) {
        Directory d2 = dirs[j];
        Lock lock2 = d2.makeLock(lockname);
        try {
          assertFalse(lock2.obtain(1));
        } catch (LockObtainFailedException e) {
          // OK
        }
      }

      lock.release();
      
      // now lock with different dir
      lock = dirs[(i+1)%sz].makeLock(lockname);
      assertTrue(lock.obtain());
      lock.release();
    }

    for (int i=0; i<sz; i++) {
      Directory dir = dirs[i];
      dir.ensureOpen();
      dir.close();
      assertFalse(dir.isOpen);
    }
  }

  // LUCENE-1464
  public void testDontCreate() throws Throwable {
    File path = new File(System.getProperty("tempDir"), "doesnotexist");
    try {
      assertTrue(!path.exists());
      Directory dir = new FSDirectory(path, null);
      assertTrue(!path.exists());
      dir.close();
    } finally {
      _TestUtil.rmDir(path);
    }
  }
}

