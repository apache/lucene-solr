package org.apache.lucene.store;

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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.MockDirectoryWrapper.Throttling;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestDirectory extends LuceneTestCase {
  public void testDetectClose() throws Throwable {
    Directory[] dirs = new Directory[] { 
        new RAMDirectory(), 
        new SimpleFSDirectory(TEMP_DIR), 
        new NIOFSDirectory(TEMP_DIR)
    };

    for (Directory dir : dirs) {
      dir.close();
      try {
        dir.createOutput("test", newIOContext(random()));
        fail("did not hit expected exception");
      } catch (AlreadyClosedException ace) {
      }
    }
  }
  
  // test is occasionally very slow, i dont know why
  // try this seed: 7D7E036AD12927F5:93333EF9E6DE44DE
  @Nightly
  public void testThreadSafety() throws Exception {
    final BaseDirectoryWrapper dir = newDirectory();
    dir.setCheckIndexOnClose(false); // we arent making an index
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper)dir).setThrottling(Throttling.NEVER); // makes this test really slow
    }
    
    if (VERBOSE) {
      System.out.println(dir);
    }

    class TheThread extends Thread {
      private String name;

      public TheThread(String name) {
        this.name = name;
      }
      
      @Override
      public void run() {
        for (int i = 0; i < 3000; i++) {
          String fileName = this.name + i;
          try {
            //System.out.println("create:" + fileName);
            IndexOutput output = dir.createOutput(fileName, newIOContext(random()));
            output.close();
            assertTrue(dir.fileExists(fileName));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    };
    
    class TheThread2 extends Thread {
      private String name;

      public TheThread2(String name) {
        this.name = name;
      }
      
      @Override
      public void run() {
        for (int i = 0; i < 10000; i++) {
          try {
            String[] files = dir.listAll();
            for (String file : files) {
              //System.out.println("file:" + file);
             try {
              IndexInput input = dir.openInput(file, newIOContext(random()));
              input.close();
              } catch (FileNotFoundException e) {
                // ignore
              } catch (IOException e) {
                if (e.getMessage().contains("still open for writing")) {
                  // ignore
                } else {
                  throw new RuntimeException(e);
                }
              }
              if (random().nextBoolean()) {
                break;
              }
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    };
    
    TheThread theThread = new TheThread("t1");
    TheThread2 theThread2 = new TheThread2("t2");
    theThread.start();
    theThread2.start();
    
    theThread.join();
    theThread2.join();
    
    dir.close();
  }


  // Test that different instances of FSDirectory can coexist on the same
  // path, can read, write, and lock files.
  public void testDirectInstantiation() throws Exception {
    File path = _TestUtil.getTempDir("testDirectInstantiation");

    int sz = 3;
    Directory[] dirs = new Directory[sz];

    dirs[0] = new SimpleFSDirectory(path, null);
    dirs[1] = new NIOFSDirectory(path, null);
    dirs[2] = new MMapDirectory(path, null);

    for (int i=0; i<sz; i++) {
      Directory dir = dirs[i];
      dir.ensureOpen();
      String fname = "foo." + i;
      String lockname = "foo" + i + ".lck";
      IndexOutput out = dir.createOutput(fname, newIOContext(random()));
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
        
        IndexInput input = d2.openInput(fname, newIOContext(random()));
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
    
    _TestUtil.rmDir(path);
  }

  // LUCENE-1464
  public void testDontCreate() throws Throwable {
    File path = new File(TEMP_DIR, "doesnotexist");
    try {
      assertTrue(!path.exists());
      Directory dir = new SimpleFSDirectory(path, null);
      assertTrue(!path.exists());
      dir.close();
    } finally {
      _TestUtil.rmDir(path);
    }
  }

  // LUCENE-1468
  public void testRAMDirectoryFilter() throws IOException {
    checkDirectoryFilter(new RAMDirectory());
  }

  // LUCENE-1468
  public void testFSDirectoryFilter() throws IOException {
    checkDirectoryFilter(newFSDirectory(_TestUtil.getTempDir("test")));
  }

  // LUCENE-1468
  private void checkDirectoryFilter(Directory dir) throws IOException {
    String name = "file";
    try {
      dir.createOutput(name, newIOContext(random())).close();
      assertTrue(dir.fileExists(name));
      assertTrue(Arrays.asList(dir.listAll()).contains(name));
    } finally {
      dir.close();
    }
  }

  // LUCENE-1468
  public void testCopySubdir() throws Throwable {
    File path = _TestUtil.getTempDir("testsubdir");
    try {
      path.mkdirs();
      new File(path, "subdir").mkdirs();
      Directory fsDir = new SimpleFSDirectory(path, null);
      assertEquals(0, new RAMDirectory(fsDir, newIOContext(random())).listAll().length);
    } finally {
      _TestUtil.rmDir(path);
    }
  }

  // LUCENE-1468
  public void testNotDirectory() throws Throwable {
    File path = _TestUtil.getTempDir("testnotdir");
    Directory fsDir = new SimpleFSDirectory(path, null);
    try {
      IndexOutput out = fsDir.createOutput("afile", newIOContext(random()));
      out.close();
      assertTrue(fsDir.fileExists("afile"));
      try {
        new SimpleFSDirectory(new File(path, "afile"), null);
        fail("did not hit expected exception");
      } catch (NoSuchDirectoryException nsde) {
        // Expected
      }
    } finally {
      fsDir.close();
      _TestUtil.rmDir(path);
    }
  }
}

