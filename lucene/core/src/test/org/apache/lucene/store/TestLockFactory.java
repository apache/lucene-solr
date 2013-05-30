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
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestLockFactory extends LuceneTestCase {

    // Verify: we can provide our own LockFactory implementation, the right
    // methods are called at the right time, locks are created, etc.

    public void testCustomLockFactory() throws IOException {
        Directory dir = new MockDirectoryWrapper(random(), new RAMDirectory());
        MockLockFactory lf = new MockLockFactory();
        dir.setLockFactory(lf);

        // Lock prefix should have been set:
        assertTrue("lock prefix was not set by the RAMDirectory", lf.lockPrefixSet);

        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

        // add 100 documents (so that commit lock is used)
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // Both write lock and commit lock should have been created:
        assertEquals("# of unique locks created (after instantiating IndexWriter)",
                     1, lf.locksCreated.size());
        assertTrue("# calls to makeLock is 0 (after instantiating IndexWriter)",
                   lf.makeLockCount >= 1);
        
        for(final String lockName : lf.locksCreated.keySet()) {
            MockLockFactory.MockLock lock = (MockLockFactory.MockLock) lf.locksCreated.get(lockName);
            assertTrue("# calls to Lock.obtain is 0 (after instantiating IndexWriter)",
                       lock.lockAttempts > 0);
        }
        
        writer.close();
    }

    // Verify: we can use the NoLockFactory with RAMDirectory w/ no
    // exceptions raised:
    // Verify: NoLockFactory allows two IndexWriters
    public void testRAMDirectoryNoLocking() throws IOException {
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), new RAMDirectory());
        dir.setLockFactory(NoLockFactory.getNoLockFactory());
        dir.setWrapLockFactory(false); // we are gonna explicitly test we get this back
        assertTrue("RAMDirectory.setLockFactory did not take",
                   NoLockFactory.class.isInstance(dir.getLockFactory()));

        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        writer.commit(); // required so the second open succeed 
        // Create a 2nd IndexWriter.  This is normally not allowed but it should run through since we're not
        // using any locks:
        IndexWriter writer2 = null;
        try {
            writer2 = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
        } catch (Exception e) {
            e.printStackTrace(System.out);
            fail("Should not have hit an IOException with no locking");
        }

        writer.close();
        if (writer2 != null) {
            writer2.close();
        }
    }

    // Verify: SingleInstanceLockFactory is the default lock for RAMDirectory
    // Verify: RAMDirectory does basic locking correctly (can't create two IndexWriters)
    public void testDefaultRAMDirectory() throws IOException {
        Directory dir = new RAMDirectory();

        assertTrue("RAMDirectory did not use correct LockFactory: got " + dir.getLockFactory(),
                   SingleInstanceLockFactory.class.isInstance(dir.getLockFactory()));

        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())));

        // Create a 2nd IndexWriter.  This should fail:
        IndexWriter writer2 = null;
        try {
            writer2 = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
            fail("Should have hit an IOException with two IndexWriters on default SingleInstanceLockFactory");
        } catch (IOException e) {
        }

        writer.close();
        if (writer2 != null) {
            writer2.close();
        }
    }
    
    public void testSimpleFSLockFactory() throws IOException {
      // test string file instantiation
      new SimpleFSLockFactory("test");
    }

    // Verify: do stress test, by opening IndexReaders and
    // IndexWriters over & over in 2 threads and making sure
    // no unexpected exceptions are raised:
    @Nightly
    public void testStressLocks() throws Exception {
      _testStressLocks(null, _TestUtil.getTempDir("index.TestLockFactory6"));
    }

    // Verify: do stress test, by opening IndexReaders and
    // IndexWriters over & over in 2 threads and making sure
    // no unexpected exceptions are raised, but use
    // NativeFSLockFactory:
    @Nightly
    public void testStressLocksNativeFSLockFactory() throws Exception {
      File dir = _TestUtil.getTempDir("index.TestLockFactory7");
      _testStressLocks(new NativeFSLockFactory(dir), dir);
    }

    public void _testStressLocks(LockFactory lockFactory, File indexDir) throws Exception {
        Directory dir = newFSDirectory(indexDir, lockFactory);

        // First create a 1 doc index:
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE));
        addDoc(w);
        w.close();

        WriterThread writer = new WriterThread(100, dir);
        SearcherThread searcher = new SearcherThread(100, dir);
        writer.start();
        searcher.start();

        while(writer.isAlive() || searcher.isAlive()) {
          Thread.sleep(1000);
        }

        assertTrue("IndexWriter hit unexpected exceptions", !writer.hitException);
        assertTrue("IndexSearcher hit unexpected exceptions", !searcher.hitException);

        dir.close();
        // Cleanup
        _TestUtil.rmDir(indexDir);
    }

    // Verify: NativeFSLockFactory works correctly
    public void testNativeFSLockFactory() throws IOException {

      NativeFSLockFactory f = new NativeFSLockFactory(TEMP_DIR);

      f.setLockPrefix("test");
      Lock l = f.makeLock("commit");
      Lock l2 = f.makeLock("commit");

      assertTrue("failed to obtain lock", l.obtain());
      assertTrue("succeeded in obtaining lock twice", !l2.obtain());
      l.release();

      assertTrue("failed to obtain 2nd lock after first one was freed", l2.obtain());
      l2.release();

      // Make sure we can obtain first one again, test isLocked():
      assertTrue("failed to obtain lock", l.obtain());
      assertTrue(l.isLocked());
      assertTrue(l2.isLocked());
      l.release();
      assertFalse(l.isLocked());
      assertFalse(l2.isLocked());
    }

    
    // Verify: NativeFSLockFactory works correctly if the lock file exists
    public void testNativeFSLockFactoryLockExists() throws IOException {
      
      File lockFile = new File(TEMP_DIR, "test.lock");
      lockFile.createNewFile();
      
      Lock l = new NativeFSLockFactory(TEMP_DIR).makeLock("test.lock");
      assertTrue("failed to obtain lock", l.obtain());
      l.release();
      assertFalse("failed to release lock", l.isLocked());
      if (lockFile.exists()) {
        lockFile.delete();
      }
    }

    public void testNativeFSLockReleaseByOtherLock() throws IOException {

      NativeFSLockFactory f = new NativeFSLockFactory(TEMP_DIR);

      f.setLockPrefix("test");
      Lock l = f.makeLock("commit");
      Lock l2 = f.makeLock("commit");

      assertTrue("failed to obtain lock", l.obtain());
      try {
        assertTrue(l2.isLocked());
        l2.release();
        fail("should not have reached here. LockReleaseFailedException should have been thrown");
      } catch (LockReleaseFailedException e) {
        // expected
      } finally {
        l.release();
      }
    }

    // Verify: NativeFSLockFactory assigns null as lockPrefix if the lockDir is inside directory
    public void testNativeFSLockFactoryPrefix() throws IOException {

      File fdir1 = _TestUtil.getTempDir("TestLockFactory.8");
      File fdir2 = _TestUtil.getTempDir("TestLockFactory.8.Lockdir");
      Directory dir1 = newFSDirectory(fdir1, new NativeFSLockFactory(fdir1));
      // same directory, but locks are stored somewhere else. The prefix of the lock factory should != null
      Directory dir2 = newFSDirectory(fdir1, new NativeFSLockFactory(fdir2));

      String prefix1 = dir1.getLockFactory().getLockPrefix();
      assertNull("Lock prefix for lockDir same as directory should be null", prefix1);
      
      String prefix2 = dir2.getLockFactory().getLockPrefix();
      assertNotNull("Lock prefix for lockDir outside of directory should be not null", prefix2);

      dir1.close();
      dir2.close();
      _TestUtil.rmDir(fdir1);
      _TestUtil.rmDir(fdir2);
    }

    // Verify: default LockFactory has no prefix (ie
    // write.lock is stored in index):
    public void testDefaultFSLockFactoryPrefix() throws IOException {

      // Make sure we get null prefix, which wont happen if setLockFactory is ever called.
      File dirName = _TestUtil.getTempDir("TestLockFactory.10");

      Directory dir = new SimpleFSDirectory(dirName);
      assertNull("Default lock prefix should be null", dir.getLockFactory().getLockPrefix());
      dir.close();
      
      dir = new MMapDirectory(dirName);
      assertNull("Default lock prefix should be null", dir.getLockFactory().getLockPrefix());
      dir.close();
      
      dir = new NIOFSDirectory(dirName);
      assertNull("Default lock prefix should be null", dir.getLockFactory().getLockPrefix());
      dir.close();
 
      _TestUtil.rmDir(dirName);
    }

    private class WriterThread extends Thread { 
        private Directory dir;
        private int numIteration;
        public boolean hitException = false;
        public WriterThread(int numIteration, Directory dir) {
            this.numIteration = numIteration;
            this.dir = dir;
        }
        @Override
        public void run() {
            IndexWriter writer = null;
            for(int i=0;i<this.numIteration;i++) {
                try {
                    writer = new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
                } catch (IOException e) {
                    if (e.toString().indexOf(" timed out:") == -1) {
                        hitException = true;
                        System.out.println("Stress Test Index Writer: creation hit unexpected IOException: " + e.toString());
                        e.printStackTrace(System.out);
                    } else {
                        // lock obtain timed out
                        // NOTE: we should at some point
                        // consider this a failure?  The lock
                        // obtains, across IndexReader &
                        // IndexWriters should be "fair" (ie
                        // FIFO).
                    }
                } catch (Exception e) {
                    hitException = true;
                    System.out.println("Stress Test Index Writer: creation hit unexpected exception: " + e.toString());
                    e.printStackTrace(System.out);
                    break;
                }
                if (writer != null) {
                    try {
                        addDoc(writer);
                    } catch (IOException e) {
                        hitException = true;
                        System.out.println("Stress Test Index Writer: addDoc hit unexpected exception: " + e.toString());
                        e.printStackTrace(System.out);
                        break;
                    }
                    try {
                        writer.close();
                    } catch (IOException e) {
                        hitException = true;
                        System.out.println("Stress Test Index Writer: close hit unexpected exception: " + e.toString());
                        e.printStackTrace(System.out);
                        break;
                    }
                    writer = null;
                }
            }
        }
    }

    private class SearcherThread extends Thread { 
        private Directory dir;
        private int numIteration;
        public boolean hitException = false;
        public SearcherThread(int numIteration, Directory dir) {
            this.numIteration = numIteration;
            this.dir = dir;
        }
        @Override
        public void run() {
            IndexReader reader = null;
            IndexSearcher searcher = null;
            Query query = new TermQuery(new Term("content", "aaa"));
            for(int i=0;i<this.numIteration;i++) {
                try{
                    reader = DirectoryReader.open(dir);
                    searcher = newSearcher(reader);
                } catch (Exception e) {
                    hitException = true;
                    System.out.println("Stress Test Index Searcher: create hit unexpected exception: " + e.toString());
                    e.printStackTrace(System.out);
                    break;
                }
                try {
                  searcher.search(query, null, 1000);
                } catch (IOException e) {
                  hitException = true;
                  System.out.println("Stress Test Index Searcher: search hit unexpected exception: " + e.toString());
                  e.printStackTrace(System.out);
                  break;
                }
                // System.out.println(hits.length() + " total results");
                try {
                  reader.close();
                } catch (IOException e) {
                  hitException = true;
                  System.out.println("Stress Test Index Searcher: close hit unexpected exception: " + e.toString());
                  e.printStackTrace(System.out);
                  break;
                }
            }
        }
    }

    public class MockLockFactory extends LockFactory {

        public boolean lockPrefixSet;
        public Map<String,Lock> locksCreated = Collections.synchronizedMap(new HashMap<String,Lock>());
        public int makeLockCount = 0;

        @Override
        public void setLockPrefix(String lockPrefix) {    
            super.setLockPrefix(lockPrefix);
            lockPrefixSet = true;
        }

        @Override
        synchronized public Lock makeLock(String lockName) {
            Lock lock = new MockLock();
            locksCreated.put(lockName, lock);
            makeLockCount++;
            return lock;
        }

        @Override
        public void clearLock(String specificLockName) {}

        public class MockLock extends Lock {
            public int lockAttempts;

            @Override
            public boolean obtain() {
                lockAttempts++;
                return true;
            }
            @Override
            public void release() {
                // do nothing
            }
            @Override
            public boolean isLocked() {
                return false;
            }
        }
    }

    private void addDoc(IndexWriter writer) throws IOException {
        Document doc = new Document();
        doc.add(newTextField("content", "aaa", Field.Store.NO));
        writer.addDocument(doc);
    }
}
