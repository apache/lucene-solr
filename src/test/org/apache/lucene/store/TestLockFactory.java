package org.apache.lucene.store;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import junit.framework.TestCase;

import java.util.Hashtable;
import java.util.Enumeration;

import java.io.IOException;
import java.io.File;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.analysis.WhitespaceAnalyzer;

public class TestLockFactory extends TestCase {

    // Verify: we can provide our own LockFactory implementation, the right
    // methods are called at the right time, locks are created, etc.

    public void testCustomLockFactory() throws IOException {
        Directory dir = new RAMDirectory();
        MockLockFactory lf = new MockLockFactory();
        dir.setLockFactory(lf);

        // Lock prefix should have been set:
        assertTrue("lock prefix was not set by the RAMDirectory", lf.lockPrefixSet);

        IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);

        // add 100 documents (so that commit lock is used)
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // Both write lock and commit lock should have been created:
        assertEquals("# of unique locks created (after instantiating IndexWriter)",
                     2, lf.locksCreated.size());
        assertTrue("# calls to makeLock <= 2 (after instantiating IndexWriter)",
                   lf.makeLockCount > 2);
        
        for(Enumeration e = lf.locksCreated.keys(); e.hasMoreElements();) {
            String lockName = (String) e.nextElement();
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
        Directory dir = new RAMDirectory();
        dir.setLockFactory(NoLockFactory.getNoLockFactory());

        assertTrue("RAMDirectory.setLockFactory did not take",
                   NoLockFactory.class.isInstance(dir.getLockFactory()));

        IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);

        // Create a 2nd IndexWriter.  This is normally not allowed but it should run through since we're not
        // using any locks:
        IndexWriter writer2 = null;
        try {
            writer2 = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
        } catch (Exception e) {
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

        IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);

        // Create a 2nd IndexWriter.  This should fail:
        IndexWriter writer2 = null;
        try {
            writer2 = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
            fail("Should have hit an IOException with two IndexWriters on default SingleInstanceLockFactory");
        } catch (IOException e) {
        }

        writer.close();
        if (writer2 != null) {
            writer2.close();
        }
    }

    // Verify: SimpleFSLockFactory is the default for FSDirectory
    // Verify: FSDirectory does basic locking correctly
    public void testDefaultFSDirectory() throws IOException {
        String indexDirName = "index.TestLockFactory1";

        IndexWriter writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true);

        assertTrue("FSDirectory did not use correct LockFactory: got " + writer.getDirectory().getLockFactory(),
                   SimpleFSLockFactory.class.isInstance(writer.getDirectory().getLockFactory()) ||
                   NativeFSLockFactory.class.isInstance(writer.getDirectory().getLockFactory()));

        IndexWriter writer2 = null;

        // Create a 2nd IndexWriter.  This should fail:
        try {
            writer2 = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), false);
            fail("Should have hit an IOException with two IndexWriters on default SimpleFSLockFactory");
        } catch (IOException e) {
        }

        writer.close();
        if (writer2 != null) {
            writer2.close();
        }

        // Cleanup
        rmDir(indexDirName);
    }

    // Verify: FSDirectory's default lockFactory clears all locks correctly
    public void testFSDirectoryTwoCreates() throws IOException {
        String indexDirName = "index.TestLockFactory2";

        IndexWriter writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true);

        assertTrue("FSDirectory did not use correct LockFactory: got " + writer.getDirectory().getLockFactory(),
                   SimpleFSLockFactory.class.isInstance(writer.getDirectory().getLockFactory()) ||
                   NativeFSLockFactory.class.isInstance(writer.getDirectory().getLockFactory()));

        writer.close();

        // Create a 2nd IndexWriter.  This should not fail:
        IndexWriter writer2 = null;
        try {
            writer2 = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true);
        } catch (IOException e) {
            fail("Should not have hit an IOException with two IndexWriters with create=true, on default SimpleFSLockFactory");
        }

        writer.close();
        if (writer2 != null) {
            writer2.close();
        }
        // Cleanup
        rmDir(indexDirName);
    }
    

    // Verify: setting custom lock factory class (as system property) works:
    // Verify: FSDirectory does basic locking correctly
    public void testLockClassProperty() throws IOException {
        String indexDirName = "index.TestLockFactory3";

        System.setProperty("org.apache.lucene.store.FSDirectoryLockFactoryClass",
                           "org.apache.lucene.store.NoLockFactory");

        IndexWriter writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true);

        assertTrue("FSDirectory did not use correct LockFactory: got " + writer.getDirectory().getLockFactory(),
                   NoLockFactory.class.isInstance(writer.getDirectory().getLockFactory()));

        // Put back to the correct default for subsequent tests:
        System.setProperty("org.apache.lucene.store.FSDirectoryLockFactoryClass",
                           "org.apache.lucene.store.SimpleFSLockFactory");

        writer.close();
        // Cleanup
        rmDir(indexDirName);
    }

    // Verify: setDisableLocks works
    public void testDisableLocks() throws IOException {
        String indexDirName = "index.TestLockFactory4";
        
        assertTrue("Locks are already disabled", !FSDirectory.getDisableLocks());
        FSDirectory.setDisableLocks(true);

        IndexWriter writer = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), true);

        assertTrue("FSDirectory did not use correct default LockFactory: got " + writer.getDirectory().getLockFactory(),
                   NoLockFactory.class.isInstance(writer.getDirectory().getLockFactory()));

        // Should be no error since locking is disabled:
        IndexWriter writer2 = null;
        try {
            writer2 = new IndexWriter(indexDirName, new WhitespaceAnalyzer(), false);
        } catch (IOException e) {
            fail("Should not have hit an IOException with locking disabled");
        }

        FSDirectory.setDisableLocks(false);
        writer.close();
        if (writer2 != null) {
            writer2.close();
        }
        // Cleanup
        rmDir(indexDirName);
    }

    // Verify: if I try to getDirectory() with two different locking implementations, I get an IOException
    public void testFSDirectoryDifferentLockFactory() throws IOException {
        String indexDirName = "index.TestLockFactory5";

        LockFactory lf = new SingleInstanceLockFactory();
        FSDirectory fs1 = FSDirectory.getDirectory(indexDirName, true, lf);

        // Different lock factory instance should hit IOException:
        try {
            FSDirectory fs2 = FSDirectory.getDirectory(indexDirName, true, new SingleInstanceLockFactory());
            fail("Should have hit an IOException because LockFactory instances differ");
        } catch (IOException e) {
        }

        FSDirectory fs2 = null;

        // Same lock factory instance should not:
        try {
            fs2 = FSDirectory.getDirectory(indexDirName, true, lf);
        } catch (IOException e) {
            fail("Should not have hit an IOException because LockFactory instances are the same");
        }

        fs1.close();
        if (fs2 != null) {
            fs2.close();
        }
        // Cleanup
        rmDir(indexDirName);
    }

    // Verify: do stress test, by opening IndexReaders and
    // IndexWriters over & over in 2 threads and making sure
    // no unexpected exceptions are raised:
    public void testStressLocks() throws IOException {
      _testStressLocks(null, "index.TestLockFactory6");
    }

    // Verify: do stress test, by opening IndexReaders and
    // IndexWriters over & over in 2 threads and making sure
    // no unexpected exceptions are raised, but use
    // NativeFSLockFactory:
    public void testStressLocksNativeFSLockFactory() throws IOException {
      _testStressLocks(NativeFSLockFactory.getLockFactory(), "index.TestLockFactory7");
    }

    public void _testStressLocks(LockFactory lockFactory, String indexDirName) throws IOException {
        FSDirectory fs1 = FSDirectory.getDirectory(indexDirName, true, lockFactory);
        // fs1.setLockFactory(NoLockFactory.getNoLockFactory());

        // First create a 1 doc index:
        IndexWriter w = new IndexWriter(fs1, new WhitespaceAnalyzer(), true);
        addDoc(w);
        w.close();

        WriterThread writer = new WriterThread(100, fs1);
        SearcherThread searcher = new SearcherThread(100, fs1);
        writer.start();
        searcher.start();

        while(writer.isAlive() || searcher.isAlive()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        assertTrue("IndexWriter hit unexpected exceptions", !writer.hitException);
        assertTrue("IndexSearcher hit unexpected exceptions", !searcher.hitException);

        // Cleanup
        rmDir(indexDirName);
    }

    // Verify: NativeFSLockFactory works correctly
    public void testNativeFSLockFactory() throws IOException {

      // Make sure we get identical instances:
      NativeFSLockFactory f = NativeFSLockFactory.getLockFactory();
      NativeFSLockFactory f2 = NativeFSLockFactory.getLockFactory();
      assertTrue("got different NativeFSLockFactory instances for same directory",
                 f == f2);

      // Make sure we get identical locks:
      f.setLockPrefix("test");
      Lock l = f.makeLock("commit");
      Lock l2 = f.makeLock("commit");
      assertTrue("got different Lock instances for same lock name",
                 l == l2);

      assertTrue("failed to obtain lock", l.obtain());
      assertTrue("succeeded in obtaining lock twice", !l.obtain());
      l.release();

      // Make sure we can obtain it again:
      assertTrue("failed to obtain lock", l.obtain());
      l.release();
    }

    private class WriterThread extends Thread { 
        private Directory dir;
        private int numIteration;
        public boolean hitException = false;
        public WriterThread(int numIteration, Directory dir) {
            this.numIteration = numIteration;
            this.dir = dir;
        }
        public void run() {
            WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer();
            IndexWriter writer = null;
            for(int i=0;i<this.numIteration;i++) {
                try {
                    writer = new IndexWriter(dir, analyzer, false);
                } catch (IOException e) {
                    if (e.toString().indexOf(" timed out:") == -1) {
                        hitException = true;
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
                }
                if (writer != null) {
                    try {
                        addDoc(writer);
                    } catch (IOException e) {
                        hitException = true;
                        System.out.println("Stress Test Index Writer: addDoc hit unexpected exception: " + e.toString());
                        e.printStackTrace(System.out);
                    }
                    try {
                        writer.close();
                    } catch (IOException e) {
                        hitException = true;
                        System.out.println("Stress Test Index Writer: close hit unexpected exception: " + e.toString());
                        e.printStackTrace(System.out);
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
        public void run() {
            IndexSearcher searcher = null;
            WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer();
            Query query = new TermQuery(new Term("content", "aaa"));
            for(int i=0;i<this.numIteration;i++) {
                try{
                    searcher = new IndexSearcher(dir);
                } catch (Exception e) {
                    hitException = true;
                    System.out.println("Stress Test Index Searcher: create hit unexpected exception: " + e.toString());
                    e.printStackTrace(System.out);
                }
                if (searcher != null) {
                    Hits hits = null;
                    try {
                        hits = searcher.search(query);
                    } catch (IOException e) {
                        hitException = true;
                        System.out.println("Stress Test Index Searcher: search hit unexpected exception: " + e.toString());
                        e.printStackTrace(System.out);
                    }
                    // System.out.println(hits.length() + " total results");
                    try {
                        searcher.close();
                    } catch (IOException e) {
                        hitException = true;
                        System.out.println("Stress Test Index Searcher: close hit unexpected exception: " + e.toString());
                        e.printStackTrace(System.out);
                    }
                    searcher = null;
                }
            }
        }
    }

    public class MockLockFactory extends LockFactory {

        public boolean lockPrefixSet;
        public Hashtable locksCreated = new Hashtable();
        public int makeLockCount = 0;

        public void setLockPrefix(String lockPrefix) {    
            super.setLockPrefix(lockPrefix);
            lockPrefixSet = true;
        }

        synchronized public Lock makeLock(String lockName) {
            Lock lock = new MockLock();
            locksCreated.put(lockName, lock);
            makeLockCount++;
            return lock;
        }

        public void clearAllLocks() {}

        public class MockLock extends Lock {
            public int lockAttempts;

            public boolean obtain() {
                lockAttempts++;
                return true;
            }
            public void release() {
                // do nothing
            }
            public boolean isLocked() {
                return false;
            }
        }
    }

    private void addDoc(IndexWriter writer) throws IOException {
        Document doc = new Document();
        doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.TOKENIZED));
        writer.addDocument(doc);
    }

    private void rmDir(String dirName) {
        File dir = new java.io.File(dirName);
        String[] files = dir.list();            // clear old files
        for (int i = 0; i < files.length; i++) {
            File file = new File(dir, files[i]);
            file.delete();
        }
        dir.delete();
    }
}
