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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;

public class TestLockFactory extends LuceneTestCase {

    // Verify: we can provide our own LockFactory implementation, the right
    // methods are called at the right time, locks are created, etc.

    public void testCustomLockFactory() throws IOException {
        MockLockFactory lf = new MockLockFactory();
        Directory dir = new MockDirectoryWrapper(random(), new RAMDirectory(lf));

        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

        // add 100 documents (so that commit lock is used)
        for (int i = 0; i < 100; i++) {
            addDoc(writer);
        }

        // Both write lock and commit lock should have been created:
        assertEquals("# of unique locks created (after instantiating IndexWriter)",
                     1, lf.locksCreated.size());
        writer.close();
    }

    // Verify: we can use the NoLockFactory with RAMDirectory w/ no
    // exceptions raised:
    // Verify: NoLockFactory allows two IndexWriters
    public void testRAMDirectoryNoLocking() throws IOException {
        MockDirectoryWrapper dir = new MockDirectoryWrapper(random(), new RAMDirectory(NoLockFactory.INSTANCE));

        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));
        writer.commit(); // required so the second open succeed 
        // Create a 2nd IndexWriter.  This is normally not allowed but it should run through since we're not
        // using any locks:
        IndexWriter writer2 = null;
        try {
            writer2 = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
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
        RAMDirectory dir = new RAMDirectory();

        assertTrue("RAMDirectory did not use correct LockFactory: got " + dir.lockFactory,
                   dir.lockFactory instanceof SingleInstanceLockFactory);

        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

        // Create a 2nd IndexWriter.  This should fail:
        IndexWriter writer2 = null;
        try {
            writer2 = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
            fail("Should have hit an IOException with two IndexWriters on default SingleInstanceLockFactory");
        } catch (IOException e) {
        }

        writer.close();
        if (writer2 != null) {
            writer2.close();
        }
    }

    // Verify: do stress test, by opening IndexReaders and
    // IndexWriters over & over in 2 threads and making sure
    // no unexpected exceptions are raised:
    @Nightly
    public void testStressLocksSimpleFSLockFactory() throws Exception {
      _testStressLocks(SimpleFSLockFactory.INSTANCE, createTempDir("index.TestLockFactory6"));
    }

    // Verify: do stress test, by opening IndexReaders and
    // IndexWriters over & over in 2 threads and making sure
    // no unexpected exceptions are raised, but use
    // NativeFSLockFactory:
    @Nightly
    public void testStressLocksNativeFSLockFactory() throws Exception {
      Path dir = createTempDir("index.TestLockFactory7");
      _testStressLocks(NativeFSLockFactory.INSTANCE, dir);
    }

    public void _testStressLocks(LockFactory lockFactory, Path indexDir) throws Exception {
        Directory dir = newFSDirectory(indexDir, lockFactory);

        // First create a 1 doc index:
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.CREATE));
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
        IOUtils.rm(indexDir);
    }

    // Verify: NativeFSLockFactory works correctly
    public void testNativeFSLockFactory() throws IOException {
      Directory dir = FSDirectory.open(createTempDir(LuceneTestCase.getTestClass().getSimpleName()), NativeFSLockFactory.INSTANCE);

      Lock l = dir.obtainLock("commit");
      l.ensureValid();
      try {
        dir.obtainLock("commit");
        fail("succeeded in obtaining lock twice, didn't get exception");
      } catch (LockObtainFailedException expected) {}
      l.close();

      // Make sure we can obtain first one again:
      l = dir.obtainLock("commit");
      l.ensureValid();
      l.close();
    }

    
    // Verify: NativeFSLockFactory works correctly if the lock file exists
    public void testNativeFSLockFactoryLockExists() throws IOException {
      Path tempDir = createTempDir(LuceneTestCase.getTestClass().getSimpleName());
      Path lockFile = tempDir.resolve("test.lock");
      Files.createFile(lockFile);
      
      Directory dir = FSDirectory.open(tempDir, NativeFSLockFactory.INSTANCE);
      Lock l = dir.obtainLock("test.lock");
      l.close();
      Files.deleteIfExists(lockFile);
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
                    writer = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())).setOpenMode(OpenMode.APPEND));
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
                  searcher.search(query, 1000);
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

    class MockLockFactory extends LockFactory {

        public Map<String,Lock> locksCreated = Collections.synchronizedMap(new HashMap<String,Lock>());

        @Override
        public synchronized Lock obtainLock(Directory dir, String lockName) {
            Lock lock = new MockLock();
            locksCreated.put(lockName, lock);
            return lock;
        }

        public class MockLock extends Lock {

            @Override
            public void close() {
                // do nothing
            }

            @Override
            public void ensureValid() throws IOException {
              // do nothing
            }

        }
    }

    private void addDoc(IndexWriter writer) throws IOException {
        Document doc = new Document();
        doc.add(newTextField("content", "aaa", Field.Store.NO));
        writer.addDocument(doc);
    }
}
