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
package org.apache.lucene.store;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Path;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.PrintStreamInfoStream;
import org.apache.lucene.util.TestUtil;

/** Base class for per-LockFactory tests. */
public abstract class BaseLockFactoryTestCase extends LuceneTestCase {
  
  /** Subclass returns the Directory to be tested; if it's
   *  an FS-based directory it should point to the specified
   *  path, else it can ignore it. */
  protected abstract Directory getDirectory(Path path) throws IOException;
  
  /** Test obtaining and releasing locks, checking validity */
  public void testBasics() throws IOException {
    Path tempPath = createTempDir();
    Directory dir = getDirectory(tempPath);
    
    Lock l = dir.obtainLock("commit");
    // shouldn't be able to get the lock twice
    expectThrows(LockObtainFailedException.class, () -> {      
      dir.obtainLock("commit");
    });
    l.close();
    
    // Make sure we can obtain first one again:
    l = dir.obtainLock("commit");
    l.close();
    
    dir.close();
  }
  
  /** Test closing locks twice */
  public void testDoubleClose() throws IOException {
    Path tempPath = createTempDir();
    Directory dir = getDirectory(tempPath);
    
    Lock l = dir.obtainLock("commit");
    l.close();
    l.close(); // close again, should be no exception
    
    dir.close();
  }
  
  /** Test ensureValid returns true after acquire */
  public void testValidAfterAcquire() throws IOException {
    Path tempPath = createTempDir();
    Directory dir = getDirectory(tempPath);
    Lock l = dir.obtainLock("commit");
    l.ensureValid(); // no exception
    l.close();
    dir.close();
  }
  
  /** Test ensureValid throws exception after close */
  public void testInvalidAfterClose() throws IOException {
    Path tempPath = createTempDir();
    Directory dir = getDirectory(tempPath);
    
    Lock l = dir.obtainLock("commit");
    l.close();

    expectThrows(AlreadyClosedException.class, () -> {      
      l.ensureValid();
    });

    dir.close();
  }
  
  public void testObtainConcurrently() throws InterruptedException, IOException {
    Path tempPath = createTempDir();
    final Directory directory = getDirectory(tempPath);
    final AtomicBoolean running = new AtomicBoolean(true);
    final AtomicInteger atomicCounter = new AtomicInteger(0);
    final ReentrantLock assertingLock = new ReentrantLock();
    int numThreads = 2 + random().nextInt(10);
    final int runs = atLeast(10000);
    CyclicBarrier barrier = new CyclicBarrier(numThreads);
    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          try {
            barrier.await();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          while (running.get()) {
            try (Lock lock = directory.obtainLock("foo.lock")) {
              assertFalse(assertingLock.isLocked());
              if (assertingLock.tryLock()) {
                assertingLock.unlock();
              } else {
                fail();
              }
              assert lock != null; // stupid compiler
            } catch (IOException ex) {
              //
            }
            if (atomicCounter.incrementAndGet() > runs) {
              running.set(false);
            }
          }
        }
      };
      threads[i].start();
    }
    
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
    directory.close();
  }
  
  // Verify: do stress test, by opening IndexReaders and
  // IndexWriters over & over in 2 threads and making sure
  // no unexpected exceptions are raised:
  public void testStressLocks() throws Exception {
    Path tempPath = createTempDir();
    assumeFalse("cannot handle buggy Files.delete", TestUtil.hasWindowsFS(tempPath));

    Directory dir = getDirectory(tempPath);

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
  }
  
  private void addDoc(IndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("content", "aaa", Field.Store.NO));
    writer.addDocument(doc);
  }
  
  private class WriterThread extends Thread { 
    private Directory dir;
    private int numIteration;
    public boolean hitException = false;
    public WriterThread(int numIteration, Directory dir) {
      this.numIteration = numIteration;
      this.dir = dir;
    }

    private String toString(ByteArrayOutputStream baos) {
      try {
        return baos.toString("UTF8");
      } catch (UnsupportedEncodingException uee) {
        // shouldn't happen
        throw new RuntimeException(uee);
      }
    }
  
    @Override
    public void run() {
      IndexWriter writer = null;
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      for(int i=0;i<this.numIteration;i++) {
        if (VERBOSE) {
          System.out.println("TEST: WriterThread iter=" + i);
        }

        IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));

        // We only print the IW infoStream output on exc, below:
        PrintStream printStream;
        try {
          printStream = new PrintStream(baos, true, "UTF8");
        } catch (UnsupportedEncodingException uee) {
          // shouldn't happen
          throw new RuntimeException(uee);
        }

        iwc.setInfoStream(new PrintStreamInfoStream(printStream));

        printStream.println("\nTEST: WriterThread iter=" + i);
        iwc.setOpenMode(OpenMode.APPEND);
        try {
          writer = new IndexWriter(dir, iwc);
        } catch (Throwable t) {
          if (Constants.WINDOWS && t instanceof AccessDeniedException) {
            // LUCENE-6684: suppress this: on Windows, a file in the curious "pending delete" state can
            // cause this exc on IW init, where one thread/process deleted an old
            // segments_N, but the delete hasn't finished yet because other threads/processes
            // still have it open
            printStream.println("TEST: AccessDeniedException on init witer");
            t.printStackTrace(printStream);
          } else {
            hitException = true;
            System.out.println("Stress Test Index Writer: creation hit unexpected exception: " + t.toString());
            t.printStackTrace(System.out);
            System.out.println(toString(baos));
          }
          break;
        }
        if (writer != null) {
          try {
            addDoc(writer);
          } catch (Throwable t) {
            hitException = true;
            System.out.println("Stress Test Index Writer: addDoc hit unexpected exception: " + t.toString());
            t.printStackTrace(System.out);
            System.out.println(toString(baos));
            break;
          }
          try {
            writer.close();
          } catch (Throwable t) {
            hitException = true;
            System.out.println("Stress Test Index Writer: close hit unexpected exception: " + t.toString());
            t.printStackTrace(System.out);
            System.out.println(toString(baos));
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
  
}
