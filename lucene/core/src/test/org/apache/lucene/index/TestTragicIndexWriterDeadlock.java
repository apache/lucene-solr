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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

public class TestTragicIndexWriterDeadlock extends LuceneTestCase {

  public void testDeadlockExcNRTReaderCommit() throws Exception {
    MockDirectoryWrapper dir = newMockDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    if (iwc.getMergeScheduler() instanceof ConcurrentMergeScheduler) {
      iwc.setMergeScheduler(new SuppressingConcurrentMergeScheduler() {
          @Override
          protected boolean isOK(Throwable th) {
            return true;
          }
        });
    }
    final IndexWriter w = new IndexWriter(dir, iwc);
    final CountDownLatch startingGun = new CountDownLatch(1);
    final AtomicBoolean done = new AtomicBoolean();
    Thread commitThread = new Thread() {
        @Override
        public void run() {
          try {
            startingGun.await();
            while (done.get() == false) {
              w.addDocument(new Document());
              w.commit();
            }
          } catch (Throwable t) {
            done.set(true);
            //System.out.println("commit exc:");
            //t.printStackTrace(System.out);
          }
        }
      };
    commitThread.start();
    final DirectoryReader r0 = DirectoryReader.open(w);
    Thread nrtThread = new Thread() {
        @Override
        public void run() {
          DirectoryReader r = r0;
          try {
            try {
              startingGun.await();
              while (done.get() == false) {
                DirectoryReader oldReader = r;                  
                DirectoryReader r2 = DirectoryReader.openIfChanged(oldReader);
                if (r2 != null) {
                  r = r2;
                  oldReader.decRef();       
                }
              }
            } finally {
              r.close();
            }
          } catch (Throwable t) {
            done.set(true);
            //System.out.println("nrt exc:");
            //t.printStackTrace(System.out);
          }
        }
      };
    nrtThread.start();
    dir.setRandomIOExceptionRate(.1);
    startingGun.countDown();
    commitThread.join();
    nrtThread.join();
    dir.setRandomIOExceptionRate(0.0);
    w.close();
    dir.close();
  }

  // LUCENE-7570
  public void testDeadlockStalledMerges() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();

    // so we merge every 2 segments:
    LogMergePolicy mp = new LogDocMergePolicy();
    mp.setMergeFactor(2);
    iwc.setMergePolicy(mp);
    CountDownLatch done = new CountDownLatch(1);
    ConcurrentMergeScheduler cms = new ConcurrentMergeScheduler() {
        @Override
        protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
          // let merge takes forever, until commit thread is stalled
          try {
            done.await();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
          }
          super.doMerge(mergeSource, merge);
        }

        @Override
        protected synchronized void doStall() {
          done.countDown();
          super.doStall();
        }

        @Override
        protected void handleMergeException(Throwable exc) {
        }
      };

    // so we stall once the 2nd merge wants to run:
    cms.setMaxMergesAndThreads(1, 1);
    iwc.setMergeScheduler(cms);

    // so we write a segment every 2 indexed docs:
    iwc.setMaxBufferedDocs(2);

    final IndexWriter w = new IndexWriter(dir, iwc) {
      @Override
      protected void mergeSuccess(MergePolicy.OneMerge merge) {
        // tragedy strikes!
        throw new OutOfMemoryError();
      }
      };

    w.addDocument(new Document());
    w.addDocument(new Document());
    // w writes first segment
    w.addDocument(new Document());
    w.addDocument(new Document());
    // w writes second segment, and kicks off merge, that takes forever (done.await)
    w.addDocument(new Document());
    w.addDocument(new Document());
    // w writes third segment
    w.addDocument(new Document());
    w.commit();
    // w writes fourth segment, and commit flushes and kicks off merge that stalls
    w.close();
    dir.close();
  }
}
