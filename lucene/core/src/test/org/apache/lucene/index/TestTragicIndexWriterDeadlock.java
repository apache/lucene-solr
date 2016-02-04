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


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
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
}
