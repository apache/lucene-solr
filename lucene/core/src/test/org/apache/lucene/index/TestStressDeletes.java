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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestStressDeletes extends LuceneTestCase {

  /** Make sure that order of adds/deletes across threads is respected as
   *  long as each ID is only changed by one thread at a time. */
  public void test() throws Exception {
    final int numIDs = atLeast(100);
    final Object[] locks = new Object[numIDs];
    for(int i=0;i<locks.length;i++) {
      locks[i] = new Object();
    }

    Directory dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    final IndexWriter w = new IndexWriter(dir, iwc);
    final int iters = atLeast(2000);
    final Map<Integer,Boolean> exists = new ConcurrentHashMap<>();
    Thread[] threads = new Thread[TestUtil.nextInt(random(), 2, 6)];
    final CountDownLatch startingGun = new CountDownLatch(1);
    final int deleteMode = random().nextInt(3);
    for(int i=0;i<threads.length;i++) {
      threads[i] = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();
              for(int iter=0;iter<iters;iter++) {
                int id = random().nextInt(numIDs);
                synchronized (locks[id]) {
                  Boolean v = exists.get(id);
                  if (v == null || v.booleanValue() == false) {
                    Document doc = new Document();
                    doc.add(newStringField("id", ""+id, Field.Store.NO));
                    w.addDocument(doc);
                    exists.put(id, true);
                  } else {
                    if (deleteMode == 0) {
                      // Always delete by term
                      w.deleteDocuments(new Term("id", ""+id));
                    } else if (deleteMode == 1) {
                      // Always delete by query
                      w.deleteDocuments(new TermQuery(new Term("id", ""+id)));
                    } else {
                      // Mixed
                      if (random().nextBoolean()) {
                        w.deleteDocuments(new Term("id", ""+id));
                      } else {
                        w.deleteDocuments(new TermQuery(new Term("id", ""+id)));
                      }
                    }
                    exists.put(id, false);
                  }
                }
                if (random().nextInt(500) == 2) {
                  DirectoryReader.open(w, random().nextBoolean()).close();
                }
                if (random().nextInt(500) == 2) {
                  w.commit();
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      threads[i].start();
    }

    startingGun.countDown();
    for(Thread thread : threads) {
      thread.join();
    }

    IndexReader r = w.getReader();
    IndexSearcher s = newSearcher(r);
    for(Map.Entry<Integer,Boolean> ent : exists.entrySet()) {
      int id = ent.getKey();
      TopDocs hits = s.search(new TermQuery(new Term("id", ""+id)), 1);
      if (ent.getValue()) {
        assertEquals(1, hits.totalHits);
      } else {
        assertEquals(0, hits.totalHits);
      }
    }
    r.close();
    w.close();
    dir.close();
  }
}
