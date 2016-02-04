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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestLiveFieldValues extends LuceneTestCase {
  public void test() throws Exception {

    Directory dir = newFSDirectory(createTempDir("livefieldupdates"));
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));

    final IndexWriter w = new IndexWriter(dir, iwc);

    final SearcherManager mgr = new SearcherManager(w, new SearcherFactory() {
        @Override
        public IndexSearcher newSearcher(IndexReader r, IndexReader previous) {
          return new IndexSearcher(r);
        }
      });

    final Integer missing = -1;

    final LiveFieldValues<IndexSearcher,Integer> rt = new LiveFieldValues<IndexSearcher,Integer>(mgr, missing) {
        @Override
        protected Integer lookupFromSearcher(IndexSearcher s, String id) throws IOException {
          TermQuery tq = new TermQuery(new Term("id", id));
          TopDocs hits = s.search(tq, 1);
          assertTrue(hits.totalHits <= 1);
          if (hits.totalHits == 0) {
            return null;
          } else {
            Document doc = s.doc(hits.scoreDocs[0].doc);
            return (Integer) doc.getField("field").numericValue();
          }
        }
    };

    int numThreads = TestUtil.nextInt(random(), 2, 5);
    if (VERBOSE) {
      System.out.println(numThreads + " threads");
    }

    final CountDownLatch startingGun = new CountDownLatch(1);
    List<Thread> threads = new ArrayList<>();

    final int iters = atLeast(1000);
    final int idCount = TestUtil.nextInt(random(), 100, 10000);

    final double reopenChance = random().nextDouble()*0.01;
    final double deleteChance = random().nextDouble()*0.25;
    final double addChance = random().nextDouble()*0.5;
    
    for(int t=0;t<numThreads;t++) {
      final int threadID = t;
      final Random threadRandom = new Random(random().nextLong());
      Thread thread = new Thread() {

          @Override
          public void run() {
            try {
              Map<String,Integer> values = new HashMap<>();
              List<String> allIDs = Collections.synchronizedList(new ArrayList<String>());

              startingGun.await();
              for(int iter=0; iter<iters;iter++) {
                // Add/update a document
                Document doc = new Document();
                // Threads must not update the same id at the
                // same time:
                if (threadRandom.nextDouble() <= addChance) {
                  String id = String.format(Locale.ROOT, "%d_%04x", threadID, threadRandom.nextInt(idCount));
                  Integer field = threadRandom.nextInt(Integer.MAX_VALUE);
                  doc.add(newStringField("id", new BytesRef(id), Field.Store.YES));
                  doc.add(new IntField("field", field.intValue(), Field.Store.YES));
                  w.updateDocument(new Term("id", id), doc);
                  rt.add(id, field);
                  if (values.put(id, field) == null) {
                    allIDs.add(id);
                  }
                }

                if (allIDs.size() > 0 && threadRandom.nextDouble() <= deleteChance) {
                  String randomID = allIDs.get(threadRandom.nextInt(allIDs.size()));
                  w.deleteDocuments(new Term("id", new BytesRef(randomID)));
                  rt.delete(randomID);
                  values.put(randomID, missing);
                }

                if (threadRandom.nextDouble() <= reopenChance || rt.size() > 10000) {
                  //System.out.println("refresh @ " + rt.size());
                  mgr.maybeRefresh();
                  if (VERBOSE) {
                    IndexSearcher s = mgr.acquire();
                    try {
                      System.out.println("TEST: reopen " + s);
                    } finally {
                      mgr.release(s);
                    }
                    System.out.println("TEST: " + values.size() + " values");
                  }
                }

                if (threadRandom.nextInt(10) == 7) {
                  assertEquals(null, rt.get("foo"));
                }

                if (allIDs.size() > 0) {
                  String randomID = allIDs.get(threadRandom.nextInt(allIDs.size()));
                  Integer expected = values.get(randomID);
                  if (expected == missing) {
                    expected = null;
                  }
                  assertEquals("id=" + randomID, expected, rt.get(randomID));
                }
              }
            } catch (Throwable t) {
              throw new RuntimeException(t);
            }
          }
        };
      threads.add(thread);
      thread.start();
    }

    startingGun.countDown();

    for(Thread thread : threads) {
      thread.join();
    }
    mgr.maybeRefresh();
    assertEquals(0, rt.size());

    rt.close();
    mgr.close();
    w.close();
    dir.close();
  }
}
