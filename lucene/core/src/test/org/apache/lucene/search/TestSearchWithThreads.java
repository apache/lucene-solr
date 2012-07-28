package org.apache.lucene.search;

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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;

@SuppressCodecs({ "SimpleText", "Memory", "Direct" })
public class TestSearchWithThreads extends LuceneTestCase {
  int NUM_DOCS;
  final int NUM_SEARCH_THREADS = 5;
  int RUN_TIME_MSEC;
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    NUM_DOCS = atLeast(10000);
    RUN_TIME_MSEC = atLeast(1000);
  }

  public void test() throws Exception {
    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final long startTime = System.currentTimeMillis();

    // TODO: replace w/ the @nightly test data; make this
    // into an optional @nightly stress test
    final Document doc = new Document();
    final Field body = newTextField("body", "", Field.Store.NO);
    doc.add(body);
    final StringBuilder sb = new StringBuilder();
    for(int docCount=0;docCount<NUM_DOCS;docCount++) {
      final int numTerms = random().nextInt(10);
      for(int termCount=0;termCount<numTerms;termCount++) {
        sb.append(random().nextBoolean() ? "aaa" : "bbb");
        sb.append(' ');
      }
      body.setStringValue(sb.toString());
      w.addDocument(doc);
      sb.delete(0, sb.length());
    }
    final IndexReader r = w.getReader();
    w.close();

    final long endTime = System.currentTimeMillis();
    if (VERBOSE) System.out.println("BUILD took " + (endTime-startTime));

    final IndexSearcher s = newSearcher(r);

    final AtomicBoolean failed = new AtomicBoolean();
    final AtomicLong netSearch = new AtomicLong();

    Thread[] threads = new Thread[NUM_SEARCH_THREADS];
    for (int threadID = 0; threadID < NUM_SEARCH_THREADS; threadID++) {
      threads[threadID] = new Thread() {
        TotalHitCountCollector col = new TotalHitCountCollector();
          @Override
          public void run() {
            try {
              long totHits = 0;
              long totSearch = 0;
              long stopAt = System.currentTimeMillis() + RUN_TIME_MSEC;
              while(System.currentTimeMillis() < stopAt && !failed.get()) {
                s.search(new TermQuery(new Term("body", "aaa")), col);
                totHits += col.getTotalHits();
                s.search(new TermQuery(new Term("body", "bbb")), col);
                totHits += col.getTotalHits();
                totSearch++;
              }
              assertTrue(totSearch > 0 && totHits > 0);
              netSearch.addAndGet(totSearch);
            } catch (Exception exc) {
              failed.set(true);
              throw new RuntimeException(exc);
            }
          }
        };
      threads[threadID].setDaemon(true);
    }

    for (Thread t : threads) {
      t.start();
    }
    
    for (Thread t : threads) {
      t.join();
    }
    
    if (VERBOSE) System.out.println(NUM_SEARCH_THREADS + " threads did " + netSearch.get() + " searches");

    r.close();
    dir.close();
  }
}
