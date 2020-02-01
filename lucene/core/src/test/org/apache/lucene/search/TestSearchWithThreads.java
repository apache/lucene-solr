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


import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

public class TestSearchWithThreads extends LuceneTestCase {

  public void test() throws Exception {
    final int numThreads = TEST_NIGHTLY ? 5 : 2;
    final int numSearches = TEST_NIGHTLY ? atLeast(2000) : atLeast(500);
    final int numDocs = TEST_NIGHTLY ? atLeast(10000) : atLeast(200);

    final Directory dir = newDirectory();
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir);

    final Document doc = new Document();
    final Field body = newTextField("body", "", Field.Store.NO);
    doc.add(body);
    final StringBuilder sb = new StringBuilder();
    for(int docCount=0;docCount<numDocs;docCount++) {
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

    final IndexSearcher s = newSearcher(r);

    final AtomicBoolean failed = new AtomicBoolean();
    final AtomicLong netSearch = new AtomicLong();

    Thread[] threads = new Thread[numThreads];
    for (int threadID = 0; threadID < numThreads; threadID++) {
      threads[threadID] = new Thread() {
        TotalHitCountCollector col = new TotalHitCountCollector();
          @Override
          public void run() {
            try {
              long totHits = 0;
              long totSearch = 0;
              for (; totSearch < numSearches & !failed.get(); totSearch++) {
                s.search(new TermQuery(new Term("body", "aaa")), col);
                totHits += col.getTotalHits();
                s.search(new TermQuery(new Term("body", "bbb")), col);
                totHits += col.getTotalHits();
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
    
    if (VERBOSE) System.out.println(numThreads + " threads did " + netSearch.get() + " searches");

    r.close();
    dir.close();
  }
}
