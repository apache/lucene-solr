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


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestSameScoresWithThreads extends LuceneTestCase {

  public void test() throws Exception {
    final Directory dir = newDirectory();
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));
    final RandomIndexWriter w = new RandomIndexWriter(random(), dir, analyzer);
    LineFileDocs docs = new LineFileDocs(random());
    int charsToIndex = atLeast(100000);
    int charsIndexed = 0;
    //System.out.println("bytesToIndex=" + charsToIndex);
    while(charsIndexed < charsToIndex) {
      Document doc = docs.nextDoc();
      charsIndexed += doc.get("body").length();
      w.addDocument(doc);
      //System.out.println("  bytes=" + charsIndexed + " add: " + doc);
    }
    IndexReader r = w.getReader();
    //System.out.println("numDocs=" + r.numDocs());
    w.close();

    final IndexSearcher s = newSearcher(r);
    Terms terms = MultiTerms.getTerms(r, "body");
    int termCount = 0;
    TermsEnum termsEnum = terms.iterator();
    while(termsEnum.next() != null) {
      termCount++;
    }
    assertTrue(termCount > 0);
    
    // Target ~10 terms to search:
    double chance = 10.0 / termCount;
    termsEnum = terms.iterator();
    final Map<BytesRef,TopDocs> answers = new HashMap<>();
    while(termsEnum.next() != null) {
      if (random().nextDouble() <= chance) {
        BytesRef term = BytesRef.deepCopyOf(termsEnum.term());
        answers.put(term,
                    s.search(new TermQuery(new Term("body", term)), 100));
      }
    }

    if (!answers.isEmpty()) {
      final CountDownLatch startingGun = new CountDownLatch(1);
      int numThreads = TEST_NIGHTLY ? TestUtil.nextInt(random(), 2, 5) : 2;
      Thread[] threads = new Thread[numThreads];
      for(int threadID=0;threadID<numThreads;threadID++) {
        Thread thread = new Thread() {
            @Override
            public void run() {
              try {
                startingGun.await();
                for(int i=0;i<20;i++) {
                  List<Map.Entry<BytesRef,TopDocs>> shuffled = new ArrayList<>(answers.entrySet());
                  Collections.shuffle(shuffled, random());
                  for(Map.Entry<BytesRef,TopDocs> ent : shuffled) {
                    TopDocs actual = s.search(new TermQuery(new Term("body", ent.getKey())), 100);
                    TopDocs expected = ent.getValue();
                    assertEquals(expected.totalHits.value, actual.totalHits.value);
                    assertEquals("query=" + ent.getKey().utf8ToString(), expected.scoreDocs.length, actual.scoreDocs.length);
                    for(int hit=0;hit<expected.scoreDocs.length;hit++) {
                      assertEquals(expected.scoreDocs[hit].doc, actual.scoreDocs[hit].doc);
                      // Floats really should be identical:
                      assertTrue(expected.scoreDocs[hit].score == actual.scoreDocs[hit].score);
                    }
                  }
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          };
        threads[threadID] = thread;
        thread.start();
      }
      startingGun.countDown();
      for(Thread thread : threads) {
        thread.join();
      }
    }
    docs.close();
    r.close();
    dir.close();
  }
}
