package org.apache.lucene.index;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Simple test that adds numeric terms, where each term has the 
 * docFreq of its integer value, and checks that the docFreq is correct. 
 */
@SuppressCodecs({"Direct", "Memory"}) // at night this makes like 200k/300k docs and will make Direct's heart beat!
public class TestBagOfPostings extends LuceneTestCase {
  public void test() throws Exception {
    List<String> postingsList = new ArrayList<String>();
    int numTerms = atLeast(300);
    final int maxTermsPerDoc = _TestUtil.nextInt(random(), 10, 20);

    boolean isSimpleText = "SimpleText".equals(_TestUtil.getPostingsFormat("field"));

    IndexWriterConfig iwc = newIndexWriterConfig(random(), TEST_VERSION_CURRENT, new MockAnalyzer(random()));

    if ((isSimpleText || iwc.getMergePolicy() instanceof MockRandomMergePolicy) && (TEST_NIGHTLY || RANDOM_MULTIPLIER > 1)) {
      // Otherwise test can take way too long (> 2 hours)
      numTerms /= 2;
    }

    if (VERBOSE) {
      System.out.println("maxTermsPerDoc=" + maxTermsPerDoc);
      System.out.println("numTerms=" + numTerms);
    }

    for (int i = 0; i < numTerms; i++) {
      String term = Integer.toString(i);
      for (int j = 0; j < i; j++) {
        postingsList.add(term);
      }
    }
    Collections.shuffle(postingsList, random());

    final ConcurrentLinkedQueue<String> postings = new ConcurrentLinkedQueue<String>(postingsList);

    Directory dir = newFSDirectory(_TestUtil.getTempDir("bagofpostings"));
    final RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

    int threadCount = _TestUtil.nextInt(random(), 1, 5);
    if (VERBOSE) {
      System.out.println("config: " + iw.w.getConfig());
      System.out.println("threadCount=" + threadCount);
    }

    Thread[] threads = new Thread[threadCount];
    final CountDownLatch startingGun = new CountDownLatch(1);

    for(int threadID=0;threadID<threadCount;threadID++) {
      threads[threadID] = new Thread() {
          @Override
          public void run() {
            try {
              Document document = new Document();
              Field field = newTextField("field", "", Field.Store.NO);
              document.add(field);
              startingGun.await();
              while (!postings.isEmpty()) {
                StringBuilder text = new StringBuilder();
                Set<String> visited = new HashSet<String>();
                for (int i = 0; i < maxTermsPerDoc; i++) {
                  String token = postings.poll();
                  if (token == null) {
                    break;
                  }
                  if (visited.contains(token)) {
                    // Put it back:
                    postings.add(token);
                    break;
                  }
                  text.append(' ');
                  text.append(token);
                  visited.add(token);
                }
                field.setStringValue(text.toString());
                iw.addDocument(document);
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      threads[threadID].start();
    }
    startingGun.countDown();
    for(Thread t : threads) {
      t.join();
    }
    
    iw.forceMerge(1);
    DirectoryReader ir = iw.getReader();
    assertEquals(1, ir.leaves().size());
    AtomicReader air = ir.leaves().get(0).reader();
    Terms terms = air.terms("field");
    // numTerms-1 because there cannot be a term 0 with 0 postings:
    assertEquals(numTerms-1, air.fields().getUniqueTermCount());
    if (!PREFLEX_IMPERSONATION_IS_ACTIVE) {
      assertEquals(numTerms-1, terms.size());
    }
    TermsEnum termsEnum = terms.iterator(null);
    BytesRef term;
    while ((term = termsEnum.next()) != null) {
      int value = Integer.parseInt(term.utf8ToString());
      assertEquals(value, termsEnum.docFreq());
      // don't really need to check more than this, as CheckIndex
      // will verify that docFreq == actual number of documents seen
      // from a docsAndPositionsEnum.
    }
    ir.close();
    iw.close();
    dir.close();
  }
}
