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
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.PackedLongDocValuesField;
import org.apache.lucene.document.SortedBytesDocValuesField;
import org.apache.lucene.document.StraightBytesDocValuesField;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

public class TestDocValuesWithThreads extends LuceneTestCase {

  public void test() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    // nocommit binary, sorted too
    final List<Long> numbers = new ArrayList<Long>();
    final List<BytesRef> binary = new ArrayList<BytesRef>();
    final List<BytesRef> sorted = new ArrayList<BytesRef>();
    final int numDocs = atLeast(100);
    for(int i=0;i<numDocs;i++) {
      Document d = new Document();
      long number = random().nextLong();
      d.add(new PackedLongDocValuesField("number", number));
      BytesRef bytes = new BytesRef(_TestUtil.randomRealisticUnicodeString(random()));
      d.add(new StraightBytesDocValuesField("bytes", bytes));
      binary.add(bytes);
      bytes = new BytesRef(_TestUtil.randomRealisticUnicodeString(random()));
      d.add(new SortedBytesDocValuesField("sorted", bytes));
      sorted.add(bytes);
      w.addDocument(d);
      numbers.add(number);
    }

    w.forceMerge(1);
    final IndexReader r = w.getReader();
    w.close();

    assertEquals(1, r.leaves().size());
    final AtomicReader ar = r.leaves().get(0).reader();

    int numThreads = _TestUtil.nextInt(random(), 2, 5);
    List<Thread> threads = new ArrayList<Thread>();
    final CountDownLatch startingGun = new CountDownLatch(1);
    for(int t=0;t<numThreads;t++) {
      final Random threadRandom = new Random(random().nextLong());
      Thread thread = new Thread() {
          @Override
          public void run() {
            try {
              //NumericDocValues ndv = ar.getNumericDocValues("number");
              FieldCache.Longs ndv = FieldCache.DEFAULT.getLongs(ar, "number", false);
              //BinaryDocValues bdv = ar.getBinaryDocValues("bytes");
              BinaryDocValues bdv = FieldCache.DEFAULT.getTerms(ar, "bytes");
              SortedDocValues sdv = FieldCache.DEFAULT.getTermsIndex(ar, "sorted");
              startingGun.await();
              int iters = atLeast(1000);
              BytesRef scratch = new BytesRef();
              for(int iter=0;iter<iters;iter++) {
                int docID = threadRandom.nextInt(numDocs);
                assertEquals(numbers.get(docID).longValue(), ndv.get(docID));
                bdv.get(docID, scratch);
                assertEquals(binary.get(docID), scratch);
                sdv.get(docID, scratch);
                assertEquals(sorted.get(docID), scratch);
              }
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };
      thread.start();
      threads.add(thread);
    }

    startingGun.countDown();

    for(Thread thread : threads) {
      thread.join();
    }

    r.close();
    dir.close();
  }

}
