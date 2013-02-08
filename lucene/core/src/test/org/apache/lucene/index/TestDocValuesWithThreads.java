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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;

@SuppressCodecs("Lucene3x")
public class TestDocValuesWithThreads extends LuceneTestCase {

  public void test() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    final List<Long> numbers = new ArrayList<Long>();
    final List<BytesRef> binary = new ArrayList<BytesRef>();
    final List<BytesRef> sorted = new ArrayList<BytesRef>();
    final int numDocs = atLeast(100);
    for(int i=0;i<numDocs;i++) {
      Document d = new Document();
      long number = random().nextLong();
      d.add(new NumericDocValuesField("number", number));
      BytesRef bytes = new BytesRef(_TestUtil.randomRealisticUnicodeString(random()));
      d.add(new BinaryDocValuesField("bytes", bytes));
      binary.add(bytes);
      bytes = new BytesRef(_TestUtil.randomRealisticUnicodeString(random()));
      d.add(new SortedDocValuesField("sorted", bytes));
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
              BytesRef scratch2 = new BytesRef();
              for(int iter=0;iter<iters;iter++) {
                int docID = threadRandom.nextInt(numDocs);
                switch(threadRandom.nextInt(6)) {
                case 0:
                  assertEquals((byte) numbers.get(docID).longValue(), FieldCache.DEFAULT.getBytes(ar, "number", false).get(docID));
                  break;
                case 1:
                  assertEquals((short) numbers.get(docID).longValue(), FieldCache.DEFAULT.getShorts(ar, "number", false).get(docID));
                  break;
                case 2:
                  assertEquals((int) numbers.get(docID).longValue(), FieldCache.DEFAULT.getInts(ar, "number", false).get(docID));
                  break;
                case 3:
                  assertEquals(numbers.get(docID).longValue(), FieldCache.DEFAULT.getLongs(ar, "number", false).get(docID));
                  break;
                case 4:
                  assertEquals(Float.intBitsToFloat((int) numbers.get(docID).longValue()), FieldCache.DEFAULT.getFloats(ar, "number", false).get(docID), 0.0f);
                  break;
                case 5:
                  assertEquals(Double.longBitsToDouble(numbers.get(docID).longValue()), FieldCache.DEFAULT.getDoubles(ar, "number", false).get(docID), 0.0);
                  break;
                }
                bdv.get(docID, scratch);
                assertEquals(binary.get(docID), scratch);
                // Cannot share a single scratch against two "sources":
                sdv.get(docID, scratch2);
                assertEquals(sorted.get(docID), scratch2);
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
  
  public void test2() throws Exception {
    Random random = random();
    final int NUM_DOCS = atLeast(100);
    final Directory dir = newDirectory();
    final RandomIndexWriter writer = new RandomIndexWriter(random, dir);
    final boolean allowDups = random.nextBoolean();
    final Set<String> seen = new HashSet<String>();
    if (VERBOSE) {
      System.out.println("TEST: NUM_DOCS=" + NUM_DOCS + " allowDups=" + allowDups);
    }
    int numDocs = 0;
    final List<BytesRef> docValues = new ArrayList<BytesRef>();

    // TODO: deletions
    while (numDocs < NUM_DOCS) {
      final String s;
      if (random.nextBoolean()) {
        s = _TestUtil.randomSimpleString(random);
      } else {
        s = _TestUtil.randomUnicodeString(random);
      }
      final BytesRef br = new BytesRef(s);

      if (!allowDups) {
        if (seen.contains(s)) {
          continue;
        }
        seen.add(s);
      }

      if (VERBOSE) {
        System.out.println("  " + numDocs + ": s=" + s);
      }
      
      final Document doc = new Document();
      doc.add(new SortedDocValuesField("stringdv", br));
      doc.add(new NumericDocValuesField("id", numDocs));
      docValues.add(br);
      writer.addDocument(doc);
      numDocs++;

      if (random.nextInt(40) == 17) {
        // force flush
        writer.getReader().close();
      }
    }

    writer.forceMerge(1);
    final DirectoryReader r = writer.getReader();
    writer.close();
    
    final AtomicReader sr = getOnlySegmentReader(r);

    final long END_TIME = System.currentTimeMillis() + (TEST_NIGHTLY ? 30 : 1);

    final int NUM_THREADS = _TestUtil.nextInt(random(), 1, 10);
    Thread[] threads = new Thread[NUM_THREADS];
    for(int thread=0;thread<NUM_THREADS;thread++) {
      threads[thread] = new Thread() {
          @Override
          public void run() {
            Random random = random();            
            final SortedDocValues stringDVDirect;
            final NumericDocValues docIDToID;
            try {
              stringDVDirect = sr.getSortedDocValues("stringdv");
              docIDToID = sr.getNumericDocValues("id");
              assertNotNull(stringDVDirect);
            } catch (IOException ioe) {
              throw new RuntimeException(ioe);
            }
            while(System.currentTimeMillis() < END_TIME) {
              final SortedDocValues source;
              source = stringDVDirect;
              final BytesRef scratch = new BytesRef();

              for(int iter=0;iter<100;iter++) {
                final int docID = random.nextInt(sr.maxDoc());
                source.get(docID, scratch);
                assertEquals(docValues.get((int) docIDToID.get(docID)), scratch);
              }
            }
          }
        };
      threads[thread].start();
    }

    for(Thread thread : threads) {
      thread.join();
    }

    r.close();
    dir.close();
  }

}
