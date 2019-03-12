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
package org.apache.solr.uninverting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCase;

// TODO: what happened to this test... its not actually uninverting?
public class TestFieldCacheWithThreads extends SolrTestCase {

  public void test() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    final List<Long> numbers = new ArrayList<>();
    final List<BytesRef> binary = new ArrayList<>();
    final List<BytesRef> sorted = new ArrayList<>();
    final int numDocs = atLeast(100);
    for(int i=0;i<numDocs;i++) {
      Document d = new Document();
      long number = random().nextLong();
      d.add(new NumericDocValuesField("number", number));
      BytesRef bytes = new BytesRef(TestUtil.randomRealisticUnicodeString(random()));
      d.add(new BinaryDocValuesField("bytes", bytes));
      binary.add(bytes);
      bytes = new BytesRef(TestUtil.randomRealisticUnicodeString(random()));
      d.add(new SortedDocValuesField("sorted", bytes));
      sorted.add(bytes);
      w.addDocument(d);
      numbers.add(number);
    }

    w.forceMerge(1);
    final IndexReader r = DirectoryReader.open(w);
    w.close();

    assertEquals(1, r.leaves().size());
    final LeafReader ar = r.leaves().get(0).reader();

    int numThreads = TestUtil.nextInt(random(), 2, 5);
    List<Thread> threads = new ArrayList<>();
    final CountDownLatch startingGun = new CountDownLatch(1);
    for(int t=0;t<numThreads;t++) {
      final Random threadRandom = new Random(random().nextLong());
      Thread thread = new Thread() {
          @Override
          public void run() {
            try {
              startingGun.await();
              int iters = atLeast(1000);
              for(int iter=0;iter<iters;iter++) {
                int docID = threadRandom.nextInt(numDocs);
                switch(threadRandom.nextInt(4)) {
                case 0:
                  {
                    NumericDocValues values = FieldCache.DEFAULT.getNumerics(ar, "number", FieldCache.INT_POINT_PARSER);
                    assertEquals(docID, values.advance(docID));
                    assertEquals(numbers.get(docID).longValue(), values.longValue());
                  }
                  break;
                case 1:
                  {
                    NumericDocValues values = FieldCache.DEFAULT.getNumerics(ar, "number", FieldCache.LONG_POINT_PARSER);
                    assertEquals(docID, values.advance(docID));
                    assertEquals(numbers.get(docID).longValue(), values.longValue());
                  }
                  break;
                case 2:
                  {
                    NumericDocValues values = FieldCache.DEFAULT.getNumerics(ar, "number", FieldCache.FLOAT_POINT_PARSER);
                    assertEquals(docID, values.advance(docID));
                    assertEquals(numbers.get(docID).longValue(), values.longValue());
                  }
                  break;
                case 3:
                  {
                    NumericDocValues values = FieldCache.DEFAULT.getNumerics(ar, "number", FieldCache.DOUBLE_POINT_PARSER);
                    assertEquals(docID, values.advance(docID));
                    assertEquals(numbers.get(docID).longValue(), values.longValue());
                  }
                  break;
                }
                BinaryDocValues bdv = FieldCache.DEFAULT.getTerms(ar, "bytes");
                assertEquals(docID, bdv.advance(docID));
                assertEquals(binary.get(docID), bdv.binaryValue());
                SortedDocValues sdv = FieldCache.DEFAULT.getTermsIndex(ar, "sorted");
                assertEquals(docID, sdv.advance(docID));
                assertEquals(sorted.get(docID), sdv.binaryValue());
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
    final Set<String> seen = new HashSet<>();
    if (VERBOSE) {
      System.out.println("TEST: NUM_DOCS=" + NUM_DOCS + " allowDups=" + allowDups);
    }
    int numDocs = 0;
    final List<BytesRef> docValues = new ArrayList<>();

    // TODO: deletions
    while (numDocs < NUM_DOCS) {
      final String s;
      if (random.nextBoolean()) {
        s = TestUtil.randomSimpleString(random);
      } else {
        s = TestUtil.randomUnicodeString(random);
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
    
    final LeafReader sr = getOnlyLeafReader(r);

    final long END_TIME = System.nanoTime() + TimeUnit.NANOSECONDS.convert((TEST_NIGHTLY ? 30 : 1), TimeUnit.SECONDS);

    final int NUM_THREADS = TestUtil.nextInt(random(), 1, 10);
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
            int[] docIDToIDArray = new int[sr.maxDoc()];
            for(int i=0;i<sr.maxDoc();i++) {
              try {
                assertEquals(i, docIDToID.nextDoc());
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
              try {
                docIDToIDArray[i] = (int) docIDToID.longValue();
              } catch (IOException ioe) {
                throw new RuntimeException(ioe);
              }
            }
            while(System.nanoTime() < END_TIME) {
              for(int iter=0;iter<100;iter++) {
                final int docID = random.nextInt(sr.maxDoc());
                try {
                  SortedDocValues dvs = sr.getSortedDocValues("stringdv");
                  assertEquals(docID, dvs.advance(docID));
                  assertEquals(docValues.get(docIDToIDArray[docID]), dvs.binaryValue());
                } catch (IOException ioe) {
                  throw new RuntimeException(ioe);
                }
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
