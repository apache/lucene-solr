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
package org.apache.lucene.rangetree;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestRangeTree extends LuceneTestCase {

  // Controls what range of values we randomly generate, so we sometimes test narrow ranges:
  static long valueMid;
  static int valueRange;

  @BeforeClass
  public static void beforeClass() {
    if (random().nextBoolean()) {
      valueMid = random().nextLong();
      if (random().nextBoolean()) {
        // Wide range
        valueRange = TestUtil.nextInt(random(), 1, Integer.MAX_VALUE);
      } else {
        // Narrow range
        valueRange = TestUtil.nextInt(random(), 1, 100000);
      }
      if (VERBOSE) {
        System.out.println("TEST: will generate long values " + valueMid + " +/- " + valueRange);
      }
    } else {
      // All longs
      valueRange = 0;
      if (VERBOSE) {
        System.out.println("TEST: will generate all long values");
      }
    }
  }

  public void testAllEqual() throws Exception {
    int numValues = atLeast(10000);
    long value = randomValue();
    long[] values = new long[numValues];
    FixedBitSet missing = new FixedBitSet(numValues);

    if (VERBOSE) {
      System.out.println("TEST: use same value=" + value);
    }

    for(int docID=0;docID<numValues;docID++) {
      int x = random().nextInt(20);
      if (x == 17) {
        // Some docs don't have a point:
        missing.set(docID);
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " is missing");
        }
        continue;
      }
      values[docID] = value;
    }

    verify(missing, values);
  }

  public void testMultiValued() throws Exception {
    int numValues = atLeast(10000);
    // Every doc has 2 values:
    long[] values = new long[2*numValues];
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();

    // We rely on docID order:
    iwc.setMergePolicy(newLogMergePolicy());
    Codec codec = TestUtil.alwaysDocValuesFormat(getDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    for (int docID=0;docID<numValues;docID++) {
      Document doc = new Document();
      values[2*docID] = randomValue();
      doc.add(new SortedNumericDocValuesField("value", values[2*docID]));
      values[2*docID+1] = randomValue();
      doc.add(new SortedNumericDocValuesField("value", values[2*docID+1]));
      w.addDocument(doc);
    }

    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    IndexReader r = w.getReader();
    w.close();
    // We can't wrap with "exotic" readers because the NumericRangeTreeQuery must see the RangeTreeDVFormat:
    IndexSearcher s = newSearcher(r, false);

    int iters = atLeast(100);
    for (int iter=0;iter<iters;iter++) {
      long lower = randomValue();
      long upper = randomValue();

      if (upper < lower) {
        long x = lower;
        lower = upper;
        upper = x;
      }

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " value=" + lower + " TO " + upper);
      }

      boolean includeLower = random().nextBoolean();
      boolean includeUpper = random().nextBoolean();
      Query query = new NumericRangeTreeQuery("value", lower, includeLower, upper, includeUpper);

      final FixedBitSet hits = new FixedBitSet(r.maxDoc());
      s.search(query, new SimpleCollector() {

          private int docBase;

          @Override
          public boolean needsScores() {
            return false;
          }

          @Override
          protected void doSetNextReader(LeafReaderContext context) throws IOException {
            docBase = context.docBase;
          }

          @Override
          public void collect(int doc) {
            hits.set(docBase+doc);
          }
        });

      for(int docID=0;docID<values.length/2;docID++) {
        long docValue1 = values[2*docID];
        long docValue2 = values[2*docID+1];
        boolean expected = matches(lower, includeLower, upper, includeUpper, docValue1) ||
          matches(lower, includeLower, upper, includeUpper, docValue2);

        if (hits.get(docID) != expected) {
          fail("docID=" + docID + " docValue1=" + docValue1 + " docValue2=" + docValue2 + " expected " + expected + " but got: " + hits.get(docID));
        }
      }
    }
    r.close();
    dir.close();
  }

  public void testMultiValuedSortedSet() throws Exception {
    int numValues = atLeast(10000);
    // Every doc has 2 values:
    long[] values = new long[2*numValues];
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();

    // We rely on docID order:
    iwc.setMergePolicy(newLogMergePolicy());
    Codec codec = TestUtil.alwaysDocValuesFormat(getDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    for (int docID=0;docID<numValues;docID++) {
      Document doc = new Document();
      values[2*docID] = randomValue();
      doc.add(new SortedSetDocValuesField("value", longToBytes(values[2*docID])));
      values[2*docID+1] = randomValue();
      doc.add(new SortedSetDocValuesField("value", longToBytes(values[2*docID+1])));
      w.addDocument(doc);
    }

    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    IndexReader r = w.getReader();
    w.close();
    // We can't wrap with "exotic" readers because the NumericRangeTreeQuery must see the RangeTreeDVFormat:
    IndexSearcher s = newSearcher(r, false);

    int iters = atLeast(100);
    for (int iter=0;iter<iters;iter++) {
      long lower = randomValue();
      long upper = randomValue();

      if (upper < lower) {
        long x = lower;
        lower = upper;
        upper = x;
      }

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " value=" + lower + " TO " + upper);
      }

      boolean includeLower = random().nextBoolean();
      boolean includeUpper = random().nextBoolean();
      Query query = new SortedSetRangeTreeQuery("value", longToBytes(lower), includeLower, longToBytes(upper), includeUpper);

      final FixedBitSet hits = new FixedBitSet(r.maxDoc());
      s.search(query, new SimpleCollector() {

          private int docBase;

          @Override
          public boolean needsScores() {
            return false;
          }

          @Override
          protected void doSetNextReader(LeafReaderContext context) throws IOException {
            docBase = context.docBase;
          }

          @Override
          public void collect(int doc) {
            hits.set(docBase+doc);
          }
        });

      for(int docID=0;docID<values.length/2;docID++) {
        long docValue1 = values[2*docID];
        long docValue2 = values[2*docID+1];
        boolean expected = matches(lower, includeLower, upper, includeUpper, docValue1) ||
          matches(lower, includeLower, upper, includeUpper, docValue2);

        if (hits.get(docID) != expected) {
          fail("docID=" + docID + " docValue1=" + docValue1 + " docValue2=" + docValue2 + " expected " + expected + " but got: " + hits.get(docID));
        }
      }
    }
    r.close();
    dir.close();
  }

  public void testRandomTiny() throws Exception {
    // Make sure single-leaf-node case is OK:
    doTestRandom(10);
  }

  public void testRandomMedium() throws Exception {
    doTestRandom(10000);
  }

  @Nightly
  public void testRandomBig() throws Exception {
    doTestRandom(200000);
  }

  private void doTestRandom(int count) throws Exception {

    int numValues = atLeast(count);

    if (VERBOSE) {
      System.out.println("TEST: numValues=" + numValues);
    }

    long[] values = new long[numValues];
    FixedBitSet missing = new FixedBitSet(numValues);

    boolean haveRealDoc = false;

    for (int docID=0;docID<numValues;docID++) {
      int x = random().nextInt(20);
      if (x == 17) {
        // Some docs don't have a point:
        missing.set(docID);
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " is missing");
        }
        continue;
      }

      if (docID > 0 && x == 0 && haveRealDoc) {
        int oldDocID;
        while (true) {
          oldDocID = random().nextInt(docID);
          if (missing.get(oldDocID) == false) {
            break;
          }
        }
            
        // Identical to old value
        values[docID] = values[oldDocID];
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " value=" + values[docID] + " bytes=" + longToBytes(values[docID]) + " (same as doc=" + oldDocID + ")");
        }
      } else {
        values[docID] = randomValue();
        haveRealDoc = true;
        if (VERBOSE) {
          System.out.println("  doc=" + docID + " value=" + values[docID] + " bytes=" + longToBytes(values[docID]));
        }
      }
    }

    verify(missing, values);
  }

  private static void verify(final Bits missing, final long[] values) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();

    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < values.length/100) {
      iwc.setMaxBufferedDocs(values.length/100);
    }
    final DocValuesFormat dvFormat = getDocValuesFormat();
    Codec codec = new Lucene54Codec() {
        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
          if (field.equals("sn_value") || field.equals("ss_value")) {
            return dvFormat;
          } else {
            return super.getDocValuesFormatForField(field);
          }
        }
      };
    iwc.setCodec(codec);
    Directory dir;
    if (values.length > 100000) {
      dir = newFSDirectory(createTempDir("TestRangeTree"));
    } else {
      dir = newDirectory();
    }
    final Set<Integer> deleted = new HashSet<>();
    // RandomIndexWriter is too slow here:
    IndexWriter w = new IndexWriter(dir, iwc);
    for(int id=0;id<values.length;id++) {
      Document doc = new Document();
      doc.add(newStringField("id", ""+id, Field.Store.NO));
      doc.add(new NumericDocValuesField("id", id));
      if (missing.get(id) == false) {
        doc.add(new SortedNumericDocValuesField("sn_value", values[id]));
        doc.add(new SortedSetDocValuesField("ss_value", longToBytes(values[id])));
      }
      w.addDocument(doc);
      if (id > 0 && random().nextInt(100) == 42) {
        int idToDelete = random().nextInt(id);
        w.deleteDocuments(new Term("id", ""+idToDelete));
        deleted.add(idToDelete);
        if (VERBOSE) {
          System.out.println("  delete id=" + idToDelete);
        }
      }
    }
    if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("  forceMerge(1)");
      }
      w.forceMerge(1);
    }
    final IndexReader r = DirectoryReader.open(w, true);
    w.close();

    // We can't wrap with "exotic" readers because the NumericRangeTreeQuery must see the RangeTreeDVFormat:
    final IndexSearcher s = newSearcher(r, false);

    int numThreads = TestUtil.nextInt(random(), 2, 5);

    if (VERBOSE) {
      System.out.println("TEST: use " + numThreads + " query threads");
    }

    List<Thread> threads = new ArrayList<>();
    final int iters = atLeast(100);

    final CountDownLatch startingGun = new CountDownLatch(1);
    final AtomicBoolean failed = new AtomicBoolean();

    for(int i=0;i<numThreads;i++) {
      Thread thread = new Thread() {
          @Override
          public void run() {
            try {
              _run();
            } catch (Exception e) {
              failed.set(true);
              throw new RuntimeException(e);
            }
          }

          private void _run() throws Exception {
            startingGun.await();

            NumericDocValues docIDToID = MultiDocValues.getNumericValues(r, "id");

            for (int iter=0;iter<iters && failed.get() == false;iter++) {
              long lower = randomValue();
              long upper = randomValue();

              if (upper < lower) {
                long x = lower;
                lower = upper;
                upper = x;
              }

              if (VERBOSE) {
                System.out.println("\n" + Thread.currentThread().getName() + ": TEST: iter=" + iter + " value=" + lower + " TO " + upper);
              }

              boolean includeLower = random().nextBoolean();
              boolean includeUpper = random().nextBoolean();
              Query query;
              if (random().nextBoolean()) {
                query = new NumericRangeTreeQuery("sn_value", lower, includeLower, upper, includeUpper);
              } else {
                query = new SortedSetRangeTreeQuery("ss_value", longToBytes(lower), includeLower, longToBytes(upper), includeUpper);
              }

              if (VERBOSE) {
                System.out.println(Thread.currentThread().getName() + ":  using query: " + query);
              }

              final FixedBitSet hits = new FixedBitSet(r.maxDoc());
              s.search(query, new SimpleCollector() {

                  private int docBase;

                  @Override
                  public boolean needsScores() {
                    return false;
                  }

                  @Override
                  protected void doSetNextReader(LeafReaderContext context) throws IOException {
                    docBase = context.docBase;
                  }

                  @Override
                  public void collect(int doc) {
                    hits.set(docBase+doc);
                  }
                });

              if (VERBOSE) {
                System.out.println(Thread.currentThread().getName() + ":  hitCount: " + hits.cardinality());
              }
      
              for(int docID=0;docID<r.maxDoc();docID++) {
                int id = (int) docIDToID.get(docID);
                boolean expected = missing.get(id) == false && deleted.contains(id) == false && matches(lower, includeLower, upper, includeUpper, values[id]);
                if (hits.get(docID) != expected) {
                  // We do exact quantized comparison so the bbox query should never disagree:
                  fail(Thread.currentThread().getName() + ": iter=" + iter + " id=" + id + " docID=" + docID + " value=" + values[id] + " (range: " + lower + " TO " + upper + ") expected " + expected + " but got: " + hits.get(docID) + " deleted?=" + deleted.contains(id) + " query=" + query);
                  }
                }
              }
            }
        };
      thread.setName("T" + i);
      thread.start();
      threads.add(thread);
    }
    startingGun.countDown();
    for(Thread thread : threads) {
      thread.join();
    }
    IOUtils.close(r, dir);
  }

  private static boolean matches(long lower, boolean includeLower, long upper, boolean includeUpper, long value) {
    if (includeLower == false) {
      if (lower == Long.MAX_VALUE) {
        return false;
      }
      lower++;
    }
    if (includeUpper == false) {
      if (upper == Long.MIN_VALUE) {
        return false;
      }
      upper--;
    }

    return value >= lower && value <= upper;
  }

  private static long randomValue() {
    if (valueRange == 0) {
      return random().nextLong();
    } else {
      return valueMid + TestUtil.nextInt(random(), -valueRange, valueRange);
    }
  }

  public void testAccountableHasDelegate() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    Codec codec = TestUtil.alwaysDocValuesFormat(new RangeTreeDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("value", 187));
    w.addDocument(doc);
    IndexReader r = w.getReader();

    // We can't wrap with "exotic" readers because the query must see the RangeTreeDVFormat:
    IndexSearcher s = newSearcher(r, false);
    // Need to run a query so the DV field is really loaded:
    TopDocs hits = s.search(new NumericRangeTreeQuery("value", -30L, true, 187L, true), 1);
    assertEquals(1, hits.totalHits);
    assertTrue(Accountables.toString((Accountable) r.leaves().get(0).reader()).contains("delegate"));
    IOUtils.close(r, w, dir);
  }

  public void testMinMaxLong() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    Codec codec = TestUtil.alwaysDocValuesFormat(new RangeTreeDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("value", Long.MIN_VALUE));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new SortedNumericDocValuesField("value", Long.MAX_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    // We can't wrap with "exotic" readers because the query must see the RangeTreeDVFormat:
    IndexSearcher s = newSearcher(r, false);

    assertEquals(1, s.count(new NumericRangeTreeQuery("value", Long.MIN_VALUE, true, 0L, true)));
    assertEquals(1, s.count(new NumericRangeTreeQuery("value", 0L, true, Long.MAX_VALUE, true)));
    assertEquals(2, s.count(new NumericRangeTreeQuery("value", Long.MIN_VALUE, true, Long.MAX_VALUE, true)));

    IOUtils.close(r, w, dir);
  }

  public void testBasicSortedSet() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    Codec codec = TestUtil.alwaysDocValuesFormat(new RangeTreeDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("abc")));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("def")));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    // We can't wrap with "exotic" readers because the query must see the RangeTreeDVFormat:
    IndexSearcher s = newSearcher(r, false);

    assertEquals(1, s.count(new SortedSetRangeTreeQuery("value", new BytesRef("aaa"), true, new BytesRef("bbb"), true)));
    assertEquals(1, s.count(new SortedSetRangeTreeQuery("value", new BytesRef("c"), true, new BytesRef("e"), true)));
    assertEquals(2, s.count(new SortedSetRangeTreeQuery("value", new BytesRef("a"), true, new BytesRef("z"), true)));

    assertEquals(1, s.count(new SortedSetRangeTreeQuery("value", null, true, new BytesRef("abc"), true)));
    assertEquals(1, s.count(new SortedSetRangeTreeQuery("value", new BytesRef("a"), true, new BytesRef("abc"), true)));
    assertEquals(0, s.count(new SortedSetRangeTreeQuery("value", new BytesRef("a"), true, new BytesRef("abc"), false)));

    assertEquals(1, s.count(new SortedSetRangeTreeQuery("value", new BytesRef("def"), true, null, false)));
    assertEquals(1, s.count(new SortedSetRangeTreeQuery("value", new BytesRef("def"), true, new BytesRef("z"), true)));
    assertEquals(0, s.count(new SortedSetRangeTreeQuery("value", new BytesRef("def"), false, new BytesRef("z"), true)));

    IOUtils.close(r, w, dir);
  }

  public void testLongMinMaxNumeric() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    Codec codec = TestUtil.alwaysDocValuesFormat(new RangeTreeDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("value", Long.MIN_VALUE));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new SortedNumericDocValuesField("value", Long.MAX_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    // We can't wrap with "exotic" readers because the query must see the RangeTreeDVFormat:
    IndexSearcher s = newSearcher(r, false);

    assertEquals(2, s.count(new NumericRangeTreeQuery("value", Long.MIN_VALUE, true, Long.MAX_VALUE, true)));
    assertEquals(1, s.count(new NumericRangeTreeQuery("value", Long.MIN_VALUE, true, Long.MAX_VALUE, false)));
    assertEquals(1, s.count(new NumericRangeTreeQuery("value", Long.MIN_VALUE, false, Long.MAX_VALUE, true)));
    assertEquals(0, s.count(new NumericRangeTreeQuery("value", Long.MIN_VALUE, false, Long.MAX_VALUE, false)));

    assertEquals(2, s.count(new NumericRangeTreeQuery("value", null, true, null, true)));

    IOUtils.close(r, w, dir);
  }

  public void testLongMinMaxSortedSet() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    Codec codec = TestUtil.alwaysDocValuesFormat(new RangeTreeDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", longToBytes(Long.MIN_VALUE)));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", longToBytes(Long.MAX_VALUE)));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    // We can't wrap with "exotic" readers because the query must see the RangeTreeDVFormat:
    IndexSearcher s = newSearcher(r, false);

    assertEquals(2, s.count(new SortedSetRangeTreeQuery("value", longToBytes(Long.MIN_VALUE), true, longToBytes(Long.MAX_VALUE), true)));
    assertEquals(1, s.count(new SortedSetRangeTreeQuery("value", longToBytes(Long.MIN_VALUE), true, longToBytes(Long.MAX_VALUE), false)));
    assertEquals(1, s.count(new SortedSetRangeTreeQuery("value", longToBytes(Long.MIN_VALUE), false, longToBytes(Long.MAX_VALUE), true)));
    assertEquals(0, s.count(new SortedSetRangeTreeQuery("value", longToBytes(Long.MIN_VALUE), false, longToBytes(Long.MAX_VALUE), false)));

    assertEquals(2, s.count(new SortedSetRangeTreeQuery("value", null, true, null, true)));

    IOUtils.close(r, w, dir);
  }

  public void testSortedSetNoOrdsMatch() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    Codec codec = TestUtil.alwaysDocValuesFormat(new RangeTreeDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("a")));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new SortedSetDocValuesField("value", new BytesRef("z")));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    // We can't wrap with "exotic" readers because the query must see the RangeTreeDVFormat:
    IndexSearcher s = newSearcher(r, false);
    assertEquals(0, s.count(new SortedSetRangeTreeQuery("value", new BytesRef("m"), true, new BytesRef("n"), false)));

    assertEquals(2, s.count(new SortedSetRangeTreeQuery("value", null, true, null, true)));

    IOUtils.close(r, w, dir);
  }

  public void testNumericNoValuesMatch() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    Codec codec = TestUtil.alwaysDocValuesFormat(new RangeTreeDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("value", 17));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new SortedNumericDocValuesField("value", 22));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    // We can't wrap with "exotic" readers because the query must see the RangeTreeDVFormat:
    IndexSearcher s = newSearcher(r, false);
    assertEquals(0, s.count(new NumericRangeTreeQuery("value", 17L, true, 13L, false)));

    IOUtils.close(r, w, dir);
  }

  public void testNoDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    Codec codec = TestUtil.alwaysDocValuesFormat(new RangeTreeDocValuesFormat());
    iwc.setCodec(codec);
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    w.addDocument(new Document());

    IndexReader r = w.getReader();

    // We can't wrap with "exotic" readers because the query must see the RangeTreeDVFormat:
    IndexSearcher s = newSearcher(r, false);
    assertEquals(0, s.count(new NumericRangeTreeQuery("value", 17L, true, 13L, false)));

    IOUtils.close(r, w, dir);
  }

  private static BytesRef longToBytes(long v) {
    // Flip the sign bit so negative longs sort before positive longs:
    v ^= 0x8000000000000000L;
    byte[] bytes = new byte[8];
    bytes[0] = (byte) (v >> 56);
    bytes[1] = (byte) (v >> 48);
    bytes[2] = (byte) (v >> 40);
    bytes[3] = (byte) (v >> 32);
    bytes[4] = (byte) (v >> 24);
    bytes[5] = (byte) (v >> 16);
    bytes[6] = (byte) (v >> 8);
    bytes[7] = (byte) v;
    return new BytesRef(bytes);
  }

  /*
  private static long bytesToLong(BytesRef bytes) {
    long v = ((bytes.bytes[bytes.offset]&0xFFL) << 56) |
      ((bytes.bytes[bytes.offset+1]&0xFFL) << 48) |
      ((bytes.bytes[bytes.offset+2]&0xFFL) << 40) |
      ((bytes.bytes[bytes.offset+3]&0xFFL) << 32) |
      ((bytes.bytes[bytes.offset+4]&0xFFL) << 24) |
      ((bytes.bytes[bytes.offset+5]&0xFFL) << 16) |
      ((bytes.bytes[bytes.offset+6]&0xFFL) << 8) |
      (bytes.bytes[bytes.offset+7]&0xFFL);
    // Flip the sign bit back:
    return v ^ 0x8000000000000000L;
  }
  */

  private static DocValuesFormat getDocValuesFormat() {
    int maxPointsInLeaf = TestUtil.nextInt(random(), 16, 2048);
    int maxPointsSortInHeap = TestUtil.nextInt(random(), maxPointsInLeaf, 1024*1024);
    return new RangeTreeDocValuesFormat(maxPointsInLeaf, maxPointsSortInHeap);
  }
}
