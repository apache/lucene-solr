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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PointsFormat;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.PointsWriter;
import org.apache.lucene.codecs.lucene60.Lucene60PointsReader;
import org.apache.lucene.codecs.lucene60.Lucene60PointsWriter;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.bkd.BKDWriter;
import org.junit.BeforeClass;

public class TestPointQueries extends LuceneTestCase {

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

  public void testBasicInts() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    doc.add(new IntPoint("point", -7));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new IntPoint("point", 0));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new IntPoint("point", 3));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(2, s.count(IntPoint.newRangeQuery("point", -8, 1)));
    assertEquals(3, s.count(IntPoint.newRangeQuery("point", -7, 3)));
    assertEquals(1, s.count(IntPoint.newExactQuery("point", -7)));
    assertEquals(0, s.count(IntPoint.newExactQuery("point", -6)));
    w.close();
    r.close();
    dir.close();
  }

  public void testBasicFloats() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    doc.add(new FloatPoint("point", -7.0f));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new FloatPoint("point", 0.0f));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new FloatPoint("point", 3.0f));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(2, s.count(FloatPoint.newRangeQuery("point", -8.0f, 1.0f)));
    assertEquals(3, s.count(FloatPoint.newRangeQuery("point", -7.0f, 3.0f)));
    assertEquals(1, s.count(FloatPoint.newExactQuery("point", -7.0f)));
    assertEquals(0, s.count(FloatPoint.newExactQuery("point", -6.0f)));
    w.close();
    r.close();
    dir.close();
  }

  public void testBasicLongs() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    doc.add(new LongPoint("point", -7));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new LongPoint("point", 0));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new LongPoint("point", 3));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(2, s.count(LongPoint.newRangeQuery("point", -8L, 1L)));
    assertEquals(3, s.count(LongPoint.newRangeQuery("point", -7L, 3L)));
    assertEquals(1, s.count(LongPoint.newExactQuery("point", -7L)));
    assertEquals(0, s.count(LongPoint.newExactQuery("point", -6L)));
    w.close();
    r.close();
    dir.close();
  }

  public void testBasicDoubles() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    doc.add(new DoublePoint("point", -7.0));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new DoublePoint("point", 0.0));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new DoublePoint("point", 3.0));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher s = new IndexSearcher(r);
    assertEquals(2, s.count(DoublePoint.newRangeQuery("point", -8.0, 1.0)));
    assertEquals(3, s.count(DoublePoint.newRangeQuery("point", -7.0, 3.0)));
    assertEquals(1, s.count(DoublePoint.newExactQuery("point", -7.0)));
    assertEquals(0, s.count(DoublePoint.newExactQuery("point", -6.0)));
    w.close();
    r.close();
    dir.close();
  }
  
  public void testCrazyDoubles() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    doc.add(new DoublePoint("point", Double.NEGATIVE_INFINITY));
    w.addDocument(doc);
    
    doc = new Document();
    doc.add(new DoublePoint("point", -0.0D));
    w.addDocument(doc);
    
    doc = new Document();
    doc.add(new DoublePoint("point", +0.0D));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new DoublePoint("point", Double.MIN_VALUE));
    w.addDocument(doc);
    
    doc = new Document();
    doc.add(new DoublePoint("point", Double.MAX_VALUE));
    w.addDocument(doc);
    
    doc = new Document();
    doc.add(new DoublePoint("point", Double.POSITIVE_INFINITY));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new DoublePoint("point", Double.NaN));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher s = new IndexSearcher(r);
    
    // exact queries
    assertEquals(1, s.count(DoublePoint.newExactQuery("point", Double.NEGATIVE_INFINITY)));
    assertEquals(1, s.count(DoublePoint.newExactQuery("point", -0.0D)));
    assertEquals(1, s.count(DoublePoint.newExactQuery("point", +0.0D)));
    assertEquals(1, s.count(DoublePoint.newExactQuery("point", Double.MIN_VALUE)));
    assertEquals(1, s.count(DoublePoint.newExactQuery("point", Double.MAX_VALUE)));
    assertEquals(1, s.count(DoublePoint.newExactQuery("point", Double.POSITIVE_INFINITY)));
    assertEquals(1, s.count(DoublePoint.newExactQuery("point", Double.NaN)));
    
    // set query
    double set[] = new double[] { Double.MAX_VALUE, Double.NaN, +0.0D, Double.NEGATIVE_INFINITY, Double.MIN_VALUE, -0.0D, Double.POSITIVE_INFINITY };
    assertEquals(7, s.count(DoublePoint.newSetQuery("point", set)));

    // ranges
    assertEquals(2, s.count(DoublePoint.newRangeQuery("point", Double.NEGATIVE_INFINITY, -0.0D)));
    assertEquals(2, s.count(DoublePoint.newRangeQuery("point", -0.0D, 0.0D)));
    assertEquals(2, s.count(DoublePoint.newRangeQuery("point", 0.0D, Double.MIN_VALUE)));
    assertEquals(2, s.count(DoublePoint.newRangeQuery("point", Double.MIN_VALUE, Double.MAX_VALUE)));
    assertEquals(2, s.count(DoublePoint.newRangeQuery("point", Double.MAX_VALUE, Double.POSITIVE_INFINITY)));
    assertEquals(2, s.count(DoublePoint.newRangeQuery("point", Double.POSITIVE_INFINITY, Double.NaN)));

    w.close();
    r.close();
    dir.close();
  }
  
  public void testCrazyFloats() throws Exception {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new MockAnalyzer(random())));

    Document doc = new Document();
    doc.add(new FloatPoint("point", Float.NEGATIVE_INFINITY));
    w.addDocument(doc);
    
    doc = new Document();
    doc.add(new FloatPoint("point", -0.0F));
    w.addDocument(doc);
    
    doc = new Document();
    doc.add(new FloatPoint("point", +0.0F));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new FloatPoint("point", Float.MIN_VALUE));
    w.addDocument(doc);
    
    doc = new Document();
    doc.add(new FloatPoint("point", Float.MAX_VALUE));
    w.addDocument(doc);
    
    doc = new Document();
    doc.add(new FloatPoint("point", Float.POSITIVE_INFINITY));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new FloatPoint("point", Float.NaN));
    w.addDocument(doc);

    DirectoryReader r = DirectoryReader.open(w);
    IndexSearcher s = new IndexSearcher(r);
    
    // exact queries
    assertEquals(1, s.count(FloatPoint.newExactQuery("point", Float.NEGATIVE_INFINITY)));
    assertEquals(1, s.count(FloatPoint.newExactQuery("point", -0.0F)));
    assertEquals(1, s.count(FloatPoint.newExactQuery("point", +0.0F)));
    assertEquals(1, s.count(FloatPoint.newExactQuery("point", Float.MIN_VALUE)));
    assertEquals(1, s.count(FloatPoint.newExactQuery("point", Float.MAX_VALUE)));
    assertEquals(1, s.count(FloatPoint.newExactQuery("point", Float.POSITIVE_INFINITY)));
    assertEquals(1, s.count(FloatPoint.newExactQuery("point", Float.NaN)));
    
    // set query
    float set[] = new float[] { Float.MAX_VALUE, Float.NaN, +0.0F, Float.NEGATIVE_INFINITY, Float.MIN_VALUE, -0.0F, Float.POSITIVE_INFINITY };
    assertEquals(7, s.count(FloatPoint.newSetQuery("point", set)));

    // ranges
    assertEquals(2, s.count(FloatPoint.newRangeQuery("point", Float.NEGATIVE_INFINITY, -0.0F)));
    assertEquals(2, s.count(FloatPoint.newRangeQuery("point", -0.0F, 0.0F)));
    assertEquals(2, s.count(FloatPoint.newRangeQuery("point", 0.0F, Float.MIN_VALUE)));
    assertEquals(2, s.count(FloatPoint.newRangeQuery("point", Float.MIN_VALUE, Float.MAX_VALUE)));
    assertEquals(2, s.count(FloatPoint.newRangeQuery("point", Float.MAX_VALUE, Float.POSITIVE_INFINITY)));
    assertEquals(2, s.count(FloatPoint.newRangeQuery("point", Float.POSITIVE_INFINITY, Float.NaN)));

    w.close();
    r.close();
    dir.close();
  }

  public void testAllEqual() throws Exception {
    int numValues = atLeast(10000);
    long value = randomValue();
    long[] values = new long[numValues];

    if (VERBOSE) {
      System.out.println("TEST: use same value=" + value);
    }
    Arrays.fill(values, value);

    verifyLongs(values, null);
  }

  public void testRandomLongsTiny() throws Exception {
    // Make sure single-leaf-node case is OK:
    doTestRandomLongs(10);
  }

  public void testRandomLongsMedium() throws Exception {
    doTestRandomLongs(10000);
  }

  @Nightly
  public void testRandomLongsBig() throws Exception {
    doTestRandomLongs(100000);
  }

  private void doTestRandomLongs(int count) throws Exception {

    int numValues = TestUtil.nextInt(random(), count, count*2);

    if (VERBOSE) {
      System.out.println("TEST: numValues=" + numValues);
    }

    long[] values = new long[numValues];
    int[] ids = new int[numValues];

    boolean singleValued = random().nextBoolean();

    int sameValuePct = random().nextInt(100);

    int id = 0;
    for (int ord=0;ord<numValues;ord++) {
      if (ord > 0 && random().nextInt(100) < sameValuePct) {
        // Identical to old value
        values[ord] = values[random().nextInt(ord)];
      } else {
        values[ord] = randomValue();
      }

      ids[ord] = id;
      if (singleValued || random().nextInt(2) == 1) {
        id++;
      }
    }

    verifyLongs(values, ids);
  }

  public void testLongEncode() {
    for(int i=0;i<10000;i++) {
      long v = random().nextLong();
      byte[] tmp = new byte[8];
      NumericUtils.longToSortableBytes(v, tmp, 0);
      long v2 = NumericUtils.sortableBytesToLong(tmp, 0);
      assertEquals("got bytes=" + Arrays.toString(tmp), v, v2);
    }
  }

  // verify for long values
  private static void verifyLongs(long[] values, int[] ids) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();

    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < values.length/100) {
      iwc.setMaxBufferedDocs(values.length/100);
    }
    iwc.setCodec(getCodec());
    Directory dir;
    if (values.length > 100000) {
      dir = newMaybeVirusCheckingFSDirectory(createTempDir("TestRangeTree"));
    } else {
      dir = newMaybeVirusCheckingDirectory();
    }

    int missingPct = random().nextInt(100);
    int deletedPct = random().nextInt(100);
    if (VERBOSE) {
      System.out.println("  missingPct=" + missingPct);
      System.out.println("  deletedPct=" + deletedPct);
    }

    BitSet missing = new BitSet();
    BitSet deleted = new BitSet();

    Document doc = null;
    int lastID = -1;

    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    for(int ord=0;ord<values.length;ord++) {

      int id;
      if (ids == null) {
        id = ord;
      } else {
        id = ids[ord];
      }
      if (id != lastID) {
        if (random().nextInt(100) < missingPct) {
          missing.set(id);
          if (VERBOSE) {
            System.out.println("  missing id=" + id);
          }
        }

        if (doc != null) {
          w.addDocument(doc);
          if (random().nextInt(100) < deletedPct) {
            int idToDelete = random().nextInt(id);
            w.deleteDocuments(new Term("id", ""+idToDelete));
            deleted.set(idToDelete);
            if (VERBOSE) {
              System.out.println("  delete id=" + idToDelete);
            }
          }
        }

        doc = new Document();
        doc.add(newStringField("id", ""+id, Field.Store.NO));
        doc.add(new NumericDocValuesField("id", id));
        lastID = id;
      }

      if (missing.get(id) == false) {
        doc.add(new LongPoint("sn_value", values[id]));
        byte[] bytes = new byte[8];
        NumericUtils.longToSortableBytes(values[id], bytes, 0);
        doc.add(new BinaryPoint("ss_value", bytes));
      }
    }

    w.addDocument(doc);

    if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("  forceMerge(1)");
      }
      w.forceMerge(1);
    }
    final IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r, false);

    int numThreads = TestUtil.nextInt(random(), 2, 5);

    if (VERBOSE) {
      System.out.println("TEST: use " + numThreads + " query threads; searcher=" + s);
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
              Long lower = randomValue();
              Long upper = randomValue();

              if (upper < lower) {
                long x = lower;
                lower = upper;
                upper = x;
              }

              Query query;

              if (VERBOSE) {
                System.out.println("\n" + Thread.currentThread().getName() + ": TEST: iter=" + iter + " value=" + lower + " TO " + upper);
                byte[] tmp = new byte[8];
                if (lower != null) {
                  NumericUtils.longToSortableBytes(lower, tmp, 0);
                  System.out.println("  lower bytes=" + Arrays.toString(tmp));
                }
                if (upper != null) {
                  NumericUtils.longToSortableBytes(upper, tmp, 0);
                  System.out.println("  upper bytes=" + Arrays.toString(tmp));
                }
              }

              if (random().nextBoolean()) {
                query = LongPoint.newRangeQuery("sn_value", lower, upper);
              } else {
                byte[] lowerBytes = new byte[8];
                NumericUtils.longToSortableBytes(lower, lowerBytes, 0);
                byte[] upperBytes = new byte[8];
                NumericUtils.longToSortableBytes(upper, upperBytes, 0);
                query = BinaryPoint.newRangeQuery("ss_value", lowerBytes, upperBytes);
              }

              if (VERBOSE) {
                System.out.println(Thread.currentThread().getName() + ":  using query: " + query);
              }

              final BitSet hits = new BitSet();
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
                boolean expected = missing.get(id) == false && deleted.get(id) == false && values[id] >= lower && values[id] <= upper;
                if (hits.get(docID) != expected) {
                  // We do exact quantized comparison so the bbox query should never disagree:
                  fail(Thread.currentThread().getName() + ": iter=" + iter + " id=" + id + " docID=" + docID + " value=" + values[id] + " (range: " + lower + " TO " + upper + ") expected " + expected + " but got: " + hits.get(docID) + " deleted?=" + deleted.get(id) + " query=" + query);
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

  public void testRandomBinaryTiny() throws Exception {
    doTestRandomBinary(10);
  }

  public void testRandomBinaryMedium() throws Exception {
    doTestRandomBinary(10000);
  }

  @Nightly
  public void testRandomBinaryBig() throws Exception {
    doTestRandomBinary(100000);
  }

  private void doTestRandomBinary(int count) throws Exception {
    int numValues = TestUtil.nextInt(random(), count, count*2);
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_DIMENSIONS);

    int sameValuePct = random().nextInt(100);
    if (VERBOSE) {
      System.out.println("TEST: sameValuePct=" + sameValuePct);
    }

    byte[][][] docValues = new byte[numValues][][];

    boolean singleValued = random().nextBoolean();
    int[] ids = new int[numValues];

    int id = 0;
    for(int ord=0;ord<numValues;ord++) {
      if (ord > 0 && random().nextInt(100) < sameValuePct) {
        // Identical to old value
        docValues[ord] = docValues[random().nextInt(ord)];
      } else {
        // Make a new random value
        byte[][] values = new byte[numDims][];
        for(int dim=0;dim<numDims;dim++) {
          values[dim] = new byte[numBytesPerDim];
          random().nextBytes(values[dim]);
        }
        docValues[ord] = values;
      }
      ids[ord] = id;
      if (singleValued || random().nextInt(2) == 1) {
        id++;
      }
    }

    verifyBinary(docValues, ids, numBytesPerDim);
  }

  // verify for byte[][] values
  private void verifyBinary(byte[][][] docValues, int[] ids, int numBytesPerDim) throws Exception {
    IndexWriterConfig iwc = newIndexWriterConfig();

    int numDims = docValues[0].length;
    int bytesPerDim = docValues[0][0].length;

    // Else we can get O(N^2) merging:
    int mbd = iwc.getMaxBufferedDocs();
    if (mbd != -1 && mbd < docValues.length/100) {
      iwc.setMaxBufferedDocs(docValues.length/100);
    }
    iwc.setCodec(getCodec());

    Directory dir;
    if (docValues.length > 100000) {
      dir = newFSDirectory(createTempDir("TestPointQueries"));
    } else {
      dir = newDirectory();
    }

    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    int numValues = docValues.length;
    if (VERBOSE) {
      System.out.println("TEST: numValues=" + numValues + " numDims=" + numDims + " numBytesPerDim=" + numBytesPerDim);
    }

    int missingPct = random().nextInt(100);
    int deletedPct = random().nextInt(100);
    if (VERBOSE) {
      System.out.println("  missingPct=" + missingPct);
      System.out.println("  deletedPct=" + deletedPct);
    }

    BitSet missing = new BitSet();
    BitSet deleted = new BitSet();

    Document doc = null;
    int lastID = -1;

    for(int ord=0;ord<numValues;ord++) {
      int id = ids[ord];
      if (id != lastID) {
        if (random().nextInt(100) < missingPct) {
          missing.set(id);
          if (VERBOSE) {
            System.out.println("  missing id=" + id);
          }
        }

        if (doc != null) {
          w.addDocument(doc);
          if (random().nextInt(100) < deletedPct) {
            int idToDelete = random().nextInt(id);
            w.deleteDocuments(new Term("id", ""+idToDelete));
            deleted.set(idToDelete);
            if (VERBOSE) {
              System.out.println("  delete id=" + idToDelete);
            }
          }
        }

        doc = new Document();
        doc.add(newStringField("id", ""+id, Field.Store.NO));
        doc.add(new NumericDocValuesField("id", id));
        lastID = id;
      }

      if (missing.get(id) == false) {
        doc.add(new BinaryPoint("value", docValues[ord]));
        if (VERBOSE) {
          System.out.println("id=" + id);
          for(int dim=0;dim<numDims;dim++) {
            System.out.println("  dim=" + dim + " value=" + bytesToString(docValues[ord][dim]));
          }
        }
      }
    }

    w.addDocument(doc);

    if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("  forceMerge(1)");
      }
      w.forceMerge(1);
    }
    final IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r, false);

    int numThreads = TestUtil.nextInt(random(), 2, 5);

    if (VERBOSE) {
      System.out.println("TEST: use " + numThreads + " query threads; searcher=" + s);
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

              byte[][] lower = new byte[numDims][];
              byte[][] upper = new byte[numDims][];
              for(int dim=0;dim<numDims;dim++) {
                lower[dim] = new byte[bytesPerDim];
                random().nextBytes(lower[dim]);

                upper[dim] = new byte[bytesPerDim];
                random().nextBytes(upper[dim]);

                if (StringHelper.compare(bytesPerDim, lower[dim], 0, upper[dim], 0) > 0) {
                  byte[] x = lower[dim];
                  lower[dim] = upper[dim];
                  upper[dim] = x;
                }
              }

              if (VERBOSE) {
                System.out.println("\n" + Thread.currentThread().getName() + ": TEST: iter=" + iter);
                for(int dim=0;dim<numDims;dim++) {
                  System.out.println("  dim=" + dim + " " +
                                     bytesToString(lower[dim]) +
                                     " TO " +
                                     bytesToString(upper[dim]));
                }
              }

              Query query = BinaryPoint.newRangeQuery("value", lower, upper);

              if (VERBOSE) {
                System.out.println(Thread.currentThread().getName() + ":  using query: " + query);
              }

              final BitSet hits = new BitSet();
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

              BitSet expected = new BitSet();
              for(int ord=0;ord<numValues;ord++) {
                int id = ids[ord];
                if (missing.get(id) == false && deleted.get(id) == false && matches(bytesPerDim, lower, upper, docValues[ord])) {
                  expected.set(id);
                }
              }

              int failCount = 0;
              for(int docID=0;docID<r.maxDoc();docID++) {
                int id = (int) docIDToID.get(docID);
                if (hits.get(docID) != expected.get(id)) {
                  System.out.println("FAIL: iter=" + iter + " id=" + id + " docID=" + docID + " expected=" + expected.get(id) + " but got " + hits.get(docID) + " deleted?=" + deleted.get(id) + " missing?=" + missing.get(id));
                  for(int dim=0;dim<numDims;dim++) {
                    System.out.println("  dim=" + dim + " range: " + bytesToString(lower[dim]) + " TO " + bytesToString(upper[dim]));
                    failCount++;
                  }
                }
              }
              if (failCount != 0) {
                fail(failCount + " hits were wrong");
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

  static String bytesToString(byte[] bytes) {
    if (bytes == null) {
      return "null";
    }
    return new BytesRef(bytes).toString();
  }

  private static boolean matches(int bytesPerDim, byte[][] lower, byte[][] upper, byte[][] value) {
    int numDims = lower.length;
    for(int dim=0;dim<numDims;dim++) {

      if (StringHelper.compare(bytesPerDim, value[dim], 0, lower[dim], 0) < 0) {
        // Value is below the lower bound, on this dim
        return false;
      }

      if (StringHelper.compare(bytesPerDim, value[dim], 0, upper[dim], 0) > 0) {
        // Value is above the upper bound, on this dim
        return false;
      }
    }

    return true;
  }

  private static long randomValue() {
    if (valueRange == 0) {
      return random().nextLong();
    } else {
      return valueMid + TestUtil.nextInt(random(), -valueRange, valueRange);
    }
  }
  
  public void testMinMaxLong() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new LongPoint("value", Long.MIN_VALUE));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new LongPoint("value", Long.MAX_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);

    assertEquals(1, s.count(LongPoint.newRangeQuery("value", Long.MIN_VALUE, 0L)));
    assertEquals(1, s.count(LongPoint.newRangeQuery("value", 0L, Long.MAX_VALUE)));
    assertEquals(2, s.count(LongPoint.newRangeQuery("value", Long.MIN_VALUE, Long.MAX_VALUE)));

    IOUtils.close(r, w, dir);
  }

  private static byte[] toUTF8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  // Right zero pads:
  private static byte[] toUTF8(String s, int length) {
    byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    if (length < bytes.length) {
      throw new IllegalArgumentException("length=" + length + " but string's UTF8 bytes has length=" + bytes.length);
    }
    byte[] result = new byte[length];
    System.arraycopy(bytes, 0, result, 0, bytes.length);
    return result;
  }

  public void testBasicSortedSet() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("value", toUTF8("abc")));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryPoint("value", toUTF8("def")));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);

    assertEquals(1, s.count(BinaryPoint.newRangeQuery("value", toUTF8("aaa"), toUTF8("bbb"))));
    assertEquals(1, s.count(BinaryPoint.newRangeQuery("value", toUTF8("c", 3), toUTF8("e", 3))));
    assertEquals(2, s.count(BinaryPoint.newRangeQuery("value", toUTF8("a", 3), toUTF8("z", 3))));
    assertEquals(1, s.count(BinaryPoint.newRangeQuery("value", toUTF8("", 3), toUTF8("abc"))));
    assertEquals(1, s.count(BinaryPoint.newRangeQuery("value", toUTF8("a", 3), toUTF8("abc"))));
    assertEquals(0, s.count(BinaryPoint.newRangeQuery("value", toUTF8("a", 3), toUTF8("abb"))));
    assertEquals(1, s.count(BinaryPoint.newRangeQuery("value", toUTF8("def"), toUTF8("zzz"))));
    assertEquals(1, s.count(BinaryPoint.newRangeQuery("value", toUTF8(("def")), toUTF8("z", 3))));
    assertEquals(0, s.count(BinaryPoint.newRangeQuery("value", toUTF8("deg"), toUTF8("z", 3))));

    IOUtils.close(r, w, dir);
  }

  public void testLongMinMaxNumeric() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new LongPoint("value", Long.MIN_VALUE));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new LongPoint("value", Long.MAX_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);

    assertEquals(2, s.count(LongPoint.newRangeQuery("value", Long.MIN_VALUE, Long.MAX_VALUE)));
    assertEquals(1, s.count(LongPoint.newRangeQuery("value", Long.MIN_VALUE, Long.MAX_VALUE-1)));
    assertEquals(1, s.count(LongPoint.newRangeQuery("value", Long.MIN_VALUE+1, Long.MAX_VALUE)));
    assertEquals(0, s.count(LongPoint.newRangeQuery("value", Long.MIN_VALUE+1, Long.MAX_VALUE-1)));

    IOUtils.close(r, w, dir);
  }

  public void testLongMinMaxSortedSet() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new LongPoint("value", Long.MIN_VALUE));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new LongPoint("value", Long.MAX_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);

    assertEquals(2, s.count(LongPoint.newRangeQuery("value", Long.MIN_VALUE, Long.MAX_VALUE)));
    assertEquals(1, s.count(LongPoint.newRangeQuery("value", Long.MIN_VALUE, Long.MAX_VALUE-1)));
    assertEquals(1, s.count(LongPoint.newRangeQuery("value", Long.MIN_VALUE+1, Long.MAX_VALUE)));
    assertEquals(0, s.count(LongPoint.newRangeQuery("value", Long.MIN_VALUE+1, Long.MAX_VALUE-1)));

    IOUtils.close(r, w, dir);
  }

  public void testSortedSetNoOrdsMatch() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("value", toUTF8("a")));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryPoint("value", toUTF8("z")));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r,false);
    assertEquals(0, s.count(BinaryPoint.newRangeQuery("value", toUTF8("m"), toUTF8("m"))));

    IOUtils.close(r, w, dir);
  }

  public void testNumericNoValuesMatch() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("value", 17));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new SortedNumericDocValuesField("value", 22));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    IndexSearcher s = new IndexSearcher(r);
    assertEquals(0, s.count(LongPoint.newRangeQuery("value", 17L, 13L)));

    IOUtils.close(r, w, dir);
  }

  public void testNoDocs() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    w.addDocument(new Document());

    IndexReader r = w.getReader();

    IndexSearcher s = newSearcher(r, false);
    assertEquals(0, s.count(LongPoint.newRangeQuery("value", 17L, 13L)));

    IOUtils.close(r, w, dir);
  }

  public void testWrongNumDims() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new LongPoint("value", Long.MIN_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    // no wrapping, else the exc might happen in executor thread:
    IndexSearcher s = new IndexSearcher(r);
    byte[][] point = new byte[2][];
    point[0] = new byte[8];
    point[1] = new byte[8];
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      s.count(BinaryPoint.newRangeQuery("value", point, point));
    });
    assertEquals("field=\"value\" was indexed with numDims=1 but this query has numDims=2", expected.getMessage());

    IOUtils.close(r, w, dir);
  }

  public void testWrongNumBytes() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    Document doc = new Document();
    doc.add(new LongPoint("value", Long.MIN_VALUE));
    w.addDocument(doc);

    IndexReader r = w.getReader();

    // no wrapping, else the exc might happen in executor thread:
    IndexSearcher s = new IndexSearcher(r);
    byte[][] point = new byte[1][];
    point[0] = new byte[10];
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      s.count(BinaryPoint.newRangeQuery("value", point, point));
    });
    assertEquals("field=\"value\" was indexed with bytesPerDim=8 but this query has bytesPerDim=10", expected.getMessage());

    IOUtils.close(r, w, dir);
  }

  public void testAllPointDocsWereDeletedAndThenMergedAgain() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    doc.add(new LongPoint("value", 0L));
    w.addDocument(doc);

    // Add document that won't be deleted to avoid IW dropping
    // segment below since it's 100% deleted:
    w.addDocument(new Document());
    w.commit();

    // Need another segment so we invoke BKDWriter.merge
    doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    doc.add(new LongPoint("value", 0L));
    w.addDocument(doc);
    w.addDocument(new Document());

    w.deleteDocuments(new Term("id", "0"));
    w.forceMerge(1);

    doc = new Document();
    doc.add(new StringField("id", "0", Field.Store.NO));
    doc.add(new LongPoint("value", 0L));
    w.addDocument(doc);
    w.addDocument(new Document());

    w.deleteDocuments(new Term("id", "0"));
    w.forceMerge(1);

    IOUtils.close(w, dir);
  }

  private static Codec getCodec() {
    if (Codec.getDefault().getName().equals("Lucene62")) {
      int maxPointsInLeafNode = TestUtil.nextInt(random(), 16, 2048);
      double maxMBSortInHeap = 5.0 + (3*random().nextDouble());
      if (VERBOSE) {
        System.out.println("TEST: using Lucene60PointsFormat with maxPointsInLeafNode=" + maxPointsInLeafNode + " and maxMBSortInHeap=" + maxMBSortInHeap);
      }

      return new FilterCodec("Lucene62", Codec.getDefault()) {
        @Override
        public PointsFormat pointsFormat() {
          return new PointsFormat() {
            @Override
            public PointsWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
              return new Lucene60PointsWriter(writeState, maxPointsInLeafNode, maxMBSortInHeap);
            }

            @Override
            public PointsReader fieldsReader(SegmentReadState readState) throws IOException {
              return new Lucene60PointsReader(readState);
            }
          };
        }
      };
    } else {
      return Codec.getDefault();
    }
  }

  public void testExactPoints() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);

    Document doc = new Document();
    doc.add(new LongPoint("long", 5L));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new IntPoint("int", 42));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new FloatPoint("float", 2.0f));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new DoublePoint("double", 1.0));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r, false);
    assertEquals(1, s.count(IntPoint.newExactQuery("int", 42)));
    assertEquals(0, s.count(IntPoint.newExactQuery("int", 41)));

    assertEquals(1, s.count(LongPoint.newExactQuery("long", 5L)));
    assertEquals(0, s.count(LongPoint.newExactQuery("long", -1L)));

    assertEquals(1, s.count(FloatPoint.newExactQuery("float", 2.0f)));
    assertEquals(0, s.count(FloatPoint.newExactQuery("float", 1.0f)));

    assertEquals(1, s.count(DoublePoint.newExactQuery("double", 1.0)));
    assertEquals(0, s.count(DoublePoint.newExactQuery("double", 2.0)));
    w.close();
    r.close();
    dir.close();
  }

  public void testToString() throws Exception {
    
    // ints
    assertEquals("field:[1 TO 2]", IntPoint.newRangeQuery("field", 1, 2).toString());
    assertEquals("field:[-2 TO 1]", IntPoint.newRangeQuery("field", -2, 1).toString());

    // longs
    assertEquals("field:[1099511627776 TO 2199023255552]", LongPoint.newRangeQuery("field", 1L<<40, 1L<<41).toString());
    assertEquals("field:[-5 TO 6]", LongPoint.newRangeQuery("field", -5L, 6L).toString());
    
    // floats
    assertEquals("field:[1.3 TO 2.5]", FloatPoint.newRangeQuery("field", 1.3F, 2.5F).toString());
    assertEquals("field:[-2.9 TO 1.0]", FloatPoint.newRangeQuery("field", -2.9F, 1.0F).toString());
    
    // doubles
    assertEquals("field:[1.3 TO 2.5]", DoublePoint.newRangeQuery("field", 1.3, 2.5).toString());
    assertEquals("field:[-2.9 TO 1.0]", DoublePoint.newRangeQuery("field", -2.9, 1.0).toString());
    
    // n-dimensional double
    assertEquals("field:[1.3 TO 2.5],[-2.9 TO 1.0]", DoublePoint.newRangeQuery("field", 
                                                                      new double[] { 1.3, -2.9 }, 
                                                                      new double[] { 2.5, 1.0 }).toString());

  }

  private int[] toArray(Set<Integer> valuesSet) {
    int[] values = new int[valuesSet.size()];
    int upto = 0;
    for(Integer value : valuesSet) {
      values[upto++] = value;
    }
    return values;
  }

  private static int randomIntValue(Integer min, Integer max) {
    if (min == null) {
      return random().nextInt();
    } else {
      return TestUtil.nextInt(random(), min, max);
    }
  }

  public void testRandomPointInSetQuery() throws Exception {

    boolean useNarrowRange = random().nextBoolean();
    final Integer valueMin;
    final Integer valueMax;
    int numValues;
    if (useNarrowRange) {
      int gap = random().nextInt(100);
      valueMin = random().nextInt(Integer.MAX_VALUE-gap);
      valueMax = valueMin + gap;
      numValues = TestUtil.nextInt(random(), 1, gap+1);
    } else {
      valueMin = null;
      valueMax = null;
      numValues = TestUtil.nextInt(random(), 1, 100);
    }
    final Set<Integer> valuesSet = new HashSet<>();
    while (valuesSet.size() < numValues) {
      valuesSet.add(randomIntValue(valueMin, valueMax));
    }
    int[] values = toArray(valuesSet);
    int numDocs = TestUtil.nextInt(random(), 1, 10000);

    if (VERBOSE) {
      System.out.println("TEST: numValues=" + numValues + " numDocs=" + numDocs);
    }

    Directory dir;
    if (numDocs > 100000) {
      dir = newFSDirectory(createTempDir("TestPointQueries"));
    } else {
      dir = newDirectory();
    }

    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

    int[] docValues = new int[numDocs];
    for(int i=0;i<numDocs;i++) {
      int x = values[random().nextInt(values.length)];
      Document doc = new Document();
      doc.add(new IntPoint("int", x));
      docValues[i] = x;
      w.addDocument(doc);
    }

    if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("  forceMerge(1)");
      }
      w.forceMerge(1);
    }
    final IndexReader r = w.getReader();
    w.close();

    IndexSearcher s = newSearcher(r, false);

    int numThreads = TestUtil.nextInt(random(), 2, 5);

    if (VERBOSE) {
      System.out.println("TEST: use " + numThreads + " query threads; searcher=" + s);
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

            for (int iter=0;iter<iters && failed.get() == false;iter++) {

              int numValidValuesToQuery = random().nextInt(values.length);

              Set<Integer> valuesToQuery = new HashSet<>();
              while (valuesToQuery.size() < numValidValuesToQuery) {
                valuesToQuery.add(values[random().nextInt(values.length)]);
              }

              int numExtraValuesToQuery = random().nextInt(20);
              while (valuesToQuery.size() < numValidValuesToQuery + numExtraValuesToQuery) {
                valuesToQuery.add(random().nextInt());
              }

              int expectedCount = 0;
              for(int value : docValues) {
                if (valuesToQuery.contains(value)) {
                  expectedCount++;
                }
              }

              if (VERBOSE) {
                System.out.println("TEST: thread=" + Thread.currentThread() + " values=" + valuesToQuery + " expectedCount=" + expectedCount);
              }

              assertEquals(expectedCount, s.count(IntPoint.newSetQuery("int", toArray(valuesToQuery))));
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

  // TODO: in the future, if there is demand for real usage, we can "graduate" this test-only query factory as IntPoint.newMultiSetQuery or
  // something (and same for other XXXPoint classes):
  private static Query newMultiDimIntSetQuery(String field, final int numDims, int... valuesIn) throws IOException {
    if (valuesIn.length % numDims != 0) {
      throw new IllegalArgumentException("incongruent number of values: valuesIn.length=" + valuesIn.length + " but numDims=" + numDims);
    }

    // Pack all values:
    byte[][] packedValues = new byte[valuesIn.length / numDims][];
    for(int i=0;i<packedValues.length;i++) {
      byte[] packedValue = new byte[numDims * Integer.BYTES];
      packedValues[i] = packedValue;
      for(int dim=0;dim<numDims;dim++) {
        IntPoint.encodeDimension(valuesIn[i*numDims+dim], packedValue, dim*Integer.BYTES);
      }
    }

    // Sort:
    Arrays.sort(packedValues,
                new Comparator<byte[]>() {
                  @Override
                  public int compare(byte[] a, byte[] b) {
                    return StringHelper.compare(a.length, a, 0, b, 0);
                  }
                });

    final BytesRef value = new BytesRef();
    value.length = numDims * Integer.BYTES;

    return new PointInSetQuery(field,
                               numDims,
                               Integer.BYTES,
                               new PointInSetQuery.Stream() {
                                 int upto;
                                 @Override
                                 public BytesRef next() {
                                   if (upto >= packedValues.length) {
                                     return null;
                                   }
                                   value.bytes = packedValues[upto];
                                   upto++;
                                   return value;
                                 }
                               }) {
      @Override
      protected String toString(byte[] value) {
        assert value.length == numDims * Integer.BYTES;
        StringBuilder sb = new StringBuilder();
        for(int dim=0;dim<numDims;dim++) {
          if (dim > 0) {
            sb.append(',');
          }
          sb.append(Integer.toString(IntPoint.decodeDimension(value, dim*Integer.BYTES)));
        }

        return sb.toString();
      }
    };
  }

  public void testBasicMultiDimPointInSetQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);

    Document doc = new Document();
    doc.add(new IntPoint("int", 17, 42));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r, false);

    assertEquals(0, s.count(newMultiDimIntSetQuery("int", 2, 17, 41)));
    assertEquals(1, s.count(newMultiDimIntSetQuery("int", 2, 17, 42)));
    assertEquals(1, s.count(newMultiDimIntSetQuery("int", 2, -7, -7, 17, 42)));
    assertEquals(1, s.count(newMultiDimIntSetQuery("int", 2, 17, 42, -14, -14)));

    w.close();
    r.close();
    dir.close();
  }

  public void testBasicMultiValueMultiDimPointInSetQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);

    Document doc = new Document();
    doc.add(new IntPoint("int", 17, 42));
    doc.add(new IntPoint("int", 34, 79));
    w.addDocument(doc);
    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r, false);

    assertEquals(0, s.count(newMultiDimIntSetQuery("int", 2, 17, 41)));
    assertEquals(1, s.count(newMultiDimIntSetQuery("int", 2, 17, 42)));
    assertEquals(1, s.count(newMultiDimIntSetQuery("int", 2, 17, 42, 34, 79)));
    assertEquals(1, s.count(newMultiDimIntSetQuery("int", 2, -7, -7, 17, 42)));
    assertEquals(1, s.count(newMultiDimIntSetQuery("int", 2, -7, -7, 34, 79)));
    assertEquals(1, s.count(newMultiDimIntSetQuery("int", 2, 17, 42, -14, -14)));

    assertEquals("int:{-14,-14 17,42}", newMultiDimIntSetQuery("int", 2, 17, 42, -14, -14).toString());

    w.close();
    r.close();
    dir.close();
  }

  public void testManyEqualValuesMultiDimPointInSetQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);

    int zeroCount = 0;
    for(int i=0;i<10000;i++) {
      int x = random().nextInt(2);
      if (x == 0) {
        zeroCount++;
      }
      Document doc = new Document();
      doc.add(new IntPoint("int", x, x));
      w.addDocument(doc);
    }
    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r, false);

    assertEquals(zeroCount, s.count(newMultiDimIntSetQuery("int", 2, 0, 0)));
    assertEquals(10000-zeroCount, s.count(newMultiDimIntSetQuery("int", 2, 1, 1)));
    assertEquals(0, s.count(newMultiDimIntSetQuery("int", 2, 2, 2)));

    w.close();
    r.close();
    dir.close();
  }

  public void testInvalidMultiDimPointInSetQuery() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
                                                     () -> {
                                                       newMultiDimIntSetQuery("int", 2, 3, 4, 5);
                                                     });
    assertEquals("incongruent number of values: valuesIn.length=3 but numDims=2", expected.getMessage());
  }

  public void testBasicPointInSetQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);

    Document doc = new Document();
    doc.add(new IntPoint("int", 17));
    doc.add(new LongPoint("long", 17L));
    doc.add(new FloatPoint("float", 17.0f));
    doc.add(new DoublePoint("double", 17.0));
    doc.add(new BinaryPoint("bytes", new byte[] {0, 17}));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new IntPoint("int", 42));
    doc.add(new LongPoint("long", 42L));
    doc.add(new FloatPoint("float", 42.0f));
    doc.add(new DoublePoint("double", 42.0));
    doc.add(new BinaryPoint("bytes", new byte[] {0, 42}));
    w.addDocument(doc);

    doc = new Document();
    doc.add(new IntPoint("int", 97));
    doc.add(new LongPoint("long", 97L));
    doc.add(new FloatPoint("float", 97.0f));
    doc.add(new DoublePoint("double", 97.0));
    doc.add(new BinaryPoint("bytes", new byte[] {0, 97}));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r, false);
    assertEquals(0, s.count(IntPoint.newSetQuery("int", 16)));
    assertEquals(1, s.count(IntPoint.newSetQuery("int", 17)));
    assertEquals(3, s.count(IntPoint.newSetQuery("int", 17, 97, 42)));
    assertEquals(3, s.count(IntPoint.newSetQuery("int", -7, 17, 42, 97)));
    assertEquals(3, s.count(IntPoint.newSetQuery("int", 17, 20, 42, 97)));
    assertEquals(3, s.count(IntPoint.newSetQuery("int", 17, 105, 42, 97)));

    assertEquals(0, s.count(LongPoint.newSetQuery("long", 16)));
    assertEquals(1, s.count(LongPoint.newSetQuery("long", 17)));
    assertEquals(3, s.count(LongPoint.newSetQuery("long", 17, 97, 42)));
    assertEquals(3, s.count(LongPoint.newSetQuery("long", -7, 17, 42, 97)));
    assertEquals(3, s.count(LongPoint.newSetQuery("long", 17, 20, 42, 97)));
    assertEquals(3, s.count(LongPoint.newSetQuery("long", 17, 105, 42, 97)));

    assertEquals(0, s.count(FloatPoint.newSetQuery("float", 16)));
    assertEquals(1, s.count(FloatPoint.newSetQuery("float", 17)));
    assertEquals(3, s.count(FloatPoint.newSetQuery("float", 17, 97, 42)));
    assertEquals(3, s.count(FloatPoint.newSetQuery("float", -7, 17, 42, 97)));
    assertEquals(3, s.count(FloatPoint.newSetQuery("float", 17, 20, 42, 97)));
    assertEquals(3, s.count(FloatPoint.newSetQuery("float", 17, 105, 42, 97)));

    assertEquals(0, s.count(DoublePoint.newSetQuery("double", 16)));
    assertEquals(1, s.count(DoublePoint.newSetQuery("double", 17)));
    assertEquals(3, s.count(DoublePoint.newSetQuery("double", 17, 97, 42)));
    assertEquals(3, s.count(DoublePoint.newSetQuery("double", -7, 17, 42, 97)));
    assertEquals(3, s.count(DoublePoint.newSetQuery("double", 17, 20, 42, 97)));
    assertEquals(3, s.count(DoublePoint.newSetQuery("double", 17, 105, 42, 97)));

    assertEquals(0, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, 16})));
    assertEquals(1, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, 17})));
    assertEquals(3, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, 17}, new byte[] {0, 97}, new byte[] {0, 42})));
    assertEquals(3, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, -7}, new byte[] {0, 17}, new byte[] {0, 42}, new byte[] {0, 97})));
    assertEquals(3, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, 17}, new byte[] {0, 20}, new byte[] {0, 42}, new byte[] {0, 97})));
    assertEquals(3, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, 17}, new byte[] {0, 105}, new byte[] {0, 42}, new byte[] {0, 97})));

    w.close();
    r.close();
    dir.close();
  }
  
  /** Boxed methods for primitive types should behave the same as unboxed: just sugar */
  public void testPointIntSetBoxed() throws Exception {
    assertEquals(IntPoint.newSetQuery("foo", 1, 2, 3), IntPoint.newSetQuery("foo", Arrays.asList(1, 2, 3)));
    assertEquals(FloatPoint.newSetQuery("foo", 1F, 2F, 3F), FloatPoint.newSetQuery("foo", Arrays.asList(1F, 2F, 3F)));
    assertEquals(LongPoint.newSetQuery("foo", 1L, 2L, 3L), LongPoint.newSetQuery("foo", Arrays.asList(1L, 2L, 3L)));
    assertEquals(DoublePoint.newSetQuery("foo", 1D, 2D, 3D), DoublePoint.newSetQuery("foo", Arrays.asList(1D, 2D, 3D)));
  }

  public void testBasicMultiValuedPointInSetQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);

    Document doc = new Document();
    doc.add(new IntPoint("int", 17));
    doc.add(new IntPoint("int", 42));
    doc.add(new LongPoint("long", 17L));
    doc.add(new LongPoint("long", 42L));
    doc.add(new FloatPoint("float", 17.0f));
    doc.add(new FloatPoint("float", 42.0f));
    doc.add(new DoublePoint("double", 17.0));
    doc.add(new DoublePoint("double", 42.0));
    doc.add(new BinaryPoint("bytes", new byte[] {0, 17}));
    doc.add(new BinaryPoint("bytes", new byte[] {0, 42}));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r, false);
    assertEquals(0, s.count(IntPoint.newSetQuery("int", 16)));
    assertEquals(1, s.count(IntPoint.newSetQuery("int", 17)));
    assertEquals(1, s.count(IntPoint.newSetQuery("int", 17, 97, 42)));
    assertEquals(1, s.count(IntPoint.newSetQuery("int", -7, 17, 42, 97)));
    assertEquals(0, s.count(IntPoint.newSetQuery("int", 16, 20, 41, 97)));

    assertEquals(0, s.count(LongPoint.newSetQuery("long", 16)));
    assertEquals(1, s.count(LongPoint.newSetQuery("long", 17)));
    assertEquals(1, s.count(LongPoint.newSetQuery("long", 17, 97, 42)));
    assertEquals(1, s.count(LongPoint.newSetQuery("long", -7, 17, 42, 97)));
    assertEquals(0, s.count(LongPoint.newSetQuery("long", 16, 20, 41, 97)));

    assertEquals(0, s.count(FloatPoint.newSetQuery("float", 16)));
    assertEquals(1, s.count(FloatPoint.newSetQuery("float", 17)));
    assertEquals(1, s.count(FloatPoint.newSetQuery("float", 17, 97, 42)));
    assertEquals(1, s.count(FloatPoint.newSetQuery("float", -7, 17, 42, 97)));
    assertEquals(0, s.count(FloatPoint.newSetQuery("float", 16, 20, 41, 97)));

    assertEquals(0, s.count(DoublePoint.newSetQuery("double", 16)));
    assertEquals(1, s.count(DoublePoint.newSetQuery("double", 17)));
    assertEquals(1, s.count(DoublePoint.newSetQuery("double", 17, 97, 42)));
    assertEquals(1, s.count(DoublePoint.newSetQuery("double", -7, 17, 42, 97)));
    assertEquals(0, s.count(DoublePoint.newSetQuery("double", 16, 20, 41, 97)));

    assertEquals(0, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, 16})));
    assertEquals(1, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, 17})));
    assertEquals(1, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, 17}, new byte[] {0, 97}, new byte[] {0, 42})));
    assertEquals(1, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, -7}, new byte[] {0, 17}, new byte[] {0, 42}, new byte[] {0, 97})));
    assertEquals(0, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0, 16}, new byte[] {0, 20}, new byte[] {0, 41}, new byte[] {0, 97})));

    w.close();
    r.close();
    dir.close();
  }

  public void testEmptyPointInSetQuery() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);

    Document doc = new Document();
    doc.add(new IntPoint("int", 17));
    doc.add(new LongPoint("long", 17L));
    doc.add(new FloatPoint("float", 17.0f));
    doc.add(new DoublePoint("double", 17.0));
    doc.add(new BinaryPoint("bytes", new byte[] {0, 17}));
    w.addDocument(doc);

    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r, false);
    assertEquals(0, s.count(IntPoint.newSetQuery("int")));
    assertEquals(0, s.count(LongPoint.newSetQuery("long")));
    assertEquals(0, s.count(FloatPoint.newSetQuery("float")));
    assertEquals(0, s.count(DoublePoint.newSetQuery("double")));
    assertEquals(0, s.count(BinaryPoint.newSetQuery("bytes")));

    w.close();
    r.close();
    dir.close();
  }

  public void testPointInSetQueryManyEqualValues() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);

    int zeroCount = 0;
    for(int i=0;i<10000;i++) {
      int x = random().nextInt(2);
      if (x == 0) {
        zeroCount++;
      }
      Document doc = new Document();
      doc.add(new IntPoint("int", x));
      doc.add(new LongPoint("long", (long) x));
      doc.add(new FloatPoint("float", (float) x));
      doc.add(new DoublePoint("double", (double) x));
      doc.add(new BinaryPoint("bytes", new byte[] {(byte) x}));
      w.addDocument(doc);
    }

    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r, false);
    assertEquals(zeroCount, s.count(IntPoint.newSetQuery("int", 0)));
    assertEquals(zeroCount, s.count(IntPoint.newSetQuery("int", 0, -7)));
    assertEquals(zeroCount, s.count(IntPoint.newSetQuery("int", 7, 0)));
    assertEquals(10000-zeroCount, s.count(IntPoint.newSetQuery("int", 1)));
    assertEquals(0, s.count(IntPoint.newSetQuery("int", 2)));

    assertEquals(zeroCount, s.count(LongPoint.newSetQuery("long", 0)));
    assertEquals(zeroCount, s.count(LongPoint.newSetQuery("long", 0, -7)));
    assertEquals(zeroCount, s.count(LongPoint.newSetQuery("long", 7, 0)));
    assertEquals(10000-zeroCount, s.count(LongPoint.newSetQuery("long", 1)));
    assertEquals(0, s.count(LongPoint.newSetQuery("long", 2)));

    assertEquals(zeroCount, s.count(FloatPoint.newSetQuery("float", 0)));
    assertEquals(zeroCount, s.count(FloatPoint.newSetQuery("float", 0, -7)));
    assertEquals(zeroCount, s.count(FloatPoint.newSetQuery("float", 7, 0)));
    assertEquals(10000-zeroCount, s.count(FloatPoint.newSetQuery("float", 1)));
    assertEquals(0, s.count(FloatPoint.newSetQuery("float", 2)));

    assertEquals(zeroCount, s.count(DoublePoint.newSetQuery("double", 0)));
    assertEquals(zeroCount, s.count(DoublePoint.newSetQuery("double", 0, -7)));
    assertEquals(zeroCount, s.count(DoublePoint.newSetQuery("double", 7, 0)));
    assertEquals(10000-zeroCount, s.count(DoublePoint.newSetQuery("double", 1)));
    assertEquals(0, s.count(DoublePoint.newSetQuery("double", 2)));

    assertEquals(zeroCount, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0})));
    assertEquals(zeroCount, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0}, new byte[] {-7})));
    assertEquals(zeroCount, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {7}, new byte[] {0})));
    assertEquals(10000-zeroCount, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {1})));
    assertEquals(0, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {2})));

    w.close();
    r.close();
    dir.close();
  }

  public void testPointInSetQueryManyEqualValuesWithBigGap() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setCodec(getCodec());
    IndexWriter w = new IndexWriter(dir, iwc);

    int zeroCount = 0;
    for(int i=0;i<10000;i++) {
      int x = 200 * random().nextInt(2);
      if (x == 0) {
        zeroCount++;
      }
      Document doc = new Document();
      doc.add(new IntPoint("int", x));
      doc.add(new LongPoint("long", (long) x));
      doc.add(new FloatPoint("float", (float) x));
      doc.add(new DoublePoint("double", (double) x));
      doc.add(new BinaryPoint("bytes", new byte[] {(byte) x}));
      w.addDocument(doc);
    }

    IndexReader r = DirectoryReader.open(w);
    IndexSearcher s = newSearcher(r, false);
    assertEquals(zeroCount, s.count(IntPoint.newSetQuery("int", 0)));
    assertEquals(zeroCount, s.count(IntPoint.newSetQuery("int", 0, -7)));
    assertEquals(zeroCount, s.count(IntPoint.newSetQuery("int", 7, 0)));
    assertEquals(10000-zeroCount, s.count(IntPoint.newSetQuery("int", 200)));
    assertEquals(0, s.count(IntPoint.newSetQuery("int", 2)));

    assertEquals(zeroCount, s.count(LongPoint.newSetQuery("long", 0)));
    assertEquals(zeroCount, s.count(LongPoint.newSetQuery("long", 0, -7)));
    assertEquals(zeroCount, s.count(LongPoint.newSetQuery("long", 7, 0)));
    assertEquals(10000-zeroCount, s.count(LongPoint.newSetQuery("long", 200)));
    assertEquals(0, s.count(LongPoint.newSetQuery("long", 2)));

    assertEquals(zeroCount, s.count(FloatPoint.newSetQuery("float", 0)));
    assertEquals(zeroCount, s.count(FloatPoint.newSetQuery("float", 0, -7)));
    assertEquals(zeroCount, s.count(FloatPoint.newSetQuery("float", 7, 0)));
    assertEquals(10000-zeroCount, s.count(FloatPoint.newSetQuery("float", 200)));
    assertEquals(0, s.count(FloatPoint.newSetQuery("float", 2)));

    assertEquals(zeroCount, s.count(DoublePoint.newSetQuery("double", 0)));
    assertEquals(zeroCount, s.count(DoublePoint.newSetQuery("double", 0, -7)));
    assertEquals(zeroCount, s.count(DoublePoint.newSetQuery("double", 7, 0)));
    assertEquals(10000-zeroCount, s.count(DoublePoint.newSetQuery("double", 200)));
    assertEquals(0, s.count(DoublePoint.newSetQuery("double", 2)));

    assertEquals(zeroCount, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0})));
    assertEquals(zeroCount, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {0}, new byte[] {-7})));
    assertEquals(zeroCount, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {7}, new byte[] {0})));
    assertEquals(10000-zeroCount, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {(byte) 200})));
    assertEquals(0, s.count(BinaryPoint.newSetQuery("bytes", new byte[] {2})));

    w.close();
    r.close();
    dir.close();
  }

  public void testInvalidPointInSetQuery() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
                                                     () -> {
                                                       new PointInSetQuery("foo", 3, 4,
                                                                           new PointInSetQuery.Stream() {
                                                                             @Override
                                                                             public BytesRef next() {
                                                                               return new BytesRef(new byte[3]);
                                                                             }
                                                                           }) {
                                                         @Override
                                                         protected String toString(byte[] point) {
                                                           return Arrays.toString(point);
                                                         }
                                                       };
                                                     });
    assertEquals("packed point length should be 12 but got 3; field=\"foo\" numDims=3 bytesPerDim=4", expected.getMessage());
  }

  public void testInvalidPointInSetBinaryQuery() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class,
                                                     () -> {
                                                       BinaryPoint.newSetQuery("bytes", new byte[] {2}, new byte[0]);
                                                     });
    assertEquals("all byte[] must be the same length, but saw 1 and 0", expected.getMessage());
  }

  public void testPointInSetQueryToString() throws Exception {
    // int
    assertEquals("int:{-42 18}", IntPoint.newSetQuery("int", -42, 18).toString());

    // long
    assertEquals("long:{-42 18}", LongPoint.newSetQuery("long", -42L, 18L).toString());

    // float
    assertEquals("float:{-42.0 18.0}", FloatPoint.newSetQuery("float", -42.0f, 18.0f).toString());

    // double
    assertEquals("double:{-42.0 18.0}", DoublePoint.newSetQuery("double", -42.0, 18.0).toString());

    // binary
    assertEquals("bytes:{[12] [2a]}", BinaryPoint.newSetQuery("bytes", new byte[] {42}, new byte[] {18}).toString());
  }

  public void testPointInSetQueryGetPackedPoints() throws Exception {
    int numValues = randomIntValue(1, 32);
    List<byte[]> values = new ArrayList<>(numValues);
    for (byte i = 0; i < numValues; i++) {
      values.add(new byte[]{i});
    }

    PointInSetQuery query = (PointInSetQuery) BinaryPoint.newSetQuery("field", values.toArray(new byte[][]{}));
    Collection<byte[]> packedPoints = query.getPackedPoints();
    assertEquals(numValues, packedPoints.size());
    Iterator<byte[]> iterator = packedPoints.iterator();
    for (byte[] expectedValue : values) {
      assertArrayEquals(expectedValue, iterator.next());
    }
    expectThrows(NoSuchElementException.class, () -> iterator.next());
    assertFalse(iterator.hasNext());
  }

  public void testRangeOptimizesIfAllPointsMatch() throws IOException {
    final int numDims = TestUtil.nextInt(random(), 1, 3);
    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    int[] value = new int[numDims];
    for (int i = 0; i < numDims; ++i) {
      value[i] = TestUtil.nextInt(random(), 1, 10);
    }
    doc.add(new IntPoint("point", value));
    w.addDocument(doc);
    IndexReader reader = w.getReader();
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    int[] lowerBound = new int[numDims];
    int[] upperBound = new int[numDims];
    for (int i = 0; i < numDims; ++i) {
      lowerBound[i] = value[i] - random().nextInt(1);
      upperBound[i] = value[i] + random().nextInt(1);
    }
    Query query = IntPoint.newRangeQuery("point", lowerBound, upperBound);
    Weight weight = searcher.createNormalizedWeight(query, false);
    Scorer scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    assertEquals(DocIdSetIterator.all(1).getClass(), scorer.iterator().getClass());

    // When not all documents in the query have a value, the optimization is not applicable
    reader.close();
    w.addDocument(new Document());
    w.forceMerge(1);
    reader = w.getReader();
    searcher = new IndexSearcher(reader);
    searcher.setQueryCache(null);
    weight = searcher.createNormalizedWeight(query, false);
    scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
    assertFalse(DocIdSetIterator.all(1).getClass().equals(scorer.iterator().getClass()));

    reader.close();
    w.close();
    dir.close();
  }

  public void testPointRangeEquals() {
    Query q1, q2;

    q1 = IntPoint.newRangeQuery("a", 0, 1000);
    q2 = IntPoint.newRangeQuery("a", 0, 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(IntPoint.newRangeQuery("a", 1, 1000)));
    assertFalse(q1.equals(IntPoint.newRangeQuery("b", 0, 1000)));

    q1 = LongPoint.newRangeQuery("a", 0, 1000);
    q2 = LongPoint.newRangeQuery("a", 0, 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(LongPoint.newRangeQuery("a", 1, 1000)));

    q1 = FloatPoint.newRangeQuery("a", 0, 1000);
    q2 = FloatPoint.newRangeQuery("a", 0, 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(FloatPoint.newRangeQuery("a", 1, 1000)));

    q1 = DoublePoint.newRangeQuery("a", 0, 1000);
    q2 = DoublePoint.newRangeQuery("a", 0, 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(DoublePoint.newRangeQuery("a", 1, 1000)));

    byte[] zeros = new byte[5];
    byte[] ones = new byte[5];
    Arrays.fill(ones, (byte) 0xff);
    q1 = BinaryPoint.newRangeQuery("a", new byte[][] {zeros}, new byte[][] {ones});
    q2 = BinaryPoint.newRangeQuery("a", new byte[][] {zeros}, new byte[][] {ones});
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    byte[] other = ones.clone();
    other[2] = (byte) 5;
    assertFalse(q1.equals(BinaryPoint.newRangeQuery("a", new byte[][] {zeros}, new byte[][] {other})));
  }

  public void testPointExactEquals() {
    Query q1, q2;

    q1 = IntPoint.newExactQuery("a", 1000);
    q2 = IntPoint.newExactQuery("a", 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(IntPoint.newExactQuery("a", 1)));
    assertFalse(q1.equals(IntPoint.newExactQuery("b", 1000)));

    q1 = LongPoint.newExactQuery("a", 1000);
    q2 = LongPoint.newExactQuery("a", 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(LongPoint.newExactQuery("a", 1)));

    q1 = FloatPoint.newExactQuery("a", 1000);
    q2 = FloatPoint.newExactQuery("a", 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(FloatPoint.newExactQuery("a", 1)));

    q1 = DoublePoint.newExactQuery("a", 1000);
    q2 = DoublePoint.newExactQuery("a", 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(DoublePoint.newExactQuery("a", 1)));

    byte[] ones = new byte[5];
    Arrays.fill(ones, (byte) 0xff);
    q1 = BinaryPoint.newExactQuery("a", ones);
    q2 = BinaryPoint.newExactQuery("a", ones);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    byte[] other = ones.clone();
    other[2] = (byte) 5;
    assertFalse(q1.equals(BinaryPoint.newExactQuery("a", other)));
  }

  public void testPointInSetEquals() {
    Query q1, q2;
    q1 = IntPoint.newSetQuery("a", 0, 1000, 17);
    q2 = IntPoint.newSetQuery("a", 17, 0, 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(IntPoint.newSetQuery("a", 1, 17, 1000)));
    assertFalse(q1.equals(IntPoint.newSetQuery("b", 0, 1000, 17)));

    q1 = LongPoint.newSetQuery("a", 0, 1000, 17);
    q2 = LongPoint.newSetQuery("a", 17, 0, 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(LongPoint.newSetQuery("a", 1, 17, 1000)));

    q1 = FloatPoint.newSetQuery("a", 0, 1000, 17);
    q2 = FloatPoint.newSetQuery("a", 17, 0, 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(FloatPoint.newSetQuery("a", 1, 17, 1000)));

    q1 = DoublePoint.newSetQuery("a", 0, 1000, 17);
    q2 = DoublePoint.newSetQuery("a", 17, 0, 1000);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    assertFalse(q1.equals(DoublePoint.newSetQuery("a", 1, 17, 1000)));

    byte[] zeros = new byte[5];
    byte[] ones = new byte[5];
    Arrays.fill(ones, (byte) 0xff);
    q1 = BinaryPoint.newSetQuery("a", new byte[][] {zeros, ones});
    q2 = BinaryPoint.newSetQuery("a", new byte[][] {zeros, ones});
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());
    byte[] other = ones.clone();
    other[2] = (byte) 5;
    assertFalse(q1.equals(BinaryPoint.newSetQuery("a", new byte[][] {zeros, other})));
  }

  public void testInvalidPointLength() {
    IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                                              () -> {
                                                new PointRangeQuery("field", new byte[4], new byte[8], 1) {
                                                  @Override
                                                  protected String toString(int dimension, byte[] value) {
                                                    return "foo";
                                                  }
                                                };
                                              });
    assertEquals("lowerPoint has length=4 but upperPoint has different length=8", e.getMessage());
  }

  public void testNextUp() {
    assertTrue(Double.compare(0d, DoublePoint.nextUp(-0d)) == 0);
    assertTrue(Double.compare(Double.MIN_VALUE, DoublePoint.nextUp(0d)) == 0);
    assertTrue(Double.compare(Double.POSITIVE_INFINITY, DoublePoint.nextUp(Double.MAX_VALUE)) == 0);
    assertTrue(Double.compare(Double.POSITIVE_INFINITY, DoublePoint.nextUp(Double.POSITIVE_INFINITY)) == 0);
    assertTrue(Double.compare(-Double.MAX_VALUE, DoublePoint.nextUp(Double.NEGATIVE_INFINITY)) == 0);

    assertTrue(Float.compare(0f, FloatPoint.nextUp(-0f)) == 0);
    assertTrue(Float.compare(Float.MIN_VALUE, FloatPoint.nextUp(0f)) == 0);
    assertTrue(Float.compare(Float.POSITIVE_INFINITY, FloatPoint.nextUp(Float.MAX_VALUE)) == 0);
    assertTrue(Float.compare(Float.POSITIVE_INFINITY, FloatPoint.nextUp(Float.POSITIVE_INFINITY)) == 0);
    assertTrue(Float.compare(-Float.MAX_VALUE, FloatPoint.nextUp(Float.NEGATIVE_INFINITY)) == 0);
  }

  public void testNextDown() {
    assertTrue(Double.compare(-0d, DoublePoint.nextDown(0d)) == 0);
    assertTrue(Double.compare(-Double.MIN_VALUE, DoublePoint.nextDown(-0d)) == 0);
    assertTrue(Double.compare(Double.NEGATIVE_INFINITY, DoublePoint.nextDown(-Double.MAX_VALUE)) == 0);
    assertTrue(Double.compare(Double.NEGATIVE_INFINITY, DoublePoint.nextDown(Double.NEGATIVE_INFINITY)) == 0);
    assertTrue(Double.compare(Double.MAX_VALUE, DoublePoint.nextDown(Double.POSITIVE_INFINITY)) == 0);

    assertTrue(Float.compare(-0f, FloatPoint.nextDown(0f)) == 0);
    assertTrue(Float.compare(-Float.MIN_VALUE, FloatPoint.nextDown(-0f)) == 0);
    assertTrue(Float.compare(Float.NEGATIVE_INFINITY, FloatPoint.nextDown(-Float.MAX_VALUE)) == 0);
    assertTrue(Float.compare(Float.NEGATIVE_INFINITY, FloatPoint.nextDown(Float.NEGATIVE_INFINITY)) == 0);
    assertTrue(Float.compare(Float.MAX_VALUE, FloatPoint.nextDown(Float.POSITIVE_INFINITY)) == 0);
  }

  public void testInversePointRange() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    final int numDims = TestUtil.nextInt(random(), 1, 3);
    final int numDocs = atLeast(10 * BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE); // we need multiple leaves to enable this optimization
    for (int i = 0; i < numDocs; ++i) {
      Document doc = new Document();
      int[] values = new int[numDims];
      Arrays.fill(values, i);
      doc.add(new IntPoint("f", values));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    IndexReader r = DirectoryReader.open(w);
    w.close();

    IndexSearcher searcher = newSearcher(r);
    int[] low = new int[numDims];
    int[] high = new int[numDims];
    Arrays.fill(high, numDocs - 2);
    assertEquals(high[0] - low[0] + 1, searcher.count(IntPoint.newRangeQuery("f", low, high)));
    Arrays.fill(low, 1);
    assertEquals(high[0] - low[0] + 1, searcher.count(IntPoint.newRangeQuery("f", low, high)));
    Arrays.fill(high, numDocs - 1);
    assertEquals(high[0] - low[0] + 1, searcher.count(IntPoint.newRangeQuery("f", low, high)));
    Arrays.fill(low, BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE + 1);
    assertEquals(high[0] - low[0] + 1, searcher.count(IntPoint.newRangeQuery("f", low, high)));
    Arrays.fill(high, numDocs - BKDWriter.DEFAULT_MAX_POINTS_IN_LEAF_NODE);
    assertEquals(high[0] - low[0] + 1, searcher.count(IntPoint.newRangeQuery("f", low, high)));

    r.close();
    dir.close();
  }
}
