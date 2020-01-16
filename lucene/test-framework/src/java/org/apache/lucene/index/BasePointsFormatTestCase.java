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
package org.apache.lucene.index;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Rethrow;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Abstract class to do basic tests for a points format.
 * NOTE: This test focuses on the points impl, nothing else.
 * The [stretch] goal is for this test to be
 * so thorough in testing a new PointsFormat that if this
 * test passes, then all Lucene/Solr tests should also pass.  Ie,
 * if there is some bug in a given PointsFormat that this
 * test fails to catch then this test needs to be improved! */
public abstract class BasePointsFormatTestCase extends BaseIndexFileFormatTestCase {

  @Override
  protected void addRandomFields(Document doc) {
    final int numValues = random().nextInt(3);
    for (int i = 0; i < numValues; i++) {
      doc.add(new IntPoint("f", random().nextInt()));
    }
  }
  
  public void testBasic() throws Exception {
    Directory dir = getDirectory(20);
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter w = new IndexWriter(dir, iwc);
    byte[] point = new byte[4];
    for(int i=0;i<20;i++) {
      Document doc = new Document();
      NumericUtils.intToSortableBytes(i, point, 0);
      doc.add(new BinaryPoint("dim", point));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    w.close();

    DirectoryReader r = DirectoryReader.open(dir);
    LeafReader sub = getOnlyLeafReader(r);
    PointValues values = sub.getPointValues("dim");

    // Simple test: make sure intersect can visit every doc:
    BitSet seen = new BitSet();
    values.intersect(
                     new IntersectVisitor() {
                       @Override
                       public Relation compare(byte[] minPacked, byte[] maxPacked) {
                         return Relation.CELL_CROSSES_QUERY;
                       }
                       public void visit(int docID) {
                         throw new IllegalStateException();
                       }
                       public void visit(int docID, byte[] packedValue) {
                         seen.set(docID);
                         assertEquals(docID, NumericUtils.sortableBytesToInt(packedValue, 0));
                       }
                     });
    assertEquals(20, seen.cardinality());
    IOUtils.close(r, dir);
  }

  public void testMerge() throws Exception {
    Directory dir = getDirectory(20);
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter w = new IndexWriter(dir, iwc);
    byte[] point = new byte[4];
    for(int i=0;i<20;i++) {
      Document doc = new Document();
      NumericUtils.intToSortableBytes(i, point, 0);
      doc.add(new BinaryPoint("dim", point));
      w.addDocument(doc);
      if (i == 10) {
        w.commit();
      }
    }
    w.forceMerge(1);
    w.close();

    DirectoryReader r = DirectoryReader.open(dir);
    LeafReader sub = getOnlyLeafReader(r);
    PointValues values = sub.getPointValues("dim");

    // Simple test: make sure intersect can visit every doc:
    BitSet seen = new BitSet();
    values.intersect(
                     new IntersectVisitor() {
                       @Override
                       public Relation compare(byte[] minPacked, byte[] maxPacked) {
                         return Relation.CELL_CROSSES_QUERY;
                       }
                       public void visit(int docID) {
                         throw new IllegalStateException();
                       }
                       public void visit(int docID, byte[] packedValue) {
                         seen.set(docID);
                         assertEquals(docID, NumericUtils.sortableBytesToInt(packedValue, 0));
                       }
                     });
    assertEquals(20, seen.cardinality());
    IOUtils.close(r, dir);
  }

  public void testAllPointDocsDeletedInSegment() throws Exception {
    Directory dir = getDirectory(20);
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    byte[] point = new byte[4];
    for(int i=0;i<10;i++) {
      Document doc = new Document();
      NumericUtils.intToSortableBytes(i, point, 0);
      doc.add(new BinaryPoint("dim", point));
      doc.add(new NumericDocValuesField("id", i));
      doc.add(newStringField("x", "x", Field.Store.NO));
      w.addDocument(doc);
    }
    w.addDocument(new Document());
    w.deleteDocuments(new Term("x", "x"));
    if (random().nextBoolean()) {
      w.forceMerge(1);
    }
    w.close();
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals(1, r.numDocs());
    Bits liveDocs = MultiBits.getLiveDocs(r);

    for(LeafReaderContext ctx : r.leaves()) {
      PointValues values = ctx.reader().getPointValues("dim");

      NumericDocValues idValues = ctx.reader().getNumericDocValues("id");
      if (idValues == null) {
        // this is (surprisingly) OK, because if the random IWC flushes all 10 docs before the 11th doc is added, and force merge runs, it
        // will drop the 100% deleted segments, and the "id" field never exists in the final single doc segment
        continue;
      }
      int[] docIDToID = new int[ctx.reader().maxDoc()];
      int docID;
      while ((docID = idValues.nextDoc()) != NO_MORE_DOCS) {
        docIDToID[docID] = (int) idValues.longValue();
      }
      
      if (values != null) {
        BitSet seen = new BitSet();
        values.intersect(
                         new IntersectVisitor() {
                           @Override
                           public Relation compare(byte[] minPacked, byte[] maxPacked) {
                             return Relation.CELL_CROSSES_QUERY;
                           }
                           public void visit(int docID) {
                             throw new IllegalStateException();
                           }
                           public void visit(int docID, byte[] packedValue) {
                             if (liveDocs.get(docID)) {
                               seen.set(docID);
                             }
                             assertEquals(docIDToID[docID], NumericUtils.sortableBytesToInt(packedValue, 0));
                           }
                         });
        assertEquals(0, seen.cardinality());
      }
    }
    IOUtils.close(r, dir);
  }

  /** Make sure we close open files, delete temp files, etc., on exception */
  public void testWithExceptions() throws Exception {
    int numDocs = atLeast(1000);
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_DIMENSIONS);
    int numIndexDims = TestUtil.nextInt(random(), 1, Math.min(numDims, PointValues.MAX_INDEX_DIMENSIONS));

    byte[][][] docValues = new byte[numDocs][][];

    for(int docID=0;docID<numDocs;docID++) {
      byte[][] values = new byte[numDims][];
      for(int dim=0;dim<numDims;dim++) {
        values[dim] = new byte[numBytesPerDim];
        random().nextBytes(values[dim]);
      }
      docValues[docID] = values;
    }

    // Keep retrying until we 1) we allow a big enough heap, and 2) we hit a random IOExc from MDW:
    boolean done = false;
    while (done == false) {
      try (MockDirectoryWrapper dir = newMockFSDirectory(createTempDir())) {
        try {
          dir.setRandomIOExceptionRate(0.05);
          dir.setRandomIOExceptionRateOnOpen(0.05);
          verify(dir, docValues, null, numDims, numIndexDims, numBytesPerDim, true);
        } catch (IllegalStateException ise) {
          done = handlePossiblyFakeException(ise);
        } catch (AssertionError ae) {
          if (ae.getMessage() != null && ae.getMessage().contains("does not exist; files=")) {
            // OK: likely we threw the random IOExc when IW was asserting the commit files exist
            done = true;
          } else {
            throw ae;
          }
        } catch (IllegalArgumentException iae) {
          // This just means we got a too-small maxMB for the maxPointsInLeafNode; just retry w/ more heap
          assertTrue(iae.getMessage().contains("either increase maxMBSortInHeap or decrease maxPointsInLeafNode"));
        } catch (IOException ioe) {
          done = handlePossiblyFakeException(ioe);
        }
      }
    }
  }

  // TODO: merge w/ BaseIndexFileFormatTestCase.handleFakeIOException
  private boolean handlePossiblyFakeException(Exception e) {
    Throwable ex = e;
    while (ex != null) {
      String message = ex.getMessage();
      if (message != null && (message.contains("a random IOException") || message.contains("background merge hit exception"))) {
        return true;
      }
      ex = ex.getCause();            
    }
    Rethrow.rethrow(e);

    // dead code yet javac disagrees:
    return false;
  }

  public void testMultiValued() throws Exception {
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_DIMENSIONS);
    int numIndexDims = TestUtil.nextInt(random(), 1, Math.min(PointValues.MAX_INDEX_DIMENSIONS, numDims));

    int numDocs = TEST_NIGHTLY ? atLeast(1000) : atLeast(100);
    List<byte[][]> docValues = new ArrayList<>();
    List<Integer> docIDs = new ArrayList<>();

    for(int docID=0;docID<numDocs;docID++) {
      int numValuesInDoc = TestUtil.nextInt(random(), 1, 5);
      for(int ord=0;ord<numValuesInDoc;ord++) {
        docIDs.add(docID);
        byte[][] values = new byte[numDims][];
        for(int dim=0;dim<numDims;dim++) {
          values[dim] = new byte[numBytesPerDim];
          random().nextBytes(values[dim]);
        }
        docValues.add(values);
      }
    }

    byte[][][] docValuesArray = docValues.toArray(new byte[docValues.size()][][]);
    int[] docIDsArray = new int[docIDs.size()];
    for(int i=0;i<docIDsArray.length;i++) {
      docIDsArray[i] = docIDs.get(i);
    }

    verify(docValuesArray, docIDsArray, numDims, numIndexDims, numBytesPerDim);
  }

  public void testAllEqual() throws Exception {
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_INDEX_DIMENSIONS);

    int numDocs = atLeast(1000);
    byte[][][] docValues = new byte[numDocs][][];

    for(int docID=0;docID<numDocs;docID++) {
      if (docID == 0) {
        byte[][] values = new byte[numDims][];
        for(int dim=0;dim<numDims;dim++) {
          values[dim] = new byte[numBytesPerDim];
          random().nextBytes(values[dim]);
        }
        docValues[docID] = values;
      } else {
        docValues[docID] = docValues[0];
      }
    }

    verify(docValues, null, numDims, numBytesPerDim);
  }

  public void testOneDimEqual() throws Exception {
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_INDEX_DIMENSIONS);

    int numDocs = atLeast(1000);
    int theEqualDim = random().nextInt(numDims);
    byte[][][] docValues = new byte[numDocs][][];

    for(int docID=0;docID<numDocs;docID++) {
      byte[][] values = new byte[numDims][];
      for(int dim=0;dim<numDims;dim++) {
        values[dim] = new byte[numBytesPerDim];
        random().nextBytes(values[dim]);
      }
      docValues[docID] = values;
      if (docID > 0) {
        docValues[docID][theEqualDim] = docValues[0][theEqualDim];
      }
    }

    verify(docValues, null, numDims, numBytesPerDim);
  }

  // this should trigger run-length compression with lengths that are greater than 255
  public void testOneDimTwoValues() throws Exception {
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_INDEX_DIMENSIONS);

    int numDocs = atLeast(1000);
    int theDim = random().nextInt(numDims);
    byte[] value1 = new byte[numBytesPerDim];
    random().nextBytes(value1);
    byte[] value2 = new byte[numBytesPerDim];
    random().nextBytes(value2);
    byte[][][] docValues = new byte[numDocs][][];

    for(int docID=0;docID<numDocs;docID++) {
      byte[][] values = new byte[numDims][];
      for(int dim=0;dim<numDims;dim++) {
        if (dim == theDim) {
          values[dim] = random().nextBoolean() ? value1 : value2;
        } else {
          values[dim] = new byte[numBytesPerDim];
          random().nextBytes(values[dim]);
        }
      }
      docValues[docID] = values;
    }

    verify(docValues, null, numDims, numBytesPerDim);
  }

  // Tests on N-dimensional points where each dimension is a BigInteger
  public void testBigIntNDims() throws Exception {

    int numDocs = atLeast(200);
    try (Directory dir = getDirectory(numDocs)) {
      int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
      int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_INDEX_DIMENSIONS);
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      // We rely on docIDs not changing:
      iwc.setMergePolicy(newLogMergePolicy());
      RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
      BigInteger[][] docs = new BigInteger[numDocs][];

      for(int docID=0;docID<numDocs;docID++) {
        BigInteger[] values = new BigInteger[numDims];
        if (VERBOSE) {
          System.out.println("  docID=" + docID);
        }
        byte[][] bytes = new byte[numDims][];
        for(int dim=0;dim<numDims;dim++) {
          values[dim] = randomBigInt(numBytesPerDim);
          bytes[dim] = new byte[numBytesPerDim];
          NumericUtils.bigIntToSortableBytes(values[dim], numBytesPerDim, bytes[dim], 0);
          if (VERBOSE) {
            System.out.println("    " + dim + " -> " + values[dim]);
          }
        }
        docs[docID] = values;
        Document doc = new Document();
        doc.add(new BinaryPoint("field", bytes));
        w.addDocument(doc);
      }

      DirectoryReader r = w.getReader();
      w.close();

      int iters = atLeast(100);
      for(int iter=0;iter<iters;iter++) {
        if (VERBOSE) {
          System.out.println("\nTEST: iter=" + iter);
        }

        // Random N dims rect query:
        BigInteger[] queryMin = new BigInteger[numDims];
        BigInteger[] queryMax = new BigInteger[numDims];    
        for(int dim=0;dim<numDims;dim++) {
          queryMin[dim] = randomBigInt(numBytesPerDim);
          queryMax[dim] = randomBigInt(numBytesPerDim);
          if (queryMin[dim].compareTo(queryMax[dim]) > 0) {
            BigInteger x = queryMin[dim];
            queryMin[dim] = queryMax[dim];
            queryMax[dim] = x;
          }
          if (VERBOSE) {
            System.out.println("  " + dim + "\n    min=" + queryMin[dim] + "\n    max=" + queryMax[dim]);
          }
        }

        final BitSet hits = new BitSet();
        for(LeafReaderContext ctx : r.leaves()) {
          PointValues dimValues = ctx.reader().getPointValues("field");
          if (dimValues == null) {
            continue;
          }

          final int docBase = ctx.docBase;
          
          dimValues.intersect(new IntersectVisitor() {
              @Override
              public void visit(int docID) {
                hits.set(docBase+docID);
                //System.out.println("visit docID=" + docID);
              }

              @Override
              public void visit(int docID, byte[] packedValue) {
                //System.out.println("visit check docID=" + docID);
                for(int dim=0;dim<numDims;dim++) {
                  BigInteger x = NumericUtils.sortableBytesToBigInt(packedValue, dim * numBytesPerDim, numBytesPerDim);
                  if (x.compareTo(queryMin[dim]) < 0 || x.compareTo(queryMax[dim]) > 0) {
                    //System.out.println("  no");
                    return;
                  }
                }

                //System.out.println("  yes");
                hits.set(docBase+docID);
              }

              @Override
              public Relation compare(byte[] minPacked, byte[] maxPacked) {
                boolean crosses = false;
                for(int dim=0;dim<numDims;dim++) {
                  BigInteger min = NumericUtils.sortableBytesToBigInt(minPacked, dim * numBytesPerDim, numBytesPerDim);
                  BigInteger max = NumericUtils.sortableBytesToBigInt(maxPacked, dim * numBytesPerDim, numBytesPerDim);
                  assert max.compareTo(min) >= 0;

                  if (max.compareTo(queryMin[dim]) < 0 || min.compareTo(queryMax[dim]) > 0) {
                    return Relation.CELL_OUTSIDE_QUERY;
                  } else if (min.compareTo(queryMin[dim]) < 0 || max.compareTo(queryMax[dim]) > 0) {
                    crosses = true;
                  }
                }

                if (crosses) {
                  return Relation.CELL_CROSSES_QUERY;
                } else {
                  return Relation.CELL_INSIDE_QUERY;
                }
              }
            });
        }

        for(int docID=0;docID<numDocs;docID++) {
          BigInteger[] docValues = docs[docID];
          boolean expected = true;
          for(int dim=0;dim<numDims;dim++) {
            BigInteger x = docValues[dim];
            if (x.compareTo(queryMin[dim]) < 0 || x.compareTo(queryMax[dim]) > 0) {
              expected = false;
              break;
            }
          }
          boolean actual = hits.get(docID);
          assertEquals("docID=" + docID, expected, actual);
        }
      }
      r.close();
      }
  }

  public void testRandomBinaryTiny() throws Exception {
    doTestRandomBinary(10);
  }

  public void testRandomBinaryMedium() throws Exception {
    doTestRandomBinary(200);
  }

  @Nightly
  public void testRandomBinaryBig() throws Exception {
    assumeFalse("too slow with SimpleText", Codec.getDefault().getName().equals("SimpleText"));
    doTestRandomBinary(200000);
  }

  private void doTestRandomBinary(int count) throws Exception {
    int numDocs = TestUtil.nextInt(random(), count, count*2);
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDataDims = TestUtil.nextInt(random(), 1, PointValues.MAX_INDEX_DIMENSIONS);
    int numIndexDims = TestUtil.nextInt(random(), 1, numDataDims);

    byte[][][] docValues = new byte[numDocs][][];

    for(int docID=0;docID<numDocs;docID++) {
      byte[][] values = new byte[numDataDims][];
      for(int dim=0;dim<numDataDims;dim++) {
        values[dim] = new byte[numBytesPerDim];
        // TODO: sometimes test on a "small" volume too, so we test the high density cases, higher chance of boundary, etc. cases:
        random().nextBytes(values[dim]);
      }
      docValues[docID] = values;
    }

    verify(docValues, null, numDataDims, numIndexDims, numBytesPerDim);
  }

  private void verify(byte[][][] docValues, int[] docIDs, int numDims, int numBytesPerDim) throws Exception {
    verify(docValues, docIDs, numDims, numDims, numBytesPerDim);
  }

  /** docIDs can be null, for the single valued case, else it maps value to docID, but all values for one doc must be adjacent */
  private void verify(byte[][][] docValues, int[] docIDs, int numDataDims, int numIndexDims, int numBytesPerDim) throws Exception {
    try (Directory dir = getDirectory(docValues.length)) {
      while (true) {
        try {
          verify(dir, docValues, docIDs, numDataDims, numIndexDims, numBytesPerDim, false);
          return;
        } catch (IllegalArgumentException iae) {
          iae.printStackTrace();
          // This just means we got a too-small maxMB for the maxPointsInLeafNode; just retry
          assertTrue(iae.getMessage().contains("either increase maxMBSortInHeap or decrease maxPointsInLeafNode"));
        }
      }
    }
  }

  private byte[] flattenBinaryPoint(byte[][] value, int numDataDims, int numBytesPerDim) {
    byte[] result = new byte[value.length * numBytesPerDim];
    for (int d = 0; d < numDataDims; ++d) {
      System.arraycopy(value[d], 0, result, d * numBytesPerDim, numBytesPerDim);
    }
    return result;
  }

  /** test selective indexing */
  private void verify(Directory dir, byte[][][] docValues, int[] ids, int numDims, int numIndexDims, int numBytesPerDim, boolean expectExceptions) throws Exception {
    int numValues = docValues.length;
    if (VERBOSE) {
      System.out.println("TEST: numValues=" + numValues + " numDims=" + numDims + " numIndexDims=" + numIndexDims + " numBytesPerDim=" + numBytesPerDim);
    }

    // RandomIndexWriter is too slow:
    boolean useRealWriter = docValues.length > 10000;

    IndexWriterConfig iwc;
    if (useRealWriter) {
      iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    } else {
      iwc = newIndexWriterConfig();
    }

    if (expectExceptions) {
      MergeScheduler ms = iwc.getMergeScheduler();
      if (ms instanceof ConcurrentMergeScheduler) {
        ((ConcurrentMergeScheduler) ms).setSuppressExceptions();
      }
    }
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    DirectoryReader r = null;

    // Compute actual min/max values:
    byte[][] expectedMinValues = new byte[numDims][];
    byte[][] expectedMaxValues = new byte[numDims][];
    for(int ord=0;ord<docValues.length;ord++) {
      for(int dim=0;dim<numDims;dim++) {
        if (ord == 0) {
          expectedMinValues[dim] = new byte[numBytesPerDim];
          System.arraycopy(docValues[ord][dim], 0, expectedMinValues[dim], 0, numBytesPerDim);
          expectedMaxValues[dim] = new byte[numBytesPerDim];
          System.arraycopy(docValues[ord][dim], 0, expectedMaxValues[dim], 0, numBytesPerDim);
        } else {
          // TODO: it's cheating that we use StringHelper.compare for "truth": what if it's buggy?
          if (FutureArrays.compareUnsigned(docValues[ord][dim], 0, numBytesPerDim, expectedMinValues[dim], 0, numBytesPerDim) < 0) {
            System.arraycopy(docValues[ord][dim], 0, expectedMinValues[dim], 0, numBytesPerDim);
          }
          if (FutureArrays.compareUnsigned(docValues[ord][dim], 0, numBytesPerDim, expectedMaxValues[dim], 0, numBytesPerDim) > 0) {
            System.arraycopy(docValues[ord][dim], 0, expectedMaxValues[dim], 0, numBytesPerDim);
          }
        }
      }
    }

    // 20% of the time we add into a separate directory, then at some point use
    // addIndexes to bring the indexed point values to the main directory:
    Directory saveDir;
    RandomIndexWriter saveW;
    int addIndexesAt;
    if (random().nextInt(5) == 1) {
      saveDir = dir;
      saveW = w;
      dir = getDirectory(numValues);
      if (useRealWriter) {
        iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      } else {
        iwc = newIndexWriterConfig();
      }
      if (expectExceptions) {
        MergeScheduler ms = iwc.getMergeScheduler();
        if (ms instanceof ConcurrentMergeScheduler) {
          ((ConcurrentMergeScheduler) ms).setSuppressExceptions();
        }
      }
      w = new RandomIndexWriter(random(), dir, iwc);
      addIndexesAt = TestUtil.nextInt(random(), 1, numValues-1);
    } else {
      saveW = null;
      saveDir = null;
      addIndexesAt = 0;
    }

    try {

      FieldType fieldType = new FieldType();
      fieldType.setDimensions(numDims, numIndexDims, numBytesPerDim);
      fieldType.freeze();

      Document doc = null;
      int lastID = -1;
      for(int ord=0;ord<numValues;ord++) {
        int id;
        if (ids == null) {
          id = ord;
        } else {
          id = ids[ord];
        }
        if (id != lastID) {
          if (doc != null) {
            if (useRealWriter) {
              w.w.addDocument(doc);
            } else {
              w.addDocument(doc);
            }
          }
          doc = new Document();
          doc.add(new NumericDocValuesField("id", id));
        }
        // pack the binary point
        byte[] val = flattenBinaryPoint(docValues[ord], numDims, numBytesPerDim);

        doc.add(new BinaryPoint("field", val, fieldType));
        lastID = id;

        if (random().nextInt(30) == 17) {
          // randomly index some documents without this field
          if (useRealWriter) {
            w.w.addDocument(new Document());
          } else {
            w.addDocument(new Document());
          }
          if (VERBOSE) {
            System.out.println("add empty doc");
          }
        }

        if (random().nextInt(30) == 17) {
          // randomly index some documents with this field, but we will delete them:
          Document xdoc = new Document();
          val = flattenBinaryPoint(docValues[ord], numDims, numBytesPerDim);
          xdoc.add(new BinaryPoint("field", val, fieldType));
          xdoc.add(new StringField("nukeme", "yes", Field.Store.NO));
          if (useRealWriter) {
            w.w.addDocument(xdoc);
          } else {
            w.addDocument(xdoc);
          }
          if (VERBOSE) {
            System.out.println("add doc doc-to-delete");
          }

          if (random().nextInt(5) == 1) {
            if (useRealWriter) {
              w.w.deleteDocuments(new Term("nukeme", "yes"));
            } else {
              w.deleteDocuments(new Term("nukeme", "yes"));
            }
          }
        }

        if (VERBOSE) {
          System.out.println("  ord=" + ord + " id=" + id);
          for(int dim=0;dim<numDims;dim++) {
            System.out.println("    dim=" + dim + " value=" + new BytesRef(docValues[ord][dim]));
          }
        }

        if (saveW != null && ord >= addIndexesAt) {
          switchIndex(w, dir, saveW);
          w = saveW;
          dir = saveDir;
          saveW = null;
          saveDir = null;
        }
      }
      w.addDocument(doc);
      w.deleteDocuments(new Term("nukeme", "yes"));

      if (random().nextBoolean()) {
        if (VERBOSE) {
          System.out.println("\nTEST: now force merge");
        }
        w.forceMerge(1);
      }

      r = w.getReader();
      w.close();

      if (VERBOSE) {
        System.out.println("TEST: reader=" + r);
      }

      NumericDocValues idValues = MultiDocValues.getNumericValues(r, "id");
      int[] docIDToID = new int[r.maxDoc()];
      {
        int docID;
        while ((docID = idValues.nextDoc()) != NO_MORE_DOCS) {
          docIDToID[docID] = (int) idValues.longValue();
        }
      }

      Bits liveDocs = MultiBits.getLiveDocs(r);

      // Verify min/max values are correct:
      byte[] minValues = new byte[numIndexDims*numBytesPerDim];
      Arrays.fill(minValues, (byte) 0xff);

      byte[] maxValues = new byte[numIndexDims*numBytesPerDim];

      for(LeafReaderContext ctx : r.leaves()) {
        PointValues dimValues = ctx.reader().getPointValues("field");
        if (dimValues == null) {
          continue;
        }

        byte[] leafMinValues = dimValues.getMinPackedValue();
        byte[] leafMaxValues = dimValues.getMaxPackedValue();
        for(int dim=0;dim<numIndexDims;dim++) {
          if (FutureArrays.compareUnsigned(leafMinValues, dim * numBytesPerDim, dim * numBytesPerDim + numBytesPerDim, minValues, dim * numBytesPerDim, dim * numBytesPerDim + numBytesPerDim) < 0) {
            System.arraycopy(leafMinValues, dim*numBytesPerDim, minValues, dim*numBytesPerDim, numBytesPerDim);
          }
          if (FutureArrays.compareUnsigned(leafMaxValues, dim * numBytesPerDim, dim * numBytesPerDim + numBytesPerDim, maxValues, dim * numBytesPerDim, dim * numBytesPerDim + numBytesPerDim) > 0) {
            System.arraycopy(leafMaxValues, dim*numBytesPerDim, maxValues, dim*numBytesPerDim, numBytesPerDim);
          }
        }
      }

      byte[] scratch = new byte[numBytesPerDim];
      for(int dim=0;dim<numIndexDims;dim++) {
        System.arraycopy(minValues, dim*numBytesPerDim, scratch, 0, numBytesPerDim);
        //System.out.println("dim=" + dim + " expectedMin=" + new BytesRef(expectedMinValues[dim]) + " min=" + new BytesRef(scratch));
        assertTrue(Arrays.equals(expectedMinValues[dim], scratch));
        System.arraycopy(maxValues, dim*numBytesPerDim, scratch, 0, numBytesPerDim);
        //System.out.println("dim=" + dim + " expectedMax=" + new BytesRef(expectedMaxValues[dim]) + " max=" + new BytesRef(scratch));
        assertTrue(Arrays.equals(expectedMaxValues[dim], scratch));
      }

      int iters = atLeast(100);
      for(int iter=0;iter<iters;iter++) {
        if (VERBOSE) {
          System.out.println("\nTEST: iter=" + iter);
        }

        // Random N dims rect query:
        byte[][] queryMin = new byte[numIndexDims][];
        byte[][] queryMax = new byte[numIndexDims][];
        for(int dim=0;dim<numIndexDims;dim++) {
          queryMin[dim] = new byte[numBytesPerDim];
          random().nextBytes(queryMin[dim]);
          queryMax[dim] = new byte[numBytesPerDim];
          random().nextBytes(queryMax[dim]);
          if (FutureArrays.compareUnsigned(queryMin[dim], 0, numBytesPerDim, queryMax[dim], 0, numBytesPerDim) > 0) {
            byte[] x = queryMin[dim];
            queryMin[dim] = queryMax[dim];
            queryMax[dim] = x;
          }
        }

        if (VERBOSE) {
          for(int dim=0;dim<numIndexDims;dim++) {
            System.out.println("  dim=" + dim + "\n    queryMin=" + new BytesRef(queryMin[dim]) + "\n    queryMax=" + new BytesRef(queryMax[dim]));
          }
        }

        final BitSet hits = new BitSet();

        for(LeafReaderContext ctx : r.leaves()) {
          PointValues dimValues = ctx.reader().getPointValues("field");
          if (dimValues == null) {
            continue;
          }

          final int docBase = ctx.docBase;

          dimValues.intersect(new PointValues.IntersectVisitor() {
              @Override
              public void visit(int docID) {
                if (liveDocs == null || liveDocs.get(docBase+docID)) {
                  hits.set(docIDToID[docBase+docID]);
                }
                //System.out.println("visit docID=" + docID);
              }

              @Override
              public void visit(int docID, byte[] packedValue) {
                if (liveDocs != null && liveDocs.get(docBase+docID) == false) {
                  return;
                }

                for(int dim=0;dim<numIndexDims;dim++) {
                  //System.out.println("  dim=" + dim + " value=" + new BytesRef(packedValue, dim*numBytesPerDim, numBytesPerDim));
                  if (FutureArrays.compareUnsigned(packedValue, dim * numBytesPerDim, dim * numBytesPerDim + numBytesPerDim, queryMin[dim], 0, numBytesPerDim) < 0 ||
                      FutureArrays.compareUnsigned(packedValue, dim * numBytesPerDim, dim * numBytesPerDim + numBytesPerDim, queryMax[dim], 0, numBytesPerDim) > 0) {
                    //System.out.println("  no");
                    return;
                  }
                }

                //System.out.println("  yes");
                hits.set(docIDToID[docBase+docID]);
              }

              @Override
              public Relation compare(byte[] minPacked, byte[] maxPacked) {
                boolean crosses = false;
                //System.out.println("compare");
                for(int dim=0;dim<numIndexDims;dim++) {
                  if (FutureArrays.compareUnsigned(maxPacked, dim * numBytesPerDim, dim * numBytesPerDim + numBytesPerDim, queryMin[dim], 0, numBytesPerDim) < 0 ||
                      FutureArrays.compareUnsigned(minPacked, dim * numBytesPerDim, dim * numBytesPerDim + numBytesPerDim, queryMax[dim], 0, numBytesPerDim) > 0) {
                    //System.out.println("  query_outside_cell");
                    return Relation.CELL_OUTSIDE_QUERY;
                  } else if (FutureArrays.compareUnsigned(minPacked, dim * numBytesPerDim, dim * numBytesPerDim + numBytesPerDim, queryMin[dim], 0, numBytesPerDim) < 0 ||
                             FutureArrays.compareUnsigned(maxPacked, dim * numBytesPerDim, dim * numBytesPerDim + numBytesPerDim, queryMax[dim], 0, numBytesPerDim) > 0) {
                    crosses = true;
                  }
                }

                if (crosses) {
                  //System.out.println("  query_crosses_cell");
                  return Relation.CELL_CROSSES_QUERY;
                } else {
                  //System.out.println("  cell_inside_query");
                  return Relation.CELL_INSIDE_QUERY;
                }
              }
            });
        }

        BitSet expected = new BitSet();
        for(int ord=0;ord<numValues;ord++) {
          boolean matches = true;
          for(int dim=0;dim<numIndexDims;dim++) {
            byte[] x = docValues[ord][dim];
            if (FutureArrays.compareUnsigned(x, 0, numBytesPerDim, queryMin[dim], 0, numBytesPerDim) < 0 ||
                FutureArrays.compareUnsigned(x, 0, numBytesPerDim, queryMax[dim], 0, numBytesPerDim) > 0) {
              matches = false;
              break;
            }
          }

          if (matches) {
            int id;
            if (ids == null) {
              id = ord;
            } else {
              id = ids[ord];
            }
            expected.set(id);
          }
        }

        int limit = Math.max(expected.length(), hits.length());
        int failCount = 0;
        int successCount = 0;
        for(int id=0;id<limit;id++) {
          if (expected.get(id) != hits.get(id)) {
            System.out.println("FAIL: id=" + id);
            failCount++;
          } else {
            successCount++;
          }
        }

        if (failCount != 0) {
          for(int docID=0;docID<r.maxDoc();docID++) {
            System.out.println("  docID=" + docID + " id=" + docIDToID[docID]);
          }

          fail(failCount + " docs failed; " + successCount + " docs succeeded");
        }
      }
    } finally {
      IOUtils.closeWhileHandlingException(r, w, saveW, saveDir == null ? null : dir);
    }
  }

  public void testAddIndexes() throws IOException {
    Directory dir1 = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir1);
    Document doc = new Document();
    doc.add(new IntPoint("int1", 17));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new IntPoint("int2", 42));
    w.addDocument(doc);
    w.close();

    // Different field number assigments:
    Directory dir2 = newDirectory();
    w = new RandomIndexWriter(random(), dir2);
    doc = new Document();
    doc.add(new IntPoint("int2", 42));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new IntPoint("int1", 17));
    w.addDocument(doc);
    w.close();

    Directory dir = newDirectory();
    w = new RandomIndexWriter(random(), dir);
    w.addIndexes(new Directory[] {dir1, dir2});
    w.forceMerge(1);

    DirectoryReader r = w.getReader();
    IndexSearcher s = newSearcher(r, false);
    assertEquals(2, s.count(IntPoint.newExactQuery("int1", 17)));
    assertEquals(2, s.count(IntPoint.newExactQuery("int2", 42)));
    r.close();
    w.close();
    dir.close();
    dir1.close();
    dir2.close();
  }

  private void switchIndex(RandomIndexWriter w, Directory dir, RandomIndexWriter saveW) throws IOException {
    if (random().nextBoolean()) {
      // Add via readers:
      try (DirectoryReader r = w.getReader()) {
        if (random().nextBoolean()) {
          // Add via CodecReaders:
          List<CodecReader> subs = new ArrayList<>();
          for (LeafReaderContext context : r.leaves()) {
            subs.add((CodecReader) context.reader());
          }
          if (VERBOSE) {
            System.out.println("TEST: now use addIndexes(CodecReader[]) to switch writers");
          }
          saveW.addIndexes(subs.toArray(new CodecReader[subs.size()]));
        } else {
          if (VERBOSE) {
            System.out.println("TEST: now use TestUtil.addIndexesSlowly(DirectoryReader[]) to switch writers");
          }
          TestUtil.addIndexesSlowly(saveW.w, r);
        }
      }
    } else {
      // Add via directory:
      if (VERBOSE) {
        System.out.println("TEST: now use addIndexes(Directory[]) to switch writers");
      }
      w.close();
      saveW.addIndexes(new Directory[] {dir});
    }
    w.close();
    dir.close();
  }

  private BigInteger randomBigInt(int numBytes) {
    BigInteger x = new BigInteger(numBytes*8-1, random());
    if (random().nextBoolean()) {
      x = x.negate();
    }
    return x;
  }

  private Directory getDirectory(int numPoints) throws IOException {
    Directory dir;
    if (numPoints > 100000) {
      dir = newFSDirectory(createTempDir("TestBKDTree"));
    } else {
      dir = newDirectory();
    }
    //dir = FSDirectory.open(createTempDir());
    return dir;
  }

  @Override
  protected boolean mergeIsStable() {
    // suppress this test from base class: merges for BKD trees are not stable because the tree created by merge will have a different
    // structure than the tree created by adding points separately
    return false;
  }

  // LUCENE-7491
  public void testMixedSchema() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    iwc.setMaxBufferedDocs(2);
    for(int i=0;i<2;i++) {
      Document doc = new Document();
      doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
      doc.add(new IntPoint("int", i));
      w.addDocument(doc);
    }
    // index has 1 segment now (with 2 docs) and that segment does have points, but the "id" field in particular does NOT

    Document doc = new Document();
    doc.add(new IntPoint("id", 0));
    w.addDocument(doc);
    // now we write another segment where the id field does have points:
    
    w.forceMerge(1);
    IOUtils.close(w, dir);
  }
}
