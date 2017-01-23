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
package org.apache.lucene.util.bkd;


import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.CorruptingIndexOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;

public class TestBKD extends LuceneTestCase {

  public void testBasicInts1D() throws Exception {
    try (Directory dir = getDirectory(100)) {
      BKDWriter w = new BKDWriter(100, dir, "tmp", 1, 4, 2, 1.0f, 100, true);
      byte[] scratch = new byte[4];
      for(int docID=0;docID<100;docID++) {
        NumericUtils.intToSortableBytes(docID, scratch, 0);
        w.add(scratch, docID);
      }

      long indexFP;
      try (IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT)) {
        indexFP = w.finish(out);
      }

      try (IndexInput in = dir.openInput("bkd", IOContext.DEFAULT)) {
        in.seek(indexFP);
        BKDReader r = new BKDReader(in);

        // Simple 1D range query:
        final int queryMin = 42;
        final int queryMax = 87;

        final BitSet hits = new BitSet();
        r.intersect(new IntersectVisitor() {
            @Override
            public void visit(int docID) {
              hits.set(docID);
              if (VERBOSE) {
                System.out.println("visit docID=" + docID);
              }
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              int x = NumericUtils.sortableBytesToInt(packedValue, 0);
              if (VERBOSE) {
                System.out.println("visit docID=" + docID + " x=" + x);
              }
              if (x >= queryMin && x <= queryMax) {
                hits.set(docID);
              }
            }

            @Override
            public Relation compare(byte[] minPacked, byte[] maxPacked) {
              int min = NumericUtils.sortableBytesToInt(minPacked, 0);
              int max = NumericUtils.sortableBytesToInt(maxPacked, 0);
              assert max >= min;
              if (VERBOSE) {
                System.out.println("compare: min=" + min + " max=" + max + " vs queryMin=" + queryMin + " queryMax=" + queryMax);
              }

              if (max < queryMin || min > queryMax) {
                return Relation.CELL_OUTSIDE_QUERY;
              } else if (min >= queryMin && max <= queryMax) {
                return Relation.CELL_INSIDE_QUERY;
              } else {
                return Relation.CELL_CROSSES_QUERY;
              }
            }
          });

        for(int docID=0;docID<100;docID++) {
          boolean expected = docID >= queryMin && docID <= queryMax;
          boolean actual = hits.get(docID);
          assertEquals("docID=" + docID, expected, actual);
        }
      }
    }
  }

  public void testRandomIntsNDims() throws Exception {
    int numDocs = atLeast(1000);
    try (Directory dir = getDirectory(numDocs)) {
      int numDims = TestUtil.nextInt(random(), 1, 5);
      int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 100);
      float maxMB = (float) 3.0 + (3*random().nextFloat());
      BKDWriter w = new BKDWriter(numDocs, dir, "tmp", numDims, 4, maxPointsInLeafNode, maxMB, numDocs, true);

      if (VERBOSE) {
        System.out.println("TEST: numDims=" + numDims + " numDocs=" + numDocs);
      }
      int[][] docs = new int[numDocs][];
      byte[] scratch = new byte[4*numDims];
      int[] minValue = new int[numDims];
      int[] maxValue = new int[numDims];
      Arrays.fill(minValue, Integer.MAX_VALUE);
      Arrays.fill(maxValue, Integer.MIN_VALUE);
      for(int docID=0;docID<numDocs;docID++) {
        int[] values = new int[numDims];
        if (VERBOSE) {
          System.out.println("  docID=" + docID);
        }
        for(int dim=0;dim<numDims;dim++) {
          values[dim] = random().nextInt();
          if (values[dim] < minValue[dim]) {
            minValue[dim] = values[dim];
          }
          if (values[dim] > maxValue[dim]) {
            maxValue[dim] = values[dim];
          }
          NumericUtils.intToSortableBytes(values[dim], scratch, dim * Integer.BYTES);
          if (VERBOSE) {
            System.out.println("    " + dim + " -> " + values[dim]);
          }
        }
        docs[docID] = values;
        w.add(scratch, docID);
      }

      long indexFP;
      try (IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT)) {
        indexFP = w.finish(out);
      }

      try (IndexInput in = dir.openInput("bkd", IOContext.DEFAULT)) {
        in.seek(indexFP);
        BKDReader r = new BKDReader(in);

        byte[] minPackedValue = r.getMinPackedValue();
        byte[] maxPackedValue = r.getMaxPackedValue();
        for(int dim=0;dim<numDims;dim++) {
          assertEquals(minValue[dim], NumericUtils.sortableBytesToInt(minPackedValue, dim * Integer.BYTES));
          assertEquals(maxValue[dim], NumericUtils.sortableBytesToInt(maxPackedValue, dim * Integer.BYTES));
        }

        int iters = atLeast(100);
        for(int iter=0;iter<iters;iter++) {
          if (VERBOSE) {
            System.out.println("\nTEST: iter=" + iter);
          }

          // Random N dims rect query:
          int[] queryMin = new int[numDims];
          int[] queryMax = new int[numDims];    
          for(int dim=0;dim<numDims;dim++) {
            queryMin[dim] = random().nextInt();
            queryMax[dim] = random().nextInt();
            if (queryMin[dim] > queryMax[dim]) {
              int x = queryMin[dim];
              queryMin[dim] = queryMax[dim];
              queryMax[dim] = x;
            }
          }

          final BitSet hits = new BitSet();
          r.intersect(new IntersectVisitor() {
            @Override
            public void visit(int docID) {
              hits.set(docID);
              //System.out.println("visit docID=" + docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              //System.out.println("visit check docID=" + docID);
              for(int dim=0;dim<numDims;dim++) {
                int x = NumericUtils.sortableBytesToInt(packedValue, dim * Integer.BYTES);
                if (x < queryMin[dim] || x > queryMax[dim]) {
                  //System.out.println("  no");
                  return;
                }
              }

              //System.out.println("  yes");
              hits.set(docID);
            }

            @Override
            public Relation compare(byte[] minPacked, byte[] maxPacked) {
              boolean crosses = false;
              for(int dim=0;dim<numDims;dim++) {
                int min = NumericUtils.sortableBytesToInt(minPacked, dim * Integer.BYTES);
                int max = NumericUtils.sortableBytesToInt(maxPacked, dim * Integer.BYTES);
                assert max >= min;

                if (max < queryMin[dim] || min > queryMax[dim]) {
                  return Relation.CELL_OUTSIDE_QUERY;
                } else if (min < queryMin[dim] || max > queryMax[dim]) {
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

          for(int docID=0;docID<numDocs;docID++) {
            int[] docValues = docs[docID];
            boolean expected = true;
            for(int dim=0;dim<numDims;dim++) {
              int x = docValues[dim];
              if (x < queryMin[dim] || x > queryMax[dim]) {
                expected = false;
                break;
              }
            }
            boolean actual = hits.get(docID);
            assertEquals("docID=" + docID, expected, actual);
          }
        }
      }
    }
  }

  // Tests on N-dimensional points where each dimension is a BigInteger
  public void testBigIntNDims() throws Exception {

    int numDocs = atLeast(1000);
    try (Directory dir = getDirectory(numDocs)) {
      int numBytesPerDim = TestUtil.nextInt(random(), 2, 30);
      int numDims = TestUtil.nextInt(random(), 1, 5);
      int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 100);
      float maxMB = (float) 3.0 + (3*random().nextFloat());
      BKDWriter w = new BKDWriter(numDocs, dir, "tmp", numDims, numBytesPerDim, maxPointsInLeafNode, maxMB, numDocs, true);
      BigInteger[][] docs = new BigInteger[numDocs][];

      byte[] scratch = new byte[numBytesPerDim*numDims];
      for(int docID=0;docID<numDocs;docID++) {
        BigInteger[] values = new BigInteger[numDims];
        if (VERBOSE) {
          System.out.println("  docID=" + docID);
        }
        for(int dim=0;dim<numDims;dim++) {
          values[dim] = randomBigInt(numBytesPerDim);
          NumericUtils.bigIntToSortableBytes(values[dim], numBytesPerDim, scratch, dim * numBytesPerDim);
          if (VERBOSE) {
            System.out.println("    " + dim + " -> " + values[dim]);
          }
        }
        docs[docID] = values;
        w.add(scratch, docID);
      }

      long indexFP;
      try (IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT)) {
        indexFP = w.finish(out);
      }

      try (IndexInput in = dir.openInput("bkd", IOContext.DEFAULT)) {
        in.seek(indexFP);
        BKDReader r = new BKDReader(in);

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
          }

          final BitSet hits = new BitSet();
          r.intersect(new IntersectVisitor() {
            @Override
            public void visit(int docID) {
              hits.set(docID);
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
              hits.set(docID);
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
      }
    }
  }

  /** Make sure we close open files, delete temp files, etc., on exception */
  public void testWithExceptions() throws Exception {
    int numDocs = atLeast(10000);
    int numBytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int numDims = TestUtil.nextInt(random(), 1, 5);

    byte[][][] docValues = new byte[numDocs][][];

    for(int docID=0;docID<numDocs;docID++) {
      byte[][] values = new byte[numDims][];
      for(int dim=0;dim<numDims;dim++) {
        values[dim] = new byte[numBytesPerDim];
        random().nextBytes(values[dim]);
      }
      docValues[docID] = values;
    }

    double maxMBHeap = 0.05;
    // Keep retrying until we 1) we allow a big enough heap, and 2) we hit a random IOExc from MDW:
    boolean done = false;
    while (done == false) {
      MockDirectoryWrapper dir = newMockFSDirectory(createTempDir());
      try {
        dir.setRandomIOExceptionRate(0.05);
        dir.setRandomIOExceptionRateOnOpen(0.05);
        verify(dir, docValues, null, numDims, numBytesPerDim, 50, maxMBHeap);
      } catch (IllegalArgumentException iae) {
        // This just means we got a too-small maxMB for the maxPointsInLeafNode; just retry w/ more heap
        assertTrue(iae.getMessage().contains("either increase maxMBSortInHeap or decrease maxPointsInLeafNode"));
        maxMBHeap *= 1.25;
      } catch (IOException ioe) {
        if (ioe.getMessage().contains("a random IOException")) {
          // BKDWriter should fully clean up after itself:
          done = true;
        } else {
          throw ioe;
        }
      }

      String[] files = dir.listAll();
      assertTrue("files=" + Arrays.toString(files), files.length == 0 || Arrays.equals(files, new String[] {"extra0"}));
      dir.close();
    }
  }

  public void testRandomBinaryTiny() throws Exception {
    doTestRandomBinary(10);
  }

  public void testRandomBinaryMedium() throws Exception {
    doTestRandomBinary(10000);
  }

  @Nightly
  public void testRandomBinaryBig() throws Exception {
    doTestRandomBinary(200000);
  }

  public void testTooLittleHeap() throws Exception { 
    try (Directory dir = getDirectory(0)) {
      IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
        new BKDWriter(1, dir, "bkd", 1, 16, 1000000, 0.001, 0, true);
      });
      assertTrue(expected.getMessage().contains("either increase maxMBSortInHeap or decrease maxPointsInLeafNode"));
    }
  }

  private void doTestRandomBinary(int count) throws Exception {
    int numDocs = TestUtil.nextInt(random(), count, count*2);
    int numBytesPerDim = TestUtil.nextInt(random(), 2, 30);

    int numDims = TestUtil.nextInt(random(), 1, 5);

    byte[][][] docValues = new byte[numDocs][][];

    for(int docID=0;docID<numDocs;docID++) {
      byte[][] values = new byte[numDims][];
      for(int dim=0;dim<numDims;dim++) {
        values[dim] = new byte[numBytesPerDim];
        random().nextBytes(values[dim]);
      }
      docValues[docID] = values;
    }

    verify(docValues, null, numDims, numBytesPerDim);
  }

  public void testAllEqual() throws Exception {
    int numBytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int numDims = TestUtil.nextInt(random(), 1, 5);

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
    int numBytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int numDims = TestUtil.nextInt(random(), 1, 5);

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

    // Use a small number of points in leaf blocks to trigger a lot of splitting
    verify(docValues, null, numDims, numBytesPerDim, TestUtil.nextInt(random(), 20, 50));
  }

  // This triggers the logic that makes sure all dimensions get indexed
  // by looking at how many times each dim has been split
  public void testOneDimLowCard() throws Exception {
    int numBytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int numDims = TestUtil.nextInt(random(), 2, 5);

    int numDocs = atLeast(10000);
    int theLowCardDim = random().nextInt(numDims);

    byte[] value1 = new byte[numBytesPerDim];
    random().nextBytes(value1);
    byte[] value2 = value1.clone();
    if (value2[numBytesPerDim-1] == 0 || random().nextBoolean()) {
      value2[numBytesPerDim-1]++;
    } else {
      value2[numBytesPerDim-1]--;
    }

    byte[][][] docValues = new byte[numDocs][][];

    for(int docID=0;docID<numDocs;docID++) {
      byte[][] values = new byte[numDims][];
      for(int dim=0;dim<numDims;dim++) {
        if (dim == theLowCardDim) {
          values[dim] = random().nextBoolean() ? value1 : value2;
        } else {
          values[dim] = new byte[numBytesPerDim];
          random().nextBytes(values[dim]);
        }
      }
      docValues[docID] = values;
    }

    // Use a small number of points in leaf blocks to trigger a lot of splitting
    verify(docValues, null, numDims, numBytesPerDim, TestUtil.nextInt(random(), 20, 50));
  }

  // this should trigger run-length compression with lengths that are greater than 255
  public void testOneDimTwoValues() throws Exception {
    int numBytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int numDims = TestUtil.nextInt(random(), 1, 5);

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

  public void testMultiValued() throws Exception {
    int numBytesPerDim = TestUtil.nextInt(random(), 2, 30);
    int numDims = TestUtil.nextInt(random(), 1, 5);

    int numDocs = atLeast(1000);
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

    verify(docValuesArray, docIDsArray, numDims, numBytesPerDim);
  }

  /** docIDs can be null, for the single valued case, else it maps value to docID */
  private void verify(byte[][][] docValues, int[] docIDs, int numDims, int numBytesPerDim) throws Exception {
    verify(docValues, docIDs, numDims, numBytesPerDim, TestUtil.nextInt(random(), 50, 1000));
  }

  private void verify(byte[][][] docValues, int[] docIDs, int numDims, int numBytesPerDim,
      int maxPointsInLeafNode) throws Exception {
    try (Directory dir = getDirectory(docValues.length)) {
      double maxMB = (float) 3.0 + (3*random().nextDouble());
      verify(dir, docValues, docIDs, numDims, numBytesPerDim, maxPointsInLeafNode, maxMB);
    }
  }

  private void verify(Directory dir, byte[][][] docValues, int[] docIDs, int numDims, int numBytesPerDim, int maxPointsInLeafNode, double maxMB) throws Exception {
    int numValues = docValues.length;
    if (VERBOSE) {
      System.out.println("TEST: numValues=" + numValues + " numDims=" + numDims + " numBytesPerDim=" + numBytesPerDim + " maxPointsInLeafNode=" + maxPointsInLeafNode + " maxMB=" + maxMB);
    }

    List<Long> toMerge = null;
    List<MergeState.DocMap> docMaps = null;
    int seg = 0;

    BKDWriter w = new BKDWriter(numValues, dir, "_" + seg, numDims, numBytesPerDim, maxPointsInLeafNode, maxMB, docValues.length, false);
    IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT);
    IndexInput in = null;

    boolean success = false;

    try {

      byte[] scratch = new byte[numBytesPerDim*numDims];
      int lastDocIDBase = 0;
      boolean useMerge = numDims == 1 && numValues >= 10 && random().nextBoolean();
      int valuesInThisSeg;
      if (useMerge) {
        // Sometimes we will call merge with a single segment:
        valuesInThisSeg = TestUtil.nextInt(random(), numValues/10, numValues);
      } else {
        valuesInThisSeg = 0;
      }

      int segCount = 0;

      for(int ord=0;ord<numValues;ord++) {
        int docID;
        if (docIDs == null) {
          docID = ord;
        } else {
          docID = docIDs[ord];
        }
        if (VERBOSE) {
          System.out.println("  ord=" + ord + " docID=" + docID + " lastDocIDBase=" + lastDocIDBase);
        }
        for(int dim=0;dim<numDims;dim++) {
          if (VERBOSE) {
            System.out.println("    " + dim + " -> " + new BytesRef(docValues[ord][dim]));
          }
          System.arraycopy(docValues[ord][dim], 0, scratch, dim*numBytesPerDim, numBytesPerDim);
        }
        w.add(scratch, docID-lastDocIDBase);

        segCount++;

        if (useMerge && segCount == valuesInThisSeg) {
          if (toMerge == null) {
            toMerge = new ArrayList<>();
            docMaps = new ArrayList<>();
          }
          final int curDocIDBase = lastDocIDBase;
          docMaps.add(new MergeState.DocMap() {
              @Override
              public int get(int docID) {
                return curDocIDBase + docID;
              }
            });
          toMerge.add(w.finish(out));
          valuesInThisSeg = TestUtil.nextInt(random(), numValues/10, numValues/2);
          segCount = 0;

          seg++;
          maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 1000);
          maxMB = (float) 3.0 + (3*random().nextDouble());
          w = new BKDWriter(numValues, dir, "_" + seg, numDims, numBytesPerDim, maxPointsInLeafNode, maxMB, docValues.length, false);
          lastDocIDBase = docID;
        }
      }

      long indexFP;

      if (toMerge != null) {
        if (segCount > 0) {
          toMerge.add(w.finish(out));
          final int curDocIDBase = lastDocIDBase;
          docMaps.add(new MergeState.DocMap() {
              @Override
              public int get(int docID) {
                return curDocIDBase + docID;
              }
            });
        }
        out.close();
        in = dir.openInput("bkd", IOContext.DEFAULT);
        seg++;
        w = new BKDWriter(numValues, dir, "_" + seg, numDims, numBytesPerDim, maxPointsInLeafNode, maxMB, docValues.length, false);
        List<BKDReader> readers = new ArrayList<>();
        for(long fp : toMerge) {
          in.seek(fp);
          readers.add(new BKDReader(in));
        }
        out = dir.createOutput("bkd2", IOContext.DEFAULT);
        indexFP = w.merge(out, docMaps, readers);
        out.close();
        in.close();
        in = dir.openInput("bkd2", IOContext.DEFAULT);
      } else {
        indexFP = w.finish(out);
        out.close();
        in = dir.openInput("bkd", IOContext.DEFAULT);
      }

      in.seek(indexFP);
      BKDReader r = new BKDReader(in);

      int iters = atLeast(100);
      for(int iter=0;iter<iters;iter++) {
        if (VERBOSE) {
          System.out.println("\nTEST: iter=" + iter);
        }

        // Random N dims rect query:
        byte[][] queryMin = new byte[numDims][];
        byte[][] queryMax = new byte[numDims][];    
        for(int dim=0;dim<numDims;dim++) {    
          queryMin[dim] = new byte[numBytesPerDim];
          random().nextBytes(queryMin[dim]);
          queryMax[dim] = new byte[numBytesPerDim];
          random().nextBytes(queryMax[dim]);
          if (StringHelper.compare(numBytesPerDim, queryMin[dim], 0, queryMax[dim], 0) > 0) {
            byte[] x = queryMin[dim];
            queryMin[dim] = queryMax[dim];
            queryMax[dim] = x;
          }
        }

        final BitSet hits = new BitSet();
        r.intersect(new IntersectVisitor() {
            @Override
            public void visit(int docID) {
              hits.set(docID);
              //System.out.println("visit docID=" + docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              //System.out.println("visit check docID=" + docID);
              for(int dim=0;dim<numDims;dim++) {
                if (StringHelper.compare(numBytesPerDim, packedValue, dim*numBytesPerDim, queryMin[dim], 0) < 0 ||
                    StringHelper.compare(numBytesPerDim, packedValue, dim*numBytesPerDim, queryMax[dim], 0) > 0) {
                  //System.out.println("  no");
                  return;
                }
              }

              //System.out.println("  yes");
              hits.set(docID);
            }

            @Override
            public Relation compare(byte[] minPacked, byte[] maxPacked) {
              boolean crosses = false;
              for(int dim=0;dim<numDims;dim++) {
                if (StringHelper.compare(numBytesPerDim, maxPacked, dim*numBytesPerDim, queryMin[dim], 0) < 0 ||
                    StringHelper.compare(numBytesPerDim, minPacked, dim*numBytesPerDim, queryMax[dim], 0) > 0) {
                  return Relation.CELL_OUTSIDE_QUERY;
                } else if (StringHelper.compare(numBytesPerDim, minPacked, dim*numBytesPerDim, queryMin[dim], 0) < 0 ||
                           StringHelper.compare(numBytesPerDim, maxPacked, dim*numBytesPerDim, queryMax[dim], 0) > 0) {
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

        BitSet expected = new BitSet();
        for(int ord=0;ord<numValues;ord++) {
          boolean matches = true;
          for(int dim=0;dim<numDims;dim++) {
            byte[] x = docValues[ord][dim];
            if (StringHelper.compare(numBytesPerDim, x, 0, queryMin[dim], 0) < 0 ||
                StringHelper.compare(numBytesPerDim, x, 0, queryMax[dim], 0) > 0) {
              matches = false;
              break;
            }
          }

          if (matches) {
            int docID;
            if (docIDs == null) {
              docID = ord;
            } else {
              docID = docIDs[ord];
            }
            expected.set(docID);
          }
        }

        int limit = Math.max(expected.length(), hits.length());
        for(int docID=0;docID<limit;docID++) {
          assertEquals("docID=" + docID, expected.get(docID), hits.get(docID));
        }
      }
      in.close();
      dir.deleteFile("bkd");
      if (toMerge != null) {
        dir.deleteFile("bkd2");
      }
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(w, in, out);
        IOUtils.deleteFilesIgnoringExceptions(dir, "bkd", "bkd2");
      }
    }
  }

  private BigInteger randomBigInt(int numBytes) {
    BigInteger x = new BigInteger(numBytes*8-1, random());
    if (random().nextBoolean()) {
      x = x.negate();
    }
    return x;
  }

  private Directory getDirectory(int numPoints) {
    Directory dir;
    if (numPoints > 100000) {
      dir = newFSDirectory(createTempDir("TestBKDTree"));
    } else {
      dir = newDirectory();
    }
    return dir;
  }

  /** Make sure corruption on an input sort file is caught, even if BKDWriter doesn't get angry */
  public void testBitFlippedOnPartition1() throws Exception {

    // Generate fixed data set:
    int numDocs = atLeast(10000);
    int numBytesPerDim = 4;
    int numDims = 3;

    byte[][][] docValues = new byte[numDocs][][];
    byte counter = 0;

    for(int docID=0;docID<numDocs;docID++) {
      byte[][] values = new byte[numDims][];
      for(int dim=0;dim<numDims;dim++) {
        values[dim] = new byte[numBytesPerDim];
        for(int i=0;i<values[dim].length;i++) {
          values[dim][i] = counter;
          counter++;
        }
      }
      docValues[docID] = values;
    }

    try (Directory dir0 = newMockDirectory()) {

      Directory dir = new FilterDirectory(dir0) {
        boolean corrupted;
        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
          IndexOutput out = in.createTempOutput(prefix, suffix, context);
          if (corrupted == false && prefix.equals("_0_bkd1") && suffix.equals("sort")) {
            corrupted = true;
            return new CorruptingIndexOutput(dir0, 22, out);
          } else {
            return out;
          }
        }
      };

      CorruptIndexException e = expectThrows(CorruptIndexException.class, () -> {
          verify(dir, docValues, null, numDims, numBytesPerDim, 50, 0.1);
        });
      assertTrue(e.getMessage().contains("checksum failed (hardware problem?)"));
    }
  }

  /** Make sure corruption on a recursed partition is caught, when BKDWriter does get angry */
  public void testBitFlippedOnPartition2() throws Exception {

    // Generate fixed data set:
    int numDocs = atLeast(10000);
    int numBytesPerDim = 4;
    int numDims = 3;

    byte[][][] docValues = new byte[numDocs][][];
    byte counter = 0;

    for(int docID=0;docID<numDocs;docID++) {
      byte[][] values = new byte[numDims][];
      for(int dim=0;dim<numDims;dim++) {
        values[dim] = new byte[numBytesPerDim];
        for(int i=0;i<values[dim].length;i++) {
          values[dim][i] = counter;
          counter++;
        }
      }
      docValues[docID] = values;
    }

    try (Directory dir0 = newMockDirectory()) {

      Directory dir = new FilterDirectory(dir0) {
        boolean corrupted;
        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
          IndexOutput out = in.createTempOutput(prefix, suffix, context);
          //System.out.println("prefix=" + prefix + " suffix=" + suffix);
          if (corrupted == false && suffix.equals("bkd_left1")) {
            //System.out.println("now corrupt byte=" + x + " prefix=" + prefix + " suffix=" + suffix);
            corrupted = true;
            return new CorruptingIndexOutput(dir0, 22072, out);
          } else {
            return out;
          }
        }
      };

      Throwable t = expectThrows(CorruptIndexException.class, () -> {
          verify(dir, docValues, null, numDims, numBytesPerDim, 50, 0.1);
        });
      assertCorruptionDetected(t);
    }
  }

  private void assertCorruptionDetected(Throwable t) {
    if (t instanceof CorruptIndexException) {
      if (t.getMessage().contains("checksum failed (hardware problem?)")) {
        return;
      }
    }

    for(Throwable suppressed : t.getSuppressed()) {
      if (suppressed instanceof CorruptIndexException) {
        if (suppressed.getMessage().contains("checksum failed (hardware problem?)")) {
          return;
        }
      }
    }
    fail("did not see a suppressed CorruptIndexException");
  }

  public void testTieBreakOrder() throws Exception {
    try (Directory dir = newDirectory()) {
      int numDocs = 10000;
      BKDWriter w = new BKDWriter(numDocs+1, dir, "tmp", 1, Integer.BYTES, 2, 0.01f, numDocs, true);
      for(int i=0;i<numDocs;i++) {
        w.add(new byte[Integer.BYTES], i);
      }

      IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT);
      long fp = w.finish(out);
      out.close();

      IndexInput in = dir.openInput("bkd", IOContext.DEFAULT);
      in.seek(fp);
      BKDReader r = new BKDReader(in);
      r.intersect(new IntersectVisitor() {
          int lastDocID = -1;

          @Override
          public void visit(int docID) {
            assertTrue("lastDocID=" + lastDocID + " docID=" + docID, docID > lastDocID);
            lastDocID = docID;
          }

          @Override
          public void visit(int docID, byte[] packedValue) {
            visit(docID);
          }

          @Override
          public Relation compare(byte[] minPacked, byte[] maxPacked) {
            return Relation.CELL_CROSSES_QUERY;
          }
      });
      in.close();
    }
  }

  public void test2DLongOrdsOffline() throws Exception {
    try (Directory dir = newDirectory()) {
      int numDocs = 100000;
      boolean singleValuePerDoc = false;
      boolean longOrds = true;
      int offlineSorterMaxTempFiles = TestUtil.nextInt(random(), 2, 20);
      BKDWriter w = new BKDWriter(numDocs+1, dir, "tmp", 2, Integer.BYTES, 2, 0.01f, numDocs,
                                  singleValuePerDoc, longOrds, 1, offlineSorterMaxTempFiles);
      byte[] buffer = new byte[2*Integer.BYTES];
      for(int i=0;i<numDocs;i++) {
        random().nextBytes(buffer);
        w.add(buffer, i);
      }

      IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT);
      long fp = w.finish(out);
      out.close();

      IndexInput in = dir.openInput("bkd", IOContext.DEFAULT);
      in.seek(fp);
      BKDReader r = new BKDReader(in);
      int[] count = new int[1];
      r.intersect(new IntersectVisitor() {

          @Override
          public void visit(int docID) {
            count[0]++;
          }

          @Override
          public void visit(int docID, byte[] packedValue) {
            visit(docID);
          }

          @Override
          public Relation compare(byte[] minPacked, byte[] maxPacked) {
            if (random().nextInt(7) == 1) {
              return Relation.CELL_CROSSES_QUERY;
            } else {
              return Relation.CELL_INSIDE_QUERY;
            }
          }
      });
      assertEquals(numDocs, count[0]);
      in.close();
    }
  }

  // Claims 16 bytes per dim, but only use the bottom N 1-3 bytes; this would happen e.g. if a user indexes what are actually just short
  // values as a LongPoint:
  public void testWastedLeadingBytes() throws Exception {
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_DIMENSIONS);
    int bytesPerDim = PointValues.MAX_NUM_BYTES;
    int bytesUsed = TestUtil.nextInt(random(), 1, 3);

    Directory dir = newFSDirectory(createTempDir());
    int numDocs = 100000;
    BKDWriter w = new BKDWriter(numDocs+1, dir, "tmp", numDims, bytesPerDim, 32, 1f, numDocs, true);
    byte[] tmp = new byte[bytesUsed];
    byte[] buffer = new byte[numDims * bytesPerDim];
    for(int i=0;i<numDocs;i++) {
      for(int dim=0;dim<numDims;dim++) {
        random().nextBytes(tmp);
        System.arraycopy(tmp, 0, buffer, dim*bytesPerDim+(bytesPerDim-bytesUsed), tmp.length);
      }
      w.add(buffer, i);
    }
    
    IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT);
    long fp = w.finish(out);
    out.close();

    IndexInput in = dir.openInput("bkd", IOContext.DEFAULT);
    in.seek(fp);
    BKDReader r = new BKDReader(in);
    int[] count = new int[1];
    r.intersect(new IntersectVisitor() {

        @Override
        public void visit(int docID) {
          count[0]++;
        }

        @Override
        public void visit(int docID, byte[] packedValue) {
          visit(docID);
        }

        @Override
        public Relation compare(byte[] minPacked, byte[] maxPacked) {
          if (random().nextInt(7) == 1) {
            return Relation.CELL_CROSSES_QUERY;
          } else {
            return Relation.CELL_INSIDE_QUERY;
          }
        }
      });
    assertEquals(numDocs, count[0]);
    in.close();
    dir.close();
  }

  public void testEstimatePointCount() throws IOException {
    Directory dir = newDirectory();
    final int numValues = atLeast(10000); // make sure to have multiple leaves
    final int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 500);
    final int numBytesPerDim = TestUtil.nextInt(random(), 1, 4);
    final byte[] pointValue = new byte[numBytesPerDim];
    final byte[] uniquePointValue = new byte[numBytesPerDim];
    random().nextBytes(uniquePointValue);

    BKDWriter w = new BKDWriter(numValues, dir, "_temp", 1, numBytesPerDim, maxPointsInLeafNode,
        BKDWriter.DEFAULT_MAX_MB_SORT_IN_HEAP, numValues, true);
    for (int i = 0; i < numValues; ++i) {
      if (i == numValues / 2) {
        w.add(uniquePointValue, i);
      } else {
        do {
          random().nextBytes(pointValue);
        } while (Arrays.equals(pointValue, uniquePointValue));
        w.add(pointValue, i);
      }
    }
    final long indexFP;
    try (IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT)) {
      indexFP = w.finish(out);
      w.close();
    }
    
    IndexInput pointsIn = dir.openInput("bkd", IOContext.DEFAULT);
    pointsIn.seek(indexFP);
    BKDReader points = new BKDReader(pointsIn);

    int actualMaxPointsInLeafNode = numValues;
    while (actualMaxPointsInLeafNode > maxPointsInLeafNode) {
      actualMaxPointsInLeafNode = (actualMaxPointsInLeafNode + 1) / 2;
    }

    // If all points match, then the point count is numLeaves * maxPointsInLeafNode
    final int numLeaves = Integer.highestOneBit((numValues - 1) / actualMaxPointsInLeafNode) << 1;
    assertEquals(numLeaves * actualMaxPointsInLeafNode,
        points.estimatePointCount(new IntersectVisitor() {
          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {}
          
          @Override
          public void visit(int docID) throws IOException {}
          
          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_INSIDE_QUERY;
          }
        }));

    // Return 0 if no points match
    assertEquals(0,
        points.estimatePointCount(new IntersectVisitor() {
          @Override
          public void visit(int docID, byte[] packedValue) throws IOException {}
          
          @Override
          public void visit(int docID) throws IOException {}
          
          @Override
          public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return Relation.CELL_OUTSIDE_QUERY;
          }
        }));

    // If only one point matches, then the point count is (actualMaxPointsInLeafNode + 1) / 2
    // in general, or maybe 2x that if the point is a split value
    final long pointCount = points.estimatePointCount(new IntersectVisitor() {
      @Override
      public void visit(int docID, byte[] packedValue) throws IOException {}

      @Override
      public void visit(int docID) throws IOException {}

      @Override
      public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
        if (StringHelper.compare(numBytesPerDim, uniquePointValue, 0, maxPackedValue, 0) > 0 ||
            StringHelper.compare(numBytesPerDim, uniquePointValue, 0, minPackedValue, 0) < 0) {
          return Relation.CELL_OUTSIDE_QUERY;
        }
        return Relation.CELL_CROSSES_QUERY;
      }
    });
    assertTrue(""+pointCount,
        pointCount == (actualMaxPointsInLeafNode + 1) / 2 || // common case
        pointCount == 2*((actualMaxPointsInLeafNode + 1) / 2)); // if the point is a split value

    pointsIn.close();
    dir.close();
  }
}
