package org.apache.lucene.util.bkd;

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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.lucene.index.DimensionalValues.IntersectVisitor;
import org.apache.lucene.index.DimensionalValues.Relation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBKD extends LuceneTestCase {

  public void testBasicInts1D() throws Exception {
    try (Directory dir = getDirectory(100)) {
      BKDWriter w = new BKDWriter(dir, "tmp", 1, 4, 2, 1.0f);
      byte[] scratch = new byte[4];
      for(int docID=0;docID<100;docID++) {
        BKDUtil.intToBytes(docID, scratch, 0);
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
              int x = BKDUtil.bytesToInt(packedValue, 0);
              if (VERBOSE) {
                System.out.println("visit docID=" + docID + " x=" + x);
              }
              if (x >= queryMin && x <= queryMax) {
                hits.set(docID);
              }
            }

            @Override
            public Relation compare(byte[] minPacked, byte[] maxPacked) {
              int min = BKDUtil.bytesToInt(minPacked, 0);
              int max = BKDUtil.bytesToInt(maxPacked, 0);
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
      float maxMB = (float) 0.1 + (3*random().nextFloat());
      BKDWriter w = new BKDWriter(dir, "tmp", numDims, 4, maxPointsInLeafNode, maxMB);

      if (VERBOSE) {
        System.out.println("TEST: numDims=" + numDims + " numDocs=" + numDocs);
      }
      int[][] docs = new int[numDocs][];
      byte[] scratch = new byte[4*numDims];
      for(int docID=0;docID<numDocs;docID++) {
        int[] values = new int[numDims];
        if (VERBOSE) {
          System.out.println("  docID=" + docID);
        }
        for(int dim=0;dim<numDims;dim++) {
          values[dim] = random().nextInt();
          BKDUtil.intToBytes(values[dim], scratch, dim);
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
                int x = BKDUtil.bytesToInt(packedValue, dim);
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
                int min = BKDUtil.bytesToInt(minPacked, dim);
                int max = BKDUtil.bytesToInt(maxPacked, dim);
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
      float maxMB = (float) 0.1 + (3*random().nextFloat());
      BKDWriter w = new BKDWriter(dir, "tmp", numDims, numBytesPerDim, maxPointsInLeafNode, maxMB);
      BigInteger[][] docs = new BigInteger[numDocs][];

      byte[] scratch = new byte[numBytesPerDim*numDims];
      for(int docID=0;docID<numDocs;docID++) {
        BigInteger[] values = new BigInteger[numDims];
        if (VERBOSE) {
          System.out.println("  docID=" + docID);
        }
        for(int dim=0;dim<numDims;dim++) {
          values[dim] = randomBigInt(numBytesPerDim);
          BKDUtil.bigIntToBytes(values[dim], scratch, dim, numBytesPerDim);
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
                BigInteger x = BKDUtil.bytesToBigInt(packedValue, dim, numBytesPerDim);
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
                BigInteger min = BKDUtil.bytesToBigInt(minPacked, dim, numBytesPerDim);
                BigInteger max = BKDUtil.bytesToBigInt(maxPacked, dim, numBytesPerDim);
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
      try (MockDirectoryWrapper dir = newMockFSDirectory(createTempDir())) {
        try {
          dir.setRandomIOExceptionRate(0.05);
          dir.setRandomIOExceptionRateOnOpen(0.05);
          if (dir instanceof MockDirectoryWrapper) {
            dir.setEnableVirusScanner(false);
          }
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
      }
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
      new BKDWriter(dir, "bkd", 1, 16, 1000000, 0.001);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
      assertTrue(iae.getMessage().contains("either increase maxMBSortInHeap or decrease maxPointsInLeafNode"));
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

  public void testBKDUtilAdd() throws Exception {
    int iters = atLeast(10000);
    int numBytes = TestUtil.nextInt(random(), 1, 100);
    for(int iter=0;iter<iters;iter++) {
      BigInteger v1 = new BigInteger(8*numBytes-1, random());
      BigInteger v2 = new BigInteger(8*numBytes-1, random());

      byte[] v1Bytes = new byte[numBytes];
      byte[] v1RawBytes = v1.toByteArray();
      assert v1RawBytes.length <= numBytes;
      System.arraycopy(v1RawBytes, 0, v1Bytes, v1Bytes.length-v1RawBytes.length, v1RawBytes.length);

      byte[] v2Bytes = new byte[numBytes];
      byte[] v2RawBytes = v2.toByteArray();
      assert v1RawBytes.length <= numBytes;
      System.arraycopy(v2RawBytes, 0, v2Bytes, v2Bytes.length-v2RawBytes.length, v2RawBytes.length);

      byte[] result = new byte[numBytes];
      BKDUtil.add(numBytes, 0, v1Bytes, v2Bytes, result);

      BigInteger sum = v1.add(v2);
      assertTrue("sum=" + sum + " v1=" + v1 + " v2=" + v2 + " but result=" + new BigInteger(1, result), sum.equals(new BigInteger(1, result)));
    }
  }

  public void testIllegalBKDUtilAdd() throws Exception {
    byte[] bytes = new byte[4];
    Arrays.fill(bytes, (byte) 0xff);
    byte[] one = new byte[4];
    one[3] = 1;
    try {
      BKDUtil.add(4, 0, bytes, one, new byte[4]);
    } catch (IllegalArgumentException iae) {
      assertEquals("a + b overflows bytesPerDim=4", iae.getMessage());
    }
  }
  
  public void testBKDUtilSubtract() throws Exception {
    int iters = atLeast(10000);
    int numBytes = TestUtil.nextInt(random(), 1, 100);
    for(int iter=0;iter<iters;iter++) {
      BigInteger v1 = new BigInteger(8*numBytes-1, random());
      BigInteger v2 = new BigInteger(8*numBytes-1, random());
      if (v1.compareTo(v2) < 0) {
        BigInteger tmp = v1;
        v1 = v2;
        v2 = tmp;
      }

      byte[] v1Bytes = new byte[numBytes];
      byte[] v1RawBytes = v1.toByteArray();
      assert v1RawBytes.length <= numBytes: "length=" + v1RawBytes.length + " vs numBytes=" + numBytes;
      System.arraycopy(v1RawBytes, 0, v1Bytes, v1Bytes.length-v1RawBytes.length, v1RawBytes.length);

      byte[] v2Bytes = new byte[numBytes];
      byte[] v2RawBytes = v2.toByteArray();
      assert v2RawBytes.length <= numBytes;
      assert v2RawBytes.length <= numBytes: "length=" + v2RawBytes.length + " vs numBytes=" + numBytes;
      System.arraycopy(v2RawBytes, 0, v2Bytes, v2Bytes.length-v2RawBytes.length, v2RawBytes.length);

      byte[] result = new byte[numBytes];
      BKDUtil.subtract(numBytes, 0, v1Bytes, v2Bytes, result);

      BigInteger diff = v1.subtract(v2);

      assertTrue("diff=" + diff + " vs result=" + new BigInteger(result) + " v1=" + v1 + " v2=" + v2, diff.equals(new BigInteger(result)));
    }
  }

  public void testIllegalBKDUtilSubtract() throws Exception {
    byte[] v1 = new byte[4];
    v1[3] = (byte) 0xf0;
    byte[] v2 = new byte[4];
    v2[3] = (byte) 0xf1;
    try {
      BKDUtil.subtract(4, 0, v1, v2, new byte[4]);
    } catch (IllegalArgumentException iae) {
      assertEquals("a < b", iae.getMessage());
    }
  }
  
  /** docIDs can be null, for the single valued case, else it maps value to docID */
  private void verify(byte[][][] docValues, int[] docIDs, int numDims, int numBytesPerDim) throws Exception {
    try (Directory dir = getDirectory(docValues.length)) {
      while (true) {
        int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 100);
        double maxMB = (float) 0.1 + (3*random().nextDouble());
        try {
          verify(dir, docValues, docIDs, numDims, numBytesPerDim, maxPointsInLeafNode, maxMB);
          return;
        } catch (IllegalArgumentException iae) {
          // This just means we got a too-small maxMB for the maxPointsInLeafNode; just retry
          assertTrue(iae.getMessage().contains("either increase maxMBSortInHeap or decrease maxPointsInLeafNode"));
        }
      }
    }
  }

  private void verify(Directory dir, byte[][][] docValues, int[] docIDs, int numDims, int numBytesPerDim, int maxPointsInLeafNode, double maxMB) throws Exception {
    int numValues = docValues.length;
    if (VERBOSE) {
      System.out.println("TEST: numValues=" + numValues + " numDims=" + numDims + " numBytesPerDim=" + numBytesPerDim + " maxPointsInLeafNode=" + maxPointsInLeafNode + " maxMB=" + maxMB);
    }
    long indexFP;
    try (BKDWriter w = new BKDWriter(dir, "tmp", numDims, numBytesPerDim, maxPointsInLeafNode, maxMB)) {

      byte[] scratch = new byte[numBytesPerDim*numDims];
      for(int ord=0;ord<numValues;ord++) {
        int docID;
        if (docIDs == null) {
          docID = ord;
        } else {
          docID = docIDs[ord];
        }
        if (VERBOSE) {
          System.out.println("  ord=" + ord + " docID=" + docID);
        }
        for(int dim=0;dim<numDims;dim++) {
          if (VERBOSE) {
            System.out.println("    " + dim + " -> " + new BytesRef(docValues[ord][dim]));
          }
          System.arraycopy(docValues[ord][dim], 0, scratch, dim*numBytesPerDim, numBytesPerDim);
        }
        w.add(scratch, docID);
      }

      boolean success = false;
      try (IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT)) {
        indexFP = w.finish(out);
        success = true;
      } finally {
        if (success == false) {
          IOUtils.deleteFilesIgnoringExceptions(dir, "bkd");
        }
      }
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
        byte[][] queryMin = new byte[numDims][];
        byte[][] queryMax = new byte[numDims][];    
        for(int dim=0;dim<numDims;dim++) {    
          queryMin[dim] = new byte[numBytesPerDim];
          random().nextBytes(queryMin[dim]);
          queryMax[dim] = new byte[numBytesPerDim];
          random().nextBytes(queryMax[dim]);
          if (BKDUtil.compare(numBytesPerDim, queryMin[dim], 0, queryMax[dim], 0) > 0) {
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
                if (BKDUtil.compare(numBytesPerDim, packedValue, dim, queryMin[dim], 0) < 0 ||
                    BKDUtil.compare(numBytesPerDim, packedValue, dim, queryMax[dim], 0) > 0) {
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
                if (BKDUtil.compare(numBytesPerDim, maxPacked, dim, queryMin[dim], 0) < 0 ||
                    BKDUtil.compare(numBytesPerDim, minPacked, dim, queryMax[dim], 0) > 0) {
                  return Relation.CELL_OUTSIDE_QUERY;
                } else if (BKDUtil.compare(numBytesPerDim, minPacked, dim, queryMin[dim], 0) < 0 ||
                           BKDUtil.compare(numBytesPerDim, maxPacked, dim, queryMax[dim], 0) > 0) {
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
            if (BKDUtil.compare(numBytesPerDim, x, 0, queryMin[dim], 0) < 0 ||
                BKDUtil.compare(numBytesPerDim, x, 0, queryMax[dim], 0) > 0) {
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
    } finally {
      dir.deleteFile("bkd");
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
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper) dir).setEnableVirusScanner(false);
    }
    return dir;
  }
}
