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

import java.util.Arrays;
import java.util.BitSet;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

@SuppressSysoutChecks(bugUrl = "Stuff gets printed.")
public class TestBKD extends LuceneTestCase {

  // nocommit accum to long then cast to int?
  static void intToBytes(int x, byte[] dest, int index) {
    // Flip the sign bit, so negative ints sort before positive ints correctly:
    x ^= 0x80000000;
    for(int i=0;i<4;i++) {
      dest[4*index+i] = (byte) (x >> 24-i*8);
    }
  }

  // nocommit accum to long then cast to int?
  static int bytesToInt(byte[] src, int index) {
    int x = 0;
    for(int i=0;i<4;i++) {
      x |= (src[4*index+i] & 0xff) << (24-i*8);
    }
    // Re-flip the sign bit to restore the original value:
    return x ^ 0x80000000;
  }

  /*
  private static abstract class VerifyHits {
    public abstract boolean accept(int docID, byte[] packedValues);
    public abstract BKDReader.relation compare(byte[] minPacked, byte[] maxPacked);
  }

  private void verify(List<byte[]> values, VerifyHits verify) throws Exception {
    int bytesPerDim = values.get(0).length;
    try (Directory dir = newDirectory()) {
      BKDWriter w = new BKDWriter(1, 4, 2, 1.0f);
      byte[] scratch = new byte[4];
      for(int docID=0;docID<values.size();docID++) {
        w.add(values.get(docID), docID);
      }

      long indexFP;
      try (IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT)) {
          indexFP = w.finish(out);
        }

      try (IndexInput in = dir.openInput("bkd", IOContext.DEFAULT)) {
        in.seek(indexFP);
        BKDReader r = new BKDReader(in, 100);

        // Simple 1D range query:
        final int queryMin = 42;
        final int queryMax = 87;

        final BitSet hits = new BitSet();
        r.intersect(new BKDReader.IntersectVisitor() {
            @Override
            public void visit(int docID) {
              hits.set(docID);
              System.out.println("visit docID=" + docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              int x = bytesToInt(packedValue, 0);
              System.out.println("visit docID=" + docID + " x=" + x);
              if (x >= queryMin && x <= queryMax) {
                hits.set(docID);
              }
            }

            @Override
            public BKDReader.Relation compare(byte[] minPacked, byte[] maxPacked) {
              int min = bytesToInt(minPacked, 0);
              int max = bytesToInt(maxPacked, 0);
              assert max >= min;
              System.out.println("compare: min=" + min + " max=" + max + " vs queryMin=" + queryMin + " queryMax=" + queryMax);

              if (max < queryMin || min > queryMax) {
                return BKDReader.Relation.QUERY_OUTSIDE_CELL;
              } else if (min >= queryMin && max <= queryMax) {
                return BKDReader.Relation.CELL_INSIDE_QUERY;
              } else {
                return BKDReader.Relation.QUERY_CROSSES_CELL;
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
  */

  public void testBasicInts1D() throws Exception {
    try (Directory dir = newDirectory()) {
      BKDWriter w = new BKDWriter(dir, "tmp", 1, 4, 2, 1.0f);
      byte[] scratch = new byte[4];
      for(int docID=0;docID<100;docID++) {
        intToBytes(docID, scratch, 0);
        w.add(scratch, docID);
      }

      long indexFP;
      try (IndexOutput out = dir.createOutput("bkd", IOContext.DEFAULT)) {
          indexFP = w.finish(out);
        }

      try (IndexInput in = dir.openInput("bkd", IOContext.DEFAULT)) {
        in.seek(indexFP);
        BKDReader r = new BKDReader(in, 100);

        // Simple 1D range query:
        final int queryMin = 42;
        final int queryMax = 87;

        final BitSet hits = new BitSet();
        r.intersect(new BKDReader.IntersectVisitor() {
            @Override
            public void visit(int docID) {
              hits.set(docID);
              System.out.println("visit docID=" + docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              int x = bytesToInt(packedValue, 0);
              System.out.println("visit docID=" + docID + " x=" + x);
              if (x >= queryMin && x <= queryMax) {
                hits.set(docID);
              }
            }

            @Override
            public BKDReader.Relation compare(byte[] minPacked, byte[] maxPacked) {
              int min = bytesToInt(minPacked, 0);
              int max = bytesToInt(maxPacked, 0);
              assert max >= min;
              System.out.println("compare: min=" + min + " max=" + max + " vs queryMin=" + queryMin + " queryMax=" + queryMax);

              if (max < queryMin || min > queryMax) {
                return BKDReader.Relation.QUERY_OUTSIDE_CELL;
              } else if (min >= queryMin && max <= queryMax) {
                return BKDReader.Relation.CELL_INSIDE_QUERY;
              } else {
                return BKDReader.Relation.QUERY_CROSSES_CELL;
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
    try (Directory dir = newDirectory()) {
      int numDims = TestUtil.nextInt(random(), 1, 5);
      int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 100);
      float maxMB = (float) 0.1 + (3*random().nextFloat());
      BKDWriter w = new BKDWriter(dir, "tmp", numDims, 4, maxPointsInLeafNode, maxMB);

      int numDocs = atLeast(1000);
      if (VERBOSE) {
        System.out.println("TEST: numDims=" + numDims + " numDocs=" + numDocs);
      }
      int[][] docs = new int[numDocs][];
      byte[] scratch = new byte[4*numDims];
      for(int docID=0;docID<numDocs;docID++) {
        int[] values = new int[numDims];
        System.out.println("  docID=" + docID);
        for(int dim=0;dim<numDims;dim++) {
          values[dim] = random().nextInt();
          intToBytes(values[dim], scratch, dim);
          System.out.println("    " + dim + " -> " + values[dim]);
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
        BKDReader r = new BKDReader(in, 100);

        int iters = atLeast(100);
        for(int iter=0;iter<iters;iter++) {
          if (VERBOSE) {
            System.out.println("\nTEST: iter=" + iter);
          }

          // Random N dime rect query:
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
          r.intersect(new BKDReader.IntersectVisitor() {
            @Override
            public void visit(int docID) {
              hits.set(docID);
              //System.out.println("visit docID=" + docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              //System.out.println("visit check docID=" + docID);
              for(int dim=0;dim<numDims;dim++) {
                int x = bytesToInt(packedValue, dim);
                if (x < queryMin[dim] || x > queryMax[dim]) {
                  //System.out.println("  no");
                  return;
                }
              }

              //System.out.println("  yes");
              hits.set(docID);
            }

            @Override
            public BKDReader.Relation compare(byte[] minPacked, byte[] maxPacked) {
              boolean crosses = false;
              for(int dim=0;dim<numDims;dim++) {
                int min = bytesToInt(minPacked, dim);
                int max = bytesToInt(maxPacked, dim);
                assert max >= min;

                if (max < queryMin[dim] || min > queryMax[dim]) {
                  return BKDReader.Relation.QUERY_OUTSIDE_CELL;
                } else if (min < queryMin[dim] || max > queryMax[dim]) {
                  crosses = true;
                }
              }

              if (crosses) {
                return BKDReader.Relation.QUERY_CROSSES_CELL;
              } else {
                return BKDReader.Relation.CELL_INSIDE_QUERY;
              }
            }
          });

          for(int docID=0;docID<100;docID++) {
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

  // nocommit multivalued test
}
