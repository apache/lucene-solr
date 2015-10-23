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
import java.util.BitSet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.DimensionalField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DimensionalValues.IntersectVisitor;
import org.apache.lucene.index.DimensionalValues.Relation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.bkd.BKDUtil;

// nocommit remove me
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;

// nocommit randomize the bkd settings w/ Lucene60DimensionalFormat

// nocommit test w/ deletes

// nocommit sometimes test on small volume too

@SuppressSysoutChecks(bugUrl = "Stuff gets printed.")
public class TestDimensionalValues extends LuceneTestCase {
  public void testBasic() throws Exception {
    try (Directory dir = getDirectory(20)) {
      // nocommit randomize codec once others support dimensional format
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(new SimpleTextCodec());
      iwc.setMergePolicy(newLogMergePolicy());
      IndexWriter w = new IndexWriter(dir, iwc);
      byte[] point = new byte[4];
      for(int i=0;i<20;i++) {
        Document doc = new Document();
        BKDUtil.intToBytes(i, point, 0);
        doc.add(new DimensionalField("dim", point));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.close();

      DirectoryReader r = DirectoryReader.open(dir);
      LeafReader sub = getOnlySegmentReader(r);
      DimensionalValues values = sub.getDimensionalValues();

      // Simple test: make sure intersect can visit every doc:
      BitSet seen = new BitSet();
      values.intersect("dim",
                       new IntersectVisitor() {
                         @Override
                         public Relation compare(byte[] minPacked, byte[] maxPacked) {
                           return Relation.QUERY_CROSSES_CELL;
                         }
                         public void visit(int docID) {
                           throw new IllegalStateException();
                         }
                         public void visit(int docID, byte[] packedValue) {
                           seen.set(docID);
                           assertEquals(docID, BKDUtil.bytesToInt(packedValue, 0));
                         }
                       });
      r.close();
    }
  }

  public void testMerge() throws Exception {
    try (Directory dir = getDirectory(20)) {
      IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(new SimpleTextCodec());
      iwc.setMergePolicy(newLogMergePolicy());
      IndexWriter w = new IndexWriter(dir, iwc);
      byte[] point = new byte[4];
      for(int i=0;i<20;i++) {
        Document doc = new Document();
        BKDUtil.intToBytes(i, point, 0);
        doc.add(new DimensionalField("dim", point));
        w.addDocument(doc);
        if (i == 10) {
          w.commit();
        }
      }
      w.forceMerge(1);
      w.close();

      DirectoryReader r = DirectoryReader.open(dir);
      LeafReader sub = getOnlySegmentReader(r);
      DimensionalValues values = sub.getDimensionalValues();

      // Simple test: make sure intersect can visit every doc:
      BitSet seen = new BitSet();
      values.intersect("dim",
                       new IntersectVisitor() {
                         @Override
                         public Relation compare(byte[] minPacked, byte[] maxPacked) {
                           return Relation.QUERY_CROSSES_CELL;
                         }
                         public void visit(int docID) {
                           throw new IllegalStateException();
                         }
                         public void visit(int docID, byte[] packedValue) {
                           seen.set(docID);
                           assertEquals(docID, BKDUtil.bytesToInt(packedValue, 0));
                         }
                       });
      r.close();
    }
  }

  /** docIDs can be null, for the single valued case, else it maps value to docID, but all values for one doc must be adjacent */
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

  public void testRandomBinaryTiny() throws Exception {
    doTestRandomBinary(10);
  }

  public void testRandomBinaryMedium() throws Exception {
    doTestRandomBinary(10000);
  }

  // nocommit enable, but not using simple text:
  /*
  @Nightly
  public void testRandomBinaryBig() throws Exception {
    doTestRandomBinary(200000);
  }
  */

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

  private void verify(Directory dir, byte[][][] docValues, int[] docIDs, int numDims, int numBytesPerDim, int maxPointsInLeafNode, double maxMB) throws Exception {
    int numValues = docValues.length;
    if (VERBOSE) {
      System.out.println("TEST: numValues=" + numValues + " numDims=" + numDims + " numBytesPerDim=" + numBytesPerDim + " maxPointsInLeafNode=" + maxPointsInLeafNode + " maxMB=" + maxMB);
    }
    System.out.println("DIR: " + ((FSDirectory) dir).getDirectory());

    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    // nocommit don't do this:
    iwc.setUseCompoundFile(false);
    // nocommit nor this
    iwc.setMergePolicy(newLogMergePolicy());
    // nocommit nor this
    iwc.getMergePolicy().setNoCFSRatio(0.0);
    IndexWriter w = new IndexWriter(dir, iwc);
    int bytesPerDim = docValues[0][0].length;

    Document doc = null;
    int lastDocID = -1;
    for(int ord=0;ord<numValues;ord++) {
      int docID;
      if (docIDs == null) {
        docID = ord;
      } else {
        docID = docIDs[ord];
      }
      if (docID != lastDocID) {
        if (doc != null) {
          w.addDocument(doc);
        }
        doc = new Document();
      }
      doc.add(new DimensionalField("field", docValues[ord]));

      if (VERBOSE) {
        System.out.println("  ord=" + ord + " docID=" + docID);
        for(int dim=0;dim<numDims;dim++) {
          System.out.println("    dim=" + dim + " value=" + new BytesRef(docValues[ord][dim]));
        }
      }
    }
    w.addDocument(doc);

    if (random().nextBoolean()) {
      if (VERBOSE) {
        System.out.println("\nTEST: now force merge");
      }
      w.forceMerge(1);
    }

    DirectoryReader r = DirectoryReader.open(w, true);
    w.close();

    DimensionalValues dimValues = MultiDimensionalValues.get(r);
    if (VERBOSE) {
      System.out.println("  dimValues=" + dimValues);
    }

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

      if (VERBOSE) {
        for(int dim=0;dim<numDims;dim++) {
          System.out.println("  dim=" + dim + "\n    queryMin=" + new BytesRef(queryMin[dim]) + "\n    queryMax=" + new BytesRef(queryMax[dim]));
        }
      }

      final BitSet hits = new BitSet();

      dimValues.intersect("field", new DimensionalValues.IntersectVisitor() {
          @Override
          public void visit(int docID) {
            hits.set(docID);
            //System.out.println("visit docID=" + docID);
          }

          @Override
          public void visit(int docID, byte[] packedValue) {
            //System.out.println("visit check docID=" + docID);
            for(int dim=0;dim<numDims;dim++) {
              //System.out.println("  dim=" + dim + " value=" + new BytesRef(packedValue, dim*bytesPerDim, bytesPerDim));
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
            //System.out.println("compare");
            for(int dim=0;dim<numDims;dim++) {
              if (BKDUtil.compare(numBytesPerDim, maxPacked, dim, queryMin[dim], 0) < 0 ||
                  BKDUtil.compare(numBytesPerDim, minPacked, dim, queryMax[dim], 0) > 0) {
                //System.out.println("  query_outside_cell");
                return Relation.QUERY_OUTSIDE_CELL;
              } else if (BKDUtil.compare(numBytesPerDim, minPacked, dim, queryMin[dim], 0) < 0 ||
                         BKDUtil.compare(numBytesPerDim, maxPacked, dim, queryMax[dim], 0) > 0) {
                crosses = true;
              }
            }

            if (crosses) {
              //System.out.println("  query_crosses_cell");
              return Relation.QUERY_CROSSES_CELL;
            } else {
              //System.out.println("  cell_inside_query");
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
    r.close();
  }

  private static Directory noVirusChecker(Directory dir) {
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper) dir).setEnableVirusScanner(false);
    }
    return dir;
  }

  private Directory getDirectory(int numPoints) throws IOException {
    Directory dir;
    /*
    if (numPoints > 100000) {
      dir = newFSDirectory(createTempDir("TestBKDTree"));
    } else {
      dir = newDirectory();
    }
    System.out.println("DIR: " + dir);
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper) dir).setEnableVirusScanner(false);
    }
    */
    // nocommit
    dir = FSDirectory.open(createTempDir());
    return dir;
  }
}
