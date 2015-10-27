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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.DimensionalField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DimensionalValues.IntersectVisitor;
import org.apache.lucene.index.DimensionalValues.Relation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.bkd.BKDUtil;
import org.apache.lucene.util.bkd.BKDWriter;

// TODO: randomize the bkd settings w/ Lucene60DimensionalFormat

// TODO: factor out a BaseTestDimensionFormat

public class TestDimensionalValues extends LuceneTestCase {
  public void testBasic() throws Exception {
    Directory dir = getDirectory(20);
    // TODO: randomize codec once others support dimensional format
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
    IOUtils.close(r, dir);
  }

  public void testMerge() throws Exception {
    Directory dir = getDirectory(20);
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
    IOUtils.close(r, dir);
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
        } catch (AssertionError ae) {
          if (ae.getMessage().contains("does not exist; files=")) {
            // OK: likely we threw the random IOExc when IW was asserting the commit files exist
            done = true;
          } else {
            throw ae;
          }
        } catch (IllegalArgumentException iae) {
          // This just means we got a too-small maxMB for the maxPointsInLeafNode; just retry w/ more heap
          assertTrue(iae.getMessage().contains("either increase maxMBSortInHeap or decrease maxPointsInLeafNode"));
          System.out.println("  more heap");
          maxMBHeap *= 1.25;
        } catch (IOException ioe) {
          if (ioe.getMessage().contains("a random IOException")) {
            // BKDWriter should fully clean up after itself:
            done = true;
          } else {
            throw ioe;
          }
        }
      }
    }
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

  // Tests on N-dimensional points where each dimension is a BigInteger
  public void testBigIntNDims() throws Exception {

    int numDocs = atLeast(1000);
    try (Directory dir = getDirectory(numDocs)) {
      int numBytesPerDim = TestUtil.nextInt(random(), 2, 30);
      int numDims = TestUtil.nextInt(random(), 1, 5);
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
      iwc.setCodec(new SimpleTextCodec());
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
          BKDUtil.bigIntToBytes(values[dim], bytes[dim], 0, numBytesPerDim);
          if (VERBOSE) {
            System.out.println("    " + dim + " -> " + values[dim]);
          }
        }
        docs[docID] = values;
        Document doc = new Document();
        doc.add(new DimensionalField("field", bytes));
        w.addDocument(doc);
      }

      DirectoryReader r = w.getReader();
      w.close();

      DimensionalValues dimValues = MultiDimensionalValues.get(r);

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
        dimValues.intersect("field", new IntersectVisitor() {
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
                  return Relation.QUERY_OUTSIDE_CELL;
                } else if (min.compareTo(queryMin[dim]) < 0 || max.compareTo(queryMax[dim]) > 0) {
                  crosses = true;
                }
              }

              if (crosses) {
                return Relation.QUERY_CROSSES_CELL;
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
      r.close();
      }
  }

  public void testRandomBinaryTiny() throws Exception {
    doTestRandomBinary(10);
  }

  public void testRandomBinaryMedium() throws Exception {
    doTestRandomBinary(10000);
  }

  // TODO: enable this, but not using simple text:
  /*
  @Nightly
  public void testRandomBinaryBig() throws Exception {
    doTestRandomBinary(200000);
  }
  */

  // Suddenly add dimensional values to an existing field:
  public void testUpgradeFieldToDimensional() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(newStringField("dim", "foo", Field.Store.NO));
    w.addDocument(doc);
    w.close();
    
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    w = new IndexWriter(dir, iwc);
    doc.add(new DimensionalField("dim", new byte[4]));
    w.close();
    dir.close();
  }

  // Illegal schema change tests:

  public void testIllegalDimChangeOneDoc() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    doc.add(new DimensionalField("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoDocs() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoSegments() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoWriters() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    w.close();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeViaAddIndexesDirectory() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4], new byte[4]));
    w.addDocument(doc);
    try {
      w.addIndexes(new Directory[] {dir});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change dimension count from 2 to 1 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(w, dir, dir2);
  }

  public void testIllegalDimChangeViaAddIndexesCodecReader() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4], new byte[4]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      w.addIndexes(new CodecReader[] {getOnlySegmentReader(r)});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change dimension count from 2 to 1 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalDimChangeViaAddIndexesSlowCodecReader() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4], new byte[4]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      TestUtil.addIndexesSlowly(w, r);
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change dimension count from 2 to 1 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalTooManyDimensions() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    byte[][] values = new byte[BKDWriter.MAX_DIMS+1][];
    for(int i=0;i<values.length;i++) {
      values[i] = new byte[4];
    }
    doc.add(new DimensionalField("dim", values));
    w.addDocument(doc);
    try {
      w.close();
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("numDims must be 1 .. 255 (got: 256)", iae.getMessage());
    }
    dir.close();
  }

  public void testIllegalNumBytesChangeOneDoc() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    doc.add(new DimensionalField("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change dimension numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoDocs() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change dimension numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoSegments() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change dimension numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoWriters() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    w.close();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change dimension numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeViaAddIndexesDirectory() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[6]));
    w.addDocument(doc);
    try {
      w.addIndexes(new Directory[] {dir});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change dimension numBytes from 6 to 4 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(w, dir, dir2);
  }

  public void testIllegalNumBytesChangeViaAddIndexesCodecReader() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[6]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      w.addIndexes(new CodecReader[] {getOnlySegmentReader(r)});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change dimension numBytes from 6 to 4 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalNumBytesChangeViaAddIndexesSlowCodecReader() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new DimensionalField("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    iwc.setCodec(new SimpleTextCodec());
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new DimensionalField("dim", new byte[6]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      TestUtil.addIndexesSlowly(w, r);
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change dimension numBytes from 6 to 4 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
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
        // TODO: sometimes test on a "small" volume too, so we test the high density cases, higher chance of boundary, etc. cases:
        random().nextBytes(values[dim]);
      }
      docValues[docID] = values;
    }

    verify(docValues, null, numDims, numBytesPerDim);
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

  private void verify(Directory dir, byte[][][] docValues, int[] ids, int numDims, int numBytesPerDim, int maxPointsInLeafNode, double maxMB) throws Exception {
    int numValues = docValues.length;
    if (VERBOSE) {
      System.out.println("TEST: numValues=" + numValues + " numDims=" + numDims + " numBytesPerDim=" + numBytesPerDim + " maxPointsInLeafNode=" + maxPointsInLeafNode + " maxMB=" + maxMB);
    }
    //System.out.println("DIR: " + ((FSDirectory) dir).getDirectory());

    //IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));  
    IndexWriterConfig iwc = newIndexWriterConfig();
    //iwc.setUseCompoundFile(false);
    //iwc.getMergePolicy().setNoCFSRatio(0.0);
    iwc.setCodec(new SimpleTextCodec());
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
    DirectoryReader r = null;

    try {

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
            w.addDocument(doc);
          }
          doc = new Document();
          doc.add(new NumericDocValuesField("id", id));
        }
        doc.add(new DimensionalField("field", docValues[ord]));
        lastID = id;

        if (random().nextInt(30) == 17) {
          // randomly index some documents without this field
          w.addDocument(new Document());
          if (VERBOSE) {
            System.out.println("add empty doc");
          }
        }

        if (random().nextInt(30) == 17) {
          // randomly index some documents with this field, but we will delete them:
          Document xdoc = new Document();
          xdoc.add(new DimensionalField("field", docValues[ord]));
          xdoc.add(new StringField("nukeme", "yes", Field.Store.NO));
          w.addDocument(xdoc);
          if (VERBOSE) {
            System.out.println("add doc doc-to-delete");
          }
        }

        if (VERBOSE) {
          System.out.println("  ord=" + ord + " id=" + id);
          for(int dim=0;dim<numDims;dim++) {
            System.out.println("    dim=" + dim + " value=" + new BytesRef(docValues[ord][dim]));
          }
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

      //System.out.println("TEST: r=" + r);

      DimensionalValues dimValues = MultiDimensionalValues.get(r);
      if (VERBOSE) {
        System.out.println("  dimValues=" + dimValues);
      }
      assertNotNull(dimValues);

      NumericDocValues idValues = MultiDocValues.getNumericValues(r, "id");
      Bits liveDocs = MultiFields.getLiveDocs(r);

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
              if (liveDocs == null || liveDocs.get(docID)) {
                hits.set((int) idValues.get(docID));
              }
              //System.out.println("visit docID=" + docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              if (liveDocs != null && liveDocs.get(docID) == false) {
                return;
              }
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
              hits.set((int) idValues.get(docID));
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
        for(int id=0;id<limit;id++) {
          assertEquals("docID=" + id, expected.get(id), hits.get(id));
        }
      }
    } finally {
      IOUtils.closeWhileHandlingException(r, w);
    }
  }

  private BigInteger randomBigInt(int numBytes) {
    BigInteger x = new BigInteger(numBytes*8-1, random());
    if (random().nextBoolean()) {
      x = x.negate();
    }
    return x;
  }

  private static Directory noVirusChecker(Directory dir) {
    if (dir instanceof MockDirectoryWrapper) {
      ((MockDirectoryWrapper) dir).setEnableVirusScanner(false);
    }
    return dir;
  }

  private Directory getDirectory(int numPoints) throws IOException {
    Directory dir;
    if (numPoints > 100000) {
      dir = newFSDirectory(createTempDir("TestBKDTree"));
    } else {
      dir = newDirectory();
    }
    noVirusChecker(dir);
    //dir = FSDirectory.open(createTempDir());
    return dir;
  }
}
