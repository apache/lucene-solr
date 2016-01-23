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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PointFormat;
import org.apache.lucene.codecs.PointReader;
import org.apache.lucene.codecs.PointWriter;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene60.Lucene60PointReader;
import org.apache.lucene.codecs.lucene60.Lucene60PointWriter;
import org.apache.lucene.document.BinaryPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.TestUtil;

// TODO: factor out a BaseTestDimensionFormat

public class TestPointValues extends LuceneTestCase {
  public void testBasic() throws Exception {
    Directory dir = getDirectory(20);
    // TODO: randomize codec once others support points format
    IndexWriterConfig iwc = newIndexWriterConfig();
    iwc.setMergePolicy(newLogMergePolicy());
    IndexWriter w = new IndexWriter(dir, iwc);
    byte[] point = new byte[4];
    for(int i=0;i<20;i++) {
      Document doc = new Document();
      NumericUtils.intToBytes(i, point, 0);
      doc.add(new BinaryPoint("dim", point));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    w.close();

    DirectoryReader r = DirectoryReader.open(dir);
    LeafReader sub = getOnlySegmentReader(r);
    PointValues values = sub.getPointValues();

    // Simple test: make sure intersect can visit every doc:
    BitSet seen = new BitSet();
    values.intersect("dim",
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
                         assertEquals(docID, NumericUtils.bytesToInt(packedValue, 0));
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
      NumericUtils.intToBytes(i, point, 0);
      doc.add(new BinaryPoint("dim", point));
      w.addDocument(doc);
      if (i == 10) {
        w.commit();
      }
    }
    w.forceMerge(1);
    w.close();

    DirectoryReader r = DirectoryReader.open(dir);
    LeafReader sub = getOnlySegmentReader(r);
    PointValues values = sub.getPointValues();

    // Simple test: make sure intersect can visit every doc:
    BitSet seen = new BitSet();
    values.intersect("dim",
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
                         assertEquals(docID, NumericUtils.bytesToInt(packedValue, 0));
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
      NumericUtils.intToBytes(i, point, 0);
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
    PointValues values = MultiPointValues.get(r);
    Bits liveDocs = MultiFields.getLiveDocs(r);
    NumericDocValues idValues = MultiDocValues.getNumericValues(r, "id");

    if (values != null) {
      BitSet seen = new BitSet();
      values.intersect("dim",
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
                           assertEquals(idValues.get(docID), NumericUtils.bytesToInt(packedValue, 0));
                         }
                       });
      assertEquals(0, seen.cardinality());
    }
    IOUtils.close(r, dir);
  }

  /** Make sure we close open files, delete temp files, etc., on exception */
  public void testWithExceptions() throws Exception {
    int numDocs = atLeast(10000);
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_DIMENSIONS);

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
          if (dir instanceof MockDirectoryWrapper) {
            dir.setEnableVirusScanner(false);
          }
          verify(dir, docValues, null, numDims, numBytesPerDim, true);
        } catch (IllegalStateException ise) {
          if (ise.getMessage().contains("this writer hit an unrecoverable error")) {
            Throwable cause = ise.getCause();
            if (cause != null && cause.getMessage().contains("a random IOException")) {
              done = true;
            } else {
              throw ise;
            }
          } else {
            throw ise;
          }
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
        } catch (IOException ioe) {
          String message = ioe.getMessage();
          if (message.contains("a random IOException") || message.contains("background merge hit exception")) {
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
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_DIMENSIONS);

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
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_DIMENSIONS);

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
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_DIMENSIONS);

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
      int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
      int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_DIMENSIONS);
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
          NumericUtils.bigIntToBytes(values[dim], bytes[dim], 0, numBytesPerDim);
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

      PointValues dimValues = MultiPointValues.get(r);

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
                BigInteger x = NumericUtils.bytesToBigInt(packedValue, dim, numBytesPerDim);
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
                BigInteger min = NumericUtils.bytesToBigInt(minPacked, dim, numBytesPerDim);
                BigInteger max = NumericUtils.bytesToBigInt(maxPacked, dim, numBytesPerDim);
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
      r.close();
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
    assumeFalse("too slow with SimpleText", Codec.getDefault().getName().equals("SimpleText"));
    doTestRandomBinary(200000);
  }

  // Suddenly add points to an existing field:
  public void testUpgradeFieldToPoints() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = newIndexWriterConfig();
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(newStringField("dim", "foo", Field.Store.NO));
    w.addDocument(doc);
    w.close();
    
    iwc = newIndexWriterConfig();
    w = new IndexWriter(dir, iwc);
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.close();
    dir.close();
  }

  // Illegal schema change tests:

  public void testIllegalDimChangeOneDoc() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoDocs() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoSegments() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeTwoWriters() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point dimension count from 1 to 2 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalDimChangeViaAddIndexesDirectory() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    w.addDocument(doc);
    try {
      w.addIndexes(new Directory[] {dir});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point dimension count from 2 to 1 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(w, dir, dir2);
  }

  public void testIllegalDimChangeViaAddIndexesCodecReader() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      w.addIndexes(new CodecReader[] {getOnlySegmentReader(r)});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point dimension count from 2 to 1 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalDimChangeViaAddIndexesSlowCodecReader() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4], new byte[4]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      TestUtil.addIndexesSlowly(w, r);
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point dimension count from 2 to 1 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalNumBytesChangeOneDoc() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    doc.add(new BinaryPoint("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoDocs() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoSegments() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.commit();
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeTwoWriters() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    try {
      w.addDocument(doc);
    } catch (IllegalArgumentException iae) {
      // expected
      assertEquals("cannot change point numBytes from 4 to 6 for field=\"dim\"", iae.getMessage());
    }
    w.close();
    dir.close();
  }

  public void testIllegalNumBytesChangeViaAddIndexesDirectory() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    w.addDocument(doc);
    try {
      w.addIndexes(new Directory[] {dir});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point numBytes from 6 to 4 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(w, dir, dir2);
  }

  public void testIllegalNumBytesChangeViaAddIndexesCodecReader() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      w.addIndexes(new CodecReader[] {getOnlySegmentReader(r)});
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point numBytes from 6 to 4 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalNumBytesChangeViaAddIndexesSlowCodecReader() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[4]));
    w.addDocument(doc);
    w.close();

    Directory dir2 = getDirectory(1);
    iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    w = new IndexWriter(dir2, iwc);
    doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[6]));
    w.addDocument(doc);
    DirectoryReader r = DirectoryReader.open(dir);
    try {
      TestUtil.addIndexesSlowly(w, r);
    } catch (IllegalArgumentException iae) {
      assertEquals("cannot change point numBytes from 6 to 4 for field=\"dim\"", iae.getMessage());
    }
    IOUtils.close(r, w, dir, dir2);
  }

  public void testIllegalTooManyBytes() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    doc.add(new BinaryPoint("dim", new byte[PointValues.MAX_NUM_BYTES+1]));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    doc = new Document();
    doc.add(new IntPoint("dim", 17));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  public void testIllegalTooManyDimensions() throws Exception {
    Directory dir = getDirectory(1);
    IndexWriterConfig iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    IndexWriter w = new IndexWriter(dir, iwc);
    Document doc = new Document();
    byte[][] values = new byte[PointValues.MAX_DIMENSIONS+1][];
    for(int i=0;i<values.length;i++) {
      values[i] = new byte[4];
    }
    doc.add(new BinaryPoint("dim", values));
    try {
      w.addDocument(doc);
      fail("did not hit exception");
    } catch (IllegalArgumentException iae) {
      // expected
    }
    doc = new Document();
    doc.add(new IntPoint("dim", 17));
    w.addDocument(doc);
    w.close();
    dir.close();
  }

  private void doTestRandomBinary(int count) throws Exception {
    int numDocs = TestUtil.nextInt(random(), count, count*2);
    int numBytesPerDim = TestUtil.nextInt(random(), 2, PointValues.MAX_NUM_BYTES);
    int numDims = TestUtil.nextInt(random(), 1, PointValues.MAX_DIMENSIONS);

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

  private Codec getCodec() {
    if (Codec.getDefault().getName().equals("Lucene60")) {
      int maxPointsInLeafNode = TestUtil.nextInt(random(), 50, 500);
      double maxMBSortInHeap = 0.1 + (3*random().nextDouble());
      if (VERBOSE) {
        System.out.println("TEST: using Lucene60PointFormat with maxPointsInLeafNode=" + maxPointsInLeafNode + " and maxMBSortInHeap=" + maxMBSortInHeap);
      }

      return new FilterCodec("Lucene60", Codec.getDefault()) {
        @Override
        public PointFormat pointFormat() {
          return new PointFormat() {
            @Override
            public PointWriter fieldsWriter(SegmentWriteState writeState) throws IOException {
              return new Lucene60PointWriter(writeState, maxPointsInLeafNode, maxMBSortInHeap);
            }

            @Override
            public PointReader fieldsReader(SegmentReadState readState) throws IOException {
              return new Lucene60PointReader(readState);
            }
          };
        }
      };
    } else {
      return Codec.getDefault();
    }
  }

  /** docIDs can be null, for the single valued case, else it maps value to docID, but all values for one doc must be adjacent */
  private void verify(byte[][][] docValues, int[] docIDs, int numDims, int numBytesPerDim) throws Exception {
    try (Directory dir = getDirectory(docValues.length)) {
      while (true) {
        try {
          verify(dir, docValues, docIDs, numDims, numBytesPerDim, false);
          return;
        } catch (IllegalArgumentException iae) {
          // This just means we got a too-small maxMB for the maxPointsInLeafNode; just retry
          assertTrue(iae.getMessage().contains("either increase maxMBSortInHeap or decrease maxPointsInLeafNode"));
        }
      }
    }
  }

  private void verify(Directory dir, byte[][][] docValues, int[] ids, int numDims, int numBytesPerDim, boolean expectExceptions) throws Exception {
    int numValues = docValues.length;
    if (VERBOSE) {
      System.out.println("TEST: numValues=" + numValues + " numDims=" + numDims + " numBytesPerDim=" + numBytesPerDim);
    }

    // RandomIndexWriter is too slow:
    boolean useRealWriter = docValues.length > 10000;

    IndexWriterConfig iwc;
    if (useRealWriter) {
      iwc = new IndexWriterConfig(new MockAnalyzer(random()));
    } else {
      iwc = newIndexWriterConfig();
    }
    iwc.setCodec(getCodec());

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
          if (StringHelper.compare(numBytesPerDim, docValues[ord][dim], 0, expectedMinValues[dim], 0) < 0) {
            System.arraycopy(docValues[ord][dim], 0, expectedMinValues[dim], 0, numBytesPerDim);
          }
          if (StringHelper.compare(numBytesPerDim, docValues[ord][dim], 0, expectedMaxValues[dim], 0) > 0) {
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
      iwc.setCodec(getCodec());
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
        doc.add(new BinaryPoint("field", docValues[ord]));
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
          xdoc.add(new BinaryPoint("field", docValues[ord]));
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

      PointValues dimValues = MultiPointValues.get(r);
      if (VERBOSE) {
        System.out.println("  dimValues=" + dimValues);
      }
      assertNotNull(dimValues);

      NumericDocValues idValues = MultiDocValues.getNumericValues(r, "id");
      Bits liveDocs = MultiFields.getLiveDocs(r);

      // Verify min/max values are correct:
      byte[] minValues = dimValues.getMinPackedValue("field");
      byte[] maxValues = dimValues.getMaxPackedValue("field");
      byte[] scratch = new byte[numBytesPerDim];
      for(int dim=0;dim<numDims;dim++) {
        System.arraycopy(minValues, dim*numBytesPerDim, scratch, 0, scratch.length);
        //System.out.println("dim=" + dim + " expectedMin=" + new BytesRef(expectedMinValues[dim]) + " min=" + new BytesRef(scratch));
        assertTrue(Arrays.equals(expectedMinValues[dim], scratch));
        System.arraycopy(maxValues, dim*numBytesPerDim, scratch, 0, scratch.length);
        //System.out.println("dim=" + dim + " expectedMax=" + new BytesRef(expectedMaxValues[dim]) + " max=" + new BytesRef(scratch));
        assertTrue(Arrays.equals(expectedMaxValues[dim], scratch));
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
          if (NumericUtils.compare(numBytesPerDim, queryMin[dim], 0, queryMax[dim], 0) > 0) {
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

        dimValues.intersect("field", new PointValues.IntersectVisitor() {
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
              //System.out.println("visit check docID=" + docID + " id=" + idValues.get(docID));
              for(int dim=0;dim<numDims;dim++) {
                //System.out.println("  dim=" + dim + " value=" + new BytesRef(packedValue, dim*numBytesPerDim, numBytesPerDim));
                if (NumericUtils.compare(numBytesPerDim, packedValue, dim, queryMin[dim], 0) < 0 ||
                    NumericUtils.compare(numBytesPerDim, packedValue, dim, queryMax[dim], 0) > 0) {
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
                if (NumericUtils.compare(numBytesPerDim, maxPacked, dim, queryMin[dim], 0) < 0 ||
                    NumericUtils.compare(numBytesPerDim, minPacked, dim, queryMax[dim], 0) > 0) {
                  //System.out.println("  query_outside_cell");
                  return Relation.CELL_OUTSIDE_QUERY;
                } else if (NumericUtils.compare(numBytesPerDim, minPacked, dim, queryMin[dim], 0) < 0 ||
                           NumericUtils.compare(numBytesPerDim, maxPacked, dim, queryMax[dim], 0) > 0) {
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

        BitSet expected = new BitSet();
        for(int ord=0;ord<numValues;ord++) {
          boolean matches = true;
          for(int dim=0;dim<numDims;dim++) {
            byte[] x = docValues[ord][dim];
            if (NumericUtils.compare(numBytesPerDim, x, 0, queryMin[dim], 0) < 0 ||
                NumericUtils.compare(numBytesPerDim, x, 0, queryMax[dim], 0) > 0) {
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
            System.out.println("  docID=" + docID + " id=" + idValues.get(docID));
          }

          fail(failCount + " docs failed; " + successCount + " docs succeeded");
        }
      }
    } finally {
      IOUtils.closeWhileHandlingException(r, w, saveW, saveDir == null ? null : dir);
    }
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
