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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatDocValuesField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/*
  Verify we can read the pre-5.0 file format, do searches
  against it, and add documents to it.
*/
// note: add this if we make a 4.x impersonator
// TODO: don't use 4.x codec, its unrealistic since it means
// we won't even be running the actual code, only the impostor
// @SuppressCodecs("Lucene4x")
// Sep codec cannot yet handle the offsets in our 4.x index!
@SuppressCodecs({"Lucene3x", "MockFixedIntBlock", "MockVariableIntBlock", "MockSep", "MockRandom", "Lucene40", "Lucene41", "Appending", "Lucene42", "Lucene45", "Lucene46", "Lucene49"})
public class TestBackwardsCompatibility extends LuceneTestCase {

  // Uncomment these cases & run them on an older Lucene version,
  // to generate indexes to test backwards compatibility.  These
  // indexes will be created under directory /tmp/idx/.
  //
  // However, you must first disable the Lucene TestSecurityManager,
  // which will otherwise disallow writing outside of the build/
  // directory - to do this, comment out the "java.security.manager"
  // <sysproperty> under the "test-macro" <macrodef>.
  //
  // Be sure to create the indexes with the actual format:
  //  ant test -Dtestcase=TestBackwardsCompatibility -Dversion=x.y.z
  //      -Dtests.codec=LuceneXY -Dtests.postingsformat=LuceneXY -Dtests.docvaluesformat=LuceneXY
  //
  // Zip up the generated indexes:
  //
  //    cd /tmp/idx/index.cfs   ; zip index.<VERSION>.cfs.zip *
  //    cd /tmp/idx/index.nocfs ; zip index.<VERSION>.nocfs.zip *
  //
  // Then move those 2 zip files to your trunk checkout and add them
  // to the oldNames array.

  /*
  public void testCreateCFS() throws IOException {
    createIndex("index.cfs", true, false);
  }

  public void testCreateNoCFS() throws IOException {
    createIndex("index.nocfs", false, false);
  }
  */

/*
  // These are only needed for the special upgrade test to verify
  // that also single-segment indexes are correctly upgraded by IndexUpgrader.
  // You don't need them to be build for non-4.0 (the test is happy with just one
  // "old" segment format, version is unimportant:
  
  public void testCreateSingleSegmentCFS() throws IOException {
    createIndex("index.singlesegment.cfs", true, true);
  }

  public void testCreateSingleSegmentNoCFS() throws IOException {
    createIndex("index.singlesegment.nocfs", false, true);
  }

*/  

  /*
  public void testCreateMoreTermsIndex() throws Exception {
    // we use a real directory name that is not cleaned up,
    // because this method is only used to create backwards
    // indexes:
    File indexDir = new File("moreterms");
    TestUtil.rmDir(indexDir);
    Directory dir = newFSDirectory(indexDir);

    LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
    mp.setUseCompoundFile(false);
    mp.setNoCFSRatio(1.0);
    mp.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    MockAnalyzer analyzer = new MockAnalyzer(random());
    analyzer.setMaxTokenLength(TestUtil.nextInt(random(), 1, IndexWriter.MAX_TERM_LENGTH));

    // TODO: remove randomness
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, analyzer)
      .setMergePolicy(mp);
    conf.setCodec(Codec.forName("Lucene40"));
    IndexWriter writer = new IndexWriter(dir, conf);
    LineFileDocs docs = new LineFileDocs(null, true);
    for(int i=0;i<50;i++) {
      writer.addDocument(docs.nextDoc());
    }
    writer.close();
    dir.close();

    // Gives you time to copy the index out!: (there is also
    // a test option to not remove temp dir...):
    Thread.sleep(100000);
  }
  */
  
  private void updateNumeric(IndexWriter writer, String id, String f, String cf, long value) throws IOException {
    writer.updateNumericDocValue(new Term("id", id), f, value);
    writer.updateNumericDocValue(new Term("id", id), cf, value*2);
  }
  
  private void updateBinary(IndexWriter writer, String id, String f, String cf, long value) throws IOException {
    writer.updateBinaryDocValue(new Term("id", id), f, TestBinaryDocValuesUpdates.toBytes(value));
    writer.updateBinaryDocValue(new Term("id", id), cf, TestBinaryDocValuesUpdates.toBytes(value*2));
  }

/*  // Creates an index with DocValues updates
  public void testCreateIndexWithDocValuesUpdates() throws Exception {
    // we use a real directory name that is not cleaned up,
    // because this method is only used to create backwards
    // indexes:
    File indexDir = new File("/tmp/idx/dvupdates");
    TestUtil.rm(indexDir);
    Directory dir = newFSDirectory(indexDir);
    
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
      .setUseCompoundFile(false).setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(dir, conf);
    // create an index w/ few doc-values fields, some with updates and some without
    for (int i = 0; i < 30; i++) {
      Document doc = new Document();
      doc.add(new StringField("id", "" + i, Store.NO));
      doc.add(new NumericDocValuesField("ndv1", i));
      doc.add(new NumericDocValuesField("ndv1_c", i*2));
      doc.add(new NumericDocValuesField("ndv2", i*3));
      doc.add(new NumericDocValuesField("ndv2_c", i*6));
      doc.add(new BinaryDocValuesField("bdv1", TestBinaryDocValuesUpdates.toBytes(i)));
      doc.add(new BinaryDocValuesField("bdv1_c", TestBinaryDocValuesUpdates.toBytes(i*2)));
      doc.add(new BinaryDocValuesField("bdv2", TestBinaryDocValuesUpdates.toBytes(i*3)));
      doc.add(new BinaryDocValuesField("bdv2_c", TestBinaryDocValuesUpdates.toBytes(i*6)));
      writer.addDocument(doc);
      if ((i+1) % 10 == 0) {
        writer.commit(); // flush every 10 docs
      }
    }
    
    // first segment: no updates
    
    // second segment: update two fields, same gen
    updateNumeric(writer, "10", "ndv1", "ndv1_c", 100L);
    updateBinary(writer, "11", "bdv1", "bdv1_c", 100L);
    writer.commit();
    
    // third segment: update few fields, different gens, few docs
    updateNumeric(writer, "20", "ndv1", "ndv1_c", 100L);
    updateBinary(writer, "21", "bdv1", "bdv1_c", 100L);
    writer.commit();
    updateNumeric(writer, "22", "ndv1", "ndv1_c", 200L); // update the field again
    writer.commit();
    
    writer.close();
    dir.close();
  }*/

  final static String[] oldNames = {"40.cfs",
                                    "40.nocfs",
                                    "41.cfs",
                                    "41.nocfs",
                                    "42.cfs",
                                    "42.nocfs",
                                    "45.cfs",
                                    "45.nocfs",
                                    "461.cfs",
                                    "461.nocfs",
                                    "49.cfs",
                                    "49.nocfs"
  };
  
  final String[] unsupportedNames = {"19.cfs",
                                     "19.nocfs",
                                     "20.cfs",
                                     "20.nocfs",
                                     "21.cfs",
                                     "21.nocfs",
                                     "22.cfs",
                                     "22.nocfs",
                                     "23.cfs",
                                     "23.nocfs",
                                     "24.cfs",
                                     "24.nocfs",
                                     "29.cfs",
                                     "29.nocfs",
  };
  
  final static String[] oldSingleSegmentNames = {"40.optimized.cfs",
                                                 "40.optimized.nocfs",
  };
  
  static Map<String,Directory> oldIndexDirs;

  /**
   * Randomizes the use of some of hte constructor variations
   */
  private static IndexUpgrader newIndexUpgrader(Directory dir) {
    final boolean streamType = random().nextBoolean();
    final int choice = TestUtil.nextInt(random(), 0, 2);
    switch (choice) {
      case 0: return new IndexUpgrader(dir, TEST_VERSION_CURRENT);
      case 1: return new IndexUpgrader(dir, TEST_VERSION_CURRENT, 
                                       streamType ? null : InfoStream.NO_OUTPUT, false);
      case 2: return new IndexUpgrader(dir, newIndexWriterConfig(null), false);
      default: fail("case statement didn't get updated when random bounds changed");
    }
    return null; // never get here
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    assertFalse("test infra is broken!", LuceneTestCase.OLD_FORMAT_IMPERSONATION_IS_ACTIVE);
    List<String> names = new ArrayList<>(oldNames.length + oldSingleSegmentNames.length);
    names.addAll(Arrays.asList(oldNames));
    names.addAll(Arrays.asList(oldSingleSegmentNames));
    oldIndexDirs = new HashMap<>();
    for (String name : names) {
      File dir = createTempDir(name);
      File dataFile = new File(TestBackwardsCompatibility.class.getResource("index." + name + ".zip").toURI());
      TestUtil.unzip(dataFile, dir);
      oldIndexDirs.put(name, newFSDirectory(dir));
    }
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    for (Directory d : oldIndexDirs.values()) {
      d.close();
    }
    oldIndexDirs = null;
  }
  
  /** This test checks that *only* IndexFormatTooOldExceptions are thrown when you open and operate on too old indexes! */
  public void testUnsupportedOldIndexes() throws Exception {
    for(int i=0;i<unsupportedNames.length;i++) {
      if (VERBOSE) {
        System.out.println("TEST: index " + unsupportedNames[i]);
      }
      File oldIndxeDir = createTempDir(unsupportedNames[i]);
      TestUtil.unzip(getDataFile("unsupported." + unsupportedNames[i] + ".zip"), oldIndxeDir);
      BaseDirectoryWrapper dir = newFSDirectory(oldIndxeDir);
      // don't checkindex, these are intentionally not supported
      dir.setCheckIndexOnClose(false);

      IndexReader reader = null;
      IndexWriter writer = null;
      try {
        reader = DirectoryReader.open(dir);
        fail("DirectoryReader.open should not pass for "+unsupportedNames[i]);
      } catch (IndexFormatTooOldException e) {
        // pass
      } finally {
        if (reader != null) reader.close();
        reader = null;
      }

      try {
        writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())));
        fail("IndexWriter creation should not pass for "+unsupportedNames[i]);
      } catch (IndexFormatTooOldException e) {
        // pass
        if (VERBOSE) {
          System.out.println("TEST: got expected exc:");
          e.printStackTrace(System.out);
        }
        // TODO: test *SOMEWHERE ELSE* that exc message includes a path=
      } finally {
        // we should fail to open IW, and so it should be null when we get here.
        // However, if the test fails (i.e., IW did not fail on open), we need
        // to close IW. However, if merges are run, IW may throw
        // IndexFormatTooOldException, and we don't want to mask the fail()
        // above, so close without waiting for merges.
        if (writer != null) {
          writer.close(false);
        }
        writer = null;
      }
      
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      CheckIndex checker = new CheckIndex(dir);
      checker.setInfoStream(new PrintStream(bos, false, IOUtils.UTF_8));
      CheckIndex.Status indexStatus = checker.checkIndex();
      assertFalse(indexStatus.clean);
      assertTrue(bos.toString(IOUtils.UTF_8).contains(IndexFormatTooOldException.class.getName()));

      dir.close();
      TestUtil.rm(oldIndxeDir);
    }
  }
  
  public void testFullyMergeOldIndex() throws Exception {
    for (String name : oldNames) {
      if (VERBOSE) {
        System.out.println("\nTEST: index=" + name);
      }
      Directory dir = newDirectory(oldIndexDirs.get(name));
      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      w.forceMerge(1);
      w.close();
      
      dir.close();
    }
  }

  public void testAddOldIndexes() throws IOException {
    for (String name : oldNames) {
      if (VERBOSE) {
        System.out.println("\nTEST: old index " + name);
      }
      Directory targetDir = newDirectory();
      IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(new MockAnalyzer(random())));
      w.addIndexes(oldIndexDirs.get(name));
      if (VERBOSE) {
        System.out.println("\nTEST: done adding indices; now close");
      }
      w.close();
      
      targetDir.close();
    }
  }

  public void testAddOldIndexesReader() throws IOException {
    for (String name : oldNames) {
      IndexReader reader = DirectoryReader.open(oldIndexDirs.get(name));
      
      Directory targetDir = newDirectory();
      IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(new MockAnalyzer(random())));
      w.addIndexes(reader);
      w.close();
      reader.close();
            
      targetDir.close();
    }
  }

  public void testSearchOldIndex() throws IOException {
    for (String name : oldNames) {
      searchIndex(oldIndexDirs.get(name), name);
    }
  }

  public void testIndexOldIndexNoAdds() throws IOException {
    for (String name : oldNames) {
      Directory dir = newDirectory(oldIndexDirs.get(name));
      changeIndexNoAdds(random(), dir);
      dir.close();
    }
  }

  public void testIndexOldIndex() throws IOException {
    for (String name : oldNames) {
      if (VERBOSE) {
        System.out.println("TEST: oldName=" + name);
      }
      Directory dir = newDirectory(oldIndexDirs.get(name));
      changeIndexWithAdds(random(), dir, name);
      dir.close();
    }
  }

  private void doTestHits(ScoreDoc[] hits, int expectedCount, IndexReader reader) throws IOException {
    final int hitCount = hits.length;
    assertEquals("wrong number of hits", expectedCount, hitCount);
    for(int i=0;i<hitCount;i++) {
      reader.document(hits[i].doc);
      reader.getTermVectors(hits[i].doc);
    }
  }

  public void searchIndex(Directory dir, String oldName) throws IOException {
    //QueryParser parser = new QueryParser("contents", new MockAnalyzer(random));
    //Query query = parser.parse("handle:1");

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);

    TestUtil.checkIndex(dir);
    
    // true if this is a 4.0+ index
    final boolean is40Index = MultiFields.getMergedFieldInfos(reader).fieldInfo("content5") != null;
    // true if this is a 4.2+ index
    final boolean is42Index = MultiFields.getMergedFieldInfos(reader).fieldInfo("dvSortedSet") != null;
    // true if this is a 4.9+ index
    final boolean is49Index = MultiFields.getMergedFieldInfos(reader).fieldInfo("dvSortedNumeric") != null;

    assert is40Index; // NOTE: currently we can only do this on trunk!

    final Bits liveDocs = MultiFields.getLiveDocs(reader);

    for(int i=0;i<35;i++) {
      if (liveDocs.get(i)) {
        Document d = reader.document(i);
        List<IndexableField> fields = d.getFields();
        boolean isProxDoc = d.getField("content3") == null;
        if (isProxDoc) {
          final int numFields = is40Index ? 7 : 5;
          assertEquals(numFields, fields.size());
          IndexableField f =  d.getField("id");
          assertEquals(""+i, f.stringValue());

          f = d.getField("utf8");
          assertEquals("Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", f.stringValue());

          f =  d.getField("autf8");
          assertEquals("Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", f.stringValue());
      
          f = d.getField("content2");
          assertEquals("here is more content with aaa aaa aaa", f.stringValue());

          f = d.getField("fie\u2C77ld");
          assertEquals("field with non-ascii name", f.stringValue());
        }

        Fields tfvFields = reader.getTermVectors(i);
        assertNotNull("i=" + i, tfvFields);
        Terms tfv = tfvFields.terms("utf8");
        assertNotNull("docID=" + i + " index=" + oldName, tfv);
      } else {
        // Only ID 7 is deleted
        assertEquals(7, i);
      }
    }

    if (is40Index) {
      // check docvalues fields
      NumericDocValues dvByte = MultiDocValues.getNumericValues(reader, "dvByte");
      BinaryDocValues dvBytesDerefFixed = MultiDocValues.getBinaryValues(reader, "dvBytesDerefFixed");
      BinaryDocValues dvBytesDerefVar = MultiDocValues.getBinaryValues(reader, "dvBytesDerefVar");
      SortedDocValues dvBytesSortedFixed = MultiDocValues.getSortedValues(reader, "dvBytesSortedFixed");
      SortedDocValues dvBytesSortedVar = MultiDocValues.getSortedValues(reader, "dvBytesSortedVar");
      BinaryDocValues dvBytesStraightFixed = MultiDocValues.getBinaryValues(reader, "dvBytesStraightFixed");
      BinaryDocValues dvBytesStraightVar = MultiDocValues.getBinaryValues(reader, "dvBytesStraightVar");
      NumericDocValues dvDouble = MultiDocValues.getNumericValues(reader, "dvDouble");
      NumericDocValues dvFloat = MultiDocValues.getNumericValues(reader, "dvFloat");
      NumericDocValues dvInt = MultiDocValues.getNumericValues(reader, "dvInt");
      NumericDocValues dvLong = MultiDocValues.getNumericValues(reader, "dvLong");
      NumericDocValues dvPacked = MultiDocValues.getNumericValues(reader, "dvPacked");
      NumericDocValues dvShort = MultiDocValues.getNumericValues(reader, "dvShort");
      SortedSetDocValues dvSortedSet = null;
      if (is42Index) {
        dvSortedSet = MultiDocValues.getSortedSetValues(reader, "dvSortedSet");
      }
      SortedNumericDocValues dvSortedNumeric = null;
      if (is49Index) {
        dvSortedNumeric = MultiDocValues.getSortedNumericValues(reader, "dvSortedNumeric");
      }
      
      for (int i=0;i<35;i++) {
        int id = Integer.parseInt(reader.document(i).get("id"));
        assertEquals(id, dvByte.get(i));
        
        byte bytes[] = new byte[] {
            (byte)(id >>> 24), (byte)(id >>> 16),(byte)(id >>> 8),(byte)id
        };
        BytesRef expectedRef = new BytesRef(bytes);
        
        BytesRef term = dvBytesDerefFixed.get(i);
        assertEquals(expectedRef, term);
        term = dvBytesDerefVar.get(i);
        assertEquals(expectedRef, term);
        term = dvBytesSortedFixed.get(i);
        assertEquals(expectedRef, term);
        term = dvBytesSortedVar.get(i);
        assertEquals(expectedRef, term);
        term = dvBytesStraightFixed.get(i);
        assertEquals(expectedRef, term);
        term = dvBytesStraightVar.get(i);
        assertEquals(expectedRef, term);
        
        assertEquals((double)id, Double.longBitsToDouble(dvDouble.get(i)), 0D);
        assertEquals((float)id, Float.intBitsToFloat((int)dvFloat.get(i)), 0F);
        assertEquals(id, dvInt.get(i));
        assertEquals(id, dvLong.get(i));
        assertEquals(id, dvPacked.get(i));
        assertEquals(id, dvShort.get(i));
        if (is42Index) {
          dvSortedSet.setDocument(i);
          long ord = dvSortedSet.nextOrd();
          assertEquals(SortedSetDocValues.NO_MORE_ORDS, dvSortedSet.nextOrd());
          term = dvSortedSet.lookupOrd(ord);
          assertEquals(expectedRef, term);
        }
        if (is49Index) {
          dvSortedNumeric.setDocument(i);
          assertEquals(1, dvSortedNumeric.count());
          assertEquals(id, dvSortedNumeric.valueAt(0));
        }
      }
    }
    
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term(new String("content"), "aaa")), null, 1000).scoreDocs;

    // First document should be #0
    Document d = searcher.getIndexReader().document(hits[0].doc);
    assertEquals("didn't get the right document first", "0", d.get("id"));

    doTestHits(hits, 34, searcher.getIndexReader());
    
    if (is40Index) {
      hits = searcher.search(new TermQuery(new Term(new String("content5"), "aaa")), null, 1000).scoreDocs;

      doTestHits(hits, 34, searcher.getIndexReader());
    
      hits = searcher.search(new TermQuery(new Term(new String("content6"), "aaa")), null, 1000).scoreDocs;

      doTestHits(hits, 34, searcher.getIndexReader());
    }

    hits = searcher.search(new TermQuery(new Term("utf8", "\u0000")), null, 1000).scoreDocs;
    assertEquals(34, hits.length);
    hits = searcher.search(new TermQuery(new Term(new String("utf8"), "lu\uD834\uDD1Ece\uD834\uDD60ne")), null, 1000).scoreDocs;
    assertEquals(34, hits.length);
    hits = searcher.search(new TermQuery(new Term("utf8", "ab\ud917\udc17cd")), null, 1000).scoreDocs;
    assertEquals(34, hits.length);

    reader.close();
  }

  private int compare(String name, String v) {
    int v0 = Integer.parseInt(name.substring(0, 2));
    int v1 = Integer.parseInt(v);
    return v0 - v1;
  }

  public void changeIndexWithAdds(Random random, Directory dir, String origOldName) throws IOException {
    // open writer
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random))
                                                 .setOpenMode(OpenMode.APPEND)
                                                 .setMergePolicy(newLogMergePolicy()));
    // add 10 docs
    for(int i=0;i<10;i++) {
      addDoc(writer, 35+i);
    }

    // make sure writer sees right total -- writer seems not to know about deletes in .del?
    final int expected;
    if (compare(origOldName, "24") < 0) {
      expected = 44;
    } else {
      expected = 45;
    }
    assertEquals("wrong doc count", expected, writer.numDocs());
    writer.close();

    // make sure searching sees right # hits
    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    Document d = searcher.getIndexReader().document(hits[0].doc);
    assertEquals("wrong first document", "0", d.get("id"));
    doTestHits(hits, 44, searcher.getIndexReader());
    reader.close();

    // fully merge
    writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random))
                                    .setOpenMode(OpenMode.APPEND)
                                    .setMergePolicy(newLogMergePolicy()));
    writer.forceMerge(1);
    writer.close();

    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 44, hits.length);
    d = searcher.doc(hits[0].doc);
    doTestHits(hits, 44, searcher.getIndexReader());
    assertEquals("wrong first document", "0", d.get("id"));
    reader.close();
  }

  public void changeIndexNoAdds(Random random, Directory dir) throws IOException {
    // make sure searching sees right # hits
    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = newSearcher(reader);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 34, hits.length);
    Document d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "0", d.get("id"));
    reader.close();

    // fully merge
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random))
                                                .setOpenMode(OpenMode.APPEND));
    writer.forceMerge(1);
    writer.close();

    reader = DirectoryReader.open(dir);
    searcher = newSearcher(reader);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 34, hits.length);
    doTestHits(hits, 34, searcher.getIndexReader());
    reader.close();
  }

  public File createIndex(String dirName, boolean doCFS, boolean fullyMerged) throws IOException {
    // we use a real directory name that is not cleaned up, because this method is only used to create backwards indexes:
    File indexDir = new File("/tmp/idx", dirName);
    TestUtil.rm(indexDir);
    Directory dir = newFSDirectory(indexDir);
    LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
    mp.setNoCFSRatio(doCFS ? 1.0 : 0.0);
    mp.setMaxCFSSegmentSizeMB(Double.POSITIVE_INFINITY);
    // TODO: remove randomness
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
      .setUseCompoundFile(doCFS).setMaxBufferedDocs(10).setMergePolicy(mp);
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for(int i=0;i<35;i++) {
      addDoc(writer, i);
    }
    assertEquals("wrong doc count", 35, writer.maxDoc());
    if (fullyMerged) {
      writer.forceMerge(1);
    }
    writer.close();

    if (!fullyMerged) {
      // open fresh writer so we get no prx file in the added segment
      mp = new LogByteSizeMergePolicy();
      mp.setNoCFSRatio(doCFS ? 1.0 : 0.0);
      // TODO: remove randomness
      conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setUseCompoundFile(doCFS)
        .setMaxBufferedDocs(10).setMergePolicy(mp);
      writer = new IndexWriter(dir, conf);
      addNoProxDoc(writer);
      writer.close();

      conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).setUseCompoundFile(doCFS)
        .setMaxBufferedDocs(10).setMergePolicy(NoMergePolicy.INSTANCE);
      writer = new IndexWriter(dir, conf);
      Term searchTerm = new Term("id", "7");
      writer.deleteDocuments(searchTerm);
      writer.close();
    }
    
    dir.close();
    
    return indexDir;
  }

  private void addDoc(IndexWriter writer, int id) throws IOException
  {
    Document doc = new Document();
    doc.add(new TextField("content", "aaa", Field.Store.NO));
    doc.add(new StringField("id", Integer.toString(id), Field.Store.YES));
    FieldType customType2 = new FieldType(TextField.TYPE_STORED);
    customType2.setStoreTermVectors(true);
    customType2.setStoreTermVectorPositions(true);
    customType2.setStoreTermVectorOffsets(true);
    doc.add(new Field("autf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", customType2));
    doc.add(new Field("utf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", customType2));
    doc.add(new Field("content2", "here is more content with aaa aaa aaa", customType2));
    doc.add(new Field("fie\u2C77ld", "field with non-ascii name", customType2));
    // add numeric fields, to test if flex preserves encoding
    doc.add(new IntField("trieInt", id, Field.Store.NO));
    doc.add(new LongField("trieLong", (long) id, Field.Store.NO));
    // add docvalues fields
    doc.add(new NumericDocValuesField("dvByte", (byte) id));
    byte bytes[] = new byte[] {
      (byte)(id >>> 24), (byte)(id >>> 16),(byte)(id >>> 8),(byte)id
    };
    BytesRef ref = new BytesRef(bytes);
    doc.add(new BinaryDocValuesField("dvBytesDerefFixed", ref));
    doc.add(new BinaryDocValuesField("dvBytesDerefVar", ref));
    doc.add(new SortedDocValuesField("dvBytesSortedFixed", ref));
    doc.add(new SortedDocValuesField("dvBytesSortedVar", ref));
    doc.add(new BinaryDocValuesField("dvBytesStraightFixed", ref));
    doc.add(new BinaryDocValuesField("dvBytesStraightVar", ref));
    doc.add(new DoubleDocValuesField("dvDouble", (double)id));
    doc.add(new FloatDocValuesField("dvFloat", (float)id));
    doc.add(new NumericDocValuesField("dvInt", id));
    doc.add(new NumericDocValuesField("dvLong", id));
    doc.add(new NumericDocValuesField("dvPacked", id));
    doc.add(new NumericDocValuesField("dvShort", (short)id));
    doc.add(new SortedSetDocValuesField("dvSortedSet", ref));
    doc.add(new SortedNumericDocValuesField("dvSortedNumeric", id));
    // a field with both offsets and term vectors for a cross-check
    FieldType customType3 = new FieldType(TextField.TYPE_STORED);
    customType3.setStoreTermVectors(true);
    customType3.setStoreTermVectorPositions(true);
    customType3.setStoreTermVectorOffsets(true);
    customType3.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
    doc.add(new Field("content5", "here is more content with aaa aaa aaa", customType3));
    // a field that omits only positions
    FieldType customType4 = new FieldType(TextField.TYPE_STORED);
    customType4.setStoreTermVectors(true);
    customType4.setStoreTermVectorPositions(false);
    customType4.setStoreTermVectorOffsets(true);
    customType4.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    doc.add(new Field("content6", "here is more content with aaa aaa aaa", customType4));
    // TODO: 
    //   index different norms types via similarity (we use a random one currently?!)
    //   remove any analyzer randomness, explicitly add payloads for certain fields.
    writer.addDocument(doc);
  }

  private void addNoProxDoc(IndexWriter writer) throws IOException {
    Document doc = new Document();
    FieldType customType = new FieldType(TextField.TYPE_STORED);
    customType.setIndexOptions(IndexOptions.DOCS_ONLY);
    Field f = new Field("content3", "aaa", customType);
    doc.add(f);
    FieldType customType2 = new FieldType();
    customType2.setStored(true);
    customType2.setIndexOptions(IndexOptions.DOCS_ONLY);
    f = new Field("content4", "aaa", customType2);
    doc.add(f);
    writer.addDocument(doc);
  }

  private int countDocs(DocsEnum docs) throws IOException {
    int count = 0;
    while((docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      count ++;
    }
    return count;
  }

  // flex: test basics of TermsEnum api on non-flex index
  public void testNextIntoWrongField() throws Exception {
    for (String name : oldNames) {
      Directory dir = oldIndexDirs.get(name);
      IndexReader r = DirectoryReader.open(dir);
      TermsEnum terms = MultiFields.getFields(r).terms("content").iterator(null);
      BytesRef t = terms.next();
      assertNotNull(t);

      // content field only has term aaa:
      assertEquals("aaa", t.utf8ToString());
      assertNull(terms.next());

      BytesRef aaaTerm = new BytesRef("aaa");

      // should be found exactly
      assertEquals(TermsEnum.SeekStatus.FOUND,
                   terms.seekCeil(aaaTerm));
      assertEquals(35, countDocs(TestUtil.docs(random(), terms, null, null, DocsEnum.FLAG_NONE)));
      assertNull(terms.next());

      // should hit end of field
      assertEquals(TermsEnum.SeekStatus.END,
                   terms.seekCeil(new BytesRef("bbb")));
      assertNull(terms.next());

      // should seek to aaa
      assertEquals(TermsEnum.SeekStatus.NOT_FOUND,
                   terms.seekCeil(new BytesRef("a")));
      assertTrue(terms.term().bytesEquals(aaaTerm));
      assertEquals(35, countDocs(TestUtil.docs(random(), terms, null, null, DocsEnum.FLAG_NONE)));
      assertNull(terms.next());

      assertEquals(TermsEnum.SeekStatus.FOUND,
                   terms.seekCeil(aaaTerm));
      assertEquals(35, countDocs(TestUtil.docs(random(), terms, null, null, DocsEnum.FLAG_NONE)));
      assertNull(terms.next());

      r.close();
    }
  }
  
  /** 
   * Test that we didn't forget to bump the current Constants.LUCENE_MAIN_VERSION.
   * This is important so that we can determine which version of lucene wrote the segment.
   */
  public void testOldVersions() throws Exception {
    // first create a little index with the current code and get the version
    Directory currentDir = newDirectory();
    RandomIndexWriter riw = new RandomIndexWriter(random(), currentDir);
    riw.addDocument(new Document());
    riw.close();
    DirectoryReader ir = DirectoryReader.open(currentDir);
    SegmentReader air = (SegmentReader)ir.leaves().get(0).reader();
    Version currentVersion = air.getSegmentInfo().info.getVersion();
    assertNotNull(currentVersion); // only 3.0 segments can have a null version
    ir.close();
    currentDir.close();
    
    // now check all the old indexes, their version should be < the current version
    for (String name : oldNames) {
      Directory dir = oldIndexDirs.get(name);
      DirectoryReader r = DirectoryReader.open(dir);
      for (AtomicReaderContext context : r.leaves()) {
        air = (SegmentReader) context.reader();
        Version oldVersion = air.getSegmentInfo().info.getVersion();
        assertNotNull(oldVersion); // only 3.0 segments can have a null version
        assertTrue("current Version.LATEST is <= an old index: did you forget to bump it?!",
                   currentVersion.onOrAfter(oldVersion));
      }
      r.close();
    }
  }
  
  public void testNumericFields() throws Exception {
    for (String name : oldNames) {
      
      Directory dir = oldIndexDirs.get(name);
      IndexReader reader = DirectoryReader.open(dir);
      IndexSearcher searcher = newSearcher(reader);
      
      for (int id=10; id<15; id++) {
        ScoreDoc[] hits = searcher.search(NumericRangeQuery.newIntRange("trieInt", NumericUtils.PRECISION_STEP_DEFAULT_32, Integer.valueOf(id), Integer.valueOf(id), true, true), 100).scoreDocs;
        assertEquals("wrong number of hits", 1, hits.length);
        Document d = searcher.doc(hits[0].doc);
        assertEquals(String.valueOf(id), d.get("id"));
        
        hits = searcher.search(NumericRangeQuery.newLongRange("trieLong", NumericUtils.PRECISION_STEP_DEFAULT, Long.valueOf(id), Long.valueOf(id), true, true), 100).scoreDocs;
        assertEquals("wrong number of hits", 1, hits.length);
        d = searcher.doc(hits[0].doc);
        assertEquals(String.valueOf(id), d.get("id"));
      }
      
      // check that also lower-precision fields are ok
      ScoreDoc[] hits = searcher.search(NumericRangeQuery.newIntRange("trieInt", NumericUtils.PRECISION_STEP_DEFAULT_32, Integer.MIN_VALUE, Integer.MAX_VALUE, false, false), 100).scoreDocs;
      assertEquals("wrong number of hits", 34, hits.length);
      
      hits = searcher.search(NumericRangeQuery.newLongRange("trieLong", NumericUtils.PRECISION_STEP_DEFAULT, Long.MIN_VALUE, Long.MAX_VALUE, false, false), 100).scoreDocs;
      assertEquals("wrong number of hits", 34, hits.length);
      
      // check decoding into field cache
      FieldCache.Ints fci = FieldCache.DEFAULT.getInts(SlowCompositeReaderWrapper.wrap(searcher.getIndexReader()), "trieInt", false);
      int maxDoc = searcher.getIndexReader().maxDoc();
      for(int doc=0;doc<maxDoc;doc++) {
        int val = fci.get(doc);
        assertTrue("value in id bounds", val >= 0 && val < 35);
      }
      
      FieldCache.Longs fcl = FieldCache.DEFAULT.getLongs(SlowCompositeReaderWrapper.wrap(searcher.getIndexReader()), "trieLong", false);
      for(int doc=0;doc<maxDoc;doc++) {
        long val = fcl.get(doc);
        assertTrue("value in id bounds", val >= 0L && val < 35L);
      }
      
      reader.close();
    }
  }
  
  private int checkAllSegmentsUpgraded(Directory dir) throws IOException {
    final SegmentInfos infos = new SegmentInfos();
    infos.read(dir);
    if (VERBOSE) {
      System.out.println("checkAllSegmentsUpgraded: " + infos);
    }
    for (SegmentCommitInfo si : infos) {
      assertEquals(Version.LATEST, si.info.getVersion());
    }
    return infos.size();
  }
  
  private int getNumberOfSegments(Directory dir) throws IOException {
    final SegmentInfos infos = new SegmentInfos();
    infos.read(dir);
    return infos.size();
  }

  public void testUpgradeOldIndex() throws Exception {
    List<String> names = new ArrayList<>(oldNames.length + oldSingleSegmentNames.length);
    names.addAll(Arrays.asList(oldNames));
    names.addAll(Arrays.asList(oldSingleSegmentNames));
    for(String name : names) {
      if (VERBOSE) {
        System.out.println("testUpgradeOldIndex: index=" +name);
      }
      Directory dir = newDirectory(oldIndexDirs.get(name));

      newIndexUpgrader(dir).upgrade();

      checkAllSegmentsUpgraded(dir);
      
      dir.close();
    }
  }

  public void testCommandLineArgs() throws Exception {

    PrintStream savedSystemOut = System.out;
    System.setOut(new PrintStream(new ByteArrayOutputStream(), false, "UTF-8"));
    try {
      for (String name : oldIndexDirs.keySet()) {
        File dir = createTempDir(name);
        File dataFile = new File(TestBackwardsCompatibility.class.getResource("index." + name + ".zip").toURI());
        TestUtil.unzip(dataFile, dir);
        
        String path = dir.getAbsolutePath();
        
        List<String> args = new ArrayList<>();
        if (random().nextBoolean()) {
          args.add("-verbose");
        }
        if (random().nextBoolean()) {
          args.add("-delete-prior-commits");
        }
        if (random().nextBoolean()) {
          // TODO: need to better randomize this, but ...
          //  - LuceneTestCase.FS_DIRECTORIES is private
          //  - newFSDirectory returns BaseDirectoryWrapper
          //  - BaseDirectoryWrapper doesn't expose delegate
          Class<? extends FSDirectory> dirImpl = random().nextBoolean() ?
              SimpleFSDirectory.class : NIOFSDirectory.class;
          
          args.add("-dir-impl");
          args.add(dirImpl.getName());
        }
        args.add(path);
        
        IndexUpgrader upgrader = null;
        try {
          upgrader = IndexUpgrader.parseArgs(args.toArray(new String[0]));
        } catch (Exception e) {
          throw new RuntimeException("unable to parse args: " + args, e);
        }
        upgrader.upgrade();
        
        Directory upgradedDir = newFSDirectory(dir);
        try {
          checkAllSegmentsUpgraded(upgradedDir);
        } finally {
          upgradedDir.close();
        }
      }
    } finally {
      System.setOut(savedSystemOut);
    }
  }

  public void testUpgradeOldSingleSegmentIndexWithAdditions() throws Exception {
    for (String name : oldSingleSegmentNames) {
      if (VERBOSE) {
        System.out.println("testUpgradeOldSingleSegmentIndexWithAdditions: index=" +name);
      }
      Directory dir = newDirectory(oldIndexDirs.get(name));

      assertEquals("Original index must be single segment", 1, getNumberOfSegments(dir));

      // create a bunch of dummy segments
      int id = 40;
      RAMDirectory ramDir = new RAMDirectory();
      for (int i = 0; i < 3; i++) {
        // only use Log- or TieredMergePolicy, to make document addition predictable and not suddenly merge:
        MergePolicy mp = random().nextBoolean() ? newLogMergePolicy() : newTieredMergePolicy();
        IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
          .setMergePolicy(mp);
        IndexWriter w = new IndexWriter(ramDir, iwc);
        // add few more docs:
        for(int j = 0; j < RANDOM_MULTIPLIER * random().nextInt(30); j++) {
          addDoc(w, id++);
        }
        w.close(false);
      }
      
      // add dummy segments (which are all in current
      // version) to single segment index
      MergePolicy mp = random().nextBoolean() ? newLogMergePolicy() : newTieredMergePolicy();
      IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, null)
        .setMergePolicy(mp);
      IndexWriter w = new IndexWriter(dir, iwc);
      w.addIndexes(ramDir);
      w.close(false);
      
      // determine count of segments in modified index
      final int origSegCount = getNumberOfSegments(dir);
      
      newIndexUpgrader(dir).upgrade();

      final int segCount = checkAllSegmentsUpgraded(dir);
      assertEquals("Index must still contain the same number of segments, as only one segment was upgraded and nothing else merged",
        origSegCount, segCount);
      
      dir.close();
    }
  }

  public static final String moreTermsIndex = "moreterms.40.zip";

  public void testMoreTerms() throws Exception {
    File oldIndexDir = createTempDir("moreterms");
    TestUtil.unzip(getDataFile(moreTermsIndex), oldIndexDir);
    Directory dir = newFSDirectory(oldIndexDir);
    // TODO: more tests
    TestUtil.checkIndex(dir);
    dir.close();
  }

  public static final String dvUpdatesIndex = "dvupdates.48.zip";

  private void assertNumericDocValues(AtomicReader r, String f, String cf) throws IOException {
    NumericDocValues ndvf = r.getNumericDocValues(f);
    NumericDocValues ndvcf = r.getNumericDocValues(cf);
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(ndvcf.get(i), ndvf.get(i)*2);
    }
  }
  
  private void assertBinaryDocValues(AtomicReader r, String f, String cf) throws IOException {
    BinaryDocValues bdvf = r.getBinaryDocValues(f);
    BinaryDocValues bdvcf = r.getBinaryDocValues(cf);
    for (int i = 0; i < r.maxDoc(); i++) {
      assertEquals(TestBinaryDocValuesUpdates.getValue(bdvcf, i), TestBinaryDocValuesUpdates.getValue(bdvf, i)*2);
    }
  }
  
  private void verifyDocValues(Directory dir) throws IOException {
    DirectoryReader reader = DirectoryReader.open(dir);
    for (AtomicReaderContext context : reader.leaves()) {
      AtomicReader r = context.reader();
      assertNumericDocValues(r, "ndv1", "ndv1_c");
      assertNumericDocValues(r, "ndv2", "ndv2_c");
      assertBinaryDocValues(r, "bdv1", "bdv1_c");
      assertBinaryDocValues(r, "bdv2", "bdv2_c");
    }
    reader.close();
  }
  
  public void testDocValuesUpdates() throws Exception {
    File oldIndexDir = createTempDir("dvupdates");
    TestUtil.unzip(getDataFile(dvUpdatesIndex), oldIndexDir);
    Directory dir = newFSDirectory(oldIndexDir);
    
    verifyDocValues(dir);
    
    // update fields and verify index
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()));
    IndexWriter writer = new IndexWriter(dir, conf);
    updateNumeric(writer, "1", "ndv1", "ndv1_c", 300L);
    updateNumeric(writer, "1", "ndv2", "ndv2_c", 300L);
    updateBinary(writer, "1", "bdv1", "bdv1_c", 300L);
    updateBinary(writer, "1", "bdv2", "bdv2_c", 300L);
    writer.commit();
    verifyDocValues(dir);
    
    // merge all segments
    writer.forceMerge(1);
    writer.commit();
    verifyDocValues(dir);
    
    writer.close();
    dir.close();
  }
  
}
