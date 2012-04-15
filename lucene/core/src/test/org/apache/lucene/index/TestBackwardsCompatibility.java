package org.apache.lucene.index;

/**
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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.LongField;
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/*
  Verify we can read the pre-4.0 file format, do searches
  against it, and add documents to it.
*/

public class TestBackwardsCompatibility extends LuceneTestCase {

  // Uncomment these cases & run them on an older Lucene
  // version, to generate an index to test backwards
  // compatibility.  Then, cd to build/test/index.cfs and
  // run "zip index.<VERSION>.cfs.zip *"; cd to
  // build/test/index.nocfs and run "zip
  // index.<VERSION>.nocfs.zip *".  Then move those 2 zip
  // files to your trunk checkout and add them to the
  // oldNames array.

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
  // You don't need them to be build for non-3.1 (the test is happy with just one
  // "old" segment format, version is unimportant:
  
  public void testCreateSingleSegmentCFS() throws IOException {
    createIndex("index.singlesegment.cfs", true, true);
  }

  public void testCreateSingleSegmentNoCFS() throws IOException {
    createIndex("index.singlesegment.nocfs", false, true);
  }

*/  
  final static String[] oldNames = {"30.cfs",
                             "30.nocfs",
                             "31.cfs",
                             "31.nocfs",
                             "32.cfs",
                             "32.nocfs",
                             "34.cfs",
                             "34.nocfs",
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
  
  final static String[] oldSingleSegmentNames = {"31.optimized.cfs",
                                          "31.optimized.nocfs",
  };
  
  static Map<String,Directory> oldIndexDirs;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    List<String> names = new ArrayList<String>(oldNames.length + oldSingleSegmentNames.length);
    names.addAll(Arrays.asList(oldNames));
    names.addAll(Arrays.asList(oldSingleSegmentNames));
    oldIndexDirs = new HashMap<String,Directory>();
    for (String name : names) {
      File dir = _TestUtil.getTempDir(name);
      File dataFile = new File(TestBackwardsCompatibility.class.getResource("index." + name + ".zip").toURI());
      _TestUtil.unzip(dataFile, dir);
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
      File oldIndxeDir = _TestUtil.getTempDir(unsupportedNames[i]);
      _TestUtil.unzip(getDataFile("unsupported." + unsupportedNames[i] + ".zip"), oldIndxeDir);
      MockDirectoryWrapper dir = newFSDirectory(oldIndxeDir);
      // don't checkindex, these are intentionally not supported
      dir.setCheckIndexOnClose(false);

      IndexReader reader = null;
      IndexWriter writer = null;
      try {
        reader = IndexReader.open(dir);
        fail("IndexReader.open should not pass for "+unsupportedNames[i]);
      } catch (IndexFormatTooOldException e) {
        // pass
      } finally {
        if (reader != null) reader.close();
        reader = null;
      }

      try {
        writer = new IndexWriter(dir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random())));
        fail("IndexWriter creation should not pass for "+unsupportedNames[i]);
      } catch (IndexFormatTooOldException e) {
        // pass
        if (VERBOSE) {
          System.out.println("TEST: got expected exc:");
          e.printStackTrace(System.out);
        }
        // Make sure exc message includes a path=
        assertTrue("got exc message: " + e.getMessage(), e.getMessage().indexOf("path=\"") != -1);
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
      checker.setInfoStream(new PrintStream(bos));
      CheckIndex.Status indexStatus = checker.checkIndex();
      assertFalse(indexStatus.clean);
      assertTrue(bos.toString().contains(IndexFormatTooOldException.class.getName()));

      dir.close();
      _TestUtil.rmDir(oldIndxeDir);
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
      Directory targetDir = newDirectory();
      IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random())));
      w.addIndexes(oldIndexDirs.get(name));
      w.close();
      
      targetDir.close();
    }
  }

  public void testAddOldIndexesReader() throws IOException {
    for (String name : oldNames) {
      IndexReader reader = IndexReader.open(oldIndexDirs.get(name));
      
      Directory targetDir = newDirectory();
      IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random())));
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

    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    _TestUtil.checkIndex(dir);

    final Bits liveDocs = MultiFields.getLiveDocs(reader);

    for(int i=0;i<35;i++) {
      if (liveDocs.get(i)) {
        Document d = reader.document(i);
        List<IndexableField> fields = d.getFields();
        if (d.getField("content3") == null) {
          final int numFields = 5;
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

        Terms tfv = reader.getTermVectors(i).terms("utf8");
        assertNotNull("docID=" + i + " index=" + oldName, tfv);
      } else
        // Only ID 7 is deleted
        assertEquals(7, i);
    }
    
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;

    // First document should be #21 since it's norm was
    // increased:
    Document d = searcher.getIndexReader().document(hits[0].doc);
    assertEquals("didn't get the right document first", "21", d.get("id"));

    doTestHits(hits, 34, searcher.getIndexReader());

    hits = searcher.search(new TermQuery(new Term("utf8", "\u0000")), null, 1000).scoreDocs;
    assertEquals(34, hits.length);
    hits = searcher.search(new TermQuery(new Term("utf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne")), null, 1000).scoreDocs;
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
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
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
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    Document d = searcher.getIndexReader().document(hits[0].doc);
    assertEquals("wrong first document", "21", d.get("id"));
    doTestHits(hits, 44, searcher.getIndexReader());
    reader.close();

    // fully merge
    writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
    writer.forceMerge(1);
    writer.close();

    reader = IndexReader.open(dir);
    searcher = new IndexSearcher(reader);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 44, hits.length);
    d = searcher.doc(hits[0].doc);
    doTestHits(hits, 44, searcher.getIndexReader());
    assertEquals("wrong first document", "21", d.get("id"));
    reader.close();
  }

  public void changeIndexNoAdds(Random random, Directory dir) throws IOException {
    // make sure searching sees right # hits
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 34, hits.length);
    Document d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "21", d.get("id"));
    reader.close();

    // fully merge
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
    writer.forceMerge(1);
    writer.close();

    reader = IndexReader.open(dir);
    searcher = new IndexSearcher(reader);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 34, hits.length);
    doTestHits(hits, 34, searcher.getIndexReader());
    reader.close();
  }

  public File createIndex(String dirName, boolean doCFS, boolean fullyMerged) throws IOException {
    // we use a real directory name that is not cleaned up, because this method is only used to create backwards indexes:
    File indexDir = new File(LuceneTestCase.TEMP_DIR, dirName);
    _TestUtil.rmDir(indexDir);
    Directory dir = newFSDirectory(indexDir);
    LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
    mp.setUseCompoundFile(doCFS);
    mp.setNoCFSRatio(1.0);
    // TODO: remove randomness
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
      .setMaxBufferedDocs(10).setMergePolicy(mp);
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
      mp.setUseCompoundFile(doCFS);
      mp.setNoCFSRatio(1.0);
      // TODO: remove randomness
      conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
        .setMaxBufferedDocs(10).setMergePolicy(mp);
      writer = new IndexWriter(dir, conf);
      addNoProxDoc(writer);
      writer.close();

      writer = new IndexWriter(dir,
        conf.setMergePolicy(doCFS ? NoMergePolicy.COMPOUND_FILES : NoMergePolicy.NO_COMPOUND_FILES)
      );
      Term searchTerm = new Term("id", "7");
      writer.deleteDocuments(searchTerm);
      writer.close();
    }
    
    dir.close();
    
    return indexDir;
  }

  /* Verifies that the expected file names were produced */

  public void testExactFileNames() throws IOException {

    String outputDirName = "lucene.backwardscompat0.index";
    File outputDir = _TestUtil.getTempDir(outputDirName);
    _TestUtil.rmDir(outputDir);

    try {
      Directory dir = newFSDirectory(outputDir);

      LogMergePolicy mergePolicy = newLogMergePolicy(true, 10);
      mergePolicy.setNoCFSRatio(1); // This test expects all of its segments to be in CFS

      IndexWriter writer = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random())).
              setMaxBufferedDocs(-1).
              setRAMBufferSizeMB(16.0).
              setMergePolicy(mergePolicy)
      );
      for(int i=0;i<35;i++) {
        addDoc(writer, i);
      }
      assertEquals("wrong doc count", 35, writer.maxDoc());
      writer.close();

      // Delete one doc so we get a .del file:
      writer = new IndexWriter(
          dir,
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random()))
            .setMergePolicy(NoMergePolicy.NO_COMPOUND_FILES)
      );
      Term searchTerm = new Term("id", "7");
      writer.deleteDocuments(searchTerm);
      writer.close();

      // Now verify file names... TODO: fix this test better, we could populate from 
      // separateFiles() or something.
      String[] expected = new String[] {"_0.cfs", "_0.cfe",
                               "_0_1.del",
                               "segments_2",
                               "segments.gen"};
      
      String[] expectedSimpleText = new String[] {"_0.cfs", "_0.cfe",
          "_0_1.liv",
          "segments_2",
          "segments.gen"};

      String[] actual = dir.listAll();
      Arrays.sort(expected);
      Arrays.sort(expectedSimpleText);
      Arrays.sort(actual);
      if (!Arrays.equals(expected, actual) && !Arrays.equals(expectedSimpleText, actual)) {
        fail("incorrect filenames in index: expected:\n    " + asString(expected) 
            + "\n or " + asString(expectedSimpleText) + "\n actual:\n    " + asString(actual));
      }
      dir.close();
    } finally {
      _TestUtil.rmDir(outputDir);
    }
  }

  private String asString(String[] l) {
    String s = "";
    for(int i=0;i<l.length;i++) {
      if (i > 0) {
        s += "\n    ";
      }
      s += l[i];
    }
    return s;
  }

  private void addDoc(IndexWriter writer, int id) throws IOException
  {
    Document doc = new Document();
    doc.add(new TextField("content", "aaa"));
    doc.add(new Field("id", Integer.toString(id), StringField.TYPE_STORED));
    FieldType customType2 = new FieldType(TextField.TYPE_STORED);
    customType2.setStoreTermVectors(true);
    customType2.setStoreTermVectorPositions(true);
    customType2.setStoreTermVectorOffsets(true);
    doc.add(new Field("autf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", customType2));
    doc.add(new Field("utf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", customType2));
    doc.add(new Field("content2", "here is more content with aaa aaa aaa", customType2));
    doc.add(new Field("fie\u2C77ld", "field with non-ascii name", customType2));
    // add numeric fields, to test if flex preserves encoding
    doc.add(new IntField("trieInt", id));
    doc.add(new LongField("trieLong", (long) id));
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
      IndexReader r = IndexReader.open(dir);
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
      assertEquals(35, countDocs(_TestUtil.docs(random(), terms, null, null, false)));
      assertNull(terms.next());

      // should hit end of field
      assertEquals(TermsEnum.SeekStatus.END,
                   terms.seekCeil(new BytesRef("bbb")));
      assertNull(terms.next());

      // should seek to aaa
      assertEquals(TermsEnum.SeekStatus.NOT_FOUND,
                   terms.seekCeil(new BytesRef("a")));
      assertTrue(terms.term().bytesEquals(aaaTerm));
      assertEquals(35, countDocs(_TestUtil.docs(random(), terms, null, null, false)));
      assertNull(terms.next());

      assertEquals(TermsEnum.SeekStatus.FOUND,
                   terms.seekCeil(aaaTerm));
      assertEquals(35, countDocs(_TestUtil.docs(random(), terms,null, null, false)));
      assertNull(terms.next());

      r.close();
    }
  }
  
  public void testNumericFields() throws Exception {
    for (String name : oldNames) {
      
      Directory dir = oldIndexDirs.get(name);
      IndexReader reader = IndexReader.open(dir);
      IndexSearcher searcher = new IndexSearcher(reader);
      
      for (int id=10; id<15; id++) {
        ScoreDoc[] hits = searcher.search(NumericRangeQuery.newIntRange("trieInt", 4, Integer.valueOf(id), Integer.valueOf(id), true, true), 100).scoreDocs;
        assertEquals("wrong number of hits", 1, hits.length);
        Document d = searcher.doc(hits[0].doc);
        assertEquals(String.valueOf(id), d.get("id"));
        
        hits = searcher.search(NumericRangeQuery.newLongRange("trieLong", 4, Long.valueOf(id), Long.valueOf(id), true, true), 100).scoreDocs;
        assertEquals("wrong number of hits", 1, hits.length);
        d = searcher.doc(hits[0].doc);
        assertEquals(String.valueOf(id), d.get("id"));
      }
      
      // check that also lower-precision fields are ok
      ScoreDoc[] hits = searcher.search(NumericRangeQuery.newIntRange("trieInt", 4, Integer.MIN_VALUE, Integer.MAX_VALUE, false, false), 100).scoreDocs;
      assertEquals("wrong number of hits", 34, hits.length);
      
      hits = searcher.search(NumericRangeQuery.newLongRange("trieLong", 4, Long.MIN_VALUE, Long.MAX_VALUE, false, false), 100).scoreDocs;
      assertEquals("wrong number of hits", 34, hits.length);
      
      // check decoding into field cache
      int[] fci = FieldCache.DEFAULT.getInts(SlowCompositeReaderWrapper.wrap(searcher.getIndexReader()), "trieInt", false);
      for (int val : fci) {
        assertTrue("value in id bounds", val >= 0 && val < 35);
      }
      
      long[] fcl = FieldCache.DEFAULT.getLongs(SlowCompositeReaderWrapper.wrap(searcher.getIndexReader()), "trieLong", false);
      for (long val : fcl) {
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
    for (SegmentInfo si : infos) {
      assertEquals(Constants.LUCENE_MAIN_VERSION, si.getVersion());
    }
    return infos.size();
  }
  
  private int getNumberOfSegments(Directory dir) throws IOException {
    final SegmentInfos infos = new SegmentInfos();
    infos.read(dir);
    return infos.size();
  }

  public void testUpgradeOldIndex() throws Exception {
    List<String> names = new ArrayList<String>(oldNames.length + oldSingleSegmentNames.length);
    names.addAll(Arrays.asList(oldNames));
    names.addAll(Arrays.asList(oldSingleSegmentNames));
    for(String name : names) {
      if (VERBOSE) {
        System.out.println("testUpgradeOldIndex: index=" +name);
      }
      Directory dir = newDirectory(oldIndexDirs.get(name));

      new IndexUpgrader(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null), false)
        .upgrade();

      checkAllSegmentsUpgraded(dir);
      
      dir.close();
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
      
      new IndexUpgrader(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null), false)
        .upgrade();

      final int segCount = checkAllSegmentsUpgraded(dir);
      assertEquals("Index must still contain the same number of segments, as only one segment was upgraded and nothing else merged",
        origSegCount, segCount);
      
      dir.close();
    }
  }
  
  public static final String surrogatesIndexName = "index.36.surrogates.zip";

  public void testSurrogates() throws Exception {
    File oldIndexDir = _TestUtil.getTempDir("surrogates");
    _TestUtil.unzip(getDataFile(surrogatesIndexName), oldIndexDir);
    Directory dir = newFSDirectory(oldIndexDir);
    // TODO: more tests
    _TestUtil.checkIndex(dir);
    dir.close();
  }

}
