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
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericField;
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
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.CompoundFileDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

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
  // that also optimized indexes are correctly upgraded by IndexUpgrader.
  // You don't need them to be build for non-3.1 (the test is happy with just one
  // "old" segment format, version is unimportant:
  
  public void testCreateOptimizedCFS() throws IOException {
    createIndex("index.optimized.cfs", true, true);
  }

  public void testCreateOptimizedNoCFS() throws IOException {
    createIndex("index.optimized.nocfs", false, true);
  }

*/  
  final String[] oldNames = {"30.cfs",
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
  
  final String[] oldOptimizedNames = {"31.optimized.cfs",
                                      "31.optimized.nocfs",
  };
  
  /** This test checks that *only* IndexFormatTooOldExceptions are thrown when you open and operate on too old indexes! */
  public void testUnsupportedOldIndexes() throws Exception {
    for(int i=0;i<unsupportedNames.length;i++) {
      if (VERBOSE) {
        System.out.println("TEST: index " + unsupportedNames[i]);
      }
      File oldIndxeDir = _TestUtil.getTempDir(unsupportedNames[i]);
      _TestUtil.unzip(getDataFile("unsupported." + unsupportedNames[i] + ".zip"), oldIndxeDir);
      Directory dir = newFSDirectory(oldIndxeDir);

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
          TEST_VERSION_CURRENT, new MockAnalyzer(random)));
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
  
  public void testOptimizeOldIndex() throws Exception {
    for(int i=0;i<oldNames.length;i++) {
      if (VERBOSE) {
        System.out.println("\nTEST: index=" + oldNames[i]);
      }
      File oldIndxeDir = _TestUtil.getTempDir(oldNames[i]);
      _TestUtil.unzip(getDataFile("index." + oldNames[i] + ".zip"), oldIndxeDir);
      Directory dir = newFSDirectory(oldIndxeDir);

      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      w.optimize();
      w.close();
      
      dir.close();
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

  public void testAddOldIndexes() throws IOException {
    for (String name : oldNames) {
      File oldIndxeDir = _TestUtil.getTempDir(name);
      _TestUtil.unzip(getDataFile("index." + name + ".zip"), oldIndxeDir);
      Directory dir = newFSDirectory(oldIndxeDir);

      Directory targetDir = newDirectory();
      IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      w.addIndexes(dir);
      w.close();
      
      dir.close();
      targetDir.close();
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

  public void testAddOldIndexesReader() throws IOException {
    for (String name : oldNames) {
      File oldIndxeDir = _TestUtil.getTempDir(name);
      _TestUtil.unzip(getDataFile("index." + name + ".zip"), oldIndxeDir);
      Directory dir = newFSDirectory(oldIndxeDir);
      IndexReader reader = IndexReader.open(dir);
      
      Directory targetDir = newDirectory();
      IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer(random)));
      w.addIndexes(reader);
      w.close();
      reader.close();
            
      dir.close();
      targetDir.close();
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

  public void testSearchOldIndex() throws IOException {
    for(int i=0;i<oldNames.length;i++) {
      File oldIndxeDir = _TestUtil.getTempDir(oldNames[i]);
      _TestUtil.unzip(getDataFile("index." + oldNames[i] + ".zip"), oldIndxeDir);
      searchIndex(oldIndxeDir, oldNames[i]);
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

  public void testIndexOldIndexNoAdds() throws IOException {
    for(int i=0;i<oldNames.length;i++) {
      File oldIndxeDir = _TestUtil.getTempDir(oldNames[i]);
      _TestUtil.unzip(getDataFile("index." + oldNames[i] + ".zip"), oldIndxeDir);
      changeIndexNoAdds(random, oldIndxeDir);
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

  public void testIndexOldIndex() throws IOException {
    for(int i=0;i<oldNames.length;i++) {
      if (VERBOSE) {
        System.out.println("TEST: oldName=" + oldNames[i]);
      }
      File oldIndxeDir = _TestUtil.getTempDir(oldNames[i]);
      _TestUtil.unzip(getDataFile("index." + oldNames[i] + ".zip"), oldIndxeDir);
      changeIndexWithAdds(random, oldIndxeDir, oldNames[i]);
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

  private void doTestHits(ScoreDoc[] hits, int expectedCount, IndexReader reader) throws IOException {
    final int hitCount = hits.length;
    assertEquals("wrong number of hits", expectedCount, hitCount);
    for(int i=0;i<hitCount;i++) {
      reader.document(hits[i].doc);
      reader.getTermFreqVectors(hits[i].doc);
    }
  }

  public void searchIndex(File indexDir, String oldName) throws IOException {
    //QueryParser parser = new QueryParser("contents", new MockAnalyzer(random));
    //Query query = parser.parse("handle:1");

    Directory dir = newFSDirectory(indexDir);
    IndexSearcher searcher = new IndexSearcher(dir, true);
    IndexReader reader = searcher.getIndexReader();

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

        TermFreqVector tfv = reader.getTermFreqVector(i, "utf8");
        assertNotNull("docID=" + i + " index=" + indexDir.getName(), tfv);
        assertTrue(tfv instanceof TermPositionVector);
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

    searcher.close();
    dir.close();
  }

  private int compare(String name, String v) {
    int v0 = Integer.parseInt(name.substring(0, 2));
    int v1 = Integer.parseInt(v);
    return v0 - v1;
  }

  public void changeIndexWithAdds(Random random, File oldIndexDir, String origOldName) throws IOException {

    Directory dir = newFSDirectory(oldIndexDir);
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
    IndexSearcher searcher = new IndexSearcher(dir, true);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    Document d = searcher.getIndexReader().document(hits[0].doc);
    assertEquals("wrong first document", "21", d.get("id"));
    doTestHits(hits, 44, searcher.getIndexReader());
    searcher.close();

    // make sure we can do delete & setNorm against this segment:
    IndexReader reader = IndexReader.open(dir, false);
    searcher = newSearcher(reader);
    Term searchTerm = new Term("id", "6");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("wrong delete count", 1, delCount);
    DefaultSimilarity sim = new DefaultSimilarity();
    reader.setNorm(searcher.search(new TermQuery(new Term("id", "22")), 10).scoreDocs[0].doc, "content", sim.encodeNormValue(2.0f));
    reader.close();
    searcher.close();

    // make sure they "took":
    searcher = new IndexSearcher(dir, true);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 43, hits.length);
    d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "22", d.get("id"));
    doTestHits(hits, 43, searcher.getIndexReader());
    searcher.close();

    // optimize
    writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
    writer.optimize();
    writer.close();

    searcher = new IndexSearcher(dir, true);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 43, hits.length);
    d = searcher.doc(hits[0].doc);
    doTestHits(hits, 43, searcher.getIndexReader());
    assertEquals("wrong first document", "22", d.get("id"));
    searcher.close();

    dir.close();
  }

  public void changeIndexNoAdds(Random random, File oldIndexDir) throws IOException {

    Directory dir = newFSDirectory(oldIndexDir);

    // make sure searching sees right # hits
    IndexSearcher searcher = new IndexSearcher(dir, true);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 34, hits.length);
    Document d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "21", d.get("id"));
    searcher.close();

    // make sure we can do a delete & setNorm against this segment:
    IndexReader reader = IndexReader.open(dir, false);
    Term searchTerm = new Term("id", "6");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("wrong delete count", 1, delCount);
    DefaultSimilarity sim = new DefaultSimilarity();
    reader.setNorm(22, "content", sim.encodeNormValue(2.0f));
    reader.close();

    // make sure they "took":
    searcher = new IndexSearcher(dir, true);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 33, hits.length);
    d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "22", d.get("id"));
    doTestHits(hits, 33, searcher.getIndexReader());
    searcher.close();

    // optimize
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).setOpenMode(OpenMode.APPEND));
    writer.optimize();
    writer.close();

    searcher = new IndexSearcher(dir, true);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 33, hits.length);
    d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "22", d.get("id"));
    doTestHits(hits, 33, searcher.getIndexReader());
    searcher.close();

    dir.close();
  }

  public File createIndex(String dirName, boolean doCFS, boolean optimized) throws IOException {
    // we use a real directory name that is not cleaned up, because this method is only used to create backwards indexes:
    File indexDir = new File(LuceneTestCase.TEMP_DIR, dirName);
    _TestUtil.rmDir(indexDir);
    Directory dir = newFSDirectory(indexDir);
    LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
    mp.setUseCompoundFile(doCFS);
    mp.setNoCFSRatio(1.0);
    // TODO: remove randomness
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random))
      .setMaxBufferedDocs(10).setMergePolicy(mp);
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for(int i=0;i<35;i++) {
      addDoc(writer, i);
    }
    assertEquals("wrong doc count", 35, writer.maxDoc());
    if (optimized) {
      writer.optimize();
    }
    writer.close();

    if (!optimized) {
      // open fresh writer so we get no prx file in the added segment
      mp = new LogByteSizeMergePolicy();
      mp.setUseCompoundFile(doCFS);
      mp.setNoCFSRatio(1.0);
      // TODO: remove randomness
      conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random))
        .setMaxBufferedDocs(10).setMergePolicy(mp);
      writer = new IndexWriter(dir, conf);
      addNoProxDoc(writer);
      writer.close();

      // Delete one doc so we get a .del file:
      IndexReader reader = IndexReader.open(dir, false);
      Term searchTerm = new Term("id", "7");
      int delCount = reader.deleteDocuments(searchTerm);
      assertEquals("didn't delete the right number of documents", 1, delCount);

      // Set one norm so we get a .s0 file:
      DefaultSimilarity sim = new DefaultSimilarity();
      reader.setNorm(21, "content", sim.encodeNormValue(1.5f));
      reader.close();
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
          newIndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random)).
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
      IndexReader reader = IndexReader.open(dir, false);
      Term searchTerm = new Term("id", "7");
      int delCount = reader.deleteDocuments(searchTerm);
      assertEquals("didn't delete the right number of documents", 1, delCount);

      // Set one norm so we get a .s0 file:
      DefaultSimilarity sim = new DefaultSimilarity();
      reader.setNorm(21, "content", sim.encodeNormValue(1.5f));
      reader.close();

      // The numbering of fields can vary depending on which
      // JRE is in use.  On some JREs we see content bound to
      // field 0; on others, field 1.  So, here we have to
      // figure out which field number corresponds to
      // "content", and then set our expected file names below
      // accordingly:
      CompoundFileDirectory cfsReader = new CompoundFileDirectory(dir, "_0.cfs", newIOContext(random), false);
      FieldInfos fieldInfos = new FieldInfos(cfsReader, "_0.fnm");
      int contentFieldIndex = -1;
      for (FieldInfo fi : fieldInfos) {
        if (fi.name.equals("content")) {
          contentFieldIndex = fi.number;
          break;
        }
      }
      cfsReader.close();
      assertTrue("could not locate the 'content' field number in the _2.cfs segment", contentFieldIndex != -1);

      // Now verify file names:
      String[] expected = new String[] {"_0.cfs", "_0.cfe",
                               "_0_1.del",
                               "_0_1.s" + contentFieldIndex,
                               "segments_2",
                               "segments.gen",
                               "_1.fnx"};

      String[] actual = dir.listAll();
      Arrays.sort(expected);
      Arrays.sort(actual);
      if (!Arrays.equals(expected, actual)) {
        fail("incorrect filenames in index: expected:\n    " + asString(expected) + "\n  actual:\n    " + asString(actual));
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
    doc.add(new NumericField("trieInt", 4).setIntValue(id));
    doc.add(new NumericField("trieLong", 4).setLongValue(id));
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
    for(int i=0;i<oldNames.length;i++) {
      File oldIndexDir = _TestUtil.getTempDir(oldNames[i]);
    	_TestUtil.unzip(getDataFile("index." + oldNames[i] + ".zip"), oldIndexDir);
      Directory dir = newFSDirectory(oldIndexDir);
      IndexReader r = IndexReader.open(dir);
      TermsEnum terms = MultiFields.getFields(r).terms("content").iterator();
      BytesRef t = terms.next();
      assertNotNull(t);

      // content field only has term aaa:
      assertEquals("aaa", t.utf8ToString());
      assertNull(terms.next());

      BytesRef aaaTerm = new BytesRef("aaa");

      // should be found exactly
      assertEquals(TermsEnum.SeekStatus.FOUND,
                   terms.seekCeil(aaaTerm));
      assertEquals(35, countDocs(terms.docs(null, null)));
      assertNull(terms.next());

      // should hit end of field
      assertEquals(TermsEnum.SeekStatus.END,
                   terms.seekCeil(new BytesRef("bbb")));
      assertNull(terms.next());

      // should seek to aaa
      assertEquals(TermsEnum.SeekStatus.NOT_FOUND,
                   terms.seekCeil(new BytesRef("a")));
      assertTrue(terms.term().bytesEquals(aaaTerm));
      assertEquals(35, countDocs(terms.docs(null, null)));
      assertNull(terms.next());

      assertEquals(TermsEnum.SeekStatus.FOUND,
                   terms.seekCeil(aaaTerm));
      assertEquals(35, countDocs(terms.docs(null, null)));
      assertNull(terms.next());

      r.close();
      dir.close();
      _TestUtil.rmDir(oldIndexDir);
    }
  }
  
  public void testNumericFields() throws Exception {
    for(int i=0;i<oldNames.length;i++) {
      
      File oldIndexDir = _TestUtil.getTempDir(oldNames[i]);
      _TestUtil.unzip(getDataFile("index." + oldNames[i] + ".zip"), oldIndexDir);
      Directory dir = newFSDirectory(oldIndexDir);
      IndexSearcher searcher = new IndexSearcher(dir, true);
      
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
      int[] fci = FieldCache.DEFAULT.getInts(searcher.getIndexReader(), "trieInt");
      for (int val : fci) {
        assertTrue("value in id bounds", val >= 0 && val < 35);
      }
      
      long[] fcl = FieldCache.DEFAULT.getLongs(searcher.getIndexReader(), "trieLong");
      for (long val : fcl) {
        assertTrue("value in id bounds", val >= 0L && val < 35L);
      }
      
      searcher.close();
      dir.close();
      _TestUtil.rmDir(oldIndexDir);
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
    List<String> names = new ArrayList<String>(oldNames.length + oldOptimizedNames.length);
    names.addAll(Arrays.asList(oldNames));
    names.addAll(Arrays.asList(oldOptimizedNames));
    for(String name : names) {
      if (VERBOSE) {
        System.out.println("testUpgradeOldIndex: index=" +name);
      }
      File oldIndxeDir = _TestUtil.getTempDir(name);
      _TestUtil.unzip(getDataFile("index." + name + ".zip"), oldIndxeDir);
      Directory dir = newFSDirectory(oldIndxeDir);

      new IndexUpgrader(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null), false)
        .upgrade();

      checkAllSegmentsUpgraded(dir);
      
      dir.close();
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

  public void testUpgradeOldOptimizedIndexWithAdditions() throws Exception {
    for (String name : oldOptimizedNames) {
      if (VERBOSE) {
        System.out.println("testUpgradeOldOptimizedIndexWithAdditions: index=" +name);
      }
      File oldIndxeDir = _TestUtil.getTempDir(name);
      _TestUtil.unzip(getDataFile("index." + name + ".zip"), oldIndxeDir);
      Directory dir = newFSDirectory(oldIndxeDir);

      assertEquals("Original index must be optimized", 1, getNumberOfSegments(dir));

      // create a bunch of dummy segments
      int id = 40;
      RAMDirectory ramDir = new RAMDirectory();
      for (int i = 0; i < 3; i++) {
        // only use Log- or TieredMergePolicy, to make document addition predictable and not suddenly merge:
        MergePolicy mp = random.nextBoolean() ? newLogMergePolicy() : newTieredMergePolicy();
        IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new MockAnalyzer(random))
          .setMergePolicy(mp);
        IndexWriter w = new IndexWriter(ramDir, iwc);
        // add few more docs:
        for(int j = 0; j < RANDOM_MULTIPLIER * random.nextInt(30); j++) {
          addDoc(w, id++);
        }
        w.close(false);
      }
      
      // add dummy segments (which are all in current version) to optimized index
      MergePolicy mp = random.nextBoolean() ? newLogMergePolicy() : newTieredMergePolicy();
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
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

}
