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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Bits;

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
  public void testCreatePreLocklessCFS() throws IOException {
    createIndex("index.cfs", true);
  }

  public void testCreatePreLocklessNoCFS() throws IOException {
    createIndex("index.nocfs", false);
  }
  */

  /* Unzips zipName --> dirName, removing dirName
     first */
  public void unzip(File zipName, String destDirName) throws IOException {

    ZipFile zipFile = new ZipFile(zipName);

    Enumeration<? extends ZipEntry> entries = zipFile.entries();

    String dirName = fullDir(destDirName);

    File fileDir = new File(dirName);
    rmDir(destDirName);

    fileDir.mkdir();

    while (entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();

      InputStream in = zipFile.getInputStream(entry);
      OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(fileDir, entry.getName())));

      byte[] buffer = new byte[8192];
      int len;
      while((len = in.read(buffer)) >= 0) {
        out.write(buffer, 0, len);
      }

      in.close();
      out.close();
    }

    zipFile.close();
  }
/*
  public void testCreateCFS() throws IOException {
    String dirName = "testindex.cfs";
    createIndex(dirName, true);
    rmDir(dirName);
  }

  public void testCreateNoCFS() throws IOException {
    String dirName = "testindex.nocfs";
    createIndex(dirName, true);
    rmDir(dirName);
  }

*/  
  final String[] oldNames = {"30.cfs",
                             "30.nocfs",
                             "31.cfs",
                             "31.nocfs",
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
  
  /** This test checks that *only* IndexFormatTooOldExceptions are throws when you open and operate on too old indexes! */
  public void testUnsupportedOldIndexes() throws Exception {
    final Random rnd = newRandom();
    for(int i=0;i<unsupportedNames.length;i++) {
      unzip(getDataFile("unsupported." + unsupportedNames[i] + ".zip"), unsupportedNames[i]);

      String fullPath = fullDir(unsupportedNames[i]);
      Directory dir = FSDirectory.open(new File(fullPath));

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
        writer = new IndexWriter(dir, newIndexWriterConfig(rnd,
          TEST_VERSION_CURRENT, new MockAnalyzer())
          .setMergeScheduler(new SerialMergeScheduler()) // no threads!
        );
        // TODO: Make IndexWriter fail on open!
        if (rnd.nextBoolean()) {
          writer.optimize();
        } else {
          reader = writer.getReader();
        }
        fail("IndexWriter creation should not pass for "+unsupportedNames[i]);
      } catch (IndexFormatTooOldException e) {
        // pass
      } finally {
        if (reader != null) reader.close();
        reader = null;
        if (writer != null) writer.close();
        writer = null;
      }
      
      ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
      CheckIndex checker = new CheckIndex(dir);
      checker.setInfoStream(new PrintStream(bos));
      CheckIndex.Status indexStatus = checker.checkIndex();
      assertFalse(indexStatus.clean);
      assertTrue(bos.toString().contains(IndexFormatTooOldException.class.getName()));

      dir.close();
      rmDir(unsupportedNames[i]);
    }
  }
  
  public void testOptimizeOldIndex() throws Exception {
    for(int i=0;i<oldNames.length;i++) {
      unzip(getDataFile("index." + oldNames[i] + ".zip"), oldNames[i]);

      String fullPath = fullDir(oldNames[i]);
      Directory dir = FSDirectory.open(new File(fullPath));

      IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(
          TEST_VERSION_CURRENT, new MockAnalyzer()));
      w.optimize();
      w.close();

      _TestUtil.checkIndex(dir);
      
      dir.close();
      rmDir(oldNames[i]);
    }
  }

  public void testAddOldIndexes() throws IOException {
    Random random = newRandom();
    for (String name : oldNames) {
      unzip(getDataFile("index." + name + ".zip"), name);
      String fullPath = fullDir(name);
      Directory dir = FSDirectory.open(new File(fullPath));

      Directory targetDir = newDirectory(random);
      IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer()));
      w.addIndexes(new Directory[] { dir });
      w.close();

      _TestUtil.checkIndex(targetDir);
      
      dir.close();
      targetDir.close();
      rmDir(name);
    }
  }

  public void testAddOldIndexesReader() throws IOException {
    Random random = newRandom();
    for (String name : oldNames) {
      unzip(getDataFile("index." + name + ".zip"), name);
      String fullPath = fullDir(name);
      Directory dir = FSDirectory.open(new File(fullPath));
      IndexReader reader = IndexReader.open(dir);
      
      Directory targetDir = newDirectory(random);
      IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(random,
          TEST_VERSION_CURRENT, new MockAnalyzer()));
      w.addIndexes(new IndexReader[] { reader });
      w.close();
      reader.close();
      
      _TestUtil.checkIndex(targetDir);
      
      dir.close();
      targetDir.close();
      rmDir(name);
    }
  }

  public void testSearchOldIndex() throws IOException {
    for(int i=0;i<oldNames.length;i++) {
      unzip(getDataFile("index." + oldNames[i] + ".zip"), oldNames[i]);
      searchIndex(oldNames[i], oldNames[i]);
      rmDir(oldNames[i]);
    }
  }

  public void testIndexOldIndexNoAdds() throws IOException {
    Random random = newRandom();
    for(int i=0;i<oldNames.length;i++) {
      unzip(getDataFile("index." + oldNames[i] + ".zip"), oldNames[i]);
      changeIndexNoAdds(random, oldNames[i]);
      rmDir(oldNames[i]);
    }
  }

  public void testIndexOldIndex() throws IOException {
    Random random = newRandom();
    for(int i=0;i<oldNames.length;i++) {
      unzip(getDataFile("index." + oldNames[i] + ".zip"), oldNames[i]);
      changeIndexWithAdds(random, oldNames[i]);
      rmDir(oldNames[i]);
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

  public void searchIndex(String dirName, String oldName) throws IOException {
    //QueryParser parser = new QueryParser("contents", new MockAnalyzer());
    //Query query = parser.parse("handle:1");

    dirName = fullDir(dirName);

    Directory dir = FSDirectory.open(new File(dirName));
    IndexSearcher searcher = new IndexSearcher(dir, true);
    IndexReader reader = searcher.getIndexReader();

    _TestUtil.checkIndex(dir);

    final Bits delDocs = MultiFields.getDeletedDocs(reader);

    for(int i=0;i<35;i++) {
      if (!delDocs.get(i)) {
        Document d = reader.document(i);
        List<Fieldable> fields = d.getFields();
        if (d.getField("content3") == null) {
          final int numFields = 5;
          assertEquals(numFields, fields.size());
          Field f =  d.getField("id");
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
      } else
        // Only ID 7 is deleted
        assertEquals(7, i);
    }
    
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;

    // First document should be #21 since it's norm was
    // increased:
    Document d = searcher.doc(hits[0].doc);
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

  public void changeIndexWithAdds(Random random, String dirName) throws IOException {
    String origDirName = dirName;
    dirName = fullDir(dirName);

    Directory dir = FSDirectory.open(new File(dirName));
    // open writer
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.APPEND));

    // add 10 docs
    for(int i=0;i<10;i++) {
      addDoc(writer, 35+i);
    }

    // make sure writer sees right total -- writer seems not to know about deletes in .del?
    final int expected;
    if (compare(origDirName, "24") < 0) {
      expected = 45;
    } else {
      expected = 46;
    }
    assertEquals("wrong doc count", expected, writer.maxDoc());
    writer.close();

    // make sure searching sees right # hits
    IndexSearcher searcher = new IndexSearcher(dir, true);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    Document d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "21", d.get("id"));
    doTestHits(hits, 44, searcher.getIndexReader());
    searcher.close();

    // make sure we can do delete & setNorm against this segment:
    IndexReader reader = IndexReader.open(dir, false);
    searcher = new IndexSearcher(reader);
    Term searchTerm = new Term("id", "6");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("wrong delete count", 1, delCount);
    reader.setNorm(searcher.search(new TermQuery(new Term("id", "22")), 10).scoreDocs[0].doc, "content", (float) 2.0);
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
    writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.APPEND));
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

  public void changeIndexNoAdds(Random random, String dirName) throws IOException {

    dirName = fullDir(dirName);

    Directory dir = FSDirectory.open(new File(dirName));

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
    reader.setNorm(22, "content", (float) 2.0);
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
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setOpenMode(OpenMode.APPEND));
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

  public void createIndex(Random random, String dirName, boolean doCFS) throws IOException {

    rmDir(dirName);

    dirName = fullDir(dirName);

    Directory dir = FSDirectory.open(new File(dirName));
    IndexWriterConfig conf = newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(10);
    ((LogMergePolicy) conf.getMergePolicy()).setUseCompoundFile(doCFS);
    ((LogMergePolicy) conf.getMergePolicy()).setUseCompoundDocStore(doCFS);
    IndexWriter writer = new IndexWriter(dir, conf);
    
    for(int i=0;i<35;i++) {
      addDoc(writer, i);
    }
    assertEquals("wrong doc count", 35, writer.maxDoc());
    writer.close();

    // open fresh writer so we get no prx file in the added segment
    conf = newIndexWriterConfig(random, TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(10);
    ((LogMergePolicy) conf.getMergePolicy()).setUseCompoundFile(doCFS);
    ((LogMergePolicy) conf.getMergePolicy()).setUseCompoundDocStore(doCFS);
    writer = new IndexWriter(dir, conf);
    addNoProxDoc(writer);
    writer.close();

    // Delete one doc so we get a .del file:
    IndexReader reader = IndexReader.open(dir, false);
    Term searchTerm = new Term("id", "7");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("didn't delete the right number of documents", 1, delCount);

    // Set one norm so we get a .s0 file:
    reader.setNorm(21, "content", (float) 1.5);
    reader.close();
  }

  /* Verifies that the expected file names were produced */

  public void testExactFileNames() throws IOException {

    String outputDir = "lucene.backwardscompat0.index";
    rmDir(outputDir);

    try {
      Directory dir = FSDirectory.open(new File(fullDir(outputDir)));

      IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(newRandom(), TEST_VERSION_CURRENT, new MockAnalyzer()).setMaxBufferedDocs(-1).setRAMBufferSizeMB(16.0));
      ((LogMergePolicy) writer.getMergePolicy()).setUseCompoundFile(true);
      ((LogMergePolicy) writer.getMergePolicy()).setMergeFactor(10);
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
      reader.setNorm(21, "content", (float) 1.5);
      reader.close();

      // The numbering of fields can vary depending on which
      // JRE is in use.  On some JREs we see content bound to
      // field 0; on others, field 1.  So, here we have to
      // figure out which field number corresponds to
      // "content", and then set our expected file names below
      // accordingly:
      CompoundFileReader cfsReader = new CompoundFileReader(dir, "_0.cfs");
      FieldInfos fieldInfos = new FieldInfos(cfsReader, "_0.fnm");
      int contentFieldIndex = -1;
      for(int i=0;i<fieldInfos.size();i++) {
        FieldInfo fi = fieldInfos.fieldInfo(i);
        if (fi.name.equals("content")) {
          contentFieldIndex = i;
          break;
        }
      }
      cfsReader.close();
      assertTrue("could not locate the 'content' field number in the _2.cfs segment", contentFieldIndex != -1);

      // Now verify file names:
      String[] expected = new String[] {"_0.cfs",
                               "_0_1.del",
                               "_0_1.s" + contentFieldIndex,
                               "segments_2",
                               "segments.gen"};

      String[] actual = dir.listAll();
      Arrays.sort(expected);
      Arrays.sort(actual);
      if (!Arrays.equals(expected, actual)) {
        fail("incorrect filenames in index: expected:\n    " + asString(expected) + "\n  actual:\n    " + asString(actual));
      }
      dir.close();
    } finally {
      rmDir(outputDir);
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
    doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(new Field("id", Integer.toString(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
    doc.add(new Field("autf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("utf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("content2", "here is more content with aaa aaa aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("fie\u2C77ld", "field with non-ascii name", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    // add numeric fields, to test if flex preserves encoding
    doc.add(new NumericField("trieInt", 4).setIntValue(id));
    doc.add(new NumericField("trieLong", 4).setLongValue(id));
    writer.addDocument(doc);
  }

  private void addNoProxDoc(IndexWriter writer) throws IOException {
    Document doc = new Document();
    Field f = new Field("content3", "aaa", Field.Store.YES, Field.Index.ANALYZED);
    f.setOmitTermFreqAndPositions(true);
    doc.add(f);
    f = new Field("content4", "aaa", Field.Store.YES, Field.Index.NO);
    f.setOmitTermFreqAndPositions(true);
    doc.add(f);
    writer.addDocument(doc);
  }

  private void rmDir(String dir) throws IOException {
    File fileDir = new File(fullDir(dir));
    if (fileDir.exists()) {
      File[] files = fileDir.listFiles();
      if (files != null) {
        for (int i = 0; i < files.length; i++) {
          files[i].delete();
        }
      }
      fileDir.delete();
    }
  }

  public static String fullDir(String dirName) throws IOException {
    return new File(TEMP_DIR, dirName).getCanonicalPath();
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
      unzip(getDataFile("index." + oldNames[i] + ".zip"), oldNames[i]);
      String fullPath = fullDir(oldNames[i]);
      Directory dir = FSDirectory.open(new File(fullPath));
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
                   terms.seek(aaaTerm));
      assertEquals(35, countDocs(terms.docs(null, null)));
      assertNull(terms.next());

      // should hit end of field
      assertEquals(TermsEnum.SeekStatus.END,
                   terms.seek(new BytesRef("bbb")));
      assertNull(terms.next());

      // should seek to aaa
      assertEquals(TermsEnum.SeekStatus.NOT_FOUND,
                   terms.seek(new BytesRef("a")));
      assertTrue(terms.term().bytesEquals(aaaTerm));
      assertEquals(35, countDocs(terms.docs(null, null)));
      assertNull(terms.next());

      assertEquals(TermsEnum.SeekStatus.FOUND,
                   terms.seek(aaaTerm));
      assertEquals(35, countDocs(terms.docs(null, null)));
      assertNull(terms.next());

      r.close();
      dir.close();
      rmDir(oldNames[i]);
    }
  }
  
  public void testNumericFields() throws Exception {
    for(int i=0;i<oldNames.length;i++) {
      
      unzip(getDataFile("index." + oldNames[i] + ".zip"), oldNames[i]);
      String fullPath = fullDir(oldNames[i]);
      Directory dir = FSDirectory.open(new File(fullPath));
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
      rmDir(oldNames[i]);
    }
  }

}
