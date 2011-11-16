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

import java.io.File;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.document.FieldSelector;
import org.apache.lucene.document.FieldSelectorResult;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.ReaderUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;
import org.apache.lucene.util.Constants;

/*
  Verify we can read the pre-2.1 file format, do searches
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

  final String[] oldNames = {"19.cfs",
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
                             "30.cfs",
                             "30.nocfs",
                             "31.cfs",
                             "31.nocfs",
                             "32.cfs",
                             "32.nocfs",
  };
  
  final String[] oldSingleSegmentNames = {"31.optimized.cfs",
                                          "31.optimized.nocfs",
  };
  
  private void assertCompressedFields29(Directory dir, boolean shouldStillBeCompressed) throws IOException {
    int count = 0;
    final int TEXT_PLAIN_LENGTH = TEXT_TO_COMPRESS.length() * 2;
    // FieldSelectorResult.SIZE returns 2*number_of_chars for String fields:
    final int BINARY_PLAIN_LENGTH = BINARY_TO_COMPRESS.length;
    
    IndexReader reader = IndexReader.open(dir, true);
    try {
      // look into sub readers and check if raw merge is on/off
      List<IndexReader> readers = new ArrayList<IndexReader>();
      ReaderUtil.gatherSubReaders(readers, reader);
      for (IndexReader ir : readers) {
        final FieldsReader fr = ((SegmentReader) ir).getFieldsReader();
        assertTrue("for a 2.9 index, FieldsReader.canReadRawDocs() must be false and other way round for a trunk index",
          shouldStillBeCompressed != fr.canReadRawDocs());
      }
    
      // test that decompression works correctly
      for(int i=0; i<reader.maxDoc(); i++) {
        if (!reader.isDeleted(i)) {
          Document d = reader.document(i);
          if (d.get("content3") != null) continue;
          count++;
          Fieldable compressed = d.getFieldable("compressed");
          if (Integer.parseInt(d.get("id")) % 2 == 0) {
            assertFalse(compressed.isBinary());
            assertEquals("incorrectly decompressed string", TEXT_TO_COMPRESS, compressed.stringValue());
          } else {
            assertTrue(compressed.isBinary());
            assertTrue("incorrectly decompressed binary", Arrays.equals(BINARY_TO_COMPRESS, compressed.getBinaryValue()));
          }
        }
      }
      
      // check if field was decompressed after full merge
      for(int i=0; i<reader.maxDoc(); i++) {
        if (!reader.isDeleted(i)) {
          Document d = reader.document(i, new FieldSelector() {
            public FieldSelectorResult accept(String fieldName) {
              return ("compressed".equals(fieldName)) ? FieldSelectorResult.SIZE : FieldSelectorResult.LOAD;
            }
          });
          if (d.get("content3") != null) continue;
          count++;
          // read the size from the binary value using DataInputStream (this prevents us from doing the shift ops ourselves):
          final DataInputStream ds = new DataInputStream(new ByteArrayInputStream(d.getFieldable("compressed").getBinaryValue()));
          final int actualSize = ds.readInt();
          ds.close();
          final int compressedSize = Integer.parseInt(d.get("compressedSize"));
          final boolean binary = Integer.parseInt(d.get("id")) % 2 > 0;
          final int shouldSize = shouldStillBeCompressed ?
            compressedSize :
            (binary ? BINARY_PLAIN_LENGTH : TEXT_PLAIN_LENGTH);
          assertEquals("size incorrect", shouldSize, actualSize);
          if (!shouldStillBeCompressed) {
            assertFalse("uncompressed field should have another size than recorded in index", compressedSize == actualSize);
          }
        }
      }
      assertEquals("correct number of tests", 34 * 2, count);
    } finally {
      reader.close();
    }
  }

  public void testUpgrade29Compression() throws IOException {
    int hasTested29 = 0;
    
    for(int i=0;i<oldNames.length;i++) {
      File oldIndxeDir = _TestUtil.getTempDir(oldNames[i]);
      _TestUtil.unzip(getDataFile("index." + oldNames[i] + ".zip"), oldIndxeDir);
      Directory dir = newFSDirectory(oldIndxeDir);

      if (oldNames[i].startsWith("29.")) {
        assertCompressedFields29(dir, true);
        hasTested29++;
      }

      new IndexUpgrader(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null), VERBOSE ? System.out : null, false)
        .upgrade();

      if (oldNames[i].startsWith("29.")) {
        assertCompressedFields29(dir, false);
        hasTested29++;
      }

      dir.close();
      _TestUtil.rmDir(oldIndxeDir);
    }
    
    assertEquals("test for compressed field should have run 4 times", 4, hasTested29);
  }

  public void testAddOldIndexes() throws IOException {
    for (String name : oldNames) {
      File oldIndxeDir = _TestUtil.getTempDir(name);
      _TestUtil.unzip(getDataFile("index." + name + ".zip"), oldIndxeDir);
      Directory dir = newFSDirectory(oldIndxeDir);

      Directory targetDir = newDirectory();
      IndexWriter w = new IndexWriter(targetDir, newIndexWriterConfig(
          TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT)));
      w.addIndexes(new Directory[] { dir });
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
          TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT)));
      w.addIndexes(new IndexReader[] { reader });
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

  private void testHits(ScoreDoc[] hits, int expectedCount, IndexReader reader) throws IOException {
    final int hitCount = hits.length;
    assertEquals("wrong number of hits", expectedCount, hitCount);
    for(int i=0;i<hitCount;i++) {
      reader.document(hits[i].doc);
      reader.getTermFreqVectors(hits[i].doc);
    }
  }

  public void searchIndex(File indexDir, String oldName) throws IOException {
    //QueryParser parser = new QueryParser("contents", new WhitespaceAnalyzer(TEST_VERSION_CURRENT));
    //Query query = parser.parse("handle:1");

    Directory dir = newFSDirectory(indexDir);
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    _TestUtil.checkIndex(dir);

    for(int i=0;i<35;i++) {
      if (!reader.isDeleted(i)) {
        Document d = reader.document(i);
        List<Fieldable> fields = d.getFields();
        if (!oldName.startsWith("19.") &&
            !oldName.startsWith("20.") &&
            !oldName.startsWith("21.") &&
            !oldName.startsWith("22.")) {

          if (d.getField("content3") == null) {
            final int numFields = oldName.startsWith("29.") ? 7 : 5;
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

          TermFreqVector tfv = reader.getTermFreqVector(i, "utf8");
          assertNotNull("docID=" + i + " index=" + indexDir.getName(), tfv);
          assertTrue(tfv instanceof TermPositionVector);
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

    testHits(hits, 34, searcher.getIndexReader());

    if (!oldName.startsWith("19.") &&
        !oldName.startsWith("20.") &&
        !oldName.startsWith("21.") &&
        !oldName.startsWith("22.")) {
      // Test on indices >= 2.3
      hits = searcher.search(new TermQuery(new Term("utf8", "\u0000")), null, 1000).scoreDocs;
      assertEquals(34, hits.length);
      hits = searcher.search(new TermQuery(new Term("utf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne")), null, 1000).scoreDocs;
      assertEquals(34, hits.length);
      hits = searcher.search(new TermQuery(new Term("utf8", "ab\ud917\udc17cd")), null, 1000).scoreDocs;
      assertEquals(34, hits.length);
    }

    searcher.close();
    reader.close();
    dir.close();
  }

  private int compare(String name, String v) {
    int v0 = Integer.parseInt(name.substring(0, 2));
    int v1 = Integer.parseInt(v);
    return v0 - v1;
  }

  /* Open pre-lockless index, add docs, do a delete &
   * setNorm, and search */
  public void changeIndexWithAdds(Random random, File oldIndexDir, String origOldName) throws IOException {
    Directory dir = newFSDirectory(oldIndexDir);
    // open writer
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT)).setOpenMode(OpenMode.APPEND));
    writer.setInfoStream(VERBOSE ? System.out : null);
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
    Document d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "21", d.get("id"));
    testHits(hits, 44, searcher.getIndexReader());
    searcher.close();
    reader.close();

    // make sure we can do delete & setNorm against this
    // pre-lockless segment:
    reader = IndexReader.open(dir, false);
    searcher = newSearcher(reader);
    Term searchTerm = new Term("id", "6");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("wrong delete count", 1, delCount);
    reader.setNorm(searcher.search(new TermQuery(new Term("id", "22")), 10).scoreDocs[0].doc, "content", (float) 2.0);
    reader.close();
    searcher.close();

    // make sure they "took":
    reader = IndexReader.open(dir, true);
    searcher = new IndexSearcher(reader);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 43, hits.length);
    d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "22", d.get("id"));
    testHits(hits, 43, searcher.getIndexReader());
    searcher.close();
    reader.close();

    // fully merge
    writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT)).setOpenMode(OpenMode.APPEND));
    writer.forceMerge(1);
    writer.close();

    reader = IndexReader.open(dir);
    searcher = new IndexSearcher(reader);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 43, hits.length);
    d = searcher.doc(hits[0].doc);
    testHits(hits, 43, searcher.getIndexReader());
    assertEquals("wrong first document", "22", d.get("id"));
    searcher.close();
    reader.close();

    dir.close();
  }

  /* Open pre-lockless index, add docs, do a delete &
   * setNorm, and search */
  public void changeIndexNoAdds(Random random, File oldIndexDir) throws IOException {

    Directory dir = newFSDirectory(oldIndexDir);

    // make sure searching sees right # hits
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 34, hits.length);
    Document d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "21", d.get("id"));
    searcher.close();
    reader.close();

    // make sure we can do a delete & setNorm against this
    // pre-lockless segment:
    reader = IndexReader.open(dir, false);
    Term searchTerm = new Term("id", "6");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("wrong delete count", 1, delCount);
    reader.setNorm(22, "content", (float) 2.0);
    reader.close();

    // make sure they "took":
    reader = IndexReader.open(dir);
    searcher = new IndexSearcher(reader);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 33, hits.length);
    d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "22", d.get("id"));
    testHits(hits, 33, searcher.getIndexReader());
    searcher.close();
    reader.close();

    // fully merge
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT)).setOpenMode(OpenMode.APPEND));
    writer.forceMerge(1);
    writer.close();

    reader = IndexReader.open(dir);
    searcher = new IndexSearcher(reader);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 33, hits.length);
    d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "22", d.get("id"));
    testHits(hits, 33, searcher.getIndexReader());
    searcher.close();
    reader.close();

    dir.close();
  }

  public File createIndex(String dirName, boolean doCFS, boolean fullyMerged) throws IOException {
    // we use a real directory name that is not cleaned up, because this method is only used to create backwards indexes:
    File indexDir = new File(LuceneTestCase.TEMP_DIR, dirName);
    _TestUtil.rmDir(indexDir);
    Directory dir = newFSDirectory(indexDir);
    LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
    mp.setUseCompoundFile(doCFS);
    mp.setNoCFSRatio(1.0);
    IndexWriterConfig conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT))
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
      conf = new IndexWriterConfig(TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT))
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
      reader.setNorm(21, "content", (float) 1.5);
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
      IndexWriterConfig conf = newIndexWriterConfig(TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT)).setMaxBufferedDocs(-1).setRAMBufferSizeMB(16.0)
        .setMergePolicy(mergePolicy);
      IndexWriter writer = new IndexWriter(dir, conf);
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
    doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.ANALYZED));
    doc.add(new Field("id", Integer.toString(id), Field.Store.YES, Field.Index.NOT_ANALYZED));
    doc.add(new Field("autf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("utf8", "Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("content2", "here is more content with aaa aaa aaa", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    doc.add(new Field("fie\u2C77ld", "field with non-ascii name", Field.Store.YES, Field.Index.ANALYZED, Field.TermVector.WITH_POSITIONS_OFFSETS));
    /* This was used in 2.9 to generate an index with compressed field:
    if (id % 2 == 0) {
      doc.add(new Field("compressed", TEXT_TO_COMPRESS, Field.Store.COMPRESS, Field.Index.NOT_ANALYZED));
      doc.add(new Field("compressedSize", Integer.toString(TEXT_COMPRESSED_LENGTH), Field.Store.YES, Field.Index.NOT_ANALYZED));
    } else {
      doc.add(new Field("compressed", BINARY_TO_COMPRESS, Field.Store.COMPRESS));    
      doc.add(new Field("compressedSize", Integer.toString(BINARY_COMPRESSED_LENGTH), Field.Store.YES, Field.Index.NOT_ANALYZED));
    }
    */
    // add numeric fields, to test if later versions preserve encoding
    doc.add(new NumericField("trieInt", 4).setIntValue(id));
    doc.add(new NumericField("trieLong", 4).setLongValue(id));
    writer.addDocument(doc);
  }

  private void addNoProxDoc(IndexWriter writer) throws IOException {
    Document doc = new Document();
    Field f = new Field("content3", "aaa", Field.Store.YES, Field.Index.ANALYZED);
    f.setIndexOptions(IndexOptions.DOCS_ONLY);
    doc.add(f);
    f = new Field("content4", "aaa", Field.Store.YES, Field.Index.NO);
    f.setIndexOptions(IndexOptions.DOCS_ONLY);
    doc.add(f);
    writer.addDocument(doc);
  }

  static final String TEXT_TO_COMPRESS = "this is a compressed field and should appear in 3.0 as an uncompressed field after merge";
  // FieldSelectorResult.SIZE returns compressed size for compressed fields, which are internally handled as binary;
  // do it in the same way like FieldsWriter, do not use CompressionTools.compressString() for compressed fields:
  /* This was used in 2.9 to generate an index with compressed field:
  static final int TEXT_COMPRESSED_LENGTH;
  static {
    try {
      TEXT_COMPRESSED_LENGTH = CompressionTools.compress(TEXT_TO_COMPRESS.getBytes("UTF-8")).length;
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }
  */
  static final byte[] BINARY_TO_COMPRESS = new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20};
  /* This was used in 2.9 to generate an index with compressed field:
  static final int BINARY_COMPRESSED_LENGTH = CompressionTools.compress(BINARY_TO_COMPRESS).length;
  */
  
  public void testNumericFields() throws Exception {
    for(int i=0;i<oldNames.length;i++) {
      // only test indexes >= 3.0
      if (oldNames[i].compareTo("30.") < 0) continue;
      
      File oldIndexDir = _TestUtil.getTempDir(oldNames[i]);
      _TestUtil.unzip(getDataFile("index." + oldNames[i] + ".zip"), oldIndexDir);
      Directory dir = newFSDirectory(oldIndexDir);
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
      int[] fci = FieldCache.DEFAULT.getInts(searcher.getIndexReader(), "trieInt");
      for (int val : fci) {
        assertTrue("value in id bounds", val >= 0 && val < 35);
      }
      
      long[] fcl = FieldCache.DEFAULT.getLongs(searcher.getIndexReader(), "trieLong");
      for (long val : fcl) {
        assertTrue("value in id bounds", val >= 0L && val < 35L);
      }
      
      searcher.close();
      reader.close();
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
    List<String> names = new ArrayList<String>(oldNames.length + oldSingleSegmentNames.length);
    names.addAll(Arrays.asList(oldNames));
    names.addAll(Arrays.asList(oldSingleSegmentNames));
    for(String name : names) {
      if (VERBOSE) {
        System.out.println("testUpgradeOldIndex: index=" +name);
      }
      File oldIndxeDir = _TestUtil.getTempDir(name);
      _TestUtil.unzip(getDataFile("index." + name + ".zip"), oldIndxeDir);
      Directory dir = newFSDirectory(oldIndxeDir);

      new IndexUpgrader(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null), VERBOSE ? System.out : null, false)
        .upgrade();

      checkAllSegmentsUpgraded(dir);
      
      dir.close();
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

  public void testUpgradeOldSingleSegmentIndexWithAdditions() throws Exception {
    for (String name : oldSingleSegmentNames) {
      if (VERBOSE) {
        System.out.println("testUpgradeOldSingleSegmentIndexWithAdditions: index=" +name);
      }
      File oldIndxeDir = _TestUtil.getTempDir(name);
      _TestUtil.unzip(getDataFile("index." + name + ".zip"), oldIndxeDir);
      Directory dir = newFSDirectory(oldIndxeDir);

      assertEquals("Original index must be single segment", 1, getNumberOfSegments(dir));

      // create a bunch of dummy segments
      int id = 40;
      RAMDirectory ramDir = new RAMDirectory();
      for (int i = 0; i < 3; i++) {
        // only use Log- or TieredMergePolicy, to make document addition predictable and not suddenly merge:
        MergePolicy mp = random.nextBoolean() ? newLogMergePolicy() : newTieredMergePolicy();
        IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, new WhitespaceAnalyzer(TEST_VERSION_CURRENT))
          .setMergePolicy(mp);
        IndexWriter w = new IndexWriter(ramDir, iwc);
        // add few more docs:
        for(int j = 0; j < RANDOM_MULTIPLIER * random.nextInt(30); j++) {
          addDoc(w, id++);
        }
        w.close(false);
      }
      
      // add dummy segments (which are all in current
      // version) to single segment index
      MergePolicy mp = random.nextBoolean() ? newLogMergePolicy() : newTieredMergePolicy();
      IndexWriterConfig iwc = new IndexWriterConfig(TEST_VERSION_CURRENT, null)
        .setMergePolicy(mp);
      IndexWriter w = new IndexWriter(dir, iwc);
      w.setInfoStream(VERBOSE ? System.out : null);
      w.addIndexes(ramDir);
      w.close(false);
      
      // determine count of segments in modified index
      final int origSegCount = getNumberOfSegments(dir);
      
      new IndexUpgrader(dir, newIndexWriterConfig(TEST_VERSION_CURRENT, null), VERBOSE ? System.out : null, false)
        .upgrade();

      final int segCount = checkAllSegmentsUpgraded(dir);
      assertEquals("Index must still contain the same number of segments, as only one segment was upgraded and nothing else merged",
        origSegCount, segCount);
      
      dir.close();
      _TestUtil.rmDir(oldIndxeDir);
    }
  }

}
