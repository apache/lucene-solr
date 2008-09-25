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
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/*
  Verify we can read the pre-2.1 file format, do searches
  against it, and add documents to it.
*/

public class TestBackwardsCompatibility extends LuceneTestCase
{

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

  /* Unzips dirName + ".zip" --> dirName, removing dirName
     first */
  public void unzip(String zipName, String destDirName) throws IOException {

    Enumeration entries;
    ZipFile zipFile;
    zipFile = new ZipFile(zipName + ".zip");

    entries = zipFile.entries();

    String dirName = fullDir(destDirName);

    File fileDir = new File(dirName);
    rmDir(destDirName);

    fileDir.mkdir();

    while (entries.hasMoreElements()) {
      ZipEntry entry = (ZipEntry) entries.nextElement();

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
  };

  public void testOptimizeOldIndex() throws IOException {
    for(int i=0;i<oldNames.length;i++) {
      String dirName = "src/test/org/apache/lucene/index/index." + oldNames[i];
      unzip(dirName, oldNames[i]);
      String fullPath = fullDir(oldNames[i]);
      Directory dir = FSDirectory.getDirectory(fullPath);
      IndexWriter w = new IndexWriter(dir, new WhitespaceAnalyzer(), IndexWriter.MaxFieldLength.LIMITED);
      w.optimize();
      w.close();

      _TestUtil.checkIndex(dir);
      dir.close();
      rmDir(oldNames[i]);
    }
  }

  public void testSearchOldIndex() throws IOException {
    for(int i=0;i<oldNames.length;i++) {
      String dirName = "src/test/org/apache/lucene/index/index." + oldNames[i];
      unzip(dirName, oldNames[i]);
      searchIndex(oldNames[i], oldNames[i]);
      rmDir(oldNames[i]);
    }
  }

  public void testIndexOldIndexNoAdds() throws IOException {
    for(int i=0;i<oldNames.length;i++) {
      String dirName = "src/test/org/apache/lucene/index/index." + oldNames[i];
      unzip(dirName, oldNames[i]);
      changeIndexNoAdds(oldNames[i], true);
      rmDir(oldNames[i]);

      unzip(dirName, oldNames[i]);
      changeIndexNoAdds(oldNames[i], false);
      rmDir(oldNames[i]);
    }
  }

  public void testIndexOldIndex() throws IOException {
    for(int i=0;i<oldNames.length;i++) {
      String dirName = "src/test/org/apache/lucene/index/index." + oldNames[i];
      unzip(dirName, oldNames[i]);
      changeIndexWithAdds(oldNames[i], true);
      rmDir(oldNames[i]);

      unzip(dirName, oldNames[i]);
      changeIndexWithAdds(oldNames[i], false);
      rmDir(oldNames[i]);
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

  public void searchIndex(String dirName, String oldName) throws IOException {
    //QueryParser parser = new QueryParser("contents", new WhitespaceAnalyzer());
    //Query query = parser.parse("handle:1");

    dirName = fullDir(dirName);

    Directory dir = FSDirectory.getDirectory(dirName);
    IndexSearcher searcher = new IndexSearcher(dir);
    IndexReader reader = searcher.getIndexReader();

    _TestUtil.checkIndex(dir);

    for(int i=0;i<35;i++) {
      if (!reader.isDeleted(i)) {
        Document d = reader.document(i);
        List fields = d.getFields();
        if (oldName.startsWith("23.")) {
          assertEquals(4, fields.size());
          Field f = (Field) d.getField("id");
          assertEquals(""+i, f.stringValue());

          f = (Field) d.getField("utf8");
          assertEquals("Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", f.stringValue());

          f = (Field) d.getField("autf8");
          assertEquals("Lu\uD834\uDD1Ece\uD834\uDD60ne \u0000 \u2620 ab\ud917\udc17cd", f.stringValue());
        
          f = (Field) d.getField("content2");
          assertEquals("here is more content with aaa aaa aaa", f.stringValue());
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
    dir.close();
  }

  /* Open pre-lockless index, add docs, do a delete &
   * setNorm, and search */
  public void changeIndexWithAdds(String dirName, boolean autoCommit) throws IOException {

    dirName = fullDir(dirName);

    Directory dir = FSDirectory.getDirectory(dirName);

    // open writer
    IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false);

    // add 10 docs
    for(int i=0;i<10;i++) {
      addDoc(writer, 35+i);
    }

    // make sure writer sees right total -- writer seems not to know about deletes in .del?
    assertEquals("wrong doc count", 45, writer.docCount());
    writer.close();

    // make sure searching sees right # hits
    IndexSearcher searcher = new IndexSearcher(dir);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    Document d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "21", d.get("id"));
    testHits(hits, 44, searcher.getIndexReader());
    searcher.close();

    // make sure we can do delete & setNorm against this
    // pre-lockless segment:
    IndexReader reader = IndexReader.open(dir);
    Term searchTerm = new Term("id", "6");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("wrong delete count", 1, delCount);
    reader.setNorm(22, "content", (float) 2.0);
    reader.close();

    // make sure they "took":
    searcher = new IndexSearcher(dir);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 43, hits.length);
    d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "22", d.get("id"));
    testHits(hits, 43, searcher.getIndexReader());
    searcher.close();

    // optimize
    writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false);
    writer.optimize();
    writer.close();

    searcher = new IndexSearcher(dir);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 43, hits.length);
    d = searcher.doc(hits[0].doc);
    testHits(hits, 43, searcher.getIndexReader());
    assertEquals("wrong first document", "22", d.get("id"));
    searcher.close();

    dir.close();
  }

  /* Open pre-lockless index, add docs, do a delete &
   * setNorm, and search */
  public void changeIndexNoAdds(String dirName, boolean autoCommit) throws IOException {

    dirName = fullDir(dirName);

    Directory dir = FSDirectory.getDirectory(dirName);

    // make sure searching sees right # hits
    IndexSearcher searcher = new IndexSearcher(dir);
    ScoreDoc[] hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 34, hits.length);
    Document d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "21", d.get("id"));
    searcher.close();

    // make sure we can do a delete & setNorm against this
    // pre-lockless segment:
    IndexReader reader = IndexReader.open(dir);
    Term searchTerm = new Term("id", "6");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("wrong delete count", 1, delCount);
    reader.setNorm(22, "content", (float) 2.0);
    reader.close();

    // make sure they "took":
    searcher = new IndexSearcher(dir);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 33, hits.length);
    d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "22", d.get("id"));
    testHits(hits, 33, searcher.getIndexReader());
    searcher.close();

    // optimize
    IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false);
    writer.optimize();
    writer.close();

    searcher = new IndexSearcher(dir);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")), null, 1000).scoreDocs;
    assertEquals("wrong number of hits", 33, hits.length);
    d = searcher.doc(hits[0].doc);
    assertEquals("wrong first document", "22", d.get("id"));
    testHits(hits, 33, searcher.getIndexReader());
    searcher.close();

    dir.close();
  }

  public void createIndex(String dirName, boolean doCFS) throws IOException {

    rmDir(dirName);

    dirName = fullDir(dirName);

    Directory dir = FSDirectory.getDirectory(dirName);
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);
    writer.setUseCompoundFile(doCFS);
    writer.setMaxBufferedDocs(10);
    
    for(int i=0;i<35;i++) {
      addDoc(writer, i);
    }
    assertEquals("wrong doc count", 35, writer.docCount());
    writer.close();

    // Delete one doc so we get a .del file:
    IndexReader reader = IndexReader.open(dir);
    Term searchTerm = new Term("id", "7");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("didn't delete the right number of documents", 1, delCount);

    // Set one norm so we get a .s0 file:
    reader.setNorm(21, "content", (float) 1.5);
    reader.close();
  }

  /* Verifies that the expected file names were produced */

  public void testExactFileNames() throws IOException {

    for(int pass=0;pass<2;pass++) {

      String outputDir = "lucene.backwardscompat0.index";
      rmDir(outputDir);

      try {
        Directory dir = FSDirectory.getDirectory(fullDir(outputDir));

        boolean autoCommit = 0 == pass;
 
        IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), true);
        writer.setRAMBufferSizeMB(16.0);
        for(int i=0;i<35;i++) {
          addDoc(writer, i);
        }
        assertEquals("wrong doc count", 35, writer.docCount());
        writer.close();

        // Delete one doc so we get a .del file:
        IndexReader reader = IndexReader.open(dir);
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
        String[] expected;
        expected = new String[] {"_0.cfs",
                    "_0_1.del",
                    "_0_1.s" + contentFieldIndex,
                    "segments_3",
                    "segments.gen"};

        String[] actual = dir.list();
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
    return new File(System.getProperty("tempDir"), dirName).getCanonicalPath();
  }
}
