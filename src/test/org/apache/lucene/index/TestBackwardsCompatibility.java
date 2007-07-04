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

import junit.framework.TestCase;
import java.util.Vector;
import java.util.Arrays;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.File;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import java.io.*;
import java.util.*;
import java.util.zip.*;

/*
  Verify we can read the pre-2.1 file format, do searches
  against it, and add documents to it.
*/

public class TestBackwardsCompatibility extends TestCase
{

  // Uncomment these cases & run in a pre-lockless checkout
  // to create indices:

  /*
  public void testCreatePreLocklessCFS() throws IOException {
    createIndex("src/test/org/apache/lucene/index/index.prelockless.cfs", true);
  }

  public void testCreatePreLocklessNoCFS() throws IOException {
    createIndex("src/test/org/apache/lucene/index/index.prelockless.nocfs", false);
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

  final String[] oldNames = {"prelockless.cfs",
                             "prelockless.nocfs",
                             "presharedstores.cfs",
                             "presharedstores.nocfs"};

  public void testSearchOldIndex() throws IOException {
    for(int i=0;i<oldNames.length;i++) {
      String dirName = "src/test/org/apache/lucene/index/index." + oldNames[i];
      unzip(dirName, oldNames[i]);
      searchIndex(oldNames[i]);
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

  public void searchIndex(String dirName) throws IOException {
    //QueryParser parser = new QueryParser("contents", new WhitespaceAnalyzer());
    //Query query = parser.parse("handle:1");

    dirName = fullDir(dirName);

    Directory dir = FSDirectory.getDirectory(dirName);
    IndexSearcher searcher = new IndexSearcher(dir);
    
    Hits hits = searcher.search(new TermQuery(new Term("content", "aaa")));
    assertEquals(34, hits.length());
    Document d = hits.doc(0);

    // First document should be #21 since it's norm was increased:
    assertEquals("didn't get the right document first", "21", d.get("id"));

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
    Hits hits = searcher.search(new TermQuery(new Term("content", "aaa")));
    assertEquals("wrong number of hits", 44, hits.length());
    Document d = hits.doc(0);
    assertEquals("wrong first document", "21", d.get("id"));
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
    hits = searcher.search(new TermQuery(new Term("content", "aaa")));
    assertEquals("wrong number of hits", 43, hits.length());
    d = hits.doc(0);
    assertEquals("wrong first document", "22", d.get("id"));
    searcher.close();

    // optimize
    writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false);
    writer.optimize();
    writer.close();

    searcher = new IndexSearcher(dir);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")));
    assertEquals("wrong number of hits", 43, hits.length());
    d = hits.doc(0);
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
    Hits hits = searcher.search(new TermQuery(new Term("content", "aaa")));
    assertEquals("wrong number of hits", 34, hits.length());
    Document d = hits.doc(0);
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
    hits = searcher.search(new TermQuery(new Term("content", "aaa")));
    assertEquals("wrong number of hits", 33, hits.length());
    d = hits.doc(0);
    assertEquals("wrong first document", "22", d.get("id"));
    searcher.close();

    // optimize
    IndexWriter writer = new IndexWriter(dir, autoCommit, new WhitespaceAnalyzer(), false);
    writer.optimize();
    writer.close();

    searcher = new IndexSearcher(dir);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")));
    assertEquals("wrong number of hits", 33, hits.length());
    d = hits.doc(0);
    assertEquals("wrong first document", "22", d.get("id"));
    searcher.close();

    dir.close();
  }

  public void createIndex(String dirName, boolean doCFS) throws IOException {

    rmDir(dirName);

    dirName = fullDir(dirName);

    Directory dir = FSDirectory.getDirectory(dirName);
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
    writer.setUseCompoundFile(doCFS);
    
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
        //IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
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
                    "segments_4",
                    "segments.gen"};

        if (!autoCommit)
          expected[3] = "segments_3";

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
    doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.TOKENIZED));
    doc.add(new Field("id", Integer.toString(id), Field.Store.YES, Field.Index.UN_TOKENIZED));
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
