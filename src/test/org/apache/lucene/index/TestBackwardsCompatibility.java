package org.apache.lucene.index;

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
  Verify we can read the pre-XXX file format, do searches
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
  public void unzip(String dirName) throws IOException {
    rmDir(dirName);

    Enumeration entries;
    ZipFile zipFile;
    zipFile = new ZipFile(dirName + ".zip");

    entries = zipFile.entries();
    File fileDir = new File(dirName);
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

  public void testSearchOldIndex() throws IOException {
    String[] oldNames = {"prelockless.cfs", "prelockless.nocfs"};
    for(int i=0;i<oldNames.length;i++) {
      String dirName = "src/test/org/apache/lucene/index/index." + oldNames[i];
      unzip(dirName);
      searchIndex(dirName);
      rmDir(dirName);
    }
  }

  public void testIndexOldIndexNoAdds() throws IOException {
    String[] oldNames = {"prelockless.cfs", "prelockless.nocfs"};
    for(int i=0;i<oldNames.length;i++) {
      String dirName = "src/test/org/apache/lucene/index/index." + oldNames[i];
      unzip(dirName);
      changeIndexNoAdds(dirName);
      rmDir(dirName);
    }
  }

  public void testIndexOldIndex() throws IOException {
    String[] oldNames = {"prelockless.cfs", "prelockless.nocfs"};
    for(int i=0;i<oldNames.length;i++) {
      String dirName = "src/test/org/apache/lucene/index/index." + oldNames[i];
      unzip(dirName);
      changeIndexWithAdds(dirName);
      rmDir(dirName);
    }
  }

  public void searchIndex(String dirName) throws IOException {
    //QueryParser parser = new QueryParser("contents", new WhitespaceAnalyzer());
    //Query query = parser.parse("handle:1");

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
  public void changeIndexWithAdds(String dirName) throws IOException {

    Directory dir = FSDirectory.getDirectory(dirName);
    // open writer
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false);

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

    // make sure we can do another delete & another setNorm against this
    // pre-lockless segment:
    IndexReader reader = IndexReader.open(dir);
    Term searchTerm = new Term("id", "6");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("wrong delete count", 1, delCount);
    reader.setNorm(22, "content", (float) 2.0);
    reader.close();

    // make sure 2nd delete & 2nd norm "took":
    searcher = new IndexSearcher(dir);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")));
    assertEquals("wrong number of hits", 43, hits.length());
    d = hits.doc(0);
    assertEquals("wrong first document", "22", d.get("id"));
    searcher.close();

    // optimize
    writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
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
  public void changeIndexNoAdds(String dirName) throws IOException {

    Directory dir = FSDirectory.getDirectory(dirName);

    // make sure searching sees right # hits
    IndexSearcher searcher = new IndexSearcher(dir);
    Hits hits = searcher.search(new TermQuery(new Term("content", "aaa")));
    assertEquals("wrong number of hits", 34, hits.length());
    Document d = hits.doc(0);
    assertEquals("wrong first document", "21", d.get("id"));
    searcher.close();

    // make sure we can do another delete & another setNorm against this
    // pre-lockless segment:
    IndexReader reader = IndexReader.open(dir);
    Term searchTerm = new Term("id", "6");
    int delCount = reader.deleteDocuments(searchTerm);
    assertEquals("wrong delete count", 1, delCount);
    reader.setNorm(22, "content", (float) 2.0);
    reader.close();

    // make sure 2nd delete & 2nd norm "took":
    searcher = new IndexSearcher(dir);
    hits = searcher.search(new TermQuery(new Term("content", "aaa")));
    assertEquals("wrong number of hits", 33, hits.length());
    d = hits.doc(0);
    assertEquals("wrong first document", "22", d.get("id"));
    searcher.close();

    // optimize
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
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

  // disable until hardcoded file names are fixes:
  public void testExactFileNames() throws IOException {

    String outputDir = "lucene.backwardscompat0.index";
    Directory dir = FSDirectory.getDirectory(outputDir);
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
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
    CompoundFileReader cfsReader = new CompoundFileReader(dir, "_2.cfs");
    FieldInfos fieldInfos = new FieldInfos(cfsReader, "_2.fnm");
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
    String[] expected = {"_0.cfs",
                         "_0_1.del",
                         "_1.cfs",
                         "_2.cfs",
                         "_2_1.s" + contentFieldIndex,
                         "_3.cfs",
                         "segments_a",
                         "segments.gen"};

    String[] actual = dir.list();
    Arrays.sort(expected);
    Arrays.sort(actual);
    if (!Arrays.equals(expected, actual)) {
      fail("incorrect filenames in index: expected:\n    " + asString(expected) + "\n  actual:\n    " + asString(actual));
    }
    dir.close();

    rmDir(outputDir);
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

  private void rmDir(String dir) {
    File fileDir = new File(dir);
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
}
