package org.apache.lucene.index;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockRAMDirectory;
import org.apache.lucene.store.RAMDirectory;

public class TestNewIndexModifierDelete extends TestCase {

  // test the simple case
  public void testSimpleCase() throws IOException {
    String[] keywords = { "1", "2" };
    String[] unindexed = { "Netherlands", "Italy" };
    String[] unstored = { "Amsterdam has lots of bridges",
        "Venice has lots of canals" };
    String[] text = { "Amsterdam", "Venice" };

    Directory dir = new RAMDirectory();
    NewIndexModifier modifier = new NewIndexModifier(dir,
        new WhitespaceAnalyzer(), true);
    modifier.setUseCompoundFile(true);
    modifier.setMaxBufferedDeleteTerms(1);

    for (int i = 0; i < keywords.length; i++) {
      Document doc = new Document();
      doc.add(new Field("id", keywords[i], Field.Store.YES,
          Field.Index.UN_TOKENIZED));
      doc.add(new Field("country", unindexed[i], Field.Store.YES,
          Field.Index.NO));
      doc.add(new Field("contents", unstored[i], Field.Store.NO,
          Field.Index.TOKENIZED));
      doc
          .add(new Field("city", text[i], Field.Store.YES,
              Field.Index.TOKENIZED));
      modifier.addDocument(doc);
    }
    modifier.optimize();

    Term term = new Term("city", "Amsterdam");
    int hitCount = getHitCount(dir, term);
    assertEquals(1, hitCount);
    modifier.deleteDocuments(term);
    hitCount = getHitCount(dir, term);
    assertEquals(0, hitCount);

    modifier.close();
  }

  // test when delete terms only apply to disk segments
  public void testNonRAMDelete() throws IOException {
    Directory dir = new RAMDirectory();
    NewIndexModifier modifier = new NewIndexModifier(dir,
        new WhitespaceAnalyzer(), true);
    modifier.setMaxBufferedDocs(2);
    modifier.setMaxBufferedDeleteTerms(2);

    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.flush();

    assertEquals(0, modifier.getRAMSegmentCount());
    assertTrue(0 < modifier.getSegmentCount());

    IndexReader reader = IndexReader.open(dir);
    assertEquals(7, reader.numDocs());
    reader.close();

    modifier.deleteDocuments(new Term("value", String.valueOf(value)));
    modifier.deleteDocuments(new Term("value", String.valueOf(value)));

    reader = IndexReader.open(dir);
    assertEquals(0, reader.numDocs());
    reader.close();

    modifier.close();
  }

  // test when delete terms only apply to ram segments
  public void testRAMDeletes() throws IOException {
    Directory dir = new RAMDirectory();
    NewIndexModifier modifier = new NewIndexModifier(dir,
        new WhitespaceAnalyzer(), true);
    modifier.setMaxBufferedDocs(4);
    modifier.setMaxBufferedDeleteTerms(4);

    int id = 0;
    int value = 100;

    addDoc(modifier, ++id, value);
    modifier.deleteDocuments(new Term("value", String.valueOf(value)));
    addDoc(modifier, ++id, value);
    modifier.deleteDocuments(new Term("value", String.valueOf(value)));

    assertEquals(2, modifier.getNumBufferedDeleteTerms());
    assertEquals(1, modifier.getBufferedDeleteTermsSize());

    addDoc(modifier, ++id, value);
    assertEquals(0, modifier.getSegmentCount());
    modifier.flush();

    IndexReader reader = IndexReader.open(dir);
    assertEquals(1, reader.numDocs());

    int hitCount = getHitCount(dir, new Term("id", String.valueOf(id)));
    assertEquals(1, hitCount);
    reader.close();

    modifier.close();
  }

  // test when delete terms apply to both disk and ram segments
  public void testBothDeletes() throws IOException {
    Directory dir = new RAMDirectory();
    NewIndexModifier modifier = new NewIndexModifier(dir,
        new WhitespaceAnalyzer(), true);
    modifier.setMaxBufferedDocs(100);
    modifier.setMaxBufferedDeleteTerms(100);

    int id = 0;
    int value = 100;

    for (int i = 0; i < 5; i++) {
      addDoc(modifier, ++id, value);
    }

    value = 200;
    for (int i = 0; i < 5; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.flush();

    for (int i = 0; i < 5; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.deleteDocuments(new Term("value", String.valueOf(value)));
    modifier.flush();

    IndexReader reader = IndexReader.open(dir);
    assertEquals(5, reader.numDocs());

    modifier.close();
  }

  // test that batched delete terms are flushed together
  public void testBatchDeletes() throws IOException {
    Directory dir = new RAMDirectory();
    NewIndexModifier modifier = new NewIndexModifier(dir,
        new WhitespaceAnalyzer(), true);
    modifier.setMaxBufferedDocs(2);
    modifier.setMaxBufferedDeleteTerms(2);

    int id = 0;
    int value = 100;

    for (int i = 0; i < 7; i++) {
      addDoc(modifier, ++id, value);
    }
    modifier.flush();

    IndexReader reader = IndexReader.open(dir);
    assertEquals(7, reader.numDocs());
    reader.close();

    id = 0;
    modifier.deleteDocuments(new Term("id", String.valueOf(++id)));
    modifier.deleteDocuments(new Term("id", String.valueOf(++id)));

    reader = IndexReader.open(dir);
    assertEquals(5, reader.numDocs());
    reader.close();

    Term[] terms = new Term[3];
    for (int i = 0; i < terms.length; i++) {
      terms[i] = new Term("id", String.valueOf(++id));
    }
    modifier.deleteDocuments(terms);

    reader = IndexReader.open(dir);
    assertEquals(2, reader.numDocs());
    reader.close();

    modifier.close();
  }

  private void addDoc(NewIndexModifier modifier, int id, int value)
      throws IOException {
    Document doc = new Document();
    doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.TOKENIZED));
    doc.add(new Field("id", String.valueOf(id), Field.Store.YES,
        Field.Index.UN_TOKENIZED));
    doc.add(new Field("value", String.valueOf(value), Field.Store.NO,
        Field.Index.UN_TOKENIZED));
    modifier.addDocument(doc);
  }

  private int getHitCount(Directory dir, Term term) throws IOException {
    IndexSearcher searcher = new IndexSearcher(dir);
    int hitCount = searcher.search(new TermQuery(term)).length();
    searcher.close();
    return hitCount;
  }

  public void testDeletesOnDiskFull() throws IOException {
    testOperationsOnDiskFull(false);
  }

  public void testUpdatesOnDiskFull() throws IOException {
    testOperationsOnDiskFull(true);
  }

  /**
   * Make sure if modifier tries to commit but hits disk full that modifier
   * remains consistent and usable. Similar to TestIndexReader.testDiskFull().
   */
  private void testOperationsOnDiskFull(boolean updates) throws IOException {

    boolean debug = false;
    Term searchTerm = new Term("content", "aaa");
    int START_COUNT = 157;
    int END_COUNT = 144;

    // First build up a starting index:
    RAMDirectory startDir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(startDir, new WhitespaceAnalyzer(),
        true);
    for (int i = 0; i < 157; i++) {
      Document d = new Document();
      d.add(new Field("id", Integer.toString(i), Field.Store.YES,
          Field.Index.UN_TOKENIZED));
      d.add(new Field("content", "aaa " + i, Field.Store.NO,
          Field.Index.TOKENIZED));
      writer.addDocument(d);
    }
    writer.close();

    long diskUsage = startDir.sizeInBytes();
    long diskFree = diskUsage + 10;

    IOException err = null;

    boolean done = false;

    // Iterate w/ ever increasing free disk space:
    while (!done) {
      MockRAMDirectory dir = new MockRAMDirectory(startDir);
      NewIndexModifier modifier = new NewIndexModifier(dir,
          new WhitespaceAnalyzer(), false);

      modifier.setMaxBufferedDocs(1000); // use flush or close
      modifier.setMaxBufferedDeleteTerms(1000); // use flush or close

      // For each disk size, first try to commit against
      // dir that will hit random IOExceptions & disk
      // full; after, give it infinite disk space & turn
      // off random IOExceptions & retry w/ same reader:
      boolean success = false;

      for (int x = 0; x < 2; x++) {

        double rate = 0.1;
        double diskRatio = ((double)diskFree) / diskUsage;
        long thisDiskFree;
        String testName;

        if (0 == x) {
          thisDiskFree = diskFree;
          if (diskRatio >= 2.0) {
            rate /= 2;
          }
          if (diskRatio >= 4.0) {
            rate /= 2;
          }
          if (diskRatio >= 6.0) {
            rate = 0.0;
          }
          if (debug) {
            System.out.println("\ncycle: " + diskFree + " bytes");
          }
          testName = "disk full during reader.close() @ " + thisDiskFree
              + " bytes";
        } else {
          thisDiskFree = 0;
          rate = 0.0;
          if (debug) {
            System.out.println("\ncycle: same writer: unlimited disk space");
          }
          testName = "reader re-use after disk full";
        }

        dir.setMaxSizeInBytes(thisDiskFree);
        dir.setRandomIOExceptionRate(rate, diskFree);

        try {
          if (0 == x) {
            int docId = 12;
            for (int i = 0; i < 13; i++) {
              if (updates) {
                Document d = new Document();
                d.add(new Field("id", Integer.toString(i), Field.Store.YES,
                    Field.Index.UN_TOKENIZED));
                d.add(new Field("content", "bbb " + i, Field.Store.NO,
                    Field.Index.TOKENIZED));
                modifier.updateDocument(
                    new Term("id", Integer.toString(docId)), d);
              } else { // deletes
                modifier
                    .deleteDocuments(new Term("id", Integer.toString(docId)));
                // modifier.setNorm(docId, "contents", (float)2.0);
              }
              docId += 12;
            }
          }
          modifier.close();
          success = true;
          if (0 == x) {
            done = true;
          }
        }
        catch (IOException e) {
          if (debug) {
            System.out.println("  hit IOException: " + e);
          }
          err = e;
          if (1 == x) {
            e.printStackTrace();
            fail(testName + " hit IOException after disk space was freed up");
          }
        }

        // Whether we succeeded or failed, check that all
        // un-referenced files were in fact deleted (ie,
        // we did not create garbage). Just create a
        // new IndexFileDeleter, have it delete
        // unreferenced files, then verify that in fact
        // no files were deleted:
        String[] startFiles = dir.list();
        SegmentInfos infos = new SegmentInfos();
        infos.read(dir);
        IndexFileDeleter d = new IndexFileDeleter(infos, dir);
        d.findDeletableFiles();
        d.deleteFiles();
        String[] endFiles = dir.list();

        Arrays.sort(startFiles);
        Arrays.sort(endFiles);

        // for(int i=0;i<startFiles.length;i++) {
        // System.out.println(" startFiles: " + i + ": " + startFiles[i]);
        // }

        if (!Arrays.equals(startFiles, endFiles)) {
          String successStr;
          if (success) {
            successStr = "success";
          } else {
            successStr = "IOException";
            err.printStackTrace();
          }
          fail("reader.close() failed to delete unreferenced files after "
              + successStr + " (" + diskFree + " bytes): before delete:\n    "
              + arrayToString(startFiles) + "\n  after delete:\n    "
              + arrayToString(endFiles));
        }

        // Finally, verify index is not corrupt, and, if
        // we succeeded, we see all docs changed, and if
        // we failed, we see either all docs or no docs
        // changed (transactional semantics):
        IndexReader newReader = null;
        try {
          newReader = IndexReader.open(dir);
        }
        catch (IOException e) {
          e.printStackTrace();
          fail(testName
              + ":exception when creating IndexReader after disk full during close: "
              + e);
        }

        IndexSearcher searcher = new IndexSearcher(newReader);
        Hits hits = null;
        try {
          hits = searcher.search(new TermQuery(searchTerm));
        }
        catch (IOException e) {
          e.printStackTrace();
          fail(testName + ": exception when searching: " + e);
        }
        int result2 = hits.length();
        if (success) {
          if (result2 != END_COUNT) {
            fail(testName
                + ": method did not throw exception but hits.length for search on term 'aaa' is "
                + result2 + " instead of expected " + END_COUNT);
          }
        } else {
          // On hitting exception we still may have added
          // all docs:
          if (result2 != START_COUNT && result2 != END_COUNT) {
            err.printStackTrace();
            fail(testName
                + ": method did throw exception but hits.length for search on term 'aaa' is "
                + result2 + " instead of expected " + START_COUNT);
          }
        }

        searcher.close();
        newReader.close();

        if (result2 == END_COUNT) {
          break;
        }
      }

      dir.close();

      // Try again with 10 more bytes of free space:
      diskFree += 10;
    }
  }

  private String arrayToString(String[] l) {
    String s = "";
    for (int i = 0; i < l.length; i++) {
      if (i > 0) {
        s += "\n    ";
      }
      s += l[i];
    }
    return s;
  }
}
