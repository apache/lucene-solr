package org.apache.lucene.index;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 * @author goller
 */
public class TestSegmentTermEnum extends TestCase
{
  Directory dir = new RAMDirectory();

  public void testTermEnum()
  {
    IndexWriter writer = null;

    try {
      writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);

      // add 100 documents with term : aaa
      // add 100 documents with terms: aaa bbb
      // Therefore, term 'aaa' has document frequency of 200 and term 'bbb' 100
      for (int i = 0; i < 100; i++) {
        addDoc(writer, "aaa");
        addDoc(writer, "aaa bbb");
      }

      writer.close();
    }
    catch (IOException e) {
      e.printStackTrace();
    }

    try {
      // verify document frequency of terms in an unoptimized index
      verifyDocFreq();

      // merge segments by optimizing the index
      writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
      writer.optimize();
      writer.close();

      // verify document frequency of terms in an optimized index
      verifyDocFreq();
    }
    catch (IOException e2) {
      e2.printStackTrace();
    }
  }

  private void verifyDocFreq()
      throws IOException
  {
      IndexReader reader = IndexReader.open(dir);
      TermEnum enum = null;

    // create enumeration of all terms
    enum = reader.terms();
    // go to the first term (aaa)
    enum.next();
    // assert that term is 'aaa'
    assertEquals("aaa", enum.term().text());
    assertEquals(200, enum.docFreq());
    // go to the second term (bbb)
    enum.next();
    // assert that term is 'bbb'
    assertEquals("bbb", enum.term().text());
    assertEquals(100, enum.docFreq());

    enum.close();


    // create enumeration of terms after term 'aaa', including 'aaa'
    enum = reader.terms(new Term("content", "aaa"));
    // assert that term is 'aaa'
    assertEquals("aaa", enum.term().text());
    assertEquals(200, enum.docFreq());
    // go to term 'bbb'
    enum.next();
    // assert that term is 'bbb'
    assertEquals("bbb", enum.term().text());
    assertEquals(100, enum.docFreq());

    enum.close();
  }

  private void addDoc(IndexWriter writer, String value)
  {
    Document doc = new Document();
    doc.add(Field.UnStored("content", value));

    try {
      writer.addDocument(doc);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }
}
