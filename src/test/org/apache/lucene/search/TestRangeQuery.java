package org.apache.lucene.search;

import java.io.IOException;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import junit.framework.TestCase;

/**
 *
 * @author goller
 */
public class TestRangeQuery extends TestCase {

    private int docCount = 0;

    public TestRangeQuery() {
        super();
    }

    public void testNotInclusive()
    {
        Directory dir = new RAMDirectory();
        IndexWriter writer = null;
        Searcher searcher = null;
        Query query = new RangeQuery(new Term("content", "A"), new Term("content", "C"), false);
        Hits hits = null;

        try {

          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
          addDoc(writer, "A");
          addDoc(writer, "B");
          addDoc(writer, "C");
          addDoc(writer, "D");
          writer.close();

          searcher = new IndexSearcher(dir);
          hits = searcher.search(query);
          assertEquals(1, hits.length());
          searcher.close();

          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
          addDoc(writer, "A");
          addDoc(writer, "B");
          addDoc(writer, "D");
          writer.close();

          searcher = new IndexSearcher(dir);
          hits = searcher.search(query);
          assertEquals(1, hits.length());
          searcher.close();

          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
          addDoc(writer, "C");
          writer.close();

          searcher = new IndexSearcher(dir);
          hits = searcher.search(query);
          assertEquals(1, hits.length());
          searcher.close();

        }
        catch (IOException e) {
          e.printStackTrace();
        }

    }

    public void testInclusive()
    {
        Directory dir = new RAMDirectory();
        IndexWriter writer = null;
        Searcher searcher = null;
        Query query = new RangeQuery(new Term("content", "A"), new Term("content", "C"), true);
        Hits hits = null;

        try {

          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
          addDoc(writer, "A");
          addDoc(writer, "B");
          addDoc(writer, "C");
          addDoc(writer, "D");
          writer.close();

          searcher = new IndexSearcher(dir);
          hits = searcher.search(query);
          assertEquals(3, hits.length());
          searcher.close();

          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
          addDoc(writer, "A");
          addDoc(writer, "B");
          addDoc(writer, "D");
          writer.close();

          searcher = new IndexSearcher(dir);
          hits = searcher.search(query);
          assertEquals(2, hits.length());
          searcher.close();

          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
          addDoc(writer, "C");
          writer.close();

          searcher = new IndexSearcher(dir);
          hits = searcher.search(query);
          assertEquals(3, hits.length());
          searcher.close();

        }
        catch (IOException e) {
          e.printStackTrace();
        }
    }

    private void addDoc(IndexWriter writer, String content)
    {
      Document doc = new Document();

      doc.add(Field.Keyword("id","id" + docCount));
      doc.add(Field.UnStored("content", content));

      try {
        writer.addDocument(doc);
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      docCount++;
    }
}

