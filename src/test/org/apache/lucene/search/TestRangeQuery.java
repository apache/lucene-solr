package org.apache.lucene.search;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

import junit.framework.TestCase;
import java.io.IOException;

/**
 * @author goller
 */
public class TestRangeQuery extends TestCase {

  private int docCount = 0;
  private RAMDirectory dir;

  public void setUp() {
    dir = new RAMDirectory();
  }

  public void testExclusive() throws Exception {
    Query query = new RangeQuery(new Term("content", "A"),
                                 new Term("content", "C"),
                                 false);
    initializeIndex(new String[] {"A", "B", "C", "D"});
    IndexSearcher searcher = new IndexSearcher(dir);
    Hits hits = searcher.search(query);
    assertEquals("A,B,C,D, only B in range", 1, hits.length());
    searcher.close();

    initializeIndex(new String[] {"A", "B", "D"});
    searcher = new IndexSearcher(dir);
    hits = searcher.search(query);
    assertEquals("A,B,D, only B in range", 1, hits.length());
    searcher.close();

    addDoc("C");
    searcher = new IndexSearcher(dir);
    hits = searcher.search(query);
    assertEquals("C added, still only B in range", 1, hits.length());
    searcher.close();
  }

  public void testInclusive() throws Exception {
    Query query = new RangeQuery(new Term("content", "A"),
                                 new Term("content", "C"),
                                 true);

    initializeIndex(new String[]{"A", "B", "C", "D"});
    IndexSearcher searcher = new IndexSearcher(dir);
    Hits hits = searcher.search(query);
    assertEquals("A,B,C,D - A,B,C in range", 3, hits.length());
    searcher.close();

    initializeIndex(new String[]{"A", "B", "D"});
    searcher = new IndexSearcher(dir);
    hits = searcher.search(query);
    assertEquals("A,B,D - A and B in range", 2, hits.length());
    searcher.close();

    addDoc("C");
    searcher = new IndexSearcher(dir);
    hits = searcher.search(query);
    assertEquals("C added - A, B, C in range", 3, hits.length());
    searcher.close();
  }

  private void initializeIndex(String[] values) throws IOException {
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
    for (int i = 0; i < values.length; i++) {
      insertDoc(writer, values[i]);
    }
    writer.close();
  }

  private void addDoc(String content) throws IOException {
    IndexWriter writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
    insertDoc(writer, content);
    writer.close();
  }

  private void insertDoc(IndexWriter writer, String content) throws IOException {
    Document doc = new Document();

    doc.add(Field.Keyword("id", "id" + docCount));
    doc.add(Field.UnStored("content", content));

    writer.addDocument(doc);
    docCount++;
  }
}

