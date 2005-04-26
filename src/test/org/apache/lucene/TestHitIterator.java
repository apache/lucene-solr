package org.apache.lucene;

import junit.framework.TestCase;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.Hit;
import org.apache.lucene.search.HitIterator;

/**
 * This test intentionally not put in the search package in order
 * to test HitIterator and Hit package protection.
 */
public class TestHitIterator extends TestCase {
  public void testIterator() throws Exception {
    RAMDirectory directory = new RAMDirectory();

    IndexWriter writer = new IndexWriter(directory, new WhitespaceAnalyzer(), true);
    Document doc = new Document();
    doc.add(new Field("field", "iterator test doc 1", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc);

    doc = new Document();
    doc.add(new Field("field", "iterator test doc 2", Field.Store.YES, Field.Index.TOKENIZED));
    writer.addDocument(doc);

    writer.close();

    IndexSearcher searcher = new IndexSearcher(directory);
    Hits hits = searcher.search(new TermQuery(new Term("field", "iterator")));

    HitIterator iterator = (HitIterator) hits.iterator();
    assertEquals(2, iterator.length());
    assertTrue(iterator.hasNext());
    Hit hit = (Hit) iterator.next();
    assertEquals("iterator test doc 1", hit.get("field"));

    assertTrue(iterator.hasNext());
    hit = (Hit) iterator.next();
    assertEquals("iterator test doc 2", hit.getDocument().get("field"));

    assertFalse(iterator.hasNext());
  }
}
