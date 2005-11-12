package org.apache.lucene.search.regex;

import junit.framework.TestCase;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.Query;

public class TestRegexQuery extends TestCase {
  public void testRegex() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, new SimpleAnalyzer(), true);
    Document doc = new Document();
    doc.add(new Field("field", "the quick brown fox jumps over the lazy dog", Field.Store.NO, Field.Index.TOKENIZED));
    writer.addDocument(doc);
    writer.optimize();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(directory);
    Query query = new RegexQuery(new Term("field", "q.[aeiou]c.*"));
    Hits hits = searcher.search(query);
    assertEquals(1, hits.length());
  }
}

