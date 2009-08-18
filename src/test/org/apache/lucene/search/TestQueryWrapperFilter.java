package org.apache.lucene.search;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;

public class TestQueryWrapperFilter extends LuceneTestCase {

  public void testBasic() throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true,
        IndexWriter.MaxFieldLength.LIMITED);
    Document doc = new Document();
    doc.add(new Field("field", "value", Store.NO, Index.ANALYZED));
    writer.addDocument(doc);
    writer.close();

    TermQuery termQuery = new TermQuery(new Term("field", "value"));

    // should not throw exception with primitive query
    QueryWrapperFilter qwf = new QueryWrapperFilter(termQuery);

    IndexSearcher searcher = new IndexSearcher(dir, true);
    TopDocs hits = searcher.search(new MatchAllDocsQuery(), qwf, 10);
    assertEquals(1, hits.totalHits);

    // should not throw exception with complex primitive query
    BooleanQuery booleanQuery = new BooleanQuery();
    booleanQuery.add(termQuery, Occur.MUST);
    booleanQuery.add(new TermQuery(new Term("field", "missing")),
        Occur.MUST_NOT);
    qwf = new QueryWrapperFilter(termQuery);

    hits = searcher.search(new MatchAllDocsQuery(), qwf, 10);
    assertEquals(1, hits.totalHits);

    // should not throw exception with non primitive Query (doesn't implement
    // Query#createWeight)
    qwf = new QueryWrapperFilter(new FuzzyQuery(new Term("field", "valu")));

    hits = searcher.search(new MatchAllDocsQuery(), qwf, 10);
    assertEquals(1, hits.totalHits);

  }
}
