package org.apache.lucene.analysis;

import junit.framework.TestCase;
import java.io.StringReader;
import java.util.ArrayList;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Hits;

public class TestStopAnalyzer extends TestCase {
  private StopAnalyzer stopAnalyzer = new StopAnalyzer();

  public Token[] tokensFromAnalyzer(Analyzer analyzer, String text)
                                                  throws Exception {
    TokenStream stream =
      analyzer.tokenStream("contents", new StringReader(text));
    ArrayList tokenList = new ArrayList();
    while (true) {
      Token token = stream.next();
      if (token == null) break;

      tokenList.add(token);
    }

    return (Token[]) tokenList.toArray(new Token[0]);
  }


  public void testPhraseQuery() throws Exception {
    RAMDirectory directory = new RAMDirectory();
    IndexWriter writer = new IndexWriter(directory, stopAnalyzer, true);
    Document doc = new Document();
    doc.add(Field.Text("field", "the stop words are here"));
    writer.addDocument(doc);
    writer.close();

    IndexSearcher searcher = new IndexSearcher(directory);

    // valid exact phrase query
    PhraseQuery query = new PhraseQuery();
    query.add(new Term("field","stop"));
    query.add(new Term("field","words"));
    Hits hits = searcher.search(query);
    assertEquals(1, hits.length());

    // currently StopAnalyzer does not leave "holes", so this matches.
    query = new PhraseQuery();
    query.add(new Term("field", "words"));
    query.add(new Term("field", "here"));
    hits = searcher.search(query);
    assertEquals(1, hits.length());

    searcher.close();
  }
}
