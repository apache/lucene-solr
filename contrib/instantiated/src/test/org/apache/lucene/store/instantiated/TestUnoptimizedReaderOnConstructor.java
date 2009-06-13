package org.apache.lucene.store.instantiated;

import junit.framework.TestCase;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

/**
 * @author kalle
 * @since 2009-mar-30 13:15:49
 */
public class TestUnoptimizedReaderOnConstructor extends TestCase {

  public void test() throws Exception {
    Directory dir = new RAMDirectory();
    IndexWriter iw = new IndexWriter(dir, new WhitespaceAnalyzer(), true, IndexWriter.MaxFieldLength.UNLIMITED);
    addDocument(iw, "Hello, world!");
    addDocument(iw, "All work and no play makes jack a dull boy");
    iw.commit("a");
    iw.close();

    iw = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.UNLIMITED);
    addDocument(iw, "Hello, tellus!");
    addDocument(iw, "All work and no play makes danny a dull boy");
    iw.commit("b");
    iw.close();

    iw = new IndexWriter(dir, new WhitespaceAnalyzer(), false, IndexWriter.MaxFieldLength.UNLIMITED);
    addDocument(iw, "Hello, earth!");
    addDocument(iw, "All work and no play makes wendy a dull girl");
    iw.commit("c");
    iw.close();

    IndexReader unoptimizedReader = IndexReader.open(dir);
    unoptimizedReader.deleteDocument(2);

    InstantiatedIndex ii = new InstantiatedIndex(unoptimizedReader);

  }

  private void addDocument(IndexWriter iw, String text) throws IOException {
    Document doc = new Document();
    doc.add(new Field("field", text, Field.Store.NO, Field.Index.ANALYZED));
    iw.addDocument(doc);
  }
}
