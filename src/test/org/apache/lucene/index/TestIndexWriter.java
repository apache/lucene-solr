package org.apache.lucene.index;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;


/**
 *
 * @author goller
 */
public class TestIndexWriter extends TestCase
{
    private int docCount = 0;

    public void testDocCount()
    {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;
        IndexReader reader = null;
        int i;

        try {
          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);

          // add 100 documents
          for (i = 0; i < 100; i++) {
              addDoc(writer);
          }
          assertEquals(100, writer.docCount());
          writer.close();

          // delete 50 documents
          reader = IndexReader.open(dir);
          for (i = 0; i < 50; i++) {
              reader.delete(i);
          }
          reader.close();

          writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
          assertEquals(50, writer.docCount());
          writer.optimize();
          assertEquals(50, writer.docCount());
          writer.close();
        }
        catch (IOException e) {
          e.printStackTrace();
        }
    }

    private void addDoc(IndexWriter writer)
    {
        Document doc = new Document();

        doc.add(Field.Keyword("id","id" + docCount));
        doc.add(Field.UnStored("content","aaa"));

        try {
          writer.addDocument(doc);
        }
        catch (IOException e) {
          e.printStackTrace();
        }
        docCount++;
    }
}
