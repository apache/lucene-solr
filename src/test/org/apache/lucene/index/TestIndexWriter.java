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
 * @author goller
 * @version $Id$
 */
public class TestIndexWriter extends TestCase
{
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
            assertEquals(100, writer.docCount());
            writer.close();

            reader = IndexReader.open(dir);
            assertEquals(100, reader.maxDoc());
            assertEquals(50, reader.numDocs());
            reader.close();

            writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
            writer.optimize();
            assertEquals(50, writer.docCount());
            writer.close();

            reader = IndexReader.open(dir);
            assertEquals(50, reader.maxDoc());
            assertEquals(50, reader.numDocs());
            reader.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addDoc(IndexWriter writer)
    {
        Document doc = new Document();
        doc.add(Field.UnStored("content", "aaa"));

        try {
            writer.addDocument(doc);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
