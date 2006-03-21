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
    public void testDocCount() throws IOException
    {
        Directory dir = new RAMDirectory();

        IndexWriter writer = null;
        IndexReader reader = null;
        int i;

        writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);

        // add 100 documents
        for (i = 0; i < 100; i++) {
            addDoc(writer);
        }
        assertEquals(100, writer.docCount());
        writer.close();

        // delete 40 documents
        reader = IndexReader.open(dir);
        for (i = 0; i < 40; i++) {
            reader.deleteDocument(i);
        }
        reader.close();

        // test doc count before segments are merged/index is optimized
        writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
        assertEquals(100, writer.docCount());
        writer.close();

        reader = IndexReader.open(dir);
        assertEquals(100, reader.maxDoc());
        assertEquals(60, reader.numDocs());
        reader.close();

        // optimize the index and check that the new doc count is correct
        writer = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
        writer.optimize();
        assertEquals(60, writer.docCount());
        writer.close();

        // check that the index reader gives the same numbers.
        reader = IndexReader.open(dir);
        assertEquals(60, reader.maxDoc());
        assertEquals(60, reader.numDocs());
        reader.close();
    }

    private void addDoc(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(new Field("content", "aaa", Field.Store.NO, Field.Index.TOKENIZED));
        writer.addDocument(doc);
    }
}
