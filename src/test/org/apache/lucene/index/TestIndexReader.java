package org.apache.lucene.index;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001, 2002, 2003 The Apache Software Foundation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 */


import junit.framework.TestCase;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import java.util.Collection;
import java.io.IOException;

public class TestIndexReader extends TestCase
{
    /**
     * Tests the IndexReader.getFieldNames implementation
     * @throws Exception on error
     */
    public void testGetFieldNames() throws Exception
    {
        RAMDirectory d = new RAMDirectory();
        // set up writer
        IndexWriter writer = new IndexWriter(d, new StandardAnalyzer(), true);
        addDocumentWithFields(writer);
        writer.close();
        // set up reader
        IndexReader reader = IndexReader.open(d);
        Collection fieldNames = reader.getFieldNames();
        assertTrue(fieldNames.contains("keyword"));
        assertTrue(fieldNames.contains("text"));
        assertTrue(fieldNames.contains("unindexed"));
        assertTrue(fieldNames.contains("unstored"));
        // add more documents
        writer = new IndexWriter(d, new StandardAnalyzer(), false);
        // want to get some more segments here
        for (int i = 0; i < 5*writer.mergeFactor; i++)
        {
            addDocumentWithFields(writer);
        }
        // new fields are in some different segments (we hope)
        for (int i = 0; i < 5*writer.mergeFactor; i++)
        {
            addDocumentWithDifferentFields(writer);
        }
        writer.close();
        // verify fields again
        reader = IndexReader.open(d);
        fieldNames = reader.getFieldNames();
        assertTrue(fieldNames.contains("keyword"));
        assertTrue(fieldNames.contains("text"));
        assertTrue(fieldNames.contains("unindexed"));
        assertTrue(fieldNames.contains("unstored"));
        assertTrue(fieldNames.contains("keyword2"));
        assertTrue(fieldNames.contains("text2"));
        assertTrue(fieldNames.contains("unindexed2"));
        assertTrue(fieldNames.contains("unstored2"));

        // verify that only indexed fields were returned
        Collection indexedFieldNames = reader.getFieldNames(true);
        assertTrue(fieldNames.contains("keyword"));
        assertTrue(fieldNames.contains("text"));
        assertTrue(fieldNames.contains("unstored"));
        assertTrue(fieldNames.contains("keyword2"));
        assertTrue(fieldNames.contains("text2"));
        assertTrue(fieldNames.contains("unindexed2"));
        assertTrue(fieldNames.contains("unstored2"));

        // verify that only unindexed fields were returned
        Collection unindexedFieldNames = reader.getFieldNames(false);
        assertTrue(fieldNames.contains("unindexed"));
    }

    public void testDeleteReaderWriterConflict()
    {
        Directory dir = new RAMDirectory();
        IndexWriter writer = null;
        IndexReader reader = null;
        Searcher searcher = null;
        Term searchTerm = new Term("content", "aaa");
        Hits hits = null;

        try
        {
            //  add 100 documents with term : aaa
            writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
            for (int i = 0; i < 100; i++)
            {
                addDoc(writer, "aaa");
            }
            writer.close();
            reader = IndexReader.open(dir);

            //  add 100 documents with term : bbb
            writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), false);
            for (int i = 0; i < 100; i++)
            {
                addDoc(writer, "bbb");
            }
            writer.optimize();
            writer.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        try
        {
            // delete documents containing term: aaa
            reader.delete(searchTerm);
            reader.close();
        }
        catch (IOException e)
        {
            try
            {
                // if reader throws IOException try once more to delete documents with a new reader
                reader.close();
                reader = IndexReader.open(dir);
                reader.delete(searchTerm);
                reader.close();
            }
            catch (IOException e1)
            {
                e1.printStackTrace();
            }
        }

        try
        {
            searcher = new IndexSearcher(dir);
            hits = searcher.search(new TermQuery(searchTerm));
            assertEquals(0, hits.length());
            searcher.close();
        }
        catch (IOException e1)
        {
            e1.printStackTrace();
        }
    }

    public void testDeleteReaderReaderConflict()
    {
        Directory dir = new RAMDirectory();
        IndexWriter writer = null;
        IndexReader reader1 = null;
        IndexReader reader2 = null;
        Searcher searcher = null;
        Hits hits = null;
        Term searchTerm1 = new Term("content", "aaa");
        Term searchTerm2 = new Term("content", "bbb");

        try
        {
            //  add 100 documents with term : aaa
            //  add 100 documents with term : bbb
            //  add 100 documents with term : ccc
            writer  = new IndexWriter(dir, new WhitespaceAnalyzer(), true);
            for (int i = 0; i < 100; i++)
            {
                addDoc(writer, "aaa");
                addDoc(writer, "bbb");
                addDoc(writer, "ccc");
            }
            writer.optimize();
            writer.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        try
        {
            reader1 = IndexReader.open(dir);
            reader2 = IndexReader.open(dir);

            // delete documents containing term: aaa
            reader2.delete(searchTerm1);
            reader2.close();

            // delete documents containing term: bbb
            reader1.delete(searchTerm2);
            reader1.close();
        }
        catch (IOException e)
        {
            try
            {
                // if reader throws IOException try once more to delete documents with a new reader
                reader1.close();
                reader1 = IndexReader.open(dir);
                reader1.delete(searchTerm2);
                reader1.close();
            }
            catch (IOException e1)
            {
                e1.printStackTrace();
            }
        }

        try
        {
            searcher = new IndexSearcher(dir);
            hits = searcher.search(new TermQuery(searchTerm1));
            assertEquals(0, hits.length());
            hits = searcher.search(new TermQuery(searchTerm2));
            assertEquals(0, hits.length());
            searcher.close();
        }
        catch (IOException e1)
        {
            e1.printStackTrace();
        }
    }


    private void addDocumentWithFields(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(Field.Keyword("keyword","test1"));
        doc.add(Field.Text("text","test1"));
        doc.add(Field.UnIndexed("unindexed","test1"));
        doc.add(Field.UnStored("unstored","test1"));
        writer.addDocument(doc);
    }

    private void addDocumentWithDifferentFields(IndexWriter writer) throws IOException
    {
        Document doc = new Document();
        doc.add(Field.Keyword("keyword2","test1"));
        doc.add(Field.Text("text2","test1"));
        doc.add(Field.UnIndexed("unindexed2","test1"));
        doc.add(Field.UnStored("unstored2","test1"));
        writer.addDocument(doc);
    }

    private void addDoc(IndexWriter writer, String value)
    {
        Document doc = new Document();
        doc.add(Field.UnStored("content", value));

        try
        {
            writer.addDocument(doc);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
