package org.apache.lucene.document;

import junit.framework.TestCase;

import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.Hits;

import java.io.IOException;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Tests {@link Document} class.
 *
 * @author Otis Gospodnetic
 * @version $Id$
 */
public class TestDocument extends TestCase
{
    /**
     * Tests {@link Document#getValues()} method for a brand new Document
     * that has not been indexed yet.
     *
     * @throws Exception on error
     */
    public void testGetValuesForNewDocument() throws Exception
    {
        doAssert(makeDocumentWithFields(), false);
    }

    /**
     * Tests {@link Document#getValues()} method for a Document retrieved from
     * an index.
     *
     * @throws Exception on error
     */
    public void testGetValuesForIndexedDocument() throws Exception
    {
        RAMDirectory dir = new RAMDirectory();
        IndexWriter writer = new IndexWriter(dir, new StandardAnalyzer(), true);
        writer.addDocument(makeDocumentWithFields());
        writer.close();

        Searcher searcher = new IndexSearcher(dir);

	// search for something that does exists
	Query query = new TermQuery(new Term("keyword", "test1"));

	// ensure that queries return expected results without DateFilter first
        Hits hits = searcher.search(query);
	assertEquals(1, hits.length());

        try
        {
            doAssert(hits.doc(0), true);
        }
        catch (Exception e)
        {
            e.printStackTrace(System.err);
            System.err.print("\n");
        }
        finally
        {
            searcher.close();
        }
    }

    private Document makeDocumentWithFields() throws IOException
    {
        Document doc = new Document();
        doc.add(Field.Keyword(  "keyword",   "test1"));
        doc.add(Field.Keyword(  "keyword",   "test2"));
        doc.add(Field.Text(     "text",      "test1"));
        doc.add(Field.Text(     "text",      "test2"));
        doc.add(Field.UnIndexed("unindexed", "test1"));
        doc.add(Field.UnIndexed("unindexed", "test2"));
        doc.add(Field.UnStored( "unstored",  "test1"));
        doc.add(Field.UnStored( "unstored",  "test2"));
        return doc;
    }

    private void doAssert(Document doc, boolean fromIndex)
    {
        String[] keywordFieldValues   = doc.getValues("keyword");
        String[] textFieldValues      = doc.getValues("text");
        String[] unindexedFieldValues = doc.getValues("unindexed");
        String[] unstoredFieldValues  = doc.getValues("unstored");

        assertTrue(keywordFieldValues.length   == 2);
        assertTrue(textFieldValues.length      == 2);
        assertTrue(unindexedFieldValues.length == 2);
        // this test cannot work for documents retrieved from the index
        // since unstored fields will obviously not be returned
        if (! fromIndex)
        {
            assertTrue(unstoredFieldValues.length  == 2);
        }

        assertTrue(keywordFieldValues[0].equals("test1"));
        assertTrue(keywordFieldValues[1].equals("test2"));
        assertTrue(textFieldValues[0].equals("test1"));
        assertTrue(textFieldValues[1].equals("test2"));
        assertTrue(unindexedFieldValues[0].equals("test1"));
        assertTrue(unindexedFieldValues[1].equals("test2"));
        // this test cannot work for documents retrieved from the index
        // since unstored fields will obviously not be returned
        if (! fromIndex)
        {
            assertTrue(unstoredFieldValues[0].equals("test1"));
            assertTrue(unstoredFieldValues[1].equals("test2"));
        }
    }
}
