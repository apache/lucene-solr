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

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2003 The Apache Software Foundation.  All rights
 * reserved.
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

        assertTrue(keywordFieldValues[0].equals("test2"));
        assertTrue(keywordFieldValues[1].equals("test1"));
        assertTrue(textFieldValues[0].equals("test2"));
        assertTrue(textFieldValues[1].equals("test1"));
        assertTrue(unindexedFieldValues[0].equals("test2"));
        assertTrue(unindexedFieldValues[1].equals("test1"));
        // this test cannot work for documents retrieved from the index
        // since unstored fields will obviously not be returned
        if (! fromIndex)
        {
            assertTrue(unstoredFieldValues[0].equals("test2"));
            assertTrue(unstoredFieldValues[1].equals("test1"));
        }
    }
}
