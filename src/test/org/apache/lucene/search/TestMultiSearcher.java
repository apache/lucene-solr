package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.store.FSDirectory;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;

/**
 * FIXME: Describe class <code>TestMultiSearcher</code> here.
 *
 * @version $Id$
 */
public class TestMultiSearcher extends TestCase
{
    public TestMultiSearcher(String name)
    {
        super(name);
    }

    public void testEmptyIndex()
        throws Exception
    {
        // creating file's for the FSDirectories
        File a = new File(System.getProperty("user.home"), "indexStoreA");
        File b = new File(System.getProperty("user.home"), "indexStoreB");

        // creating two directories for indices
        FSDirectory indexStoreA = FSDirectory.getDirectory(a, true);
        FSDirectory indexStoreB = FSDirectory.getDirectory(b, true);

        // creating a document to store
        Document lDoc = new Document();
        lDoc.add(Field.Text("fulltext", "Once upon a time....."));
        lDoc.add(Field.Keyword("id", "doc1"));
        lDoc.add(Field.Keyword("handle", "1"));

        // creating a document to store
        Document lDoc2 = new Document();
        lDoc2.add(Field.Text("fulltext", "in a galaxy far far away....."));
        lDoc2.add(Field.Keyword("id", "doc2"));
        lDoc2.add(Field.Keyword("handle", "1"));

        // creating a document to store
        Document lDoc3 = new Document();
        lDoc3.add(Field.Text("fulltext", "a bizarre bug manifested itself...."));
        lDoc3.add(Field.Keyword("id", "doc3"));
        lDoc3.add(Field.Keyword("handle", "1"));

        // creating an index writer for the first index
        IndexWriter writerA = new IndexWriter(indexStoreA, new StandardAnalyzer(), true);
        // creating an index writer for the second index, but writing nothing
        IndexWriter writerB = new IndexWriter(indexStoreB, new StandardAnalyzer(), true);

        //--------------------------------------------------------------------
        // scenario 1
        //--------------------------------------------------------------------

        // writing the documents to the first index
        writerA.addDocument(lDoc);
        writerA.addDocument(lDoc2);
        writerA.addDocument(lDoc3);
        writerA.close();
        writerA.optimize();

        // closing the second index
        writerB.close();

        // creating the query
        Query query = QueryParser.parse("handle:1", "fulltext", new StandardAnalyzer());

        // building the searchables
        Searcher[] searchers = new Searcher[2];
        // VITAL STEP:adding the searcher for the empty index first, before the searcher for the populated index
        searchers[0] = new IndexSearcher(indexStoreB);
        searchers[1] = new IndexSearcher(indexStoreA);
        // creating the multiSearcher
        Searcher mSearcher = new MultiSearcher(searchers);
        // performing the search
        Hits hits = mSearcher.search(query);

        assertEquals(3, hits.length());

        try {
            // iterating over the hit documents
            for (int i = 0; i < hits.length(); i++) {
                Document d = hits.doc(i);
            }
        }
        catch (ArrayIndexOutOfBoundsException e)
        {
            fail("ArrayIndexOutOfBoundsException thrown: " + e.getMessage());
            e.printStackTrace();
        } finally{
            mSearcher.close();
        }


        //--------------------------------------------------------------------
        // scenario 2
        //--------------------------------------------------------------------

        // adding one document to the empty index
        writerB = new IndexWriter(indexStoreB, new StandardAnalyzer(), false);
        writerB.addDocument(lDoc);
        writerB.optimize();
        writerB.close();

        // building the searchables
        Searcher[] searchers2 = new Searcher[2];
        // VITAL STEP:adding the searcher for the empty index first, before the searcher for the populated index
        searchers2[0] = new IndexSearcher(indexStoreB);
        searchers2[1] = new IndexSearcher(indexStoreA);
        // creating the mulitSearcher
        Searcher mSearcher2 = new MultiSearcher(searchers2);
        // performing the same search
        Hits hits2 = mSearcher2.search(query);

        assertEquals(4, hits.length());

        try {
            // iterating over the hit documents
            for (int i = 0; i < hits2.length(); i++) {
                // no exception should happen at this point
                Document d = hits2.doc(i);
            }
        }
        catch (Exception e)
        {
            fail("Exception thrown: " + e.getMessage());
            e.printStackTrace();
        } finally{
            mSearcher2.close();
        }

        //--------------------------------------------------------------------
        // scenario 3
        //--------------------------------------------------------------------

        // deleting the document just added, this will cause a different exception to take place
        Term term = new Term("id", "doc1");
        IndexReader readerB = IndexReader.open(indexStoreB);
        readerB.delete(term);
        readerB.close();

        // optimizing the index with the writer
        writerB = new IndexWriter(indexStoreB, new StandardAnalyzer(), false);
        writerB.optimize();
        writerB.close();

        // building the searchables
        Searcher[] searchers3 = new Searcher[2];

        searchers3[0] = new IndexSearcher(indexStoreB);
        searchers3[1] = new IndexSearcher(indexStoreA);
        // creating the mulitSearcher
        Searcher mSearcher3 = new MultiSearcher(searchers3);
        // performing the same search
        Hits hits3 = mSearcher3.search(query);

        assertEquals(3, hits.length());

        try {
            // iterating over the hit documents
            for (int i = 0; i < hits3.length(); i++) {
                Document d = hits3.doc(i);
            }
        }
        catch (IOException e)
        {
            fail("IOException thrown: " + e.getMessage());
            e.printStackTrace();
        } finally{
            mSearcher3.close();
        }
    }
}
