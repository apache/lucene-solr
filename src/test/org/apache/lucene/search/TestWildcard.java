package org.apache.lucene.search;

/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001, 2002, 2003 The Apache Software Foundation.  All
 * rights reserved.
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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import junit.framework.TestCase;

import java.io.IOException;

/**
 * TestWildcard tests the '*' and '?' wildard characters.
 *
 * @author Otis Gospodnetic
 */
public class TestWildcard
    extends TestCase
{
    /**
     * Creates a new <code>TestWildcard</code> instance.
     *
     * @param name the name of the test
     */
    public TestWildcard(String name)
    {
	super(name);
    }

    /**
     * Tests Wildcard queries with an asterisk.
     *
     */
    public void testAsterisk()
        throws IOException
    {
        RAMDirectory indexStore = getIndexStore("body", new String[]
	    { "metal", "metals" }
						);
	IndexSearcher searcher = new IndexSearcher(indexStore);
	Query query1 = new TermQuery(new Term("body", "metal"));
        Query query2 = new WildcardQuery(new Term("body", "metal*"));
        Query query3 = new WildcardQuery(new Term("body", "m*tal"));
        Query query4 = new WildcardQuery(new Term("body", "m*tal*"));
        Query query5 = new WildcardQuery(new Term("body", "m*tals"));

        BooleanQuery query6 = new BooleanQuery();
        query6.add(query5, false, false);

        BooleanQuery query7 = new BooleanQuery();
        query7.add(query3, false, false);
        query7.add(query5, false, false);

	// Queries do not automatically lower-case search terms:
        Query query8 = new WildcardQuery(new Term("body", "M*tal*"));

	assertMatches(searcher, query1, 1);
	assertMatches(searcher, query2, 2);
	assertMatches(searcher, query3, 1);
	assertMatches(searcher, query4, 2);
	assertMatches(searcher, query5, 1);
	assertMatches(searcher, query6, 1);
	assertMatches(searcher, query7, 2);
	assertMatches(searcher, query8, 0);
    }

    /**
     * Tests Wildcard queries with a question mark.
     *
     * @exception IOException if an error occurs
     */
    public void testQuestionmark()
	throws IOException
    {
        RAMDirectory indexStore = getIndexStore("body", new String[]
	    { "metal", "metals", "mXtals", "mXtXls" }
						);
	IndexSearcher searcher = new IndexSearcher(indexStore);
        Query query1 = new WildcardQuery(new Term("body", "m?tal"));
        Query query2 = new WildcardQuery(new Term("body", "metal?"));
        Query query3 = new WildcardQuery(new Term("body", "metals?"));
        Query query4 = new WildcardQuery(new Term("body", "m?t?ls"));
        Query query5 = new WildcardQuery(new Term("body", "M?t?ls"));

	assertMatches(searcher, query1, 1);
	assertMatches(searcher, query2, 2);
	assertMatches(searcher, query3, 1);
	assertMatches(searcher, query4, 3);
	assertMatches(searcher, query5, 0);
    }

    private RAMDirectory getIndexStore(String field, String[] contents)
	throws IOException
    {
        RAMDirectory indexStore = new RAMDirectory();
        IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true);
	for (int i = 0; i < contents.length; ++i) {
	    Document doc = new Document();
	    doc.add(Field.Text(field, contents[i]));
	    writer.addDocument(doc);
	}
	writer.optimize();
	writer.close();

	return indexStore;
    }

    private void assertMatches(IndexSearcher searcher, Query q, int expectedMatches)
	throws IOException
    {
	Hits result = searcher.search(q);
	assertEquals(expectedMatches, result.length());
    }
}
