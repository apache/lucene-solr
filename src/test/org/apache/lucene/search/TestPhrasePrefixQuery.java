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

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import junit.framework.TestCase;

import java.io.IOException;
import java.util.LinkedList;

/**
 * This class tests PhrasePrefixQuery class.
 *
 * @author Otis Gospodnetic
 * @version $Id$
 */
public class TestPhrasePrefixQuery
    extends TestCase
{
    public TestPhrasePrefixQuery(String name)
    {
	super(name);
    }

    /**
     *
     */
    public void testPhrasePrefix()
        throws IOException
    {
        RAMDirectory indexStore = new RAMDirectory();
        IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true);
        Document doc1 = new Document();
        Document doc2 = new Document();
        Document doc3 = new Document();
	Document doc4 = new Document();
	doc1.add(Field.Text("body", "blueberry pie"));
        doc2.add(Field.Text("body", "blueberry pizza"));
        doc3.add(Field.Text("body", "blueberry chewing gum"));
        doc4.add(Field.Text("body", "picadelly circus"));
        writer.addDocument(doc1);
        writer.addDocument(doc2);
        writer.addDocument(doc3);
        writer.addDocument(doc4);
	writer.optimize();
	writer.close();

	IndexSearcher searcher = new IndexSearcher(indexStore);

	PhrasePrefixQuery query1 = new PhrasePrefixQuery();
	PhrasePrefixQuery query2 = new PhrasePrefixQuery();
	query1.add(new Term("body", "blueberry"));
	query2.add(new Term("body", "strawberry"));

	LinkedList termsWithPrefix = new LinkedList();
        IndexReader ir = IndexReader.open(indexStore);

	// this TermEnum gives "picadelly", "pie" and "pizza".
        TermEnum te = ir.terms(new Term("body", "pi*"));
        do {
            termsWithPrefix.add(te.term());
        } while (te.next());
	query1.add((Term[])termsWithPrefix.toArray(new Term[0]));
	query2.add((Term[])termsWithPrefix.toArray(new Term[0]));

	Hits result;
	result = searcher.search(query1);
	assertEquals(2, result.length());

	result = searcher.search(query2);
	assertEquals(0, result.length());
    }
}
