package org.apache.lucene.search;

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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.DateField;

import java.io.IOException;

import junit.framework.TestCase;

 /**
  * DateFilter JUnit tests.
  *
  * @author Otis Gospodnetic
  * @version $Revision$
  */
public class TestDateFilter
    extends TestCase
{
    public TestDateFilter(String name)
    {
	super(name);
    }

    /**
     *
     */
    public static void testBefore()
	throws IOException
    {
	// create an index
        RAMDirectory indexStore = new RAMDirectory();
        IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true);

 	long now = System.currentTimeMillis();

 	Document doc = new Document();
 	// add time that is in the past
 	doc.add(Field.Keyword("datefield", DateField.timeToString(now - 1000)));
 	doc.add(Field.Text("body", "Today is a very sunny day in New York City"));
  	writer.addDocument(doc);
 	writer.optimize();
	writer.close();

	IndexSearcher searcher = new IndexSearcher(indexStore);

	// filter that should preserve matches
	DateFilter df1 = DateFilter.Before("datefield", now);

	// filter that should discard matches
	DateFilter df2 = DateFilter.Before("datefield", now - 999999);

	// search something that doesn't exist with DateFilter
	Query query1 = new TermQuery(new Term("body", "NoMatchForThis"));

	// search for something that does exists
	Query query2 = new TermQuery(new Term("body", "sunny"));

	Hits result;

	// ensure that queries return expected results without DateFilter first
	result = searcher.search(query1);
	assertEquals(0, result.length());

	result = searcher.search(query2);
	assertEquals(1, result.length());


	// run queries with DateFilter
	result = searcher.search(query1, df1);
	assertEquals(0, result.length());

	result = searcher.search(query1, df2);
	assertEquals(0, result.length());

 	result = searcher.search(query2, df1);
 	assertEquals(1, result.length());

	result = searcher.search(query2, df2);
	assertEquals(0, result.length());
    }

    /**
     *
     */
    public static void testAfter()
	throws IOException
    {
	// create an index
        RAMDirectory indexStore = new RAMDirectory();
        IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true);

 	long now = System.currentTimeMillis();

 	Document doc = new Document();
 	// add time that is in the future
 	doc.add(Field.Keyword("datefield", DateField.timeToString(now + 888888)));
 	doc.add(Field.Text("body", "Today is a very sunny day in New York City"));
  	writer.addDocument(doc);
 	writer.optimize();
	writer.close();

	IndexSearcher searcher = new IndexSearcher(indexStore);

	// filter that should preserve matches
	DateFilter df1 = DateFilter.After("datefield", now);

	// filter that should discard matches
	DateFilter df2 = DateFilter.After("datefield", now + 999999);

	// search something that doesn't exist with DateFilter
	Query query1 = new TermQuery(new Term("body", "NoMatchForThis"));

	// search for something that does exists
	Query query2 = new TermQuery(new Term("body", "sunny"));

	Hits result;

	// ensure that queries return expected results without DateFilter first
	result = searcher.search(query1);
	assertEquals(0, result.length());

	result = searcher.search(query2);
	assertEquals(1, result.length());


	// run queries with DateFilter
	result = searcher.search(query1, df1);
	assertEquals(0, result.length());

	result = searcher.search(query1, df2);
	assertEquals(0, result.length());

 	result = searcher.search(query2, df1);
 	assertEquals(1, result.length());

	result = searcher.search(query2, df2);
	assertEquals(0, result.length());
    }
}
