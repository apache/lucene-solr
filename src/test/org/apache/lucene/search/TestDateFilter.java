package org.apache.lucene.search;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

 /**
  * DateFilter JUnit tests.
  *
  *
  * @version $Revision$
  */
public class TestDateFilter
    extends LuceneTestCase
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
        IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

 	long now = System.currentTimeMillis();

 	Document doc = new Document();
 	// add time that is in the past
 	doc.add(new Field("datefield", DateTools.timeToString(now - 1000, DateTools.Resolution.MILLISECOND), Field.Store.YES, Field.Index.NOT_ANALYZED));
 	doc.add(new Field("body", "Today is a very sunny day in New York City", Field.Store.YES, Field.Index.ANALYZED));
  	writer.addDocument(doc);
 	writer.optimize();
	writer.close();

	IndexSearcher searcher = new IndexSearcher(indexStore);

	// filter that should preserve matches
	//DateFilter df1 = DateFilter.Before("datefield", now);
    TermRangeFilter df1 = new TermRangeFilter("datefield", DateTools.timeToString(now - 2000, DateTools.Resolution.MILLISECOND),
                                      DateTools.timeToString(now, DateTools.Resolution.MILLISECOND), false, true);
	// filter that should discard matches
	//DateFilter df2 = DateFilter.Before("datefield", now - 999999);
    TermRangeFilter df2 = new TermRangeFilter("datefield", DateTools.timeToString(0, DateTools.Resolution.MILLISECOND),
                                      DateTools.timeToString(now - 2000, DateTools.Resolution.MILLISECOND), true, false);

    // search something that doesn't exist with DateFilter
	Query query1 = new TermQuery(new Term("body", "NoMatchForThis"));

	// search for something that does exists
	Query query2 = new TermQuery(new Term("body", "sunny"));

  ScoreDoc[] result;

	// ensure that queries return expected results without DateFilter first
  result = searcher.search(query1, null, 1000).scoreDocs;
  assertEquals(0, result.length);

  result = searcher.search(query2, null, 1000).scoreDocs;
  assertEquals(1, result.length);


	// run queries with DateFilter
  result = searcher.search(query1, df1, 1000).scoreDocs;
  assertEquals(0, result.length);

  result = searcher.search(query1, df2, 1000).scoreDocs;
  assertEquals(0, result.length);

   result = searcher.search(query2, df1, 1000).scoreDocs;
   assertEquals(1, result.length);

  result = searcher.search(query2, df2, 1000).scoreDocs;
  assertEquals(0, result.length);
    }

    /**
     *
     */
    public static void testAfter()
	throws IOException
    {
	// create an index
        RAMDirectory indexStore = new RAMDirectory();
        IndexWriter writer = new IndexWriter(indexStore, new SimpleAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED);

 	long now = System.currentTimeMillis();

 	Document doc = new Document();
 	// add time that is in the future
 	doc.add(new Field("datefield", DateTools.timeToString(now + 888888, DateTools.Resolution.MILLISECOND), Field.Store.YES, Field.Index.NOT_ANALYZED));
 	doc.add(new Field("body", "Today is a very sunny day in New York City", Field.Store.YES, Field.Index.ANALYZED));
  	writer.addDocument(doc);
 	writer.optimize();
	writer.close();

	IndexSearcher searcher = new IndexSearcher(indexStore);

	// filter that should preserve matches
	//DateFilter df1 = DateFilter.After("datefield", now);
    TermRangeFilter df1 = new TermRangeFilter("datefield", DateTools.timeToString(now, DateTools.Resolution.MILLISECOND),
                                      DateTools.timeToString(now + 999999, DateTools.Resolution.MILLISECOND), true, false);
	// filter that should discard matches
	//DateFilter df2 = DateFilter.After("datefield", now + 999999);
    TermRangeFilter df2 = new TermRangeFilter("datefield", DateTools.timeToString(now + 999999, DateTools.Resolution.MILLISECOND),
                                          DateTools.timeToString(now + 999999999, DateTools.Resolution.MILLISECOND), false, true);

    // search something that doesn't exist with DateFilter
	Query query1 = new TermQuery(new Term("body", "NoMatchForThis"));

	// search for something that does exists
	Query query2 = new TermQuery(new Term("body", "sunny"));

  ScoreDoc[] result;

	// ensure that queries return expected results without DateFilter first
  result = searcher.search(query1, null, 1000).scoreDocs;
  assertEquals(0, result.length);

  result = searcher.search(query2, null, 1000).scoreDocs;
  assertEquals(1, result.length);


	// run queries with DateFilter
  result = searcher.search(query1, df1, 1000).scoreDocs;
  assertEquals(0, result.length);

  result = searcher.search(query1, df2, 1000).scoreDocs;
  assertEquals(0, result.length);

   result = searcher.search(query2, df1, 1000).scoreDocs;
   assertEquals(1, result.length);

  result = searcher.search(query2, df2, 1000).scoreDocs;
  assertEquals(0, result.length);
    }
}
