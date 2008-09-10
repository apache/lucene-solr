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

import java.io.IOException;
import java.util.HashSet;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermDocs;
import org.apache.lucene.store.RAMDirectory;

public class DuplicateFilterTest extends TestCase
{
	private static final String KEY_FIELD = "url";
	private RAMDirectory directory;
	private IndexReader reader;
	TermQuery tq=new TermQuery(new Term("text","lucene"));
	private IndexSearcher searcher;

	protected void setUp() throws Exception
	{
		directory = new RAMDirectory();
		IndexWriter writer = new IndexWriter(directory, new StandardAnalyzer(), true);
		
		//Add series of docs with filterable fields : url, text and dates  flags
		addDoc(writer, "http://lucene.apache.org", "lucene 1.4.3 available", "20040101");
		addDoc(writer, "http://lucene.apache.org", "New release pending", "20040102");
		addDoc(writer, "http://lucene.apache.org", "Lucene 1.9 out now", "20050101");		
		addDoc(writer, "http://www.bar.com", "Local man bites dog", "20040101");
		addDoc(writer, "http://www.bar.com", "Dog bites local man", "20040102");
		addDoc(writer, "http://www.bar.com", "Dog uses Lucene", "20050101");
		addDoc(writer, "http://lucene.apache.org", "Lucene 2.0 out", "20050101");
		addDoc(writer, "http://lucene.apache.org", "Oops. Lucene 2.1 out", "20050102");
		
		writer.close();
		reader=IndexReader.open(directory);			
		searcher =new IndexSearcher(reader);
		
	}
	
	protected void tearDown() throws Exception
	{
		reader.close();
		searcher.close();
		directory.close();
	}

	private void addDoc(IndexWriter writer, String url, String text, String date) throws IOException
	{
		Document doc=new Document();
		doc.add(new Field(KEY_FIELD,url,Field.Store.YES,Field.Index.NOT_ANALYZED));
		doc.add(new Field("text",text,Field.Store.YES,Field.Index.ANALYZED));
		doc.add(new Field("date",date,Field.Store.YES,Field.Index.ANALYZED));
		writer.addDocument(doc);
	}
		
	public void testDefaultFilter() throws Throwable
	{
		DuplicateFilter df=new DuplicateFilter(KEY_FIELD);		
		HashSet results=new HashSet();
		Hits h = searcher.search(tq,df);
		for(int i=0;i<h.length();i++)
		{
			Document d=h.doc(i);
			String url=d.get(KEY_FIELD);
			assertFalse("No duplicate urls should be returned",results.contains(url));
			results.add(url);
		}
	}
	public void testNoFilter() throws Throwable
	{
		HashSet results=new HashSet();
		Hits h = searcher.search(tq);
		assertTrue("Default searching should have found some matches",h.length()>0);
		boolean dupsFound=false;
		for(int i=0;i<h.length();i++)
		{
			Document d=h.doc(i);
			String url=d.get(KEY_FIELD);
			if(!dupsFound)
				dupsFound=results.contains(url);
			results.add(url);
		}
		assertTrue("Default searching should have found duplicate urls",dupsFound);
	}
	
	public void testFastFilter() throws Throwable
	{
		DuplicateFilter df=new DuplicateFilter(KEY_FIELD);
		df.setProcessingMode(DuplicateFilter.PM_FAST_INVALIDATION);
		HashSet results=new HashSet();
		Hits h = searcher.search(tq,df);
		assertTrue("Filtered searching should have found some matches",h.length()>0);
		for(int i=0;i<h.length();i++)
		{
			Document d=h.doc(i);
			String url=d.get(KEY_FIELD);
			assertFalse("No duplicate urls should be returned",results.contains(url));
			results.add(url);
		}
		assertEquals("Two urls found",2, results.size());
	}	
	public void testKeepsLastFilter() throws Throwable
	{
		DuplicateFilter df=new DuplicateFilter(KEY_FIELD);
		df.setKeepMode(DuplicateFilter.KM_USE_LAST_OCCURRENCE);
		Hits h = searcher.search(tq,df);
		assertTrue("Filtered searching should have found some matches",h.length()>0);
		for(int i=0;i<h.length();i++)
		{
			Document d=h.doc(i);
			String url=d.get(KEY_FIELD);
			TermDocs td = reader.termDocs(new Term(KEY_FIELD,url));
			int lastDoc=0;
			while(td.next())
			{
				lastDoc=td.doc();
			}
			assertEquals("Duplicate urls should return last doc",lastDoc, h.id((i)));
		}
	}	
	
	
	public void testKeepsFirstFilter() throws Throwable
	{
		DuplicateFilter df=new DuplicateFilter(KEY_FIELD);
		df.setKeepMode(DuplicateFilter.KM_USE_FIRST_OCCURRENCE);
		Hits h = searcher.search(tq,df);
		assertTrue("Filtered searching should have found some matches",h.length()>0);
		for(int i=0;i<h.length();i++)
		{
			Document d=h.doc(i);
			String url=d.get(KEY_FIELD);
			TermDocs td = reader.termDocs(new Term(KEY_FIELD,url));
			int lastDoc=0;
			td.next();
			lastDoc=td.doc();
			assertEquals("Duplicate urls should return first doc",lastDoc, h.id((i)));
		}
	}	
	
	
}
