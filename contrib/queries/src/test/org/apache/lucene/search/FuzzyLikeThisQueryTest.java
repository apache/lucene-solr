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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.store.RAMDirectory;

public class FuzzyLikeThisQueryTest extends TestCase
{
	private RAMDirectory directory;
	private IndexSearcher searcher;
	private Analyzer analyzer=new WhitespaceAnalyzer();

	protected void setUp() throws Exception
	{
		directory = new RAMDirectory();
		IndexWriter writer = new IndexWriter(directory, analyzer,true, MaxFieldLength.UNLIMITED);
		
		//Add series of docs with misspelt names
		addDoc(writer, "jonathon smythe","1");
		addDoc(writer, "jonathan smith","2");
		addDoc(writer, "johnathon smyth","3");
		addDoc(writer, "johnny smith","4" );
		addDoc(writer, "jonny smith","5" );
		addDoc(writer, "johnathon smythe","6");
	
		writer.close();
		searcher=new IndexSearcher(directory);			
	}
	
	private void addDoc(IndexWriter writer, String name, String id) throws IOException
	{
		Document doc=new Document();
		doc.add(new Field("name",name,Field.Store.YES,Field.Index.ANALYZED));
		doc.add(new Field("id",id,Field.Store.YES,Field.Index.ANALYZED));
		writer.addDocument(doc);
	}
	
		
	//Tests that idf ranking is not favouring rare mis-spellings over a strong edit-distance match 
	public void testClosestEditDistanceMatchComesFirst() throws Throwable
	{
		FuzzyLikeThisQuery flt=new FuzzyLikeThisQuery(10,analyzer);
		flt.addTerms("smith", "name", 0.3f, 1);
		Query q=flt.rewrite(searcher.getIndexReader());
		HashSet queryTerms=new HashSet();
		q.extractTerms(queryTerms);
		assertTrue("Should have variant smythe",queryTerms.contains(new Term("name","smythe")));
		assertTrue("Should have variant smith",queryTerms.contains(new Term("name","smith")));
		assertTrue("Should have variant smyth",queryTerms.contains(new Term("name","smyth")));
		TopDocs topDocs = searcher.search(flt, 1);
		ScoreDoc[] sd = topDocs.scoreDocs;
		assertTrue("score docs must match 1 doc", (sd!=null)&&(sd.length>0));
		Document doc=searcher.doc(sd[0].doc);
		assertEquals("Should match most similar not most rare variant", "2",doc.get("id"));
	}
	//Test multiple input words are having variants produced
	public void testMultiWord() throws Throwable
	{
		FuzzyLikeThisQuery flt=new FuzzyLikeThisQuery(10,analyzer);
		flt.addTerms("jonathin smoth", "name", 0.3f, 1);
		Query q=flt.rewrite(searcher.getIndexReader());
		HashSet queryTerms=new HashSet();
		q.extractTerms(queryTerms);
		assertTrue("Should have variant jonathan",queryTerms.contains(new Term("name","jonathan")));
		assertTrue("Should have variant smith",queryTerms.contains(new Term("name","smith")));
		TopDocs topDocs = searcher.search(flt, 1);
		ScoreDoc[] sd = topDocs.scoreDocs;
		assertTrue("score docs must match 1 doc", (sd!=null)&&(sd.length>0));
		Document doc=searcher.doc(sd[0].doc);
		assertEquals("Should match most similar when using 2 words", "2",doc.get("id"));
	}
	//Test bug found when first query word does not match anything
	public void testNoMatchFirstWordBug() throws Throwable
	{
		FuzzyLikeThisQuery flt=new FuzzyLikeThisQuery(10,analyzer);
		flt.addTerms("fernando smith", "name", 0.3f, 1);
		Query q=flt.rewrite(searcher.getIndexReader());
		HashSet queryTerms=new HashSet();
		q.extractTerms(queryTerms);
		assertTrue("Should have variant smith",queryTerms.contains(new Term("name","smith")));
		TopDocs topDocs = searcher.search(flt, 1);
		ScoreDoc[] sd = topDocs.scoreDocs;
		assertTrue("score docs must match 1 doc", (sd!=null)&&(sd.length>0));
		Document doc=searcher.doc(sd[0].doc);
		assertEquals("Should match most similar when using 2 words", "2",doc.get("id"));
	}
}
