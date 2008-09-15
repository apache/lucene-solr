package org.apache.lucene.xmlparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
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

public class TestParser extends TestCase {

	CoreParser builder;
	static Directory dir;
	Analyzer analyzer=new StandardAnalyzer();
	IndexReader reader;
	private IndexSearcher searcher;
	
	//CHANGE THIS TO SEE OUTPUT
	boolean printResults=false;
	
	
	/*
	 * @see TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		
		//initialize the parser
		builder=new CorePlusExtensionsParser("contents",analyzer);
		
		//initialize the index (done once, then cached in static data for use with ALL tests)		
		if(dir==null)
		{
			BufferedReader d = new BufferedReader(new InputStreamReader(TestParser.class.getResourceAsStream("reuters21578.txt"))); 
			dir=new RAMDirectory();
			IndexWriter writer=new IndexWriter(dir,analyzer,true);
			String line = d.readLine();		
			while(line!=null)
			{
				int endOfDate=line.indexOf('\t');
				String date=line.substring(0,endOfDate).trim();
				String content=line.substring(endOfDate).trim();
				org.apache.lucene.document.Document doc =new org.apache.lucene.document.Document();
				doc.add(new Field("date",date,Field.Store.YES,Field.Index.ANALYZED));
				doc.add(new Field("contents",content,Field.Store.YES,Field.Index.ANALYZED));
				writer.addDocument(doc);
				line=d.readLine();
			}			
			d.close();
                        writer.close();
		}
		reader=IndexReader.open(dir);
		searcher=new IndexSearcher(reader);
		
	}
	
	
	
	
	protected void tearDown() throws Exception {
		reader.close();
		searcher.close();
//		dir.close();
		
	}
	public void testSimpleXML() throws ParserException, IOException
	{
			Query q=parse("TermQuery.xml");
			dumpResults("TermQuery", q, 5);
	}
	public void testSimpleTermsQueryXML() throws ParserException, IOException
	{
			Query q=parse("TermsQuery.xml");
			dumpResults("TermsQuery", q, 5);
	}
	public void testBooleanQueryXML() throws ParserException, IOException
	{
			Query q=parse("BooleanQuery.xml");
			dumpResults("BooleanQuery", q, 5);
	}
	public void testRangeFilterQueryXML() throws ParserException, IOException
	{
			Query q=parse("RangeFilterQuery.xml");
			dumpResults("RangeFilter", q, 5);
	}
	public void testUserQueryXML() throws ParserException, IOException
	{
			Query q=parse("UserInputQuery.xml");
			dumpResults("UserInput with Filter", q, 5);
	}
	
	public void testCustomFieldUserQueryXML() throws ParserException, IOException
	{
			Query q=parse("UserInputQueryCustomField.xml");
			Hits h = searcher.search(q);
			assertEquals("UserInputQueryCustomField should produce 0 result ", 0,h.length());
	}
	
	public void testLikeThisQueryXML() throws Exception
	{
			Query q=parse("LikeThisQuery.xml");
			dumpResults("like this", q, 5);
	}
	public void testBoostingQueryXML() throws Exception
	{
			Query q=parse("BoostingQuery.xml");
			dumpResults("boosting ",q, 5);
	}
	public void testFuzzyLikeThisQueryXML() throws Exception
	{
			Query q=parse("FuzzyLikeThisQuery.xml");
			//show rewritten fuzzyLikeThisQuery - see what is being matched on
			if(printResults)
			{
				System.out.println(q.rewrite(reader));
			}
			dumpResults("FuzzyLikeThis", q, 5);
	}
	public void testTermsFilterXML() throws Exception
	{
			Query q=parse("TermsFilterQuery.xml");
			dumpResults("Terms Filter",q, 5);
	}
	public void testSpanTermXML() throws Exception
	{
			Query q=parse("SpanQuery.xml");
			dumpResults("Span Query",q, 5);
	}
	public void testConstantScoreQueryXML() throws Exception
	{
			Query q=parse("ConstantScoreQuery.xml");
			dumpResults("ConstantScoreQuery",q, 5);
	}
	public void testMatchAllDocsPlusFilterXML() throws ParserException, IOException
	{
			Query q=parse("MatchAllDocsQuery.xml");
			dumpResults("MatchAllDocsQuery with range filter", q, 5);
	}
	public void testBooleanFilterXML() throws ParserException, IOException
	{
			Query q=parse("BooleanFilter.xml");
			dumpResults("Boolean filter", q, 5);
	}
	public void testNestedBooleanQuery() throws ParserException, IOException
	{
			Query q=parse("NestedBooleanQuery.xml");
			dumpResults("Nested Boolean query", q, 5);
	}
	public void testCachedFilterXML() throws ParserException, IOException
	{
			Query q=parse("CachedFilter.xml");
			dumpResults("Cached filter", q, 5);
	}
	public void testDuplicateFilterQueryXML() throws ParserException, IOException
	{
			Query q=parse("DuplicateFilterQuery.xml");
			Hits h = searcher.search(q);
			assertEquals("DuplicateFilterQuery should produce 1 result ", 1,h.length());
	}
	


	//================= Helper methods ===================================
	private Query parse(String xmlFileName) throws ParserException, IOException
	{
		InputStream xmlStream=TestParser.class.getResourceAsStream(xmlFileName);
		Query result=builder.parse(xmlStream);
		xmlStream.close();
		return result;
	}
	private void dumpResults(String qType,Query q, int numDocs) throws IOException
	{
		Hits h = searcher.search(q);
		assertTrue(qType +" should produce results ", h.length()>0);
		if(printResults)
		{
			System.out.println("========="+qType+"============");
			for(int i=0;i<Math.min(numDocs,h.length());i++)
			{
				org.apache.lucene.document.Document ldoc=h.doc(i);
				System.out.println("["+ldoc.get("date")+"]"+ldoc.get("contents"));
			}
			System.out.println();
		}
	}
	

}
