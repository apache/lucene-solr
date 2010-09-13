package org.apache.lucene.xmlparser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.LuceneTestCase;
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

public class TestParser extends LuceneTestCase {

	CoreParser builder;
	static Directory dir;
  // TODO: change to CURRENT and rewrite test (this needs to set QueryParser.enablePositionIncrements, too, for work with CURRENT):
	Analyzer analyzer=new StandardAnalyzer(org.apache.lucene.util.Version.LUCENE_24); 
	IndexReader reader;
	private IndexSearcher searcher;

	/*
	 * @see TestCase#setUp()
	 */
	@Override
	public void setUp() throws Exception {
		super.setUp();
		
		//initialize the parser
		builder=new CorePlusExtensionsParser("contents",analyzer);
		
			BufferedReader d = new BufferedReader(new InputStreamReader(TestParser.class.getResourceAsStream("reuters21578.txt"))); 
			dir=newDirectory();
			IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(Version.LUCENE_24, analyzer));
			String line = d.readLine();		
			while(line!=null)
			{
				int endOfDate=line.indexOf('\t');
				String date=line.substring(0,endOfDate).trim();
				String content=line.substring(endOfDate).trim();
				org.apache.lucene.document.Document doc =new org.apache.lucene.document.Document();
				doc.add(newField("date",date,Field.Store.YES,Field.Index.ANALYZED));
				doc.add(newField("contents",content,Field.Store.YES,Field.Index.ANALYZED));
				NumericField numericField = new NumericField("date2");
				numericField.setIntValue(Integer.valueOf(date));
				doc.add(numericField);
				writer.addDocument(doc);
				line=d.readLine();
			}			
			d.close();
      writer.close();
		reader=IndexReader.open(dir, true);
		searcher=new IndexSearcher(reader);
		
	}
	
	
	
	
	@Override
	public void tearDown() throws Exception {
		reader.close();
		searcher.close();
		dir.close();
		super.tearDown();
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
			int h = searcher.search(q, null, 1000).totalHits;
			assertEquals("UserInputQueryCustomField should produce 0 result ", 0,h);
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
			if(VERBOSE)
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
  public void testBoostingTermQueryXML() throws Exception
	{
			Query q=parse("BoostingTermQuery.xml");
			dumpResults("BoostingTermQuery",q, 5);
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
			int h = searcher.search(q, null, 1000).totalHits;
			assertEquals("DuplicateFilterQuery should produce 1 result ", 1,h);
	}
	
	public void testNumericRangeFilterQueryXML() throws ParserException, IOException
	{
			Query q=parse("NumericRangeFilterQuery.xml");
			dumpResults("NumericRangeFilter", q, 5);
	}
	
	public void testNumericRangeQueryQueryXML() throws ParserException, IOException
	{
			Query q=parse("NumericRangeQueryQuery.xml");
			dumpResults("NumericRangeQuery", q, 5);
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
		TopDocs hits = searcher.search(q, null, numDocs);
		assertTrue(qType +" should produce results ", hits.totalHits>0);
		if(VERBOSE)
		{
			System.out.println("========="+qType+"============");
			ScoreDoc[] scoreDocs = hits.scoreDocs;
			for(int i=0;i<Math.min(numDocs,hits.totalHits);i++)
			{
				org.apache.lucene.document.Document ldoc=searcher.doc(scoreDocs[i].doc);
				System.out.println("["+ldoc.get("date")+"]"+ldoc.get("contents"));
			}
			System.out.println();
		}
	}
	

}
