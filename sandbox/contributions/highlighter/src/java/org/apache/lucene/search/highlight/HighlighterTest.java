package org.apache.lucene.search.highlight;
/**
 * Copyright 2002-2004 The Apache Software Foundation
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

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
//import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.store.RAMDirectory;

/**
 * JUnit Test for Highlighter class.
 * @author mark@searcharea.co.uk
 */
public class HighlighterTest extends TestCase implements Formatter
{
	private IndexReader reader;
	private static final String FIELD_NAME = "contents";
	private Query query;
	RAMDirectory ramDir;
	public Searcher searcher = null;
	public Hits hits = null;
	int numHighlights = 0;
	Analyzer analyzer=new StandardAnalyzer();

	String texts[] =
		{
			"Hello this is a piece of text that is very long and contains too much preamble and the meat is really here which says kennedy has been shot",
			"This piece of text refers to Kennedy at the beginning then has a longer piece of text that is very long in the middle and finally ends with another reference to Kennedy",
			"JFK has been shot",
			"John Kennedy has been shot",
			"This text has a typo in referring to Keneddy" };

	/**
	 * Constructor for HighlightExtractorTest.
	 * @param arg0
	 */
	public HighlighterTest(String arg0)
	{
		super(arg0);
	}

	public void testSimpleHighlighter() throws Exception
	{
		doSearching("Kennedy");
		Highlighter highlighter =	new Highlighter(new QueryScorer(query));
		highlighter.setTextFragmenter(new SimpleFragmenter(40));			
		int maxNumFragmentsRequired = 2;
		for (int i = 0; i < hits.length(); i++)
		{
			String text = hits.doc(i).get(FIELD_NAME);
			TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(text));
			
			String result =
				highlighter.getBestFragments(tokenStream,text,maxNumFragmentsRequired, "...");
			System.out.println("\t" + result);
		}
		//Not sure we can assert anything here - just running to check we dont throw any exceptions 
	}



	public void testGetBestFragmentsSimpleQuery() throws Exception
	{
		doSearching("Kennedy");
		doStandardHighlights();
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 4);
	}
	public void testGetFuzzyFragments() throws Exception
	{
		doSearching("Kinnedy~");
		doStandardHighlights();
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 4);
	}

	public void testGetWildCardFragments() throws Exception
	{
		doSearching("K?nnedy");
		doStandardHighlights();
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 4);
	}
	public void testGetMidWildCardFragments() throws Exception
	{
		doSearching("K*dy");
		doStandardHighlights();
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 5);
	}
	public void testGetRangeFragments() throws Exception
	{
		doSearching(FIELD_NAME + ":[kannedy TO kznnedy]"); //bug?needs lower case
		doStandardHighlights();
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 5);
	}

	public void testGetBestFragmentsPhrase() throws Exception
	{
		doSearching("\"John Kennedy\"");
		doStandardHighlights();
		//Currently highlights "John" and "Kennedy" separately
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 2);
	}

	public void testGetBestFragmentsMultiTerm() throws Exception
	{
		doSearching("John Kenn*");
		doStandardHighlights();
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 5);
	}
	public void testGetBestFragmentsWithOr() throws Exception
	{
		doSearching("JFK OR Kennedy");
		doStandardHighlights();
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 5);
	}


	public void testGetBestSingleFragment() throws Exception
	{
		doSearching("Kennedy");
//		QueryHighlightExtractor highlighter = new QueryHighlightExtractor(this, query, new StandardAnalyzer());
		Highlighter highlighter =new Highlighter(this,new QueryScorer(query));
		highlighter.setTextFragmenter(new SimpleFragmenter(40));

		for (int i = 0; i < hits.length(); i++)
		{
			String text = hits.doc(i).get(FIELD_NAME);
			TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(text));
			String result = highlighter.getBestFragment(tokenStream,text);
			System.out.println("\t" + result);
		}
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 4);
	}
	
	public void testGetBestSingleFragmentWithWeights() throws Exception
	{
		WeightedTerm[]wTerms=new WeightedTerm[2];
		wTerms[0]=new WeightedTerm(10f,"hello");
		wTerms[1]=new WeightedTerm(1f,"kennedy");
		Highlighter highlighter =new Highlighter(new QueryScorer(wTerms));
		TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(texts[0]));
		highlighter.setTextFragmenter(new SimpleFragmenter(2));
		
		String result = highlighter.getBestFragment(tokenStream,texts[0]).trim();
		assertTrue("Failed to find best section using weighted terms. Found: "+result
			, "<B>Hello</B>".equals(result));

		//readjust weights
		wTerms[1].setWeight(50f);
		tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(texts[0]));
		highlighter =new Highlighter(new QueryScorer(wTerms));
		highlighter.setTextFragmenter(new SimpleFragmenter(2));
		
		result = highlighter.getBestFragment(tokenStream,texts[0]).trim();
		assertTrue("Failed to find best section using weighted terms. Found: "+result
			, "<B>kennedy</B>".equals(result));
	}
	
	
	
	public void testGetSimpleHighlight() throws Exception
	{
		doSearching("Kennedy");
		Highlighter highlighter =
			new Highlighter(this,new QueryScorer(query));

		for (int i = 0; i < hits.length(); i++)
		{
			String text = hits.doc(i).get(FIELD_NAME);
			TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(text));
			
			String result = highlighter.getBestFragment(tokenStream,text);
			System.out.println("\t" + result);
		}
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 4);
	}

	public void testMaxSizeHighlight() throws Exception
	{
		doSearching("meat");
		Highlighter highlighter =
			new Highlighter(this,new QueryScorer(query));
		highlighter.setMaxDocBytesToAnalyze(30);
		TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(texts[0]));
		String result = highlighter.getBestFragment(tokenStream,texts[0]);
		assertTrue("Setting MaxDocBytesToAnalyze should have prevented " +
			"us from finding matches for this record" + numHighlights +
			 " found", numHighlights == 0);
	}


	
	public void testUnRewrittenQuery() throws IOException, ParseException
	{
		//test to show how rewritten query can still be used
		searcher = new IndexSearcher(ramDir);
		Analyzer analyzer=new StandardAnalyzer();
		Query query = QueryParser.parse("JF? or Kenned*", FIELD_NAME, analyzer);
		System.out.println("Searching with primitive query");
		//forget to set this and...
		//query=query.rewrite(reader);
		Hits hits = searcher.search(query);

		//create an instance of the highlighter with the tags used to surround highlighted text
//		QueryHighlightExtractor highlighter = new QueryHighlightExtractor(this, query, new StandardAnalyzer());
		Highlighter highlighter =
			new Highlighter(this,new QueryScorer(query));

		highlighter.setTextFragmenter(new SimpleFragmenter(40));		

		int maxNumFragmentsRequired = 3;

		for (int i = 0; i < hits.length(); i++)
		{
			String text = hits.doc(i).get(FIELD_NAME);
			TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(text));
			
			String highlightedText = highlighter.getBestFragments(tokenStream,text,maxNumFragmentsRequired,"...");
			System.out.println(highlightedText);
		}
		//We expect to have zero highlights if the query is multi-terms and is not rewritten!
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 0);
	}
	
	public void testNoFragments() throws Exception
	{
		doSearching("AnInvalidQueryWhichShouldYieldNoResults");
		Highlighter highlighter =
			new Highlighter(this,new QueryScorer(query));

		int highlightFragmentSizeInBytes = 40;
		for (int i = 0; i < texts.length; i++)
		{
			String text = texts[i];
			TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(text));
			
			String result = highlighter.getBestFragment(tokenStream,text);
			assertNull("The highlight result should be null for text with no query terms", result);
		}
	}
	
	public void testMultiSearcher() throws Exception
	{
		//setup index 1
		RAMDirectory ramDir1 = new RAMDirectory();
		IndexWriter writer1 = new IndexWriter(ramDir1, new StandardAnalyzer(), true);
		Document d = new Document();
		Field f = new Field(FIELD_NAME, "multiOne", true, true, true);
		d.add(f);		
		writer1.addDocument(d);
		writer1.optimize();
		writer1.close();
		IndexReader reader1 = IndexReader.open(ramDir1);

		//setup index 2
		RAMDirectory ramDir2 = new RAMDirectory();
		IndexWriter writer2 = new IndexWriter(ramDir2, new StandardAnalyzer(), true);
		d = new Document();
		f = new Field(FIELD_NAME, "multiTwo", true, true, true);
		d.add(f);		
		writer2.addDocument(d);
		writer2.optimize();
		writer2.close();
		IndexReader reader2 = IndexReader.open(ramDir2);

		

		IndexSearcher searchers[]=new IndexSearcher[2]; 
		searchers[0] = new IndexSearcher(ramDir1);
		searchers[1] = new IndexSearcher(ramDir2);
		MultiSearcher multiSearcher=new MultiSearcher(searchers);
		query = QueryParser.parse("multi*", FIELD_NAME, new StandardAnalyzer());
		System.out.println("Searching for: " + query.toString(FIELD_NAME));
		//at this point the multisearcher calls combine(query[])
		hits = multiSearcher.search(query);

		//query = QueryParser.parse("multi*", FIELD_NAME, new StandardAnalyzer());
		Query expandedQueries[]=new Query[2];
		expandedQueries[0]=query.rewrite(reader1);
		expandedQueries[1]=query.rewrite(reader2);
		query=query.combine(expandedQueries);
		
		
		//create an instance of the highlighter with the tags used to surround highlighted text
		Highlighter highlighter =
			new Highlighter(this,new QueryScorer(query));

		for (int i = 0; i < hits.length(); i++)
		{
			String text = hits.doc(i).get(FIELD_NAME);
			TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(text));
			String highlightedText = highlighter.getBestFragment(tokenStream,text);
			System.out.println(highlightedText);
		}
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 2);		
		
		
		
	}
	
/*	

	public void testBigramAnalyzer() throws IOException, ParseException
	{
		//test to ensure analyzers with none-consecutive start/end offsets
		//dont double-highlight text
		//setup index 1
		RAMDirectory ramDir = new RAMDirectory();
		Analyzer bigramAnalyzer=new CJKAnalyzer();
		IndexWriter writer = new IndexWriter(ramDir,bigramAnalyzer , true);
		Document d = new Document();
		Field f = new Field(FIELD_NAME, "java abc def", true, true, true);
		d.add(f);
		writer.addDocument(d);		
		writer.close();
		IndexReader reader = IndexReader.open(ramDir);

		IndexSearcher searcher=new IndexSearcher(reader); 
		query = QueryParser.parse("abc", FIELD_NAME, bigramAnalyzer);
		System.out.println("Searching for: " + query.toString(FIELD_NAME));
		hits = searcher.search(query);

		Highlighter highlighter =
			new Highlighter(this,new QueryFragmentScorer(query));

		for (int i = 0; i < hits.length(); i++)
		{
			String text = hits.doc(i).get(FIELD_NAME);
			TokenStream tokenStream=bigramAnalyzer.tokenStream(FIELD_NAME,new StringReader(text));
			String highlightedText = highlighter.getBestFragment(tokenStream,text);
			System.out.println(highlightedText);
		}		
		
	}
*/	


	public String highlightTerm(String originalText , String weightedTerm, float score, int startOffset)
	{
		if(score<=0)
		{
			return originalText;
		}
		numHighlights++; //update stats used in assertions
		return "<b>" + originalText + "</b>";
	}

	public void doSearching(String queryString) throws Exception
	{
		searcher = new IndexSearcher(ramDir);
		query = QueryParser.parse(queryString, FIELD_NAME, new StandardAnalyzer());
		//for any multi-term queries to work (prefix, wildcard, range,fuzzy etc) you must use a rewritten query! 
		query=query.rewrite(reader);
		System.out.println("Searching for: " + query.toString(FIELD_NAME));
		hits = searcher.search(query);
	}

	void doStandardHighlights() throws Exception
	{
		Highlighter highlighter =new Highlighter(this,new QueryScorer(query));
		highlighter.setTextFragmenter(new SimpleFragmenter(20));
		for (int i = 0; i < hits.length(); i++)
		{
			String text = hits.doc(i).get(FIELD_NAME);
			int maxNumFragmentsRequired = 2;
			String fragmentSeparator = "...";
			TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(text));
			
			String result =
				highlighter.getBestFragments(
					tokenStream,
					text,
					maxNumFragmentsRequired,
					fragmentSeparator);
			System.out.println("\t" + result);
		}
	}

	/*
	 * @see TestCase#setUp()
	 */
	protected void setUp() throws Exception
	{
		ramDir = new RAMDirectory();
		IndexWriter writer = new IndexWriter(ramDir, new StandardAnalyzer(), true);
		for (int i = 0; i < texts.length; i++)
		{
			addDoc(writer, texts[i]);
		}

		writer.optimize();
		writer.close();
		reader = IndexReader.open(ramDir);
		numHighlights = 0;
	}

	private void addDoc(IndexWriter writer, String text) throws IOException
	{
		Document d = new Document();
		Field f = new Field(FIELD_NAME, text, true, true, true);
		d.add(f);
		writer.addDocument(d);

	}

	/*
	 * @see TestCase#tearDown()
	 */
	protected void tearDown() throws Exception
	{
		super.tearDown();
	}

}
