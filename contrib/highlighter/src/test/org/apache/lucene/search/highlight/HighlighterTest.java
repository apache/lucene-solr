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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseTokenizer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RangeFilter;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

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
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 5);
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
	public void testGetBestFragmentsSpan() throws Exception
	{
		SpanQuery clauses[]={
			new SpanTermQuery(new Term("contents","john")),
			new SpanTermQuery(new Term("contents","kennedy")),
			}; 
		
		SpanNearQuery snq=new SpanNearQuery(clauses,1,true);
		doSearching(snq);
		doStandardHighlights();
		//Currently highlights "John" and "Kennedy" separately
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 2);
	}
	public void testGetBestFragmentsFilteredQuery() throws Exception
	{
		RangeFilter rf=new RangeFilter("contents","john","john",true,true);
		SpanQuery clauses[]={
				new SpanTermQuery(new Term("contents","john")),
				new SpanTermQuery(new Term("contents","kennedy")),
				}; 
		SpanNearQuery snq=new SpanNearQuery(clauses,1,true);
		FilteredQuery fq=new FilteredQuery(snq,rf);
		
		doSearching(fq);
		doStandardHighlights();
		//Currently highlights "John" and "Kennedy" separately
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 2);
	}
	public void testGetBestFragmentsFilteredPhraseQuery() throws Exception
	{
		RangeFilter rf=new RangeFilter("contents","john","john",true,true);
		PhraseQuery pq=new PhraseQuery();
		pq.add(new Term("contents","john"));
		pq.add(new  Term("contents","kennedy"));
		FilteredQuery fq=new FilteredQuery(pq,rf);
		
		doSearching(fq);
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

		numHighlights = 0;
		for (int i = 0; i < hits.length(); i++)
		{
    		String text = hits.doc(i).get(FIELD_NAME);
    		highlighter.getBestFragment(analyzer, FIELD_NAME,text);
		}
		assertTrue("Failed to find correct number of highlights " + numHighlights + " found", numHighlights == 4);

		numHighlights = 0;
		for (int i = 0; i < hits.length(); i++)
		{
    		String text = hits.doc(i).get(FIELD_NAME);
    		highlighter.getBestFragments(analyzer,FIELD_NAME, text, 10);
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
		assertTrue("Failed to find best section using weighted terms. Found: ["+result+"]"
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
	
	
	// tests a "complex" analyzer that produces multiple 
	// overlapping tokens 
	public void testOverlapAnalyzer() throws Exception
	{
		HashMap synonyms = new HashMap();
		synonyms.put("football", "soccer,footie");
		Analyzer analyzer = new SynonymAnalyzer(synonyms);
		String srchkey = "football";

		String s = "football-soccer in the euro 2004 footie competition";
		QueryParser parser=new QueryParser("bookid",analyzer);
		Query query = parser.parse(srchkey);

		Highlighter highlighter = new Highlighter(new QueryScorer(query));
		TokenStream tokenStream =
			analyzer.tokenStream(null, new StringReader(s));
		// Get 3 best fragments and seperate with a "..."
		String result = highlighter.getBestFragments(tokenStream, s, 3, "...");
		String expectedResult="<B>football</B>-<B>soccer</B> in the euro 2004 <B>footie</B> competition";
		assertTrue("overlapping analyzer should handle highlights OK",expectedResult.equals(result));
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


	public void testGetTextFragments() throws Exception
	{
		doSearching("Kennedy");
		Highlighter highlighter =
			new Highlighter(this,new QueryScorer(query));
		highlighter.setTextFragmenter(new SimpleFragmenter(20));

		for (int i = 0; i < hits.length(); i++)
		{
			String text = hits.doc(i).get(FIELD_NAME);
			TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(text));

			String stringResults[] = highlighter.getBestFragments(tokenStream,text,10);

			tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(text));
			TextFragment fragmentResults[] = highlighter.getBestTextFragments(tokenStream,text,true,10);

			assertTrue("Failed to find correct number of text Fragments: " + 
				fragmentResults.length + " vs "+ stringResults.length, fragmentResults.length==stringResults.length);
			for (int j = 0; j < stringResults.length; j++) 
			{
				System.out.println(fragmentResults[j]);
				assertTrue("Failed to find same text Fragments: " + 
					fragmentResults[j] + " found", fragmentResults[j].toString().equals(stringResults[j]));
				
			}
			
		}
	}

	public void testMaxSizeHighlight() throws Exception
	{
		doSearching("meat");
		Highlighter highlighter =
			new Highlighter(this,new QueryScorer(query));
		highlighter.setMaxDocBytesToAnalyze(30);
		TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(texts[0]));
		highlighter.getBestFragment(tokenStream,texts[0]);
		assertTrue("Setting MaxDocBytesToAnalyze should have prevented " +
			"us from finding matches for this record: " + numHighlights +
			 " found", numHighlights == 0);
	}



	public void testUnRewrittenQuery() throws IOException, ParseException
	{
		//test to show how rewritten query can still be used
		searcher = new IndexSearcher(ramDir);
		Analyzer analyzer=new StandardAnalyzer();

		QueryParser parser=new QueryParser(FIELD_NAME,analyzer);	
		Query query = parser.parse("JF? or Kenned*");
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

		for (int i = 0; i < texts.length; i++)
		{
			String text = texts[i];
			TokenStream tokenStream=analyzer.tokenStream(FIELD_NAME,new StringReader(text));

			String result = highlighter.getBestFragment(tokenStream,text);
			assertNull("The highlight result should be null for text with no query terms", result);
		}
	}
	
	/**
	 * Demonstrates creation of an XHTML compliant doc using new encoding facilities.
	 * @throws Exception
	 */
	public void testEncoding() throws Exception
    {
        String rawDocContent = "\"Smith & sons' prices < 3 and >4\" claims article";
        //run the highlighter on the raw content (scorer does not score any tokens for 
        // highlighting but scores a single fragment for selection
        Highlighter highlighter = new Highlighter(this,
                new SimpleHTMLEncoder(), new Scorer()
                {
                    public void startFragment(TextFragment newFragment)
                    {
                    }
                    public float getTokenScore(Token token)
                    {
                        return 0;
                    }
                    public float getFragmentScore()
                    {
                        return 1;
                    }
                });
        highlighter.setTextFragmenter(new SimpleFragmenter(2000));
        TokenStream tokenStream = analyzer.tokenStream(FIELD_NAME,
                new StringReader(rawDocContent));

        String encodedSnippet = highlighter.getBestFragments(tokenStream, rawDocContent,1,"");
        //An ugly bit of XML creation:
        String xhtml="<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
            		"<!DOCTYPE html\n"+
            		"PUBLIC \"//W3C//DTD XHTML 1.0 Transitional//EN\"\n"+
            		"\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">\n"+
            		"<html xmlns=\"http://www.w3.org/1999/xhtml\" xml:lang=\"en\" lang=\"en\">\n"+
            		"<head>\n"+
            		"<title>My Test HTML Document</title>\n"+
            		"</head>\n"+
            		"<body>\n"+
            		"<h2>"+encodedSnippet+"</h2>\n"+
            		"</body>\n"+
            		"</html>";
        //now an ugly built of XML parsing to test the snippet is encoded OK 
  		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
  		DocumentBuilder db = dbf.newDocumentBuilder();
  		org.w3c.dom.Document doc = db.parse(new ByteArrayInputStream(xhtml.getBytes()));
  		Element root=doc.getDocumentElement();  		
  		NodeList nodes=root.getElementsByTagName("body");
  		Element body=(Element) nodes.item(0);
  		nodes=body.getElementsByTagName("h2");
        Element h2=(Element) nodes.item(0); 
        String decodedSnippet=h2.getFirstChild().getNodeValue();
        assertEquals("XHTML Encoding should have worked:", rawDocContent,decodedSnippet);
    }

	public void testMultiSearcher() throws Exception
	{
		//setup index 1
		RAMDirectory ramDir1 = new RAMDirectory();
		IndexWriter writer1 = new IndexWriter(ramDir1, new StandardAnalyzer(), true);
		Document d = new Document();
		Field f = new Field(FIELD_NAME, "multiOne", Field.Store.YES, Field.Index.TOKENIZED);
		d.add(f);
		writer1.addDocument(d);
		writer1.optimize();
		writer1.close();
		IndexReader reader1 = IndexReader.open(ramDir1);

		//setup index 2
		RAMDirectory ramDir2 = new RAMDirectory();
		IndexWriter writer2 = new IndexWriter(ramDir2, new StandardAnalyzer(), true);
		d = new Document();
		f = new Field(FIELD_NAME, "multiTwo", Field.Store.YES, Field.Index.TOKENIZED);
		d.add(f);
		writer2.addDocument(d);
		writer2.optimize();
		writer2.close();
		IndexReader reader2 = IndexReader.open(ramDir2);



		IndexSearcher searchers[]=new IndexSearcher[2];
		searchers[0] = new IndexSearcher(ramDir1);
		searchers[1] = new IndexSearcher(ramDir2);
		MultiSearcher multiSearcher=new MultiSearcher(searchers);
		QueryParser parser=new QueryParser(FIELD_NAME, new StandardAnalyzer());
		query = parser.parse("multi*");
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
	
	public void testFieldSpecificHighlighting() throws IOException, ParseException
	{
		String docMainText="fred is one of the people";
		QueryParser parser=new QueryParser(FIELD_NAME,analyzer);
		Query query=parser.parse("fred category:people");
		
		//highlighting respects fieldnames used in query
		QueryScorer fieldSpecificScorer=new QueryScorer(query, "contents");
		Highlighter fieldSpecificHighlighter =
			new Highlighter(new SimpleHTMLFormatter(),fieldSpecificScorer);
		fieldSpecificHighlighter.setTextFragmenter(new NullFragmenter());
		String result=fieldSpecificHighlighter.getBestFragment(analyzer,FIELD_NAME,docMainText);
		assertEquals("Should match",result,"<B>fred</B> is one of the people");
		
		//highlighting does not respect fieldnames used in query
		QueryScorer fieldInSpecificScorer=new QueryScorer(query);
		Highlighter fieldInSpecificHighlighter =
			new Highlighter(new SimpleHTMLFormatter(),fieldInSpecificScorer);
		fieldInSpecificHighlighter.setTextFragmenter(new NullFragmenter());
		result=fieldInSpecificHighlighter.getBestFragment(analyzer,FIELD_NAME,docMainText);
		assertEquals("Should match",result,"<B>fred</B> is one of the <B>people</B>");
		
		
		reader.close();
		
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


	public String highlightTerm(String originalText , TokenGroup group)
	{
		if(group.getTotalScore()<=0)
		{
			return originalText;
		}
		numHighlights++; //update stats used in assertions
		return "<b>" + originalText + "</b>";
	}
	
	public void doSearching(String queryString) throws Exception
	{
		QueryParser parser=new QueryParser(FIELD_NAME, new StandardAnalyzer());
		query = parser.parse(queryString);
		doSearching(query);
	}
	public void doSearching(Query unReWrittenQuery) throws Exception
	{
		searcher = new IndexSearcher(ramDir);
		//for any multi-term queries to work (prefix, wildcard, range,fuzzy etc) you must use a rewritten query!
		query=unReWrittenQuery.rewrite(reader);
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
		Field f = new Field(FIELD_NAME, text,Field.Store.YES, Field.Index.TOKENIZED);
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


//===================================================================
//========== BEGIN TEST SUPPORTING CLASSES
//========== THESE LOOK LIKE, WITH SOME MORE EFFORT THESE COULD BE
//========== MADE MORE GENERALLY USEFUL.
// TODO - make synonyms all interchangeable with each other and produce
// a version that does hyponyms - the "is a specialised type of ...."
// so that car = audi, bmw and volkswagen but bmw != audi so different
// behaviour to synonyms
//===================================================================

class SynonymAnalyzer extends Analyzer
{
	private Map synonyms;

	public SynonymAnalyzer(Map synonyms)
	{
		this.synonyms = synonyms;
	}

	/* (non-Javadoc)
	 * @see org.apache.lucene.analysis.Analyzer#tokenStream(java.lang.String, java.io.Reader)
	 */
	public TokenStream tokenStream(String arg0, Reader arg1)
	{
		return new SynonymTokenizer(new LowerCaseTokenizer(arg1), synonyms);
	}
}

/**
 * Expands a token stream with synonyms (TODO - make the synonyms analyzed by choice of analyzer)
 * @author MAHarwood
 */
class SynonymTokenizer extends TokenStream
{
	private TokenStream realStream;
	private Token currentRealToken = null;
	private Map synonyms;
	StringTokenizer st = null;
	public SynonymTokenizer(TokenStream realStream, Map synonyms)
	{
		this.realStream = realStream;
		this.synonyms = synonyms;
	}
	public Token next() throws IOException
	{
		if (currentRealToken == null)
		{
			Token nextRealToken = realStream.next();
			if (nextRealToken == null)
			{
				return null;
			}
			String expansions = (String) synonyms.get(nextRealToken.termText());
			if (expansions == null)
			{
				return nextRealToken;
			}
			st = new StringTokenizer(expansions, ",");
			if (st.hasMoreTokens())
			{
				currentRealToken = nextRealToken;
			}
			return currentRealToken;
		}
		else
		{
			String nextExpandedValue = st.nextToken();
			Token expandedToken =
				new Token(
					nextExpandedValue,
					currentRealToken.startOffset(),
					currentRealToken.endOffset());
			expandedToken.setPositionIncrement(0);
			if (!st.hasMoreTokens())
			{
				currentRealToken = null;
				st = null;
			}
			return expandedToken;
		}
	}

}


