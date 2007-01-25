package org.apache.lucene.xmlparser;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.RAMDirectory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * This class illustrates how form input (such as from a web page or Swing gui) can be
 * turned into Lucene queries using a choice of XSL templates for different styles of queries. 
 * @author maharwood
 */
public class TestQueryTemplateManager extends TestCase {

	CoreParser builder;
	Analyzer analyzer=new StandardAnalyzer();
	HashMap templates=new HashMap();
	private IndexSearcher searcher;
	
	//A collection of documents' field values for use in our tests
	String docFieldValues []=
	{
			"artist=Jeff Buckley \talbum=Grace \treleaseDate=1999 \tgenre=rock",
			"artist=Fugazi \talbum=Repeater \treleaseDate=1990 \tgenre=alternative",
			"artist=Fugazi \talbum=Red Medicine \treleaseDate=1995 \tgenre=alternative",
			"artist=Peeping Tom \talbum=Peeping Tom \treleaseDate=2006 \tgenre=rock",
			"artist=Red Snapper \talbum=Prince Blimey \treleaseDate=1996 \tgenre=electronic"
	};
	
	//A collection of example queries, consisting of name/value pairs representing form content plus 
	// a choice of query style template to use in the test, with expected number of hits
	String queryForms[]=
	{
			"artist=Fugazi \texpectedMatches=2 \ttemplate=albumBooleanQuery.xsl",
			"artist=Fugazi \treleaseDate=1990 \texpectedMatches=1 \ttemplate=albumBooleanQuery.xsl",
			"artist=Buckley \tgenre=rock \texpectedMatches=1 \ttemplate=albumFilteredQuery.xsl",
			"artist=Buckley \tgenre=electronic \texpectedMatches=0 \ttemplate=albumFilteredQuery.xsl",
			"queryString=artist:buckly~ NOT genre:electronic \texpectedMatches=1 \ttemplate=albumLuceneClassicQuery.xsl"
	};
	
	
	public void testFormTransforms() throws SAXException, IOException, ParserConfigurationException, TransformerException, ParserException 
	{
		//Run all of our test queries
		for (int i = 0; i < queryForms.length; i++)
		{
			Properties queryFormProperties=getPropsFromString(queryForms[i]);
			
			//Get the required query XSL template for this test
			Source template=getTemplate(queryFormProperties.getProperty("template"));
			
			//Transform the queryFormProperties into a Lucene XML query
			Document doc=QueryTemplateManager.getQueryAsDOM(queryFormProperties,template);
			
			//Parse the XML query using the XML parser
			Query q=builder.getQuery(doc.getDocumentElement());
			
			//Run the query
			Hits h=searcher.search(q);
			
			//Check we have the expected number of results
			int expectedHits=Integer.parseInt(queryFormProperties.getProperty("expectedMatches"));
			assertEquals("Number of results should match for query "+queryForms[i],expectedHits,h.length());
			
		}
	}
	
		
	private Source getTemplate(String templateName) throws ParserConfigurationException, SAXException, IOException 
	{
		Source result=(Source) templates.get(templateName);
		if(result==null)
		{
			//Not yet loaded - load the stylesheet
			result=QueryTemplateManager.getDOMSource(getClass().getResourceAsStream(templateName));
			templates.put(templateName,result);
		}
		return result;
	}


	//Helper method to construct Lucene query forms used in our test
	Properties getPropsFromString(String nameValuePairs)
	{
		Properties result=new Properties();
		StringTokenizer st=new StringTokenizer(nameValuePairs,"\t=");
		while(st.hasMoreTokens())
		{
			String name=st.nextToken().trim();
			if(st.hasMoreTokens())
			{
				String value=st.nextToken().trim();
				result.setProperty(name,value);
			}
		}
		return result;
	}
	
	//Helper method to construct Lucene documents used in our tests
	org.apache.lucene.document.Document getDocumentFromString(String nameValuePairs)
	{
		org.apache.lucene.document.Document result=new org.apache.lucene.document.Document();
		StringTokenizer st=new StringTokenizer(nameValuePairs,"\t=");
		while(st.hasMoreTokens())
		{
			String name=st.nextToken().trim();
			if(st.hasMoreTokens())
			{
				String value=st.nextToken().trim();
				result.add(new Field(name,value,Field.Store.YES,Field.Index.TOKENIZED));
			}
		}
		return result;
	}
	
	/*
	 * @see TestCase#setUp()
	 */
	protected void setUp() throws Exception {
		super.setUp();
		
		
		//Create an index
		RAMDirectory dir=new RAMDirectory();
		IndexWriter w=new IndexWriter(dir,analyzer,true);
		for (int i = 0; i < docFieldValues.length; i++)
		{
			w.addDocument(getDocumentFromString(docFieldValues[i]));
		}
		w.optimize();
		w.close();
		searcher=new IndexSearcher(dir);
		
		//initialize the parser
		builder=new CorePlusExtensionsParser(analyzer,new QueryParser("artist", analyzer));
		
	}
	
	
	protected void tearDown() throws Exception {
		searcher.close();
	}
}
