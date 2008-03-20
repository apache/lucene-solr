package org.apache.solr.highlight;

import java.io.IOException;
import java.util.HashMap;

import org.apache.lucene.search.Query;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.Config;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.TestHarness;


public class HighlighterConfigTest extends AbstractSolrTestCase {
	  @Override public String getSchemaFile() { return "schema.xml"; }
	  // the default case (i.e. <highlight> without a class attribute) is tested every time sorlconfig.xml is used
	  @Override public String getSolrConfigFile() { return "solrconfig-highlight.xml"; }

	  @Override 
	  public void setUp() throws Exception {
	    // if you override setUp or tearDown, you better call
	    // the super classes version
	    super.setUp();
	  }
	  
	  @Override 
	  public void tearDown() throws Exception {
	    // if you override setUp or tearDown, you better call
	    // the super classes version
	    super.tearDown();
	  }
	  
	  public void testConfig()
	  {
	    SolrHighlighter highlighter = SolrCore.getSolrCore().getHighlighter();
	    System.out.println( "highlighter" );

	    assertTrue( highlighter instanceof DummyHighlighter );
	    
	    // check to see that doHighlight is called from the DummyHighlighter
	    HashMap<String,String> args = new HashMap<String,String>();
	    args.put("hl", "true");
	    args.put("df", "t_text");
	    args.put("hl.fl", "");
	    TestHarness.LocalRequestFactory sumLRF = h.getRequestFactory(
	      "standard", 0, 200, args);
	    
	    assertU(adoc("t_text", "a long day's night", "id", "1"));
	    assertU(commit());
	    assertU(optimize());
	    assertQ("Basic summarization",
	            sumLRF.makeRequest("long"),
	            "//lst[@name='highlighting']/str[@name='dummy']"
	            );
	  }
}


