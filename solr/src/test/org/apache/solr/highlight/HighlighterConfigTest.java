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
package org.apache.solr.highlight;

import org.apache.solr.handler.component.HighlightComponent;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.TestHarness;

import java.util.HashMap;

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
            SolrHighlighter highlighter = HighlightComponent.getHighlighter(h.getCore());
	    log.info( "highlighter" );

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


