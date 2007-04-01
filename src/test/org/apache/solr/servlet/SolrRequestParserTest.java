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

package org.apache.solr.servlet;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.MultiMapSolrParams;
import org.apache.solr.request.SolrParams;
import org.apache.solr.util.AbstractSolrTestCase;
import org.apache.solr.util.ContentStream;

public class SolrRequestParserTest extends AbstractSolrTestCase {

  public String getSchemaFile() { return "schema.xml"; } 
  public String getSolrConfigFile() { return "solrconfig.xml"; } 
  
  SolrRequestParsers parser;

  public void setUp() throws Exception {
    super.setUp();
    parser = new SolrRequestParsers( SolrCore.getSolrCore(), SolrConfig.config );
  }
  
  public void testStreamBody() throws Exception
  {
    String body1 = "AMANAPLANPANAMA";
    String body2 = "qwertasdfgzxcvb";
    String body3 = "1234567890";
    
    Map<String,String[]> args = new HashMap<String, String[]>();
    args.put( SolrParams.STREAM_BODY, new String[] {body1} );
    
    // Make sure it got a single stream in and out ok
    List<ContentStream> streams = new ArrayList<ContentStream>();
    parser.buildRequestFrom( new MultiMapSolrParams( args ), streams );
    assertEquals( 1, streams.size() );
    assertEquals( body1, IOUtils.toString( streams.get(0).getStream() ) );
    
    // Now add three and make sure they come out ok
    streams = new ArrayList<ContentStream>();
    args.put( SolrParams.STREAM_BODY, new String[] {body1,body2,body3} );
    parser.buildRequestFrom( new MultiMapSolrParams( args ), streams );
    assertEquals( 3, streams.size() );
    ArrayList<String> input  = new ArrayList<String>();
    ArrayList<String> output = new ArrayList<String>();
    input.add( body1 );
    input.add( body2 );
    input.add( body3 );
    output.add( IOUtils.toString( streams.get(0).getStream() ) );
    output.add( IOUtils.toString( streams.get(1).getStream() ) );
    output.add( IOUtils.toString( streams.get(2).getStream() ) );
    // sort them so the output is consistent
    Collections.sort( input );
    Collections.sort( output );
    assertEquals( input.toString(), output.toString() );
    
    // set the contentType and make sure tat gets set
    String ctype = "text/xxx";
    streams = new ArrayList<ContentStream>();
    args.put( SolrParams.STREAM_CONTENTTYPE, new String[] {ctype} );
    parser.buildRequestFrom( new MultiMapSolrParams( args ), streams );
    for( ContentStream s : streams ) {
      assertEquals( ctype, s.getContentType() );
    }
  }
  

  public void testStreamURL() throws Exception
  {
    boolean ok = false;
    String url = "http://svn.apache.org/repos/asf/lucene/solr/trunk/";
    String txt = null;
    try {
      txt = IOUtils.toString( new URL(url).openStream() );
    }
    catch( Exception ex ) {
      // TODO - should it fail/skip?
      fail( "this test only works if you have a network connection." );
      return;
    }
    
    Map<String,String[]> args = new HashMap<String, String[]>();
    args.put( SolrParams.STREAM_URL, new String[] {url} );
    
    // Make sure it got a single stream in and out ok
    List<ContentStream> streams = new ArrayList<ContentStream>();
    parser.buildRequestFrom( new MultiMapSolrParams( args ), streams );
    assertEquals( 1, streams.size() );
    assertEquals( txt, IOUtils.toString( streams.get(0).getStream() ) );
  }
  
  public void testUrlParamParsing()
  {
    String[][] teststr = new String[][] {
      { "this is simple", "this%20is%20simple" },
      { "this is simple", "this+is+simple" },
      { "\u00FC", "%C3%BC" },   // lower-case "u" with diaeresis/umlaut
      { "\u0026", "%26" },      // &
      { "\u20AC", "%E2%82%AC" } // euro
    };
    
    for( String[] tst : teststr ) {
      MultiMapSolrParams params = SolrRequestParsers.parseQueryString( "val="+tst[1] );
      assertEquals( tst[0], params.get( "val" ) );
    }
  }
}
