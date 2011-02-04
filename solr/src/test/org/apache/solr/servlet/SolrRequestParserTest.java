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

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class SolrRequestParserTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
    parser = new SolrRequestParsers( h.getCore().getSolrConfig() );
  }
  
  static SolrRequestParsers parser;

  @AfterClass
  public static void afterClass() throws Exception {
    parser = null;
  }
  
  @Test
  public void testStreamBody() throws Exception
  {
    String body1 = "AMANAPLANPANAMA";
    String body2 = "qwertasdfgzxcvb";
    String body3 = "1234567890";
    
    SolrCore core = h.getCore();
    
    Map<String,String[]> args = new HashMap<String, String[]>();
    args.put( CommonParams.STREAM_BODY, new String[] {body1} );
    
    // Make sure it got a single stream in and out ok
    List<ContentStream> streams = new ArrayList<ContentStream>();
    SolrQueryRequest req = parser.buildRequestFrom( core, new MultiMapSolrParams( args ), streams );
    assertEquals( 1, streams.size() );
    assertEquals( body1, IOUtils.toString( streams.get(0).getReader() ) );
    req.close();

    // Now add three and make sure they come out ok
    streams = new ArrayList<ContentStream>();
    args.put( CommonParams.STREAM_BODY, new String[] {body1,body2,body3} );
    req = parser.buildRequestFrom( core, new MultiMapSolrParams( args ), streams );
    assertEquals( 3, streams.size() );
    ArrayList<String> input  = new ArrayList<String>();
    ArrayList<String> output = new ArrayList<String>();
    input.add( body1 );
    input.add( body2 );
    input.add( body3 );
    output.add( IOUtils.toString( streams.get(0).getReader() ) );
    output.add( IOUtils.toString( streams.get(1).getReader() ) );
    output.add( IOUtils.toString( streams.get(2).getReader() ) );
    // sort them so the output is consistent
    Collections.sort( input );
    Collections.sort( output );
    assertEquals( input.toString(), output.toString() );
    req.close();

    // set the contentType and make sure tat gets set
    String ctype = "text/xxx";
    streams = new ArrayList<ContentStream>();
    args.put( CommonParams.STREAM_CONTENTTYPE, new String[] {ctype} );
    req = parser.buildRequestFrom( core, new MultiMapSolrParams( args ), streams );
    for( ContentStream s : streams ) {
      assertEquals( ctype, s.getContentType() );
    }
    req.close();
  }
  
  @Test
  public void testStreamURL() throws Exception
  {
    boolean ok = false;
    String url = "http://www.apache.org/dist/lucene/solr/";
    byte[] bytes = null;
    try {
      URLConnection connection = new URL(url).openConnection();
      connection.setConnectTimeout(5000);
      connection.setReadTimeout(5000);
      connection.connect();
      bytes = IOUtils.toByteArray( connection.getInputStream());
    }
    catch( Exception ex ) {
      assumeNoException("Unable to connect to " + url + " to run the test.", ex);
      return;
    }

    SolrCore core = h.getCore();
    
    Map<String,String[]> args = new HashMap<String, String[]>();
    args.put( CommonParams.STREAM_URL, new String[] {url} );
    
    // Make sure it got a single stream in and out ok
    List<ContentStream> streams = new ArrayList<ContentStream>();
    SolrQueryRequest req = parser.buildRequestFrom( core, new MultiMapSolrParams( args ), streams );
    assertEquals( 1, streams.size() );
    assertArrayEquals( bytes, IOUtils.toByteArray( streams.get(0).getStream() ) );
    req.close();
  }
  
  @Test
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
  
  @Test
  public void testStandardParseParamsAndFillStreams() throws Exception
  {
    ArrayList<ContentStream> streams = new ArrayList<ContentStream>();
    Map<String,String[]> params = new HashMap<String, String[]>();
    params.put( "q", new String[] { "hello" } );
    
    // Set up the expected behavior
    String[] ct = new String[] {
        "application/x-www-form-urlencoded",
        "Application/x-www-form-urlencoded",
        "application/x-www-form-urlencoded; charset=utf-8",
        "application/x-www-form-urlencoded;"
    };
    
    for( String contentType : ct ) {
      HttpServletRequest request = createMock(HttpServletRequest.class);
      expect(request.getMethod()).andReturn("POST").anyTimes();
      expect(request.getContentType()).andReturn( contentType ).anyTimes();
      expect(request.getParameterMap()).andReturn(params).anyTimes();
      replay(request);
      
      MultipartRequestParser multipart = new MultipartRequestParser( 1000000 );
      RawRequestParser raw = new RawRequestParser();
      StandardRequestParser standard = new StandardRequestParser( multipart, raw );
      
      SolrParams p = standard.parseParamsAndFillStreams( request, streams );
      
      assertEquals( "contentType: "+contentType, "hello", p.get("q") );
    }
  }
}
