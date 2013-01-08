/*
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

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
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
  public static void afterClass() {
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
    String url = "http://www.apache.org/dist/lucene/solr/";
    byte[] bytes = null;
    try {
      URL u = new URL(url);
      HttpURLConnection connection = (HttpURLConnection)u.openConnection();
      connection.setConnectTimeout(5000);
      connection.setReadTimeout(5000);
      connection.connect();
      int code = connection.getResponseCode();
      assumeTrue("wrong response code from server: " + code, 200 == code);
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
    try {
      assertArrayEquals( bytes, IOUtils.toByteArray( streams.get(0).getStream() ) );
    } catch (SocketTimeoutException ex) {
      assumeNoException("Problems retrieving from " + url + " to run the test.", ex);
    } finally {
      req.close();
    }
  }
  
  @Test
  public void testUrlParamParsing() throws Exception
  {
    final String[][] teststr = new String[][] {
      { "this is simple", "this%20is%20simple" },
      { "this is simple", "this+is+simple" },
      { "\u00FC", "%C3%BC" },   // lower-case "u" with diaeresis/umlaut
      { "\u0026", "%26" },      // &
      { "", "" },               // empty
      { "\u20AC", "%E2%82%ac" } // euro, also with lowercase escapes
    };
    
    for( String[] tst : teststr ) {
      SolrParams params = SolrRequestParsers.parseQueryString( "val="+tst[1] );
      assertEquals( tst[0], params.get( "val" ) );
      params = SolrRequestParsers.parseQueryString( "val="+tst[1]+"&" );
      assertEquals( tst[0], params.get( "val" ) );
      params = SolrRequestParsers.parseQueryString( "&&val="+tst[1]+"&" );
      assertEquals( tst[0], params.get( "val" ) );
      params = SolrRequestParsers.parseQueryString( "&&val="+tst[1]+"&&&val="+tst[1]+"&" );
      assertArrayEquals(new String[]{tst[0],tst[0]}, params.getParams("val") );
   }
    
    SolrParams params = SolrRequestParsers.parseQueryString("val");
    assertEquals("", params.get("val"));
    
    params = SolrRequestParsers.parseQueryString("val&foo=bar=bar&muh&");
    assertEquals("", params.get("val"));
    assertEquals("bar=bar", params.get("foo"));
    assertEquals("", params.get("muh"));
    
    final String[] invalid = {
      "q=h%FCllo",     // non-UTF-8
      "q=h\u00FCllo",  // encoded string is not pure US-ASCII
      "q=hallo%",      // incomplete escape
      "q=hallo%1",     // incomplete escape
      "q=hallo%XX123", // invalid digit 'X' in escape
      "=hallo"         // missing key
    };
    for (String s : invalid) {
      try {
        SolrRequestParsers.parseQueryString(s);
        fail("Should throw SolrException");
      } catch (SolrException se) {
        // pass
      }
    }
  }
  
  @Test
  public void testStandardParseParamsAndFillStreams() throws Exception
  {
    final String getParams = "qt=%C3%BC&dup=foo", postParams = "q=hello&d%75p=bar";
    final byte[] postBytes = postParams.getBytes("US-ASCII");
    
    // Set up the expected behavior
    final String[] ct = new String[] {
        "application/x-www-form-urlencoded",
        "Application/x-www-form-urlencoded",
        "application/x-www-form-urlencoded; charset=utf-8",
        "application/x-www-form-urlencoded;"
    };
    
    for( String contentType : ct ) {
      HttpServletRequest request = createMock(HttpServletRequest.class);
      expect(request.getMethod()).andReturn("POST").anyTimes();
      expect(request.getContentType()).andReturn( contentType ).anyTimes();
      expect(request.getQueryString()).andReturn(getParams).anyTimes();
      expect(request.getContentLength()).andReturn(postBytes.length).anyTimes();
      expect(request.getInputStream()).andReturn(new ServletInputStream() {
        private final ByteArrayInputStream in = new ByteArrayInputStream(postBytes);
        @Override public int read() { return in.read(); }
      });
      replay(request);
      
      MultipartRequestParser multipart = new MultipartRequestParser( 2048 );
      RawRequestParser raw = new RawRequestParser();
      FormDataRequestParser formdata = new FormDataRequestParser( 2048 );
      StandardRequestParser standard = new StandardRequestParser( multipart, raw, formdata );
      
      SolrParams p = standard.parseParamsAndFillStreams(request, new ArrayList<ContentStream>());
      
      assertEquals( "contentType: "+contentType, "hello", p.get("q") );
      assertEquals( "contentType: "+contentType, "\u00FC", p.get("qt") );
      assertArrayEquals( "contentType: "+contentType, new String[]{"foo","bar"}, p.getParams("dup") );
    }
  }
  
  @Test
  public void testStandardFormdataUploadLimit() throws Exception
  {
    final int limitKBytes = 128;

    final StringBuilder large = new StringBuilder("q=hello");
    // grow exponentially to reach 128 KB limit:
    while (large.length() <= limitKBytes * 1024) {
      large.append('&').append(large);
    }
    HttpServletRequest request = createMock(HttpServletRequest.class);
    expect(request.getMethod()).andReturn("POST").anyTimes();
    expect(request.getContentType()).andReturn("application/x-www-form-urlencoded").anyTimes();
    // we dont pass a content-length to let the security mechanism limit it:
    expect(request.getContentLength()).andReturn(-1).anyTimes();
    expect(request.getQueryString()).andReturn(null).anyTimes();
    expect(request.getInputStream()).andReturn(new ServletInputStream() {
      private final ByteArrayInputStream in = new ByteArrayInputStream(large.toString().getBytes("US-ASCII"));
      @Override public int read() { return in.read(); }
    });
    replay(request);
    
    FormDataRequestParser formdata = new FormDataRequestParser( limitKBytes );    
    try {
      formdata.parseParamsAndFillStreams(request, new ArrayList<ContentStream>());
      fail("should throw SolrException");
    } catch (SolrException solre) {
      assertTrue(solre.getMessage().contains("upload limit"));
      assertEquals(400, solre.code());
    }
  }
  
  @Test
  public void testParameterIncompatibilityException1() throws Exception
  {
    HttpServletRequest request = createMock(HttpServletRequest.class);
    expect(request.getMethod()).andReturn("POST").anyTimes();
    expect(request.getContentType()).andReturn("application/x-www-form-urlencoded").anyTimes();
    expect(request.getContentLength()).andReturn(100).anyTimes();
    expect(request.getQueryString()).andReturn(null).anyTimes();
    // we emulate Jetty that returns empty stream when parameters were parsed before:
    expect(request.getInputStream()).andReturn(new ServletInputStream() {
      @Override public int read() { return -1; }
    });
    replay(request);
    
    FormDataRequestParser formdata = new FormDataRequestParser( 2048 );    
    try {
      formdata.parseParamsAndFillStreams(request, new ArrayList<ContentStream>());
      fail("should throw SolrException");
    } catch (SolrException solre) {
      assertTrue(solre.getMessage().startsWith("Solr requires that request parameters"));
      assertEquals(500, solre.code());
    }
  }
  
  @Test
  public void testParameterIncompatibilityException2() throws Exception
  {
    HttpServletRequest request = createMock(HttpServletRequest.class);
    expect(request.getMethod()).andReturn("POST").anyTimes();
    expect(request.getContentType()).andReturn("application/x-www-form-urlencoded").anyTimes();
    expect(request.getContentLength()).andReturn(100).anyTimes();
    expect(request.getQueryString()).andReturn(null).anyTimes();
    // we emulate Tomcat that throws IllegalStateException when parameters were parsed before:
    expect(request.getInputStream()).andThrow(new IllegalStateException());
    replay(request);
    
    FormDataRequestParser formdata = new FormDataRequestParser( 2048 );    
    try {
      formdata.parseParamsAndFillStreams(request, new ArrayList<ContentStream>());
      fail("should throw SolrException");
    } catch (SolrException solre) {
      assertTrue(solre.getMessage().startsWith("Solr requires that request parameters"));
      assertEquals(500, solre.code());
    }
  }
}
