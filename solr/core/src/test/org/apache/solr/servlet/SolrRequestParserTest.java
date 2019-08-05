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

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.servlet.SolrRequestParsers.FormDataRequestParser;
import org.apache.solr.servlet.SolrRequestParsers.MultipartRequestParser;
import org.apache.solr.servlet.SolrRequestParsers.RawRequestParser;
import org.apache.solr.servlet.SolrRequestParsers.StandardRequestParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SolrRequestParserTest extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeWorkingMockito();
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
    
    Map<String,String[]> args = new HashMap<>();
    args.put( CommonParams.STREAM_BODY, new String[] {body1} );
    
    // Make sure it got a single stream in and out ok
    List<ContentStream> streams = new ArrayList<>();
    SolrQueryRequest req = parser.buildRequestFrom( core, new MultiMapSolrParams( args ), streams );
    assertEquals( 1, streams.size() );
    assertEquals( body1, IOUtils.toString( streams.get(0).getReader() ) );
    req.close();

    // Now add three and make sure they come out ok
    streams = new ArrayList<>();
    args.put( CommonParams.STREAM_BODY, new String[] {body1,body2,body3} );
    req = parser.buildRequestFrom( core, new MultiMapSolrParams( args ), streams );
    assertEquals( 3, streams.size() );
    ArrayList<String> input  = new ArrayList<>();
    ArrayList<String> output = new ArrayList<>();
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
    streams = new ArrayList<>();
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
    URL url = getClass().getResource("/README");
    assertNotNull("Missing file 'README' in test-resources root folder.", url);
    
    byte[] bytes = IOUtils.toByteArray(url);

    SolrCore core = h.getCore();
    
    Map<String,String[]> args = new HashMap<>();
    args.put( CommonParams.STREAM_URL, new String[] { url.toExternalForm() } );
    
    // Make sure it got a single stream in and out ok
    List<ContentStream> streams = new ArrayList<>();
    try (SolrQueryRequest req = parser.buildRequestFrom( core, new MultiMapSolrParams( args ), streams )) {
      assertEquals( 1, streams.size() );
      try (InputStream in = streams.get(0).getStream()) {
        assertArrayEquals( bytes, IOUtils.toByteArray( in ) );
      }
    }
  }
  
  @Test
  public void testStreamFile() throws Exception
  {
    File file = getFile("README");
    
    byte[] bytes = FileUtils.readFileToByteArray(file);

    SolrCore core = h.getCore();
    
    Map<String,String[]> args = new HashMap<>();
    args.put( CommonParams.STREAM_FILE, new String[] { file.getAbsolutePath() } );
    
    // Make sure it got a single stream in and out ok
    List<ContentStream> streams = new ArrayList<>();
    try (SolrQueryRequest req = parser.buildRequestFrom( core, new MultiMapSolrParams( args ), streams )) {
      assertEquals( 1, streams.size() );
      try (InputStream in = streams.get(0).getStream()) {
        assertArrayEquals( bytes, IOUtils.toByteArray( in ) );
      }
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
      expectThrows(SolrException.class, () -> SolrRequestParsers.parseQueryString(s));
    }
  }
  
  @Test
  public void testStandardParseParamsAndFillStreams() throws Exception
  {
    final String getParams = "qt=%C3%BC&dup=foo", postParams = "q=hello&d%75p=bar";
    final byte[] postBytes = postParams.getBytes(StandardCharsets.US_ASCII);
    
    // Set up the expected behavior
    final String[] ct = new String[] {
        "application/x-www-form-urlencoded",
        "Application/x-www-form-urlencoded",
        "application/x-www-form-urlencoded; charset=utf-8",
        "application/x-www-form-urlencoded;"
    };
    
    for( String contentType : ct ) {
      HttpServletRequest request = getMock("/solr/select", contentType, postBytes.length);
      when(request.getMethod()).thenReturn("POST");
      when(request.getQueryString()).thenReturn(getParams);
      when(request.getInputStream()).thenReturn(new ByteServletInputStream(postBytes));

      MultipartRequestParser multipart = new MultipartRequestParser( 2048 );
      RawRequestParser raw = new RawRequestParser();
      FormDataRequestParser formdata = new FormDataRequestParser( 2048 );
      StandardRequestParser standard = new StandardRequestParser( multipart, raw, formdata );
      
      SolrParams p = standard.parseParamsAndFillStreams(request, new ArrayList<ContentStream>());
      
      assertEquals( "contentType: "+contentType, "hello", p.get("q") );
      assertEquals( "contentType: "+contentType, "\u00FC", p.get("qt") );
      assertArrayEquals( "contentType: "+contentType, new String[]{"foo","bar"}, p.getParams("dup") );

      verify(request).getInputStream();
    }
  }

  static class ByteServletInputStream extends ServletInputStream  {
    final BufferedInputStream in;
    final int len;
    int readCount = 0;

    public ByteServletInputStream(byte[] data) {
      this.len = data.length;
      this.in = new BufferedInputStream(new ByteArrayInputStream(data));
    }

    @Override
    public boolean isFinished() {
      return readCount == len;
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setReadListener(ReadListener readListener) {
      throw new IllegalStateException("Not supported");
    }

    @Override
    public int read() throws IOException {
      int read = in.read();
      readCount += read;
      return read;
    }
  }
  
  @Test
  public void testStandardParseParamsAndFillStreamsISO88591() throws Exception
  {
    final String getParams = "qt=%FC&dup=foo&ie=iso-8859-1&dup=%FC", postParams = "qt2=%FC&q=hello&d%75p=bar";
    final byte[] postBytes = postParams.getBytes(StandardCharsets.US_ASCII);
    final String contentType = "application/x-www-form-urlencoded; charset=iso-8859-1";
    
    // Set up the expected behavior
    HttpServletRequest request = getMock("/solr/select", contentType, postBytes.length);
    when(request.getMethod()).thenReturn("POST");
    when(request.getQueryString()).thenReturn(getParams);
    when(request.getInputStream()).thenReturn(new ByteServletInputStream(postBytes));
    
    MultipartRequestParser multipart = new MultipartRequestParser( 2048 );
    RawRequestParser raw = new RawRequestParser();
    FormDataRequestParser formdata = new FormDataRequestParser( 2048 );
    StandardRequestParser standard = new StandardRequestParser( multipart, raw, formdata );
    
    SolrParams p = standard.parseParamsAndFillStreams(request, new ArrayList<ContentStream>());
    
    assertEquals( "contentType: "+contentType, "hello", p.get("q") );
    assertEquals( "contentType: "+contentType, "\u00FC", p.get("qt") );
    assertEquals( "contentType: "+contentType, "\u00FC", p.get("qt2") );
    assertArrayEquals( "contentType: "+contentType, new String[]{"foo","\u00FC","bar"}, p.getParams("dup") );

    verify(request).getInputStream();
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
    HttpServletRequest request = getMock("/solr/select", "application/x-www-form-urlencoded", -1);
    when(request.getMethod()).thenReturn("POST");
    when(request.getInputStream()).thenReturn(new ByteServletInputStream(large.toString().getBytes(StandardCharsets.US_ASCII)));

    FormDataRequestParser formdata = new FormDataRequestParser( limitKBytes );
    SolrException e = expectThrows(SolrException.class, () -> {
      formdata.parseParamsAndFillStreams(request, new ArrayList<>());
    });
    assertTrue(e.getMessage().contains("upload limit"));
    assertEquals(400, e.code());
    verify(request).getInputStream();
  }
  
  @Test
  public void testParameterIncompatibilityException1() throws Exception
  {
    HttpServletRequest request = getMock("/solr/select", "application/x-www-form-urlencoded", 100);
    // we emulate Jetty that returns empty stream when parameters were parsed before:
    when(request.getInputStream()).thenReturn(new ServletInputStream() {
      @Override public int read() { return -1; }

      @Override
      public boolean isFinished() {
        return true;
      }

      @Override
      public boolean isReady() {
        return true;
      }

      @Override
      public void setReadListener(ReadListener readListener) {

      }
    });

    FormDataRequestParser formdata = new FormDataRequestParser( 2048 );
    SolrException e = expectThrows(SolrException.class, () -> {
      formdata.parseParamsAndFillStreams(request, new ArrayList<>());
    });
    assertTrue(e.getMessage().startsWith("Solr requires that request parameters"));
    assertEquals(500, e.code());
    verify(request).getInputStream();
  }
  
  @Test
  public void testParameterIncompatibilityException2() throws Exception
  {
    HttpServletRequest request = getMock("/solr/select", "application/x-www-form-urlencoded", 100);
    when(request.getMethod()).thenReturn("POST");
    // we emulate Tomcat that throws IllegalStateException when parameters were parsed before:
    when(request.getInputStream()).thenThrow(new IllegalStateException());

    FormDataRequestParser formdata = new FormDataRequestParser( 2048 );
    SolrException e = expectThrows(SolrException.class, () -> {
      formdata.parseParamsAndFillStreams(request, new ArrayList<>());
    });
    assertTrue(e.getMessage().startsWith("Solr requires that request parameters"));
    assertEquals(500, e.code());
    verify(request).getInputStream();
  }
  
  @Test
  public void testAddHttpRequestToContext() throws Exception {
    HttpServletRequest request = getMock("/solr/select", null, -1);
    when(request.getMethod()).thenReturn("GET");
    when(request.getQueryString()).thenReturn("q=title:solr");
    Map<String, String> headers = new HashMap<>();
    headers.put("X-Forwarded-For", "10.0.0.1");
    when(request.getHeaderNames()).thenReturn(new Vector<>(headers.keySet()).elements());
    for(Map.Entry<String,String> entry:headers.entrySet()) {
      Vector<String> v = new Vector<>();
      v.add(entry.getValue());
      when(request.getHeaders(entry.getKey())).thenReturn(v.elements());
    }

    SolrRequestParsers parsers = new SolrRequestParsers(h.getCore().getSolrConfig());
    assertFalse(parsers.isAddRequestHeadersToContext());
    SolrQueryRequest solrReq = parsers.parse(h.getCore(), "/select", request);
    assertFalse(solrReq.getContext().containsKey("httpRequest"));
    
    parsers.setAddRequestHeadersToContext(true);
    solrReq = parsers.parse(h.getCore(), "/select", request);
    assertEquals(request, solrReq.getContext().get("httpRequest"));
    assertEquals("10.0.0.1", ((HttpServletRequest)solrReq.getContext().get("httpRequest")).getHeaders("X-Forwarded-For").nextElement());
    
  }

  public void testPostMissingContentType() throws Exception {
    HttpServletRequest request = getMock();
    when(request.getMethod()).thenReturn("POST");

    SolrRequestParsers parsers = new SolrRequestParsers(h.getCore().getSolrConfig());
    try {
      parsers.parse(h.getCore(), "/select", request);
    } catch (SolrException e) {
      log.error("should not throw SolrException", e);
      fail("should not throw SolrException");
    }
  }

  @Test
  public void testAutoDetect() throws Exception {
    String curl = "curl/7.30.0";
    for (String method : new String[]{"GET","POST"}) {
      doAutoDetect(null, method, "{}=a", null,                "{}", "a");  // unknown agent should not auto-detect
      doAutoDetect(curl, method, "{}",   "application/json", null, null);  // curl should auto-detect
      doAutoDetect(curl, method, "  \t\n\r  {}  ", "application/json", null, null); // starting with whitespace
      doAutoDetect(curl, method, "  \t\n\r  // how now brown cow\n {}  ", "application/json", null, null);     // supporting comments
      doAutoDetect(curl, method, "  \t\n\r  #different style comment\n {}  ", "application/json", null, null);
      doAutoDetect(curl, method, "  \t\n\r  /* C style comment */\n {}  ", "application/json", null, null);
      doAutoDetect(curl, method, "  \t\n\r  <tag>hi</tag>  ", "text/xml", null, null);

      doAutoDetect(curl, method, "  \t\r\n  aaa=1&bbb=2&ccc=3",   null, "bbb", "2");  // params with whitespace first
      doAutoDetect(curl, method, "/x=foo&aaa=1&bbb=2&ccc=3",   null, "/x", "foo");  // param name that looks like a path
      doAutoDetect(curl, method, " \t\r\n /x=foo&aaa=1&bbb=2&ccc=3",   null, "bbb", "2");  // param name that looks like a path
    }
  }

  public void doAutoDetect(String userAgent, String method, final String body, String expectedContentType, String expectedKey, String expectedValue) throws Exception {
    String uri = "/solr/select";
    String contentType = "application/x-www-form-urlencoded";
    int contentLength = -1;  // does this mean auto-detect?

    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getHeader("User-Agent")).thenReturn(userAgent);
    when(request.getRequestURI()).thenReturn(uri);
    when(request.getContentType()).thenReturn(contentType);
    when(request.getContentLength()).thenReturn(contentLength);

    when(request.getMethod()).thenReturn(method);
    // we dont pass a content-length to let the security mechanism limit it:
    when(request.getQueryString()).thenReturn("foo=1&bar=2");
    when(request.getInputStream()).thenReturn(new ByteServletInputStream(body.getBytes(StandardCharsets.US_ASCII)));

    SolrRequestParsers parsers = new SolrRequestParsers(h.getCore().getSolrConfig());
    SolrQueryRequest req = parsers.parse(h.getCore(), "/select", request);
    int num=0;
    if (expectedContentType != null) {
      for (ContentStream cs : req.getContentStreams()) {
        num++;
        assertTrue(cs.getContentType().startsWith(expectedContentType));
        String returnedBody = IOUtils.toString(cs.getReader());
        assertEquals(body, returnedBody);
      }
      assertEquals(1, num);
    }

    assertEquals("1", req.getParams().get("foo"));
    assertEquals("2", req.getParams().get("bar"));

    if (expectedKey != null) {
      assertEquals(expectedValue, req.getParams().get(expectedKey));
    }

    req.close();
    verify(request).getInputStream();
  }


  public HttpServletRequest getMock() {
    return getMock("/solr/select", null, -1);
  }

  public HttpServletRequest getMock(String uri, String contentType, int contentLength) {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getRequestURI()).thenReturn(uri);
    when(request.getContentType()).thenReturn(contentType);
    when(request.getContentLength()).thenReturn(contentLength);
    return request;
  }

}
