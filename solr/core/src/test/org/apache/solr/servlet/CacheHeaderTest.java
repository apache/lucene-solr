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



import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Date;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.cookie.DateUtils;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SuppressForbidden;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A test case for the several HTTP cache headers emitted by Solr
 */
public class CacheHeaderTest extends CacheHeaderTestBase {
  private static File solrHomeDirectory;
    
  @BeforeClass
  public static void beforeTest() throws Exception {
    solrHomeDirectory = createTempDir().toFile();
    setupJettyTestHome(solrHomeDirectory, "collection1");
    createAndStartJetty(solrHomeDirectory.getAbsolutePath());
  }

  @AfterClass
  public static void afterTest() throws Exception {

  }

  protected static final String CONTENTS = "id\n100\n101\n102";

  @Test
  public void testCacheVetoHandler() throws Exception {
    File f=makeFile(CONTENTS);
    HttpRequestBase m=getUpdateMethod("GET", 
        CommonParams.STREAM_FILE, f.getCanonicalPath(),
        CommonParams.STREAM_CONTENTTYPE, "text/csv" );
    HttpResponse response = getClient().execute(m);
    assertEquals(200, response.getStatusLine().getStatusCode());
    checkVetoHeaders(response, true);
    Files.delete(f.toPath());
  }
  
  @Test
  public void testCacheVetoException() throws Exception {
    HttpRequestBase m = getSelectMethod("GET", "q", "xyz_ignore_exception:solr", "qt", "standard");
    // We force an exception from Solr. This should emit "no-cache" HTTP headers
    HttpResponse response = getClient().execute(m);
    assertFalse(response.getStatusLine().getStatusCode() == 200);
    checkVetoHeaders(response, false);
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis to check against expiry headers from Solr")
  protected void checkVetoHeaders(HttpResponse response, boolean checkExpires) throws Exception {
    Header head = response.getFirstHeader("Cache-Control");
    assertNotNull("We got no Cache-Control header", head);
    assertTrue("We got no no-cache in the Cache-Control header ["+head+"]", head.getValue().contains("no-cache"));
    assertTrue("We got no no-store in the Cache-Control header ["+head+"]", head.getValue().contains("no-store"));

    head = response.getFirstHeader("Pragma");
    assertNotNull("We got no Pragma header", head);
    assertEquals("no-cache", head.getValue());

    if (checkExpires) {
      head = response.getFirstHeader("Expires");
      assertNotNull("We got no Expires header:" + Arrays.asList(response.getAllHeaders()), head);
      Date d = DateUtils.parseDate(head.getValue());
      assertTrue("We got no Expires header far in the past", System
          .currentTimeMillis()
          - d.getTime() > 100000);
    }
  }

  @Override
  protected void doLastModified(String method) throws Exception {
    // We do a first request to get the last modified
    // This must result in a 200 OK response
    HttpRequestBase get = getSelectMethod(method);
    HttpResponse response = getClient().execute(get);
    checkResponseBody(method, response);

    assertEquals("Got no response code 200 in initial request", 200, response.
        getStatusLine().getStatusCode());

    Header head = response.getFirstHeader("Last-Modified");
    assertNotNull("We got no Last-Modified header", head);

    Date lastModified = DateUtils.parseDate(head.getValue());

    // If-Modified-Since tests
    get = getSelectMethod(method);
    get.addHeader("If-Modified-Since", DateUtils.formatDate(new Date()));

    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals("Expected 304 NotModified response with current date", 304,
        response.getStatusLine().getStatusCode());

    get = getSelectMethod(method);
    get.addHeader("If-Modified-Since", DateUtils.formatDate(new Date(
        lastModified.getTime() - 10000)));
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals("Expected 200 OK response with If-Modified-Since in the past",
        200, response.getStatusLine().getStatusCode());

    // If-Unmodified-Since tests
    get = getSelectMethod(method);
    get.addHeader("If-Unmodified-Since", DateUtils.formatDate(new Date(
        lastModified.getTime() - 10000)));

    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals(
        "Expected 412 Precondition failed with If-Unmodified-Since in the past",
        412, response.getStatusLine().getStatusCode());

    get = getSelectMethod(method);
    get.addHeader("If-Unmodified-Since", DateUtils
            .formatDate(new Date()));
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals(
        "Expected 200 OK response with If-Unmodified-Since and current date",
        200, response.getStatusLine().getStatusCode());
  }

  // test ETag
  @Override
  protected void doETag(String method) throws Exception {
    HttpRequestBase get = getSelectMethod(method);
    HttpResponse response = getClient().execute(get);
    checkResponseBody(method, response);

    assertEquals("Got no response code 200 in initial request", 200, response
        .getStatusLine().getStatusCode());

    Header head = response.getFirstHeader("ETag");
    assertNotNull("We got no ETag in the response", head);
    assertTrue("Not a valid ETag", head.getValue().startsWith("\"")
        && head.getValue().endsWith("\""));

    String etag = head.getValue();

    // If-None-Match tests
    // we set a non matching ETag
    get = getSelectMethod(method);
    get.addHeader("If-None-Match", "\"xyz123456\"");
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals(
        "If-None-Match: Got no response code 200 in response to non matching ETag",
        200, response.getStatusLine().getStatusCode());

    // now we set matching ETags
    get = getSelectMethod(method);
    get.addHeader("If-None-Match", "\"xyz1223\"");
    get.addHeader("If-None-Match", "\"1231323423\", \"1211211\",   "
        + etag);
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals("If-None-Match: Got no response 304 to matching ETag", 304,
        response.getStatusLine().getStatusCode());

    // we now set the special star ETag
    get = getSelectMethod(method);
    get.addHeader("If-None-Match", "*");
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals("If-None-Match: Got no response 304 for star ETag", 304,
        response.getStatusLine().getStatusCode());

    // If-Match tests
    // we set a non matching ETag
    get = getSelectMethod(method);
    get.addHeader("If-Match", "\"xyz123456\"");
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals(
        "If-Match: Got no response code 412 in response to non matching ETag",
        412, response.getStatusLine().getStatusCode());

    // now we set matching ETags
    get = getSelectMethod(method);
    get.addHeader("If-Match", "\"xyz1223\"");
    get.addHeader("If-Match", "\"1231323423\", \"1211211\",   " + etag);
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals("If-Match: Got no response 200 to matching ETag", 200,
        response.getStatusLine().getStatusCode());

    // now we set the special star ETag
    get = getSelectMethod(method);
    get.addHeader("If-Match", "*");
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals("If-Match: Got no response 200 to star ETag", 200, response
        .getStatusLine().getStatusCode());
  }

  @Override
  protected void doCacheControl(String method) throws Exception {
    if ("POST".equals(method)) {
      HttpRequestBase m = getSelectMethod(method);
      HttpResponse response = getClient().execute(m);
      checkResponseBody(method, response);

      Header head = response.getFirstHeader("Cache-Control");
      assertNull("We got a cache-control header in response to POST", head);

      head = response.getFirstHeader("Expires");
      assertNull("We got an Expires  header in response to POST", head);
    } else {
      HttpRequestBase m = getSelectMethod(method);
      HttpResponse response = getClient().execute(m);
      checkResponseBody(method, response);

      Header head = response.getFirstHeader("Cache-Control");
      assertNotNull("We got no cache-control header", head);

      head = response.getFirstHeader("Expires");
      assertNotNull("We got no Expires header in response", head);
    }
  }

  protected File makeFile(String contents) {
    return makeFile(contents, StandardCharsets.UTF_8.name());
  }

  protected File makeFile(String contents, String charset) {
    try {
      File f = createTempFile("cachetest","csv").toFile();
      try (Writer out = new OutputStreamWriter(new FileOutputStream(f), charset)) {
        out.write(contents);
      }
      return f;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
