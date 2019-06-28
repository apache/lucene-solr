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

import java.util.Date;

import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.cookie.DateUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A test case for the several HTTP cache headers emitted by Solr
 */
public class NoCacheHeaderTest extends CacheHeaderTestBase {
  // TODO: fix this test not to directly use the test-files copied to build/
  // as its home. it could interfere with other tests!
  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(TEST_HOME(), "solr/collection1/conf/solrconfig-nocache.xml", null);
  }

  // The tests
  @Override
  @Test
  public void testLastModified() throws Exception {
    doLastModified("GET");
    doLastModified("HEAD");
  }

  @Override
  @Test
  public void testEtag() throws Exception {
    doETag("GET");
    doETag("HEAD");
  }

  @Override
  @Test
  public void testCacheControl() throws Exception {
    doCacheControl("GET");
    doCacheControl("HEAD");
    doCacheControl("POST");
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis for testing caching headers")
  @Override
  protected void doLastModified(String method) throws Exception {
    // We do a first request to get the last modified
    // This must result in a 200 OK response
    HttpRequestBase get = getSelectMethod(method);
    HttpResponse response = getClient().execute(get);
    checkResponseBody(method, response);

    assertEquals("Got no response code 200 in initial request", 200, response
        .getStatusLine().getStatusCode());

    Header head = response.getFirstHeader("Last-Modified");
    assertNull("We got a Last-Modified header", head);

    // If-Modified-Since tests
    get = getSelectMethod(method);
    get.addHeader("If-Modified-Since", DateUtils.formatDate(new Date()));

    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals("Expected 200 with If-Modified-Since header. We should never get a 304 here", 200,
        response.getStatusLine().getStatusCode());

    get = getSelectMethod(method);
    get.addHeader("If-Modified-Since", DateUtils.formatDate(new Date(System.currentTimeMillis()-10000)));
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals("Expected 200 with If-Modified-Since header. We should never get a 304 here",
        200, response.getStatusLine().getStatusCode());

    // If-Unmodified-Since tests
    get = getSelectMethod(method);
    get.addHeader("If-Unmodified-Since", DateUtils.formatDate(new Date(System.currentTimeMillis()-10000)));

    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals(
        "Expected 200 with If-Unmodified-Since header. We should never get a 304 here",
        200, response.getStatusLine().getStatusCode());

    get = getSelectMethod(method);
    get.addHeader("If-Unmodified-Since", DateUtils.formatDate(new Date()));
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals(
        "Expected 200 with If-Unmodified-Since header. We should never get a 304 here",
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
    assertNull("We got an ETag in the response", head);

    // If-None-Match tests
    // we set a non matching ETag
    get = getSelectMethod(method);
    get.addHeader("If-None-Match", "\"xyz123456\"");
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals(
        "If-None-Match: Got no response code 200 in response to non matching ETag",
        200, response.getStatusLine().getStatusCode());

    // we now set the special star ETag
    get = getSelectMethod(method);
    get.addHeader("If-None-Match", "*");
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals("If-None-Match: Got no response 200 for star ETag", 200,
        response.getStatusLine().getStatusCode());

    // If-Match tests
    // we set a non matching ETag
    get = getSelectMethod(method);
    get.addHeader("If-Match", "\"xyz123456\"");
    response = getClient().execute(get);
    checkResponseBody(method, response);
    assertEquals(
        "If-Match: Got no response code 200 in response to non matching ETag",
        200, response.getStatusLine().getStatusCode());

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
      HttpRequestBase m = getSelectMethod(method);
      HttpResponse response = getClient().execute(m);
      checkResponseBody(method, response);

      Header head = response.getFirstHeader("Cache-Control");
      assertNull("We got a cache-control header in response", head);
      
      head = response.getFirstHeader("Expires");
      assertNull("We got an Expires header in response", head);
  }
}
