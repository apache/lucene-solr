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

import java.util.Date;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpMethodBase;
import org.apache.commons.httpclient.util.DateUtil;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * A test case for the several HTTP cache headers emitted by Solr
 */
public class NoCacheHeaderTest extends CacheHeaderTestBase {
  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(TEST_HOME(), "solr/conf/solrconfig-nocache.xml", null);
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
  
  @Override
  protected void doLastModified(String method) throws Exception {
    // We do a first request to get the last modified
    // This must result in a 200 OK response
    HttpMethodBase get = getSelectMethod(method);
    getClient().executeMethod(get);
    checkResponseBody(method, get);

    assertEquals("Got no response code 200 in initial request", 200, get
        .getStatusCode());

    Header head = get.getResponseHeader("Last-Modified");
    assertNull("We got a Last-Modified header", head);

    // If-Modified-Since tests
    get = getSelectMethod(method);
    get.addRequestHeader("If-Modified-Since", DateUtil.formatDate(new Date()));

    getClient().executeMethod(get);
    checkResponseBody(method, get);
    assertEquals("Expected 200 with If-Modified-Since header. We should never get a 304 here", 200,
        get.getStatusCode());

    get = getSelectMethod(method);
    get.addRequestHeader("If-Modified-Since", DateUtil.formatDate(new Date(System.currentTimeMillis()-10000)));
    getClient().executeMethod(get);
    checkResponseBody(method, get);
    assertEquals("Expected 200 with If-Modified-Since header. We should never get a 304 here",
        200, get.getStatusCode());

    // If-Unmodified-Since tests
    get = getSelectMethod(method);
    get.addRequestHeader("If-Unmodified-Since", DateUtil.formatDate(new Date(System.currentTimeMillis()-10000)));

    getClient().executeMethod(get);
    checkResponseBody(method, get);
    assertEquals(
        "Expected 200 with If-Unmodified-Since header. We should never get a 304 here",
        200, get.getStatusCode());

    get = getSelectMethod(method);
    get
        .addRequestHeader("If-Unmodified-Since", DateUtil
            .formatDate(new Date()));
    getClient().executeMethod(get);
    checkResponseBody(method, get);
    assertEquals(
        "Expected 200 with If-Unmodified-Since header. We should never get a 304 here",
        200, get.getStatusCode());
  }

  // test ETag
  @Override
  protected void doETag(String method) throws Exception {
    HttpMethodBase get = getSelectMethod(method);
    getClient().executeMethod(get);
    checkResponseBody(method, get);

    assertEquals("Got no response code 200 in initial request", 200, get
        .getStatusCode());

    Header head = get.getResponseHeader("ETag");
    assertNull("We got an ETag in the response", head);

    // If-None-Match tests
    // we set a non matching ETag
    get = getSelectMethod(method);
    get.addRequestHeader("If-None-Match", "\"xyz123456\"");
    getClient().executeMethod(get);
    checkResponseBody(method, get);
    assertEquals(
        "If-None-Match: Got no response code 200 in response to non matching ETag",
        200, get.getStatusCode());

    // we now set the special star ETag
    get = getSelectMethod(method);
    get.addRequestHeader("If-None-Match", "*");
    getClient().executeMethod(get);
    checkResponseBody(method, get);
    assertEquals("If-None-Match: Got no response 200 for star ETag", 200, get
        .getStatusCode());

    // If-Match tests
    // we set a non matching ETag
    get = getSelectMethod(method);
    get.addRequestHeader("If-Match", "\"xyz123456\"");
    getClient().executeMethod(get);
    checkResponseBody(method, get);
    assertEquals(
        "If-Match: Got no response code 200 in response to non matching ETag",
        200, get.getStatusCode());

    // now we set the special star ETag
    get = getSelectMethod(method);
    get.addRequestHeader("If-Match", "*");
    getClient().executeMethod(get);
    checkResponseBody(method, get);
    assertEquals("If-Match: Got no response 200 to star ETag", 200, get
        .getStatusCode());
  }

  @Override
  protected void doCacheControl(String method) throws Exception {
      HttpMethodBase m = getSelectMethod(method);
      getClient().executeMethod(m);
      checkResponseBody(method, m);

      Header head = m.getResponseHeader("Cache-Control");
      assertNull("We got a cache-control header in response", head);
      
      head = m.getResponseHeader("Expires");
      assertNull("We got an Expires header in response", head);
  }
}