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
package org.apache.solr.util;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.servlet.SolrRequestParsers;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.SortedMap;

abstract public class RestTestBase extends SolrJettyTestBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static RestTestHarness restTestHarness;

  @AfterClass
  public static void cleanUpHarness() throws IOException {
    if (restTestHarness != null) {
      restTestHarness.close();
    }
    restTestHarness = null;
  }

  public static void createJettyAndHarness
      (String solrHome, String configFile, String schemaFile, String context,
       boolean stopAtShutdown, SortedMap<ServletHolder,String> extraServlets) throws Exception {

    createAndStartJetty(solrHome, configFile, schemaFile, context, stopAtShutdown, extraServlets);

    restTestHarness = new RestTestHarness(() -> jetty.getBaseUrl().toString() + "/" + DEFAULT_TEST_CORENAME);
  }

  /** Validates an update XML String is successful
   */
  public static void assertU(String update) {
    assertU(null, update);
  }

  /** Validates an update XML String is successful
   */
  public static void assertU(String message, String update) {
    checkUpdateU(message, update, true);
  }

  /** Validates an update XML String failed
   */
  public static void assertFailedU(String update) {
    assertFailedU(null, update);
  }

  /** Validates an update XML String failed
   */
  public static void assertFailedU(String message, String update) {
    checkUpdateU(message, update, false);
  }

  /** Checks the success or failure of an update message
   */
  private static void checkUpdateU(String message, String update, boolean shouldSucceed) {
    try {
      String m = (null == message) ? "" : message + " ";
      if (shouldSucceed) {
        String response = restTestHarness.validateUpdate(update);
        if (response != null) fail(m + "update was not successful: " + response);
      } else {
        String response = restTestHarness.validateErrorUpdate(update);
        if (response != null) fail(m + "update succeeded, but should have failed: " + response);
      }
    } catch (SAXException e) {
      throw new RuntimeException("Invalid XML", e);
    }
  }

  /** 
   * Validates a query matches some XPath test expressions
   * 
   * @param request a URL path with optional query params, e.g. "/schema/fields?fl=id,_version_" 
   */
  public static void assertQ(String request, String... tests) {
    try {
      int queryStartPos = request.indexOf('?');
      String query;
      String path;
      if (-1 == queryStartPos) {
        query = "";
        path = request;
      } else {
        query = request.substring(queryStartPos + 1);
        path = request.substring(0, queryStartPos);
      }
      if ( ! query.matches(".*wt=schema\\.xml.*")) { // don't overwrite wt=schema.xml
        query = setParam(query, "wt", "xml");
      }
      request = path + '?' + setParam(query, "indent", "on");

      String response = restTestHarness.query(request);

      // TODO: should the facet handling below be converted to parse the URL?
      /*
      if (req.getParams().getBool("facet", false)) {
        // add a test to ensure that faceting did not throw an exception
        // internally, where it would be added to facet_counts/exception
        String[] allTests = new String[tests.length+1];
        System.arraycopy(tests,0,allTests,1,tests.length);
        allTests[0] = "*[count(//lst[@name='facet_counts']/*[@name='exception'])=0]";
        tests = allTests;
      }
      */

      String results = TestHarness.validateXPath(response, tests);

      if (null != results) {
        String msg = "REQUEST FAILED: xpath=" + results
            + "\n\txml response was: " + response
            + "\n\trequest was:" + request;

        log.error(msg);
        throw new RuntimeException(msg);
      }

    } catch (XPathExpressionException e1) {
      throw new RuntimeException("XPath is invalid", e1);
    } catch (Exception e2) {
      SolrException.log(log, "REQUEST FAILED: " + request, e2);
      throw new RuntimeException("Exception during query", e2);
    }
  }

  /**
   *  Makes a query request and returns the JSON string response 
   *
   * @param request a URL path with optional query params, e.g. "/schema/fields?fl=id,_version_" 
   */
  public static String JQ(String request) throws Exception {
    int queryStartPos = request.indexOf('?');
    String query;
    String path;
    if (-1 == queryStartPos) {
      query = "";
      path = request;
    } else {
      query = request.substring(queryStartPos + 1);
      path = request.substring(0, queryStartPos);
    }
    query = setParam(query, "wt", "json");
    request = path + '?' + setParam(query, "indent", "on"); 

    String response;
    boolean failed=true;
    try {
      response = restTestHarness.query(request);
      failed = false;
    } finally {
      if (failed) {
        log.error("REQUEST FAILED: {}", request);
      }
    }

    return response;
  }

  /**
   * Validates a query matches some JSON test expressions using the default double delta tolerance.
   * @see org.apache.solr.JSONTestUtil#DEFAULT_DELTA
   * @see #assertJQ(String,double,String...)
   */
  public static void assertJQ(String request, String... tests) throws Exception {
    assertJQ(request, JSONTestUtil.DEFAULT_DELTA, tests);
  }

  /**
   * Validates a query matches some JSON test expressions and closes the
   * query. The text expression is of the form path:JSON.  To facilitate
   * easy embedding in Java strings, the JSON can have double quotes
   * replaced with single quotes.
   * <p>
   * Please use this with care: this makes it easy to match complete
   * structures, but doing so can result in fragile tests if you are
   * matching more than what you want to test.
   * </p>
   * @param request a URL path with optional query params, e.g. "/schema/fields?fl=id,_version_"
   * @param delta tolerance allowed in comparing float/double values
   * @param tests JSON path expression + '==' + expected value
   */
  public static void assertJQ(String request, double delta, String... tests) throws Exception {
    int queryStartPos = request.indexOf('?');
    String query;
    String path;
    if (-1 == queryStartPos) {
      query = "";
      path = request;
    } else {
      query = request.substring(queryStartPos + 1);
      path = request.substring(0, queryStartPos);
    }
    query = setParam(query, "wt", "json");
    request = path + '?' + setParam(query, "indent", "on");

    String response;
    boolean failed = true;
    try {
      response = restTestHarness.query(request);
      failed = false;
    } finally {
      if (failed) {
        log.error("REQUEST FAILED: {}", request);
      }
    }

    for (String test : tests) {
      if (null == test || 0 == test.length()) continue;
      String testJSON = json(test);

      try {
        failed = true;
        String err = JSONTestUtil.match(response, testJSON, delta);
        failed = false;
        if (err != null) {
          log.error("query failed JSON validation. error={}"
              + "\n expected ={}\n response = {}\n request = {}\n"
              , err, testJSON, response, request);
          throw new RuntimeException(err);
        }
      } finally {
        if (failed) {
          log.error("JSON query validation threw an exception."
              +"\n expected ={}\n response = {}\n request = {}\n"
              , testJSON, response, request);
        }
      }
    }
  }

  
  
  /**
   * Validates the response from a PUT request matches some JSON test expressions
   * 
   * @see org.apache.solr.JSONTestUtil#DEFAULT_DELTA
   * @see #assertJQ(String,double,String...)
   */
  public static void assertJPut(String request, String content, String... tests) throws Exception {
    assertJPut(request, content, JSONTestUtil.DEFAULT_DELTA, tests);
  }


  /**
   * Validates the response from a PUT request matches some JSON test expressions
   * and closes the query. The text expression is of the form path==JSON.
   * To facilitate easy embedding in Java strings, the JSON can have double
   * quotes replaced with single quotes.
   * <p>
   * Please use this with care: this makes it easy to match complete
   * structures, but doing so can result in fragile tests if you are
   * matching more than what you want to test.
   * </p>
   * @param request a URL path with optional query params, e.g. "/schema/fields?fl=id,_version_"
   * @param content The content to include with the PUT request
   * @param delta tolerance allowed in comparing float/double values
   * @param tests JSON path expression + '==' + expected value
   */
  public static void assertJPut(String request, String content, double delta, String... tests) throws Exception {
    int queryStartPos = request.indexOf('?');
    String query;
    String path;
    if (-1 == queryStartPos) {
      query = "";
      path = request;
    } else {
      query = request.substring(queryStartPos + 1);
      path = request.substring(0, queryStartPos);
    }
    query = setParam(query, "wt", "json");
    request = path + '?' + setParam(query, "indent", "on");

    String response;
    boolean failed = true;
    try {
      response = restTestHarness.put(request, content);
      failed = false;
    } finally {
      if (failed) {
        log.error("REQUEST FAILED: {}", request);
      }
    }

    for (String test : tests) {
      if (null == test || 0 == test.length()) continue;
      String testJSON = json(test);

      try {
        failed = true;
        String err = JSONTestUtil.match(response, testJSON, delta);
        failed = false;
        if (err != null) {
          log.error("query failed JSON validation. error={}"
              + "\n expected ={}\n response = {}\n request = {}\n"
              ,err, testJSON, response, request);
          throw new RuntimeException(err);
        }
      } finally {
        if (failed) {
          log.error("JSON query validation threw an exception."
              + "\n expected ={}\n response = {}\n request = {}"
              , testJSON, response, request);
        }
      }
    }
  }

  /**
   * Validates the response from a POST request matches some JSON test expressions
   *
   * @see org.apache.solr.JSONTestUtil#DEFAULT_DELTA
   * @see #assertJQ(String,double,String...)
   */
  public static void assertJPost(String request, String content, String... tests) throws Exception {
    assertJPost(request, content, JSONTestUtil.DEFAULT_DELTA, tests);
  }


  /**
   * Validates the response from a PUT request matches some JSON test expressions
   * and closes the query. The text expression is of the form path==JSON.
   * To facilitate easy embedding in Java strings, the JSON can have double
   * quotes replaced with single quotes.
   * <p>
   * Please use this with care: this makes it easy to match complete
   * structures, but doing so can result in fragile tests if you are
   * matching more than what you want to test.
   * </p>
   * @param request a URL path with optional query params, e.g. "/schema/fields?fl=id,_version_"
   * @param content The content to include with the PUT request
   * @param delta tolerance allowed in comparing float/double values
   * @param tests JSON path expression + '==' + expected value
   */
  public static void assertJPost(String request, String content, double delta, String... tests) throws Exception {
    int queryStartPos = request.indexOf('?');
    String query;
    String path;
    if (-1 == queryStartPos) {
      query = "";
      path = request;
    } else {
      query = request.substring(queryStartPos + 1);
      path = request.substring(0, queryStartPos);
    }
    query = setParam(query, "wt", "json");
    request = path + '?' + setParam(query, "indent", "on");

    String response;
    boolean failed = true;
    try {
      response = restTestHarness.post(request, content);
      failed = false;
    } finally {
      if (failed) {
        log.error("REQUEST FAILED: {}", request);
      }
    }

    for (String test : tests) {
      if (null == test || 0 == test.length()) continue;
      String testJSON = json(test);

      try {
        failed = true;
        String err = JSONTestUtil.match(response, testJSON, delta);
        failed = false;
        if (err != null) {
          log.error("query failed JSON validation. error={}"
              + "\n expected ={}\n response = {}\n request = {}\n"
              , err, testJSON, response, request);
          throw new RuntimeException(err);
        }
      } finally {
        if (failed) {
          log.error("JSON query validation threw an exception." +
              "\n expected ={}\n response = {}\n request = {}\n"
              ,testJSON, response, request);
        }
      }
    }
  }
  
  /**
   * Deletes a resource and then matches some JSON test expressions against the 
   * response using the default double delta tolerance.
   * @see org.apache.solr.JSONTestUtil#DEFAULT_DELTA
   * @see #assertJDelete(String,double,String...)
   */
  public static void assertJDelete(String request, String... tests) throws Exception {
    assertJDelete(request, JSONTestUtil.DEFAULT_DELTA, tests);
  }

  /**
   * Deletes a resource and then matches some JSON test expressions against the 
   * response using the specified double delta tolerance.
   */
  public static void assertJDelete(String request, double delta, String... tests) throws Exception {
    int queryStartPos = request.indexOf('?');
    String query;
    String path;
    if (-1 == queryStartPos) {
      query = "";
      path = request;
    } else {
      query = request.substring(queryStartPos + 1);
      path = request.substring(0, queryStartPos);
    }
    query = setParam(query, "wt", "json");
    request = path + '?' + setParam(query, "indent", "on");

    String response;
    boolean failed = true;
    try {
      response = restTestHarness.delete(request);
      failed = false;
    } finally {
      if (failed) {
        log.error("REQUEST FAILED: {}", request);
      }
    }

    for (String test : tests) {
      if (null == test || 0 == test.length()) continue;
      String testJSON = json(test);

      try {
        failed = true;
        String err = JSONTestUtil.match(response, testJSON, delta);
        failed = false;
        if (err != null) {
          log.error("query failed JSON validation. error={}\n expected ={}"
              + "\n response = {}\n request = {}"
              , err, testJSON, response, request);
          throw new RuntimeException(err);
        }
      } finally {
        if (failed) {
          log.error("JSON query validation threw an exception.\n expected ={}\n"
              + "\n response = {}\n request = {}"
              , testJSON, response, request
          );
        }
      }
    }
  }

  /**
   * Insures that the given param is included in the query with the given value.
   *
   * <ol>
   *   <li>If the param is already included with the given value, the request is returned unchanged.</li>
   *   <li>If the param is not already included, it is added with the given value.</li>
   *   <li>If the param is already included, but with a different value, the value is replaced with the given value.</li>
   *   <li>If the param is already included multiple times, they are replaced with a single param with given value.</li>
   * </ol>
   *
   * The passed-in valueToSet should NOT be URL encoded, as it will be URL encoded by this method.
   *
   * @param query The query portion of a request URL, e.g. "wt=xml&indent=off&fl=id,_version_"
   * @param paramToSet The parameter name to insure the presence of in the returned request 
   * @param valueToSet The parameter value to insure in the returned request
   * @return The query with the given param set to the given value 
   */
  private static String setParam(String query, String paramToSet, String valueToSet) {
    if (null == valueToSet) {
      valueToSet = "";
    }
    try {
      StringBuilder builder = new StringBuilder();
      if (null == query || query.trim().isEmpty()) {
        // empty query -> return "paramToSet=valueToSet"
        builder.append(paramToSet);
        builder.append('=');
        StrUtils.partialURLEncodeVal(builder, valueToSet);
        return builder.toString();
      }
      MultiMapSolrParams requestParams = SolrRequestParsers.parseQueryString(query);
      String[] values = requestParams.getParams(paramToSet);
      if (null == values) {
        // paramToSet isn't present in the request -> append "&paramToSet=valueToSet"
        builder.append(query);
        builder.append('&');
        builder.append(paramToSet);
        builder.append('=');
        StrUtils.partialURLEncodeVal(builder, valueToSet);
        return builder.toString();
      }
      if (1 == values.length && valueToSet.equals(values[0])) {
        // paramToSet=valueToSet is already in the query - just return the query as-is.
        return query;
      }
      // More than one value for paramToSet on the request, or paramToSet's value is not valueToSet
      // -> rebuild the query
      boolean isFirst = true;
      for (Map.Entry<String,String[]> entry : requestParams.getMap().entrySet()) {
        String key = entry.getKey();
        String[] valarr = entry.getValue();

        if ( ! key.equals(paramToSet)) {
          for (String val : valarr) {
            builder.append(isFirst ? "" : '&');
            isFirst = false;
            builder.append(key);
            builder.append('=');
            StrUtils.partialURLEncodeVal(builder, null == val ? "" : val);
          }
        }
      }
      builder.append(isFirst ? "" : '&');
      builder.append(paramToSet);
      builder.append('=');
      StrUtils.partialURLEncodeVal(builder, valueToSet);
      return builder.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}  
