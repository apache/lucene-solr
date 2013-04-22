package org.apache.solr.util;
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

import org.apache.commons.io.IOUtils;

import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/**
 * Facilitates testing Solr's REST API via a provided embedded Jetty
 */
public class RestTestHarness extends BaseTestHarness {
  private RESTfulServerProvider serverProvider;
  
  public RestTestHarness(RESTfulServerProvider serverProvider) {
    this.serverProvider = serverProvider;
  }
  
  public String getBaseURL() {
    return serverProvider.getBaseURL();
  }
  
  /**
   * Validates an XML "query" response against an array of XPath test strings
   *
   * @param request the Query to process
   * @return null if all good, otherwise the first test that fails.
   * @exception Exception any exception in the response.
   * @exception java.io.IOException if there is a problem writing the XML
   */
  public String validateQuery(String request, String... tests) throws Exception {

    String res = query(request);
    return validateXPath(res, tests);
  }


  /**
   * Validates an XML PUT response against an array of XPath test strings
   *
   * @param request the PUT request to process
   * @param content the content to send with the PUT request
   * @param tests the validating XPath tests
   * @return null if all good, otherwise the first test that fails.
   * @exception Exception any exception in the response.
   * @exception java.io.IOException if there is a problem writing the XML
   */
  public String validatePut(String request, String content, String... tests) throws Exception {

    String res = put(request, content);
    return validateXPath(res, tests);
  }


  /**
   * Processes a "query" using a URL path (with no context path) + optional query params,
   * e.g. "/schema/fields?indent=on"
   *
   * @param request the URL path and optional query params
   * @return The response to the query
   * @exception Exception any exception in the response.
   */
  public String query(String request) throws Exception {
    URL url = new URL(getBaseURL() + request);
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    InputStream inputStream = null;
    StringWriter strWriter;
    try {
      try {
        inputStream = connection.getInputStream();
      } catch (IOException e) {
        inputStream = connection.getErrorStream();
      }
      strWriter = new StringWriter();
      IOUtils.copy(new InputStreamReader(inputStream, "UTF-8"), strWriter);
    } finally {
      IOUtils.closeQuietly(inputStream);
    }
    return strWriter.toString();
  }

  /**
   * Processes a PUT request using a URL path (with no context path) + optional query params,
   * e.g. "/schema/fields/newfield", PUTs the given content, and returns the response content.
   * 
   * @param request The URL path and optional query params
   * @param content The content to include with the PUT request
   * @return The response to the PUT request
   */
  public String put(String request, String content) throws IOException {
    URL url = new URL(getBaseURL() + request);
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("PUT");
    OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream(), "UTF-8");
    out.write(content);
    out.close();
    InputStream inputStream = null;
    StringWriter stringWriter;
    try {
      try {
        inputStream = connection.getInputStream();
      } catch (IOException e) {
        inputStream = connection.getErrorStream();
      }
      stringWriter = new StringWriter();
      IOUtils.copy(new InputStreamReader(inputStream, "UTF-8"), stringWriter);
    } finally {
      IOUtils.closeQuietly(inputStream);
    }
    return stringWriter.toString();
  }

  /**
   * Processes a POST request using a URL path (with no context path) + optional query params,
   * e.g. "/schema/fields/newfield", PUTs the given content, and returns the response content.
   *
   * @param request The URL path and optional query params
   * @param content The content to include with the POST request
   * @return The response to the PUT request
   */
  public String post(String request, String content) throws IOException {
    URL url = new URL(getBaseURL() + request);
    HttpURLConnection connection = (HttpURLConnection)url.openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("POST");
    connection.setRequestProperty("Content-Type", "application/json; charset=utf-8");

    OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream(), "UTF-8");
    out.write(content);
    out.close();
    InputStream inputStream = null;
    StringWriter stringWriter;
    try {
      try {
        inputStream = connection.getInputStream();
      } catch (IOException e) {
        inputStream = connection.getErrorStream();
      }
      stringWriter = new StringWriter();
      IOUtils.copy(new InputStreamReader(inputStream, "UTF-8"), stringWriter);
    } finally {
      IOUtils.closeQuietly(inputStream);
    }
    return stringWriter.toString();
  }


  public String checkResponseStatus(String xml, String code) throws Exception {
    try {
      String response = query(xml);
      String valid = validateXPath(response, "//int[@name='status']="+code );
      return (null == valid) ? null : response;
    } catch (XPathExpressionException e) {
      throw new RuntimeException("?!? static xpath has bug?", e);
    }
  }

  
  @Override
  public void reload() throws Exception {
    String xml = checkResponseStatus("/admin/cores?action=RELOAD", "0");
    if (null != xml) {
      throw new RuntimeException("RELOAD failed:\n" + xml);
    }
  }
  
  /**
   * Processes an "update" (add, commit or optimize) and
   * returns the response as a String.
   *
   * @param xml The XML of the update
   * @return The XML response to the update
   */
  @Override
  public String update(String xml) {
    try {
      return query("/update?stream.body=" + URLEncoder.encode(xml, "UTF-8"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
