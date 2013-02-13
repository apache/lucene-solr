/**
 * 
 */
package org.apache.solr.client.solrj.impl;

import static org.junit.Assert.*;

import java.net.MalformedURLException;

import org.apache.solr.client.solrj.ResponseParser;
import org.junit.Test;

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

import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * Test the LBHttpSolrServer.
 */
public class LBHttpSolrServerTest {
  
  /**
   * Test method for {@link org.apache.solr.client.solrj.impl.LBHttpSolrServer#LBHttpSolrServer(org.apache.http.client.HttpClient, org.apache.solr.client.solrj.ResponseParser, java.lang.String[])}.
   * 
   * Validate that the parser passed in is used in the <code>HttpSolrServer</code> instances created.
   * 
   * @throws MalformedURLException If URL is invalid, no URL passed, so won't happen.
   */
  @Test
  public void testLBHttpSolrServerHttpClientResponseParserStringArray() throws MalformedURLException {
    LBHttpSolrServer testServer = new LBHttpSolrServer(HttpClientUtil.createClient(new ModifiableSolrParams()), (ResponseParser) null);
    HttpSolrServer httpServer = testServer.makeServer("http://127.0.0.1:8080");
    assertNull("Generated server should have null parser.", httpServer.getParser());

    ResponseParser parser = new BinaryResponseParser();
    testServer = new LBHttpSolrServer(HttpClientUtil.createClient(new ModifiableSolrParams()), parser);
    httpServer = testServer.makeServer("http://127.0.0.1:8080");
    assertEquals("Invalid parser passed to generated server.", parser, httpServer.getParser());
  }
  
}
