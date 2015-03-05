package org.apache.solr.servlet;

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

import org.apache.commons.io.FileUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map.Entry;


public class ResponseHeaderTest extends SolrJettyTestBase {
  
  private static File solrHomeDirectory;
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    solrHomeDirectory = createTempDir().toFile();
    setupJettyTestHome(solrHomeDirectory, "collection1");
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    FileUtils.copyFile(new File(top, "solrconfig-headers.xml"), new File(solrHomeDirectory + "/collection1/conf", "solrconfig.xml"));
    createJetty(solrHomeDirectory.getAbsolutePath());
  }
  
  @AfterClass
  public static void afterTest() throws Exception {
    cleanUpJettyHome(solrHomeDirectory);
  }
  
  @Test
  public void testHttpResponse() throws SolrServerException, IOException {
    HttpSolrClient client = (HttpSolrClient) getSolrClient();
    HttpClient httpClient = client.getHttpClient();
    URI uri = URI.create(client.getBaseURL() + "/withHeaders?q=*:*");
    HttpGet httpGet = new HttpGet(uri);
    HttpResponse response = httpClient.execute(httpGet);
    Header[] headers = response.getAllHeaders();
    boolean containsWarningHeader = false;
    for (Header header:headers) {
      if ("Warning".equals(header.getName())) {
        containsWarningHeader = true;
        assertEquals("This is a test warning", header.getValue());
        break;
      }
    }
    assertTrue("Expected header not found", containsWarningHeader);
  }
  
  @Test
  public void testAddHttpHeader() {
    SolrQueryResponse response = new SolrQueryResponse();
    Iterator<Entry<String, String>> it = response.httpHeaders();
    assertFalse(it.hasNext());
    
    response.addHttpHeader("key1", "value1");
    it = response.httpHeaders();
    assertTrue(it.hasNext());
    Entry<String, String> entry = it.next();
    assertEquals("key1", entry.getKey());
    assertEquals("value1", entry.getValue());
    assertFalse(it.hasNext());
    
    response.addHttpHeader("key1", "value2");
    it = response.httpHeaders();
    assertTrue(it.hasNext());
    entry = it.next();
    assertEquals("key1", entry.getKey());
    assertEquals("value1", entry.getValue());
    assertTrue(it.hasNext());
    entry = it.next();
    assertEquals("key1", entry.getKey());
    assertEquals("value2", entry.getValue());
    assertFalse(it.hasNext());
    
    response.addHttpHeader("key2", "value2");
    it = response.httpHeaders();
    assertTrue(it.hasNext());
    entry = it.next();
    assertEquals("key1", entry.getKey());
    assertEquals("value1", entry.getValue());
    assertTrue(it.hasNext());
    entry = it.next();
    assertEquals("key1", entry.getKey());
    assertEquals("value2", entry.getValue());
    assertTrue(it.hasNext());
    entry = it.next();
    assertEquals("key2", entry.getKey());
    assertEquals("value2", entry.getValue());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void testSetHttpHeader() {
    SolrQueryResponse response = new SolrQueryResponse();
    Iterator<Entry<String, String>> it = response.httpHeaders();
    assertFalse(it.hasNext());
    
    response.setHttpHeader("key1", "value1");
    it = response.httpHeaders();
    assertTrue(it.hasNext());
    Entry<String, String> entry = it.next();
    assertEquals("key1", entry.getKey());
    assertEquals("value1", entry.getValue());
    assertFalse(it.hasNext());
    
    response.setHttpHeader("key1", "value2");
    it = response.httpHeaders();
    assertTrue(it.hasNext());
    entry = it.next();
    assertEquals("key1", entry.getKey());
    assertEquals("value2", entry.getValue());
    assertFalse(it.hasNext());
    
    response.addHttpHeader("key1", "value3");
    response.setHttpHeader("key1", "value4");
    it = response.httpHeaders();
    assertTrue(it.hasNext());
    entry = it.next();
    assertEquals("key1", entry.getKey());
    assertEquals("value4", entry.getValue());
    assertFalse(it.hasNext());
    
    response.setHttpHeader("key2", "value5");
    it = response.httpHeaders();
    assertTrue(it.hasNext());
    entry = it.next();
    assertEquals("key1", entry.getKey());
    assertEquals("value4", entry.getValue());
    assertTrue(it.hasNext());
    entry = it.next();
    assertEquals("key2", entry.getKey());
    assertEquals("value5", entry.getValue());
    assertFalse(it.hasNext());
  }
  
  @Test
  public void testRemoveHttpHeader() {
    SolrQueryResponse response = new SolrQueryResponse();
    Iterator<Entry<String, String>> it = response.httpHeaders();
    assertFalse(it.hasNext());
    response.addHttpHeader("key1", "value1");
    assertTrue(response.httpHeaders().hasNext());
    assertEquals("value1", response.removeHttpHeader("key1"));
    assertFalse(response.httpHeaders().hasNext());
    
    response.addHttpHeader("key1", "value2");
    response.addHttpHeader("key1", "value3");
    response.addHttpHeader("key2", "value4");
    assertTrue(response.httpHeaders().hasNext());
    assertEquals("value2", response.removeHttpHeader("key1"));
    assertEquals("value3", response.httpHeaders().next().getValue());
    assertEquals("value3", response.removeHttpHeader("key1"));
    assertNull(response.removeHttpHeader("key1"));
    assertEquals("key2", response.httpHeaders().next().getKey());
    
  }
  
  @Test
  public void testRemoveHttpHeaders() {
    SolrQueryResponse response = new SolrQueryResponse();
    Iterator<Entry<String, String>> it = response.httpHeaders();
    assertFalse(it.hasNext());
    response.addHttpHeader("key1", "value1");
    assertTrue(response.httpHeaders().hasNext());
    assertEquals(Arrays.asList("value1"), response.removeHttpHeaders("key1"));
    assertFalse(response.httpHeaders().hasNext());
    
    response.addHttpHeader("key1", "value2");
    response.addHttpHeader("key1", "value3");
    response.addHttpHeader("key2", "value4");
    assertTrue(response.httpHeaders().hasNext());
    assertEquals(Arrays.asList(new String[]{"value2", "value3"}), response.removeHttpHeaders("key1"));
    assertNull(response.removeHttpHeaders("key1"));
    assertEquals("key2", response.httpHeaders().next().getKey());
  }
  
  public static class ComponentThatAddsHeader extends SearchComponent {
    
    @Override
    public void prepare(ResponseBuilder rb) throws IOException {
      rb.rsp.addHttpHeader("Warning", "This is a test warning");
    }
    
    @Override
    public void process(ResponseBuilder rb) throws IOException {}
    
    @Override
    public String getDescription() {
      return null;
    }
  }
  
}
