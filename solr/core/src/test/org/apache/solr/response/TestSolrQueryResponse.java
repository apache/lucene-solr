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
package org.apache.solr.response;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.search.SolrReturnFields;
import org.junit.Test;

public class TestSolrQueryResponse extends SolrTestCase {
  
  @Test
  public void testName() throws Exception {
    assertEquals("SolrQueryResponse.NAME value changed", "response", SolrQueryResponse.NAME);
  }

  @Test
  public void testResponseHeaderPartialResults() throws Exception {
    assertEquals("SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY value changed",
        "partialResults", SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY);
  }

  @Test
  public void testResponseHeaderSegmentTerminatedEarly() throws Exception {
    assertEquals("SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY value changed",
        "segmentTerminatedEarly", SolrQueryResponse.RESPONSE_HEADER_SEGMENT_TERMINATED_EARLY_KEY);
  }

  @Test
  public void testValues() throws Exception {
    final SolrQueryResponse response = new SolrQueryResponse();
    assertEquals("values initially not empty", 0, response.getValues().size());
    // initially empty, then add something
    final NamedList<Object> newValue = new SimpleOrderedMap<>();
    newValue.add("key1", "value1");
    response.setAllValues(newValue);
    assertEquals("values new value", newValue, response.getValues());
    response.add("key2", "value2");
    {
      @SuppressWarnings({"unchecked"})
      final Iterator<Map.Entry<String,Object>> it = response.getValues().iterator();
      assertTrue(it.hasNext());
      final Map.Entry<String,Object> entry1 = it.next();
      assertEquals("key1", entry1.getKey());
      assertEquals("value1", entry1.getValue());
      assertTrue(it.hasNext());
      final Map.Entry<String,Object> entry2 = it.next();
      assertEquals("key2", entry2.getKey());
      assertEquals("value2", entry2.getValue());
      assertFalse(it.hasNext());
    }
  }

  @Test
  public void testResponse() throws Exception {
    final SolrQueryResponse response = new SolrQueryResponse();
    assertEquals("response initial value", null, response.getResponse());
    final Object newValue = (random().nextBoolean()
        ? (random().nextBoolean() ? "answer" : Integer.valueOf(42)) : null);
    response.addResponse(newValue);
    assertEquals("response new value", newValue, response.getResponse());
  }

  @Test
  public void testToLog() throws Exception {
    final SolrQueryResponse response = new SolrQueryResponse();
    assertEquals("toLog initially not empty", 0, response.getToLog().size());
    assertEquals("logid_only", response.getToLogAsString("logid_only"));
    // initially empty, then add something
    response.addToLog("key1", "value1");
    {
      final Iterator<Map.Entry<String,Object>> it = response.getToLog().iterator();
      assertTrue(it.hasNext());
      final Map.Entry<String,Object> entry1 = it.next();
      assertEquals("key1", entry1.getKey());
      assertEquals("value1", entry1.getValue());
      assertFalse(it.hasNext());
    }
    assertEquals("key1=value1", response.getToLogAsString(""));
    assertEquals("abc123 key1=value1", response.getToLogAsString("abc123"));
    // and then add something else
    response.addToLog("key2", "value2");
    {
      final Iterator<Map.Entry<String,Object>> it = response.getToLog().iterator();
      assertTrue(it.hasNext());
      final Map.Entry<String,Object> entry1 = it.next();
      assertEquals("key1", entry1.getKey());
      assertEquals("value1", entry1.getValue());
      assertTrue(it.hasNext());
      final Map.Entry<String,Object> entry2 = it.next();
      assertEquals("key2", entry2.getKey());
      assertEquals("value2", entry2.getValue());
      assertFalse(it.hasNext());
    }
    assertEquals("key1=value1 key2=value2", response.getToLogAsString(""));
    assertEquals("xyz789 key1=value1 key2=value2", response.getToLogAsString("xyz789"));
  }

  @Test
  public void testReturnFields() throws Exception {
    final SolrQueryResponse response = new SolrQueryResponse();
    final ReturnFields defaultReturnFields = new SolrReturnFields();
    assertEquals("returnFields initial value", defaultReturnFields.toString(), response.getReturnFields().toString());
    final SolrReturnFields newValue = new SolrReturnFields((random().nextBoolean()
        ? SolrReturnFields.SCORE : "value"), null);
    response.setReturnFields(newValue);
    assertEquals("returnFields new value", newValue.toString(), response.getReturnFields().toString());
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
  
  @Test
  public void testException() throws Exception {
    final SolrQueryResponse response = new SolrQueryResponse();
    assertEquals("exception initial value", null, response.getException());
    final Exception newValue = (random().nextBoolean()
        ? (random().nextBoolean() ? new ArithmeticException() : new IOException()) : null);
    response.setException(newValue);
    assertEquals("exception new value", newValue, response.getException());
  }

  @Test
  public void testResponseHeader() throws Exception {
    final SolrQueryResponse response = new SolrQueryResponse();
    assertEquals("responseHeader initially present", null, response.getResponseHeader());
    final NamedList<Object> newValue = new SimpleOrderedMap<>();
    newValue.add("key1", "value1");
    response.add("key2", "value2");
    response.addResponseHeader(newValue);
    assertEquals("responseHeader new value", newValue, response.getResponseHeader());
    response.removeResponseHeader();
    assertEquals("responseHeader removed value", null, response.getResponseHeader());
  }

  @Test
  public void testHttpCaching() throws Exception {
    final SolrQueryResponse response = new SolrQueryResponse();
    assertEquals("httpCaching initial value", true, response.isHttpCaching());
    final boolean newValue = random().nextBoolean();
    response.setHttpCaching(newValue);
    assertEquals("httpCaching new value", newValue, response.isHttpCaching());
  }

  @Test
  public void testConvertToHEADStyleResponse() throws Exception {
    final SolrQueryResponse response = new SolrQueryResponse();
    final NamedList<Object> newValue = new SimpleOrderedMap<>();
    newValue.add("responseHeaderKey1", "value1");
    response.add("responseHeaderKey2", "value2");
    response.addResponseHeader(newValue);

    response.addResponse("foo");
    response.add("bob", "dole");

    response.addHttpHeader("key1", "value1");

    NamedList<Object> blank = new SimpleOrderedMap<>();
    response.setAllValues(blank);

    Iterator<Entry<String, String>> it = response.httpHeaders();
    assertTrue(it.hasNext());
    Entry<String, String> entry = it.next();
    assertEquals("key1", entry.getKey());
    assertEquals("value1", entry.getValue());
    assertFalse(it.hasNext());




  }

}
