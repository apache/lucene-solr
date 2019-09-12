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
package org.apache.solr.handler.component;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DistributedDebugComponentTest extends SolrJettyTestBase {
  
  private static SolrClient collection1;
  private static SolrClient collection2;
  private static String shard1;
  private static String shard2;
  private static File solrHome;
  
  private static File createSolrHome() throws Exception {
    File workDir = createTempDir().toFile();
    setupJettyTestHome(workDir, "collection1");
    FileUtils.copyDirectory(new File(workDir, "collection1"), new File(workDir, "collection2"));
    return workDir;
  }

  
  @BeforeClass
  public static void createThings() throws Exception {
    systemSetPropertySolrDisableShardsWhitelist("true");
    solrHome = createSolrHome();
    createAndStartJetty(solrHome.getAbsolutePath());
    String url = jetty.getBaseUrl().toString();

    collection1 = getHttpSolrClient(url + "/collection1");
    collection2 = getHttpSolrClient(url + "/collection2");
    
    String urlCollection1 = jetty.getBaseUrl().toString() + "/" + "collection1";
    String urlCollection2 = jetty.getBaseUrl().toString() + "/" + "collection2";
    shard1 = urlCollection1.replaceAll("https?://", "");
    shard2 = urlCollection2.replaceAll("https?://", "");
    
    //create second core
    try (HttpSolrClient nodeClient = getHttpSolrClient(url)) {
      CoreAdminRequest.Create req = new CoreAdminRequest.Create();
      req.setCoreName("collection2");
      req.setConfigSet("collection1");
      nodeClient.request(req);
    }

    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    doc.setField("text", "batman");
    collection1.add(doc);
    collection1.commit();
    
    doc.setField("id", "2");
    doc.setField("text", "superman");
    collection2.add(doc);
    collection2.commit();
    
  }
  
  @AfterClass
  public static void destroyThings() throws Exception {
    if (null != collection1) {
      collection1.close();
      collection1 = null;
    }
    if (null != collection2) {
      collection2.close();
      collection2 = null;
    }
    if (null != jetty) {
      jetty.stop();
      jetty=null;
    }
    resetExceptionIgnores();
    systemClearPropertySolrDisableShardsWhitelist();
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleSearch() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.set("debug",  "track");
    query.set("distrib", "true");
    query.setFields("id", "text");
    query.set("shards", shard1 + "," + shard2);
    
    if (random().nextBoolean()) {
      query.add("omitHeader", Boolean.toString(random().nextBoolean()));
    }
    QueryResponse response = collection1.query(query);
    NamedList<Object> track = (NamedList<Object>) response.getDebugMap().get("track");
    assertNotNull(track);
    assertNotNull(track.get("rid"));
    assertNotNull(track.get("EXECUTE_QUERY"));
    assertNotNull(((NamedList<Object>)track.get("EXECUTE_QUERY")).get(shard1));
    assertNotNull(((NamedList<Object>)track.get("EXECUTE_QUERY")).get(shard2));
    
    assertNotNull(((NamedList<Object>)track.get("GET_FIELDS")).get(shard1));
    assertNotNull(((NamedList<Object>)track.get("GET_FIELDS")).get(shard2));
    
    assertElementsPresent((NamedList<String>)((NamedList<Object>)track.get("EXECUTE_QUERY")).get(shard1), 
        "QTime", "ElapsedTime", "RequestPurpose", "NumFound", "Response");
    assertElementsPresent((NamedList<String>)((NamedList<Object>)track.get("EXECUTE_QUERY")).get(shard2), 
        "QTime", "ElapsedTime", "RequestPurpose", "NumFound", "Response");
    
    assertElementsPresent((NamedList<String>)((NamedList<Object>)track.get("GET_FIELDS")).get(shard1), 
        "QTime", "ElapsedTime", "RequestPurpose", "NumFound", "Response");
    assertElementsPresent((NamedList<String>)((NamedList<Object>)track.get("GET_FIELDS")).get(shard2), 
        "QTime", "ElapsedTime", "RequestPurpose", "NumFound", "Response");
    
    query.setQuery("id:1");
    response = collection1.query(query);
    track = (NamedList<Object>) response.getDebugMap().get("track");
    assertNotNull(((NamedList<Object>)track.get("EXECUTE_QUERY")).get(shard1));
    assertNotNull(((NamedList<Object>)track.get("EXECUTE_QUERY")).get(shard2));
    
    assertNotNull(((NamedList<Object>)track.get("GET_FIELDS")).get(shard1));
    // This test is invalid, as GET_FIELDS should not be executed in shard 2
    assertNull(((NamedList<Object>)track.get("GET_FIELDS")).get(shard2));
  }
  
  @Test
  @SuppressWarnings("resource") // Cannot close client in this loop!
  public void testRandom() throws Exception {
    final int NUM_ITERS = atLeast(50);

    for (int i = 0; i < NUM_ITERS; i++) {
      final SolrClient client = random().nextBoolean() ? collection1 : collection2;

      SolrQuery q = new SolrQuery();
      q.set("distrib", "true");
      q.setFields("id", "text");

      boolean shard1Results = random().nextBoolean();
      boolean shard2Results = random().nextBoolean();

      String qs = "_query_with_no_results_";
      if (shard1Results) {
        qs += " OR batman";
      }
      if (shard2Results) {
        qs += " OR superman";
      }
      q.setQuery(qs);

      Set<String> shards = new HashSet<String>(Arrays.asList(shard1, shard2));
      if (random().nextBoolean()) {
        shards.remove(shard1);
      } else if (random().nextBoolean()) {
        shards.remove(shard2);
      }
      q.set("shards", String.join(",", shards));


      List<String> debug = new ArrayList<String>(10);

      boolean all = false;
      final boolean timing = random().nextBoolean();
      final boolean query = random().nextBoolean();
      final boolean results = random().nextBoolean();
      final boolean track = random().nextBoolean();

      if (timing) { debug.add("timing"); }
      if (query) { debug.add("query"); }
      if (results) { debug.add("results"); }
      if (track) { debug.add("track"); }
      if (debug.isEmpty()) {
        debug.add("true");
        all = true;
      }
      q.set("debug", (String[])debug.toArray(new String[debug.size()]));

      QueryResponse r = client.query(q);
      try {
        assertDebug(r, all || track, "track");
        assertDebug(r, all || query, "rawquerystring");
        assertDebug(r, all || query, "querystring");
        assertDebug(r, all || query, "parsedquery");
        assertDebug(r, all || query, "parsedquery_toString");
        assertDebug(r, all || query, "QParser");
        assertDebug(r, all || results, "explain");
        assertDebug(r, all || timing, "timing");
      } catch (AssertionError e) {
        throw new AssertionError(q.toString() + ": " + e.getMessage(), e);
      }
    }
  }

  /**
   * Asserts that the specified debug result key does or does not exist in the 
   * response based on the expected boolean.
   */
  private void assertDebug(QueryResponse response, boolean expected, String key) {
    if (expected) {
      assertInDebug(response, key);
    } else {
      assertNotInDebug(response, key);
    }
  }
  /**
   * Asserts that the specified debug result key does exist in the response and is non-null
   */
  private void assertInDebug(QueryResponse response, String key) {
    assertNotNull("debug map is null", response.getDebugMap());
    assertNotNull("debug map has null for : " + key, response.getDebugMap().get(key));
  }

  /**
   * Asserts that the specified debug result key does NOT exist in the response
   */
  private void assertNotInDebug(QueryResponse response, String key) {
    assertNotNull("debug map is null", response.getDebugMap());
    assertFalse("debug map contains: " + key, response.getDebugMap().containsKey(key));
  }


  @Test
  public void testDebugSections() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("text:_query_with_no_results_");
    query.set("distrib", "true");
    query.setFields("id", "text");
    query.set("shards", shard1 + "," + shard2);
    verifyDebugSections(query, collection1);
    query.setQuery("id:1 OR text:_query_with_no_results_ OR id:[0 TO 300]");
    verifyDebugSections(query, collection1);
    
  }
  
  private void verifyDebugSections(SolrQuery query, SolrClient client) throws SolrServerException, IOException {
    query.set("debugQuery", "true");
    query.remove("debug");
    QueryResponse response = client.query(query);
    assertFalse(response.getDebugMap().isEmpty());
    assertInDebug(response, "track");
    assertInDebug(response, "rawquerystring");
    assertInDebug(response, "querystring");
    assertInDebug(response, "parsedquery");
    assertInDebug(response, "parsedquery_toString");
    assertInDebug(response, "QParser");
    assertInDebug(response, "explain");
    assertInDebug(response, "timing");
    
    query.set("debug", "true");
    query.remove("debugQuery");
    response = client.query(query);
    assertFalse(response.getDebugMap().isEmpty());
    assertInDebug(response, "track");
    assertInDebug(response, "rawquerystring");
    assertInDebug(response, "querystring");
    assertInDebug(response, "parsedquery");
    assertInDebug(response, "parsedquery_toString");
    assertInDebug(response, "QParser");
    assertInDebug(response, "explain");
    assertInDebug(response, "timing");
    
    query.set("debug", "track");
    response = client.query(query);
    assertFalse(response.getDebugMap().isEmpty());
    assertInDebug(response, "track");
    assertNotInDebug(response, "rawquerystring");
    assertNotInDebug(response, "querystring");
    assertNotInDebug(response, "parsedquery");
    assertNotInDebug(response, "parsedquery_toString");
    assertNotInDebug(response, "QParser");
    assertNotInDebug(response, "explain");
    assertNotInDebug(response, "timing");
    
    query.set("debug", "query");
    response = client.query(query);
    assertFalse(response.getDebugMap().isEmpty());
    assertNotInDebug(response, "track");
    assertInDebug(response, "rawquerystring");
    assertInDebug(response, "querystring");
    assertInDebug(response, "parsedquery");
    assertInDebug(response, "parsedquery_toString");
    assertInDebug(response, "QParser");
    assertNotInDebug(response, "explain");
    assertNotInDebug(response, "timing");
    
    query.set("debug", "results");
    response = client.query(query);
    assertFalse(response.getDebugMap().isEmpty());
    assertNotInDebug(response, "track");
    assertNotInDebug(response, "rawquerystring");
    assertNotInDebug(response, "querystring");
    assertNotInDebug(response, "parsedquery");
    assertNotInDebug(response, "parsedquery_toString");
    assertNotInDebug(response, "QParser");
    assertInDebug(response, "explain");
    assertNotInDebug(response, "timing");
    
    query.set("debug", "timing");
    response = client.query(query);
    assertFalse(response.getDebugMap().isEmpty());
    assertNotInDebug(response, "track");
    assertNotInDebug(response, "rawquerystring");
    assertNotInDebug(response, "querystring");
    assertNotInDebug(response, "parsedquery");
    assertNotInDebug(response, "parsedquery_toString");
    assertNotInDebug(response, "QParser");
    assertNotInDebug(response, "explain");
    assertInDebug(response, "timing");
    
    query.set("debug", "false");
    response = client.query(query);
    assertNull(response.getDebugMap());
  }

  @Test
  // commented out on: 24-Dec-2018   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 20-Sep-2018
  public void testCompareWithNonDistributedRequest() throws SolrServerException, IOException {
    SolrQuery query = new SolrQuery();
    query.setQuery("id:1 OR id:2");
    query.setFilterQueries("id:[0 TO 10]", "id:[0 TO 5]");
    query.setRows(1);
    query.setSort("id", SolrQuery.ORDER.asc); // thus only return id:1 since rows 1
    query.set("debug",  "true");
    query.set("distrib", "true");
    query.setFields("id");
    if (random().nextBoolean()) { // can affect rb.onePassDistributedQuery
      query.addField("text");
    }
    query.set(ShardParams.DISTRIB_SINGLE_PASS, random().nextBoolean());
    query.set("shards", shard1 + "," + shard2);
    QueryResponse distribResponse = collection1.query(query);
    
    // same query but not distributed
    query.set("distrib", "false");
    query.remove("shards");
    QueryResponse nonDistribResponse = collection1.query(query);
    
    assertNotNull(distribResponse.getDebugMap().get("track"));
    assertNull(nonDistribResponse.getDebugMap().get("track"));
    assertEquals(distribResponse.getDebugMap().size() - 1, nonDistribResponse.getDebugMap().size());
    
    assertSectionEquals(distribResponse, nonDistribResponse, "explain");
    assertSectionEquals(distribResponse, nonDistribResponse, "rawquerystring");
    assertSectionEquals(distribResponse, nonDistribResponse, "querystring");
    assertSectionEquals(distribResponse, nonDistribResponse, "parsedquery");
    assertSectionEquals(distribResponse, nonDistribResponse, "parsedquery_toString");
    assertSectionEquals(distribResponse, nonDistribResponse, "QParser");
    assertSectionEquals(distribResponse, nonDistribResponse, "filter_queries");
    assertSectionEquals(distribResponse, nonDistribResponse, "parsed_filter_queries");
    
    // timing should have the same sections:
    assertSameKeys((NamedList<?>)nonDistribResponse.getDebugMap().get("timing"), (NamedList<?>)distribResponse.getDebugMap().get("timing"));
  }
  
  public void testTolerantSearch() throws SolrServerException, IOException {
    String badShard = "[ff01::0083]:3334";
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.set("debug",  "true");
    query.set("distrib", "true");
    query.setFields("id", "text");
    query.set("shards", shard1 + "," + shard2 + "," + badShard);

    // verify that the request would fail if shards.tolerant=false
    ignoreException("Server refused connection");
    expectThrows(SolrException.class, () -> collection1.query(query));

    query.set(ShardParams.SHARDS_TOLERANT, "true");
    QueryResponse response = collection1.query(query);
    assertTrue((Boolean)response.getResponseHeader().get(SolrQueryResponse.RESPONSE_HEADER_PARTIAL_RESULTS_KEY));
    @SuppressWarnings("unchecked")
    NamedList<String> badShardTrack = (NamedList<String>) ((NamedList<NamedList<String>>)
        ((NamedList<NamedList<NamedList<String>>>)response.getDebugMap().get("track")).get("EXECUTE_QUERY")).get(badShard);
    assertEquals("Unexpected response size for shard", 1, badShardTrack.size());
    Entry<String, String> exception = badShardTrack.iterator().next();
    assertEquals("Expected key 'Exception' not found", "Exception", exception.getKey());
    assertNotNull("Exception message should not be null", exception.getValue());
    unIgnoreException("Server refused connection");
  }
  
  /**
   * Compares the same section on the two query responses
   */
  private void assertSectionEquals(QueryResponse distrib, QueryResponse nonDistrib, String section) {
    assertEquals(section + " debug should be equal", distrib.getDebugMap().get(section), nonDistrib.getDebugMap().get(section));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private void assertSameKeys(NamedList object, NamedList object2) {
    Iterator<Map.Entry<String,Object>> iteratorObj2 = ((NamedList)object2).iterator();
    for (Map.Entry<String,Object> entry:(NamedList<Object>)object) {
      assertTrue(iteratorObj2.hasNext());
      Map.Entry<String,Object> entry2 = iteratorObj2.next();
      assertEquals(entry.getKey(), entry2.getKey());
      if (entry.getValue() instanceof NamedList) {
        assertTrue(entry2.getValue() instanceof NamedList);
        assertSameKeys((NamedList)entry.getValue(), (NamedList)entry2.getValue());
      }
    }
    assertFalse(iteratorObj2.hasNext());
  }

  private void assertElementsPresent(NamedList<String> namedList, String...elements) {
    for(String element:elements) {
      String value = namedList.get(element);
      assertNotNull("Expected element '" + element + "' but was not found", value);
      assertTrue("Expected element '" + element + "' but was empty", !value.isEmpty());
    }
  }
  
}
