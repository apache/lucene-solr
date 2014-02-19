package org.apache.solr.handler.component;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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

public class DistributedDebugComponentTest extends SolrJettyTestBase {
  
  private static SolrServer collection1;
  private static SolrServer collection2;
  private static String shard1;
  private static String shard2;
  private static File solrHome;
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    solrHome = createSolrHome();
  }
  
  private static File createSolrHome() throws Exception {
    File workDir = new File(TEMP_DIR, DistributedDebugComponentTest.class.getName());
    setupJettyTestHome(workDir, "collection1");
    FileUtils.copyDirectory(new File(workDir, "collection1"), new File(workDir, "collection2"));
    return workDir;
  }

  @AfterClass
  public static void afterTest() throws Exception {
    cleanUpJettyHome(solrHome);
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    createJetty(solrHome.getAbsolutePath(), null, null);
    String url = jetty.getBaseUrl().toString();
    collection1 = new HttpSolrServer(url);
    collection2 = new HttpSolrServer(url + "/collection2");
    
    String urlCollection1 = jetty.getBaseUrl().toString() + "/" + "collection1";
    String urlCollection2 = jetty.getBaseUrl().toString() + "/" + "collection2";
    shard1 = urlCollection1.replaceAll("https?://", "");
    shard2 = urlCollection2.replaceAll("https?://", "");
    
    //create second core
    CoreAdminRequest.Create req = new CoreAdminRequest.Create();
    req.setCoreName("collection2");
    collection1.request(req);
    
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
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    collection1.shutdown();
    collection2.shutdown();
    collection1 = null;
    collection2 = null;
    jetty.stop();
    jetty=null;
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
    
    query.add("omitHeader", "true");
    response = collection1.query(query);
    assertNull("QTime is not included in the response when omitHeader is set to true", 
        ((NamedList<Object>)response.getDebugMap().get("track")).findRecursive("EXECUTE_QUERY", shard1, "QTime"));
    assertNull("QTime is not included in the response when omitHeader is set to true", 
        ((NamedList<Object>)response.getDebugMap().get("track")).findRecursive("GET_FIELDS", shard2, "QTime"));
    
    query.setQuery("id:1");
    response = collection1.query(query);
    track = (NamedList<Object>) response.getDebugMap().get("track");
    assertNotNull(((NamedList<Object>)track.get("EXECUTE_QUERY")).get(shard1));
    assertNotNull(((NamedList<Object>)track.get("EXECUTE_QUERY")).get(shard2));
    
    assertNotNull(((NamedList<Object>)track.get("GET_FIELDS")).get(shard1));
    // This test is invalid, as GET_FIELDS should not be executed in shard 2
    assertNull(((NamedList<Object>)track.get("GET_FIELDS")).get(shard2));
  }
  
  private void assertElementsPresent(NamedList<String> namedList, String...elements) {
    for(String element:elements) {
      String value = namedList.get(element);
      assertNotNull("Expected element '" + element + "' but was not found", value);
      assertTrue("Expected element '" + element + "' but was empty", !value.isEmpty());
    }
  }
  
}
