package org.apache.solr.cloud;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Test sync phase that occurs when Leader goes down and a new Leader is
 * elected.
 */
@Slow
public class AliasIntegrationTest extends AbstractFullDistribZkTestBase {
  
  @BeforeClass
  public static void beforeSuperClass() throws Exception {
  }
  
  @AfterClass
  public static void afterSuperClass() {
    
  }
  
  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("numShards", Integer.toString(sliceCount));
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    resetExceptionIgnores();
  }
  
  public AliasIntegrationTest() {
    super();
    sliceCount = 1;
    shardCount = random().nextBoolean() ? 3 : 4;
  }
  
  @Override
  public void doTest() throws Exception {
    
    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);
    
    waitForThingsToLevelOut(15);

    del("*:*");
    
    createCollection("collection2", 2, 1, 10);
    
    List<Integer> numShardsNumReplicaList = new ArrayList<Integer>(2);
    numShardsNumReplicaList.add(2);
    numShardsNumReplicaList.add(1);
    checkForCollection("collection2", numShardsNumReplicaList, null);
    waitForRecoveriesToFinish("collection2", true);
    
    cloudClient.setDefaultCollection("collection1");
    
    SolrInputDocument doc1 = getDoc(id, 6, i1, -600, tlong, 600, t1,
        "humpty dumpy sat on a wall");
    SolrInputDocument doc2 = getDoc(id, 7, i1, -600, tlong, 600, t1,
        "humpty dumpy3 sat on a walls");
    SolrInputDocument doc3 = getDoc(id, 8, i1, -600, tlong, 600, t1,
        "humpty dumpy2 sat on a walled");

    cloudClient.add(doc1);
    cloudClient.add(doc2);
    cloudClient.add(doc3);
    
    cloudClient.commit();
    
    SolrInputDocument doc6 = getDoc(id, 9, i1, -600, tlong, 600, t1,
        "humpty dumpy sat on a wall");
    SolrInputDocument doc7 = getDoc(id, 10, i1, -600, tlong, 600, t1,
        "humpty dumpy3 sat on a walls");

    cloudClient.setDefaultCollection("collection2");
    
    cloudClient.add(doc6);
    cloudClient.add(doc7);

    cloudClient.commit();
    
    // create alias
    createAlias("testalias", "collection1");
    
    // search for alias
    SolrQuery query = new SolrQuery("*:*");
    query.set("collection", "testalias");
    QueryResponse res = cloudClient.query(query);
    assertEquals(3, res.getResults().getNumFound());
    
    // search for alias with random non cloud client
    query = new SolrQuery("*:*");
    query.set("collection", "testalias");
    JettySolrRunner jetty = jettys.get(random().nextInt(jettys.size()));
    int port = jetty.getLocalPort();
    HttpSolrServer server = new HttpSolrServer("http://127.0.0.1:" + port + context + "/testalias");
    res = server.query(query);
    assertEquals(3, res.getResults().getNumFound());
    
    // now without collections param
    query = new SolrQuery("*:*");
    jetty = jettys.get(random().nextInt(jettys.size()));
    port = jetty.getLocalPort();
    server = new HttpSolrServer("http://127.0.0.1:" + port + context + "/testalias");
    res = server.query(query);
    assertEquals(3, res.getResults().getNumFound());
    
    // create alias, collection2 first because it's not on every node
    createAlias("testalias", "collection2,collection1");
    
    // search with new cloud client
    CloudSolrServer cloudSolrServer = new CloudSolrServer(zkServer.getZkAddress(), random().nextBoolean());
    cloudSolrServer.setParallelUpdates(random().nextBoolean());
    query = new SolrQuery("*:*");
    query.set("collection", "testalias");
    res = cloudSolrServer.query(query);
    assertEquals(5, res.getResults().getNumFound());
    
    // Try with setDefaultCollection
    query = new SolrQuery("*:*");
    cloudSolrServer.setDefaultCollection("testalias");
    res = cloudSolrServer.query(query);
    cloudSolrServer.shutdown();
    assertEquals(5, res.getResults().getNumFound());
    
    // search for alias with random non cloud client
    query = new SolrQuery("*:*");
    query.set("collection", "testalias");
    jetty = jettys.get(random().nextInt(jettys.size()));
    port = jetty.getLocalPort();
    server = new HttpSolrServer("http://127.0.0.1:" + port + context + "/testalias");
    res = server.query(query);
    assertEquals(5, res.getResults().getNumFound());
    
    // now without collections param
    query = new SolrQuery("*:*");
    jetty = jettys.get(random().nextInt(jettys.size()));
    port = jetty.getLocalPort();
    server = new HttpSolrServer("http://127.0.0.1:" + port + context + "/testalias");
    res = server.query(query);
    assertEquals(5, res.getResults().getNumFound());
    
    // update alias
    createAlias("testalias", "collection2");
    //checkForAlias("testalias", "collection2");
    
    // search for alias
    query = new SolrQuery("*:*");
    query.set("collection", "testalias");
    res = cloudClient.query(query);
    assertEquals(2, res.getResults().getNumFound());
    
    // set alias to two collections
    createAlias("testalias", "collection1,collection2");
    //checkForAlias("testalias", "collection1,collection2");
    
    query = new SolrQuery("*:*");
    query.set("collection", "testalias");
    res = cloudClient.query(query);
    assertEquals(5, res.getResults().getNumFound());
    
    // try a std client
    // search 1 and 2, but have no collections param
    query = new SolrQuery("*:*");
    HttpSolrServer client = new HttpSolrServer(getBaseUrl((HttpSolrServer) clients.get(0)) + "/testalias");
    res = client.query(query);
    assertEquals(5, res.getResults().getNumFound());
    
    createAlias("testalias", "collection2");
    
    // a second alias
    createAlias("testalias2", "collection2");
    
    client = new HttpSolrServer(getBaseUrl((HttpSolrServer) clients.get(0)) + "/testalias");
    SolrInputDocument doc8 = getDoc(id, 11, i1, -600, tlong, 600, t1,
        "humpty dumpy4 sat on a walls");
    client.add(doc8);
    client.commit();
    res = client.query(query);
    assertEquals(3, res.getResults().getNumFound());
    
    createAlias("testalias", "collection2,collection1");
    
    query = new SolrQuery("*:*");
    query.set("collection", "testalias");
    res = cloudClient.query(query);
    assertEquals(6, res.getResults().getNumFound());
    
    deleteAlias("testalias");
    deleteAlias("testalias2");

    boolean sawException = false;
    try {
      res = cloudClient.query(query);
    } catch (SolrException e) {
      sawException = true;
    }
    assertTrue(sawException);
  }

  private void createAlias(String alias, String collections)
      throws SolrServerException, IOException {
    if (random().nextBoolean()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("collections", collections);
      params.set("name", alias);
      params.set("action", CollectionAction.CREATEALIAS.toString());
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      NamedList<Object> result = createNewSolrServer("",
          getBaseUrl((HttpSolrServer) clients.get(0))).request(request);
    } else {
      CollectionAdminResponse resp = CollectionAdminRequest.CreateAlias
          .createAlias(alias, collections, createNewSolrServer("",
              getBaseUrl((HttpSolrServer) clients.get(0))));
    }
  }
  
  private void deleteAlias(String alias) throws SolrServerException,
      IOException {
    if (random().nextBoolean()) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("name", alias);
      params.set("action", CollectionAction.DELETEALIAS.toString());
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      NamedList<Object> result = createNewSolrServer("",
          getBaseUrl((HttpSolrServer) clients.get(0))).request(request);
    } else {
      CollectionAdminResponse resp = CollectionAdminRequest.deleteAlias(alias,
          createNewSolrServer("", getBaseUrl((HttpSolrServer) clients.get(0))));
    }
  }
  
  protected void indexDoc(List<CloudJettyRunner> skipServers, Object... fields) throws IOException,
      SolrServerException {
    SolrInputDocument doc = new SolrInputDocument();
    
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    
    controlClient.add(doc);
    
    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    ModifiableSolrParams params = new ModifiableSolrParams();
    for (CloudJettyRunner skip : skipServers) {
      params.add("test.distrib.skip.servers", skip.url + "/");
    }
    ureq.setParams(params);
    ureq.process(cloudClient);
  }

}
