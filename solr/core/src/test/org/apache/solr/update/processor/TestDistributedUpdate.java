package org.apache.solr.update.processor;

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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequestExt;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;

public class TestDistributedUpdate extends SolrTestCaseJ4 {
  
  private static final int NUM_JETTIES = 2;
  
  File testDir;
  
  List<SolrServer> clients = new ArrayList<SolrServer>();
  List<JettySolrRunner> jettys = new ArrayList<JettySolrRunner>();
  String context = "/solr/collection1";
  String shardStr;
  String[] shards;
  boolean updateSelf;
  
  String id = "id";
  String t1 = "a_t";
  String i1 = "a_i";
  String oddField = "oddField_s";
  String missingField = "missing_but_valid_field_t";
  String invalidField = "invalid_field_not_in_schema";
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    testDir = new File(TEMP_DIR, "distrib_update_test");
    testDir.mkdirs();
  }
  
  @Override
  public void tearDown() throws Exception {
    destroyServers();
    super.tearDown();
  }
  
  private void createServers() throws Exception {
    StringBuilder sb = new StringBuilder();
    shards = new String[NUM_JETTIES];
    for (int i = 0; i < NUM_JETTIES; i++) {
      
      if (sb.length() > 0) sb.append(',');
      JettySolrRunner jetty = createJetty(testDir, testDir + "/shard" + i
          + "/data", "solrconfig-distrib-update.xml");
      jettys.add(jetty);
      int port = jetty.getLocalPort();
      clients.add(createNewSolrServer(port));
      shards[i] = "localhost:" + port + context;
      sb.append(shards[i]);
    }
    
    shardStr = sb.toString();
    
    // Assure that Solr starts with no documents
    send(commit(u().deleteByQuery("*:*")));
  }
  
  private void destroyServers() throws Exception {
    for (JettySolrRunner jetty : jettys)
      jetty.stop();
    clients.clear();
    jettys.clear();
  }
  
  public JettySolrRunner createJetty(File baseDir, String dataDir)
      throws Exception {
    return createJetty(baseDir, dataDir, null, null);
  }
  
  public JettySolrRunner createJetty(File baseDir, String dataDir,
      String shardId) throws Exception {
    return createJetty(baseDir, dataDir, shardId,
        "solrconfig-distrib-update.xml");
  }
  
  public JettySolrRunner createJetty(File baseDir, String dataDir,
      String shardList, String solrConfigOverride) throws Exception {
    System.setProperty("solr.data.dir", dataDir);
    
    JettySolrRunner jetty = new JettySolrRunner(TEST_HOME(), "/solr", 0, solrConfigOverride);
    if (shardList != null) {
      System.setProperty("shard", shardList);
    }
    jetty.start();
    System.clearProperty("shard");
    return jetty;
  }
  
  protected SolrServer createNewSolrServer(int port) {
    try {
      // setup the server...
      String url = "http://localhost:" + port + context;
      CommonsHttpSolrServer s = new CommonsHttpSolrServer(url);
      s.setConnectionTimeout(1000); // 1 sec
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  QueryResponse query(Object... q) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    
    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }
    
    params.set("shards", shardStr);
    
    // query a random server
    int which = random.nextInt(clients.size());
    SolrServer client = clients.get(which);
    QueryResponse rsp = client.query(params);
    return rsp;
  }
  
  void send(int which, AbstractUpdateRequest ureq) throws Exception {
    ureq.setParam("update.chain", "distrib-update-chain");
    ureq.setParam("shards", shardStr);
    ureq.setParam("self", updateSelf ? shards[which] : "foo");
    
    SolrServer client = clients.get(which);
    client.request(ureq);
  }
  
  SolrInputDocument doc(Object... fields) {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    return doc;
  }
  
  // send request to a random server
  void send(AbstractUpdateRequest ureq) throws Exception {
    send((random.nextInt() >>> 1) % shards.length, ureq);
  }
  
  UpdateRequest u() {
    return new UpdateRequest();
  }
  
  AbstractUpdateRequest commit(AbstractUpdateRequest ureq) {
    ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true);
    return ureq;
  }
  
  UpdateRequest optimize(UpdateRequest ureq) {
    ureq.setAction(UpdateRequest.ACTION.OPTIMIZE, true, true);
    return ureq;
  }
  
  UpdateRequest add(UpdateRequest ureq, Object... ids) {
    for (Object id : ids) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", id.toString());
      ureq.add(doc);
    }
    return ureq;
  }
  
  void verifyCount(String q, int count) throws Exception {
    verifyCount(q, count, 0);
  }
  
  void verifyCount(String q, int count, int retries) throws Exception {
    long found = query("q", q).getResults().getNumFound();
    for (int i = 0; i < retries; i++) {
      if (found == count) {
        break;
      }
      Thread.sleep(500);
      found = query("q", q).getResults().getNumFound();
    }
    
    assertEquals(count, found);
    // use a facet to get the "real" count since distributed search
    // can do some dedup for us.
    assertEquals(count, query("q", "*:*", "facet", "true", "facet.query", q)
        .getFacetQuery().get(q).longValue());
  }
  
  public void testStress() throws Exception {
    int iter = 10; // crank this number up for a long term test
    
    createServers();
    updateSelf = true;
    
    List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>(1000);
    for (int i = 0; i < 1000; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "" + i);
      docs.add(doc);
    }
    
    List<String> ids = new ArrayList<String>(1000);
    for (int i = 0; i < 1000; i++)
      ids.add("" + i);
    
    boolean haveAddedDocs = false;
    for (int i = 0; i < iter; i++) {
      // System.out.println("ITERATION"+i);
      if ((random.nextInt() & 0x01) == 0) {
        // System.out.println("ITERATION"+i+" - 0");
        haveAddedDocs = true;
        UpdateRequest addReq = new UpdateRequest();
        addReq.add(docs);
        send(commit(addReq));
        verifyCount("id:[* TO *]", 1000);
      } else {
        // System.out.println("ITERATION"+i+" - 1");
        UpdateRequest delReq = new UpdateRequest();
        for (int j = 0; j < 1000; j += 2) {
          delReq.deleteById(ids.get(j));
        }
        send(commit(delReq));
        verifyCount("id:[* TO *]", (haveAddedDocs ? 500 : 0));
      }
      
      // optimize to keep the index size under control.
      if (i % 25 == 0) {
        send(optimize(u()));
      }
      
    }
    
    destroyServers();
  }
  
  public void testDistribUpdate() throws Exception {
    for (int nServers = 2; nServers < 4; nServers++) {
      
      createServers();
      
      // node doesn't know who it is... sends to itself over HTTP
      updateSelf = false;
      doTest();
      
      // node does know who it is... updates index directly for itself
      updateSelf = true;
      doTest();
      
      destroyServers();
    }
  }
  
  public void doTest() throws Exception {
    send(0, commit(u().deleteByQuery("*:*")));
    verifyCount("id:1", 0);
    
    send(0, add(u(), 1));
    send(1, add(u(), 1));
    verifyCount("id:1", 0); // no commit yet
    send(commit(u()));
    verifyCount("id:1", 1); // doc should only have been sent to single server
    
    send(u().deleteById("1"));
    verifyCount("id:1", 1); // no commit yet
    send(commit(u()));
    verifyCount("id:1", 0);
    
    // test adding a commit onto an add
    send(commit(add(u(), 1)));
    verifyCount("id:1", 1);
    
    // test adding a commmit onto a delete
    send(commit(u().deleteById("1")));
    verifyCount("id:1", 0);
    
    // test that batching adds doesn't mess anything up
    send(add(u(), 1, 2, 3, 4, 5, 6, 7, 8, 9));
    send(commit(u()));
    // Thread.sleep(1000000000);
    verifyCount("id:[1 TO 9]", 9);
    
    // test delete by query
    send(commit(u().deleteByQuery("id:[2 TO 8]")));
    verifyCount("id:[1 TO 9]", 2);
    
    send(commit(add(u(), 1, 2, 3, 4, 5, 6, 7, 8, 9)));
    verifyCount("id:[1 TO 9]", 9);
    
    send(commit(u().deleteByQuery("*:*")));
    verifyCount("id:[1 TO 9]", 0);
    
    // this test can cause failures if a commit can sneak in ahead of
    // add requests that are still pending.
    Object[] docs = new Object[1000];
    for (int i = 0; i < 1000; i++)
      docs[i] = i;
    send(commit(add(u(), docs)));
    verifyCount("id:[* TO *]", 1000);
    
    // test delete batching
    UpdateRequest ureq = u();
    for (int i = 0; i < 1000; i += 2) {
      ureq.deleteById("" + i);
    }
    send(commit(ureq));
    verifyCount("id:[* TO *]", 500);
    
    // test commit within
    ureq = u();
    ureq.setCommitWithin(1);
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", "999999");
    ureq.add(doc);
    send(ureq);
    

    verifyCount("id:[* TO *]", 501, 300);
    
    send(commit(ureq));
    
    // test overwrite
    UpdateRequestExt lweureq = new UpdateRequestExt();
    doc = new SolrInputDocument();
    doc.addField("id", "999999");
    lweureq.add(doc, 3, false);
    send(commit(lweureq));
    
    verifyCount("id:[* TO *]", 502);
    
    // test overwrite with no commitWithin
    lweureq = new UpdateRequestExt();
    doc = new SolrInputDocument();
    doc.addField("id", "999999");
    lweureq.add(doc, -1, false);
    send(commit(lweureq));
    
    verifyCount("id:[* TO *]", 503);
  }
  
}
