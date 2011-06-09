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
package org.apache.solr.handler;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.TestDistributedSearch;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.Map;
import java.util.HashMap;

/**
 * Test for ReplicationHandler
 *
 *
 * @since 1.4
 */
public class TestReplicationHandler extends SolrTestCaseJ4 {


  private static final String CONF_DIR = "." + File.separator + "solr" + File.separator + "conf" + File.separator;

  static JettySolrRunner masterJetty, slaveJetty;
  static SolrServer masterClient, slaveClient;
  static SolrInstance master = null, slave = null;

  static String context = "/solr";

  // number of docs to index... decremented for each test case to tell if we accidentally reuse
  // index from previous test method
  static int nDocs = 500;

  @BeforeClass
  public static void beforeClass() throws Exception {
    master = new SolrInstance("master", null);
    master.setUp();
    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    slave = new SolrInstance("slave", masterJetty.getLocalPort());
    slave.setUp();
    slaveJetty = createJetty(slave);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());
  }

  public void clearIndexWithReplication() throws Exception {
    NamedList res = query("*:*", masterClient);
    SolrDocumentList docs = (SolrDocumentList)res.get("response");
    if (docs.getNumFound() != 0) {
      masterClient.deleteByQuery("*:*");
      masterClient.commit();
      // wait for replication to sync
      res = rQuery(0, "*:*", slaveClient);
      assertEquals(0, ((SolrDocumentList) res.get("response")).getNumFound());
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    masterJetty.stop();
    slaveJetty.stop();
    master.tearDown();
    slave.tearDown();
  }

  private static JettySolrRunner createJetty(SolrInstance instance) throws Exception {
    System.setProperty("solr.solr.home", instance.getHomeDir());
    System.setProperty("solr.data.dir", instance.getDataDir());

    JettySolrRunner jetty = new JettySolrRunner("/solr", 0);

    jetty.start();
    return jetty;
  }

  private static SolrServer createNewSolrServer(int port) {
    try {
      // setup the server...
      String url = "http://localhost:" + port + context;
      CommonsHttpSolrServer s = new CommonsHttpSolrServer(url);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  int index(SolrServer s, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    return s.add(doc).getStatus();
  }

  NamedList query(String query, SolrServer s) throws SolrServerException {
    NamedList res = new SimpleOrderedMap();
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add("q", query);

    QueryResponse qres = s.query(params);

    res = qres.getResponse();

    return res;
  }

  /** will sleep up to 30 seconds, looking for expectedDocCount */
  private NamedList rQuery(int expectedDocCount, String query, SolrServer server) throws Exception {
    int timeSlept = 0;
    NamedList res = null;
    SolrDocumentList docList = null;
    do {
      res = query(query, server);
      docList = (SolrDocumentList) res.get("response");
      timeSlept += 100;
      Thread.sleep(100);
    } while(docList.getNumFound() != expectedDocCount && timeSlept < 30000);
    return res;
  }
  
  private NamedList<Object> getDetails(SolrServer s) throws Exception {
    

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command","details");
    params.set("qt","/replication");
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);

    assertNotNull("null response from server", res);

    @SuppressWarnings("unchecked") NamedList<Object> details 
      = (NamedList<Object>) res.get("details");

    assertNotNull("null details", details);

    return details;
  }

  @Test
  public void testDetails() throws Exception {
    { 
      NamedList<Object> details = getDetails(masterClient);
      
      assertEquals("master isMaster?", 
                   "true", details.get("isMaster"));
      assertEquals("master isSlave?", 
                   "false", details.get("isSlave"));
      assertNotNull("master has master section", 
                    details.get("master"));
    }
    {
      NamedList<Object> details = getDetails(slaveClient);
      
      assertEquals("slave isMaster?", 
                   "false", details.get("isMaster"));
      assertEquals("slave isSlave?", 
                   "true", details.get("isSlave"));
      assertNotNull("slave has slave section", 
                    details.get("slave"));
    }

    SolrInstance repeater = null;
    JettySolrRunner repeaterJetty = null;
    SolrServer repeaterClient = null;
    try {
      repeater = new SolrInstance("repeater", masterJetty.getLocalPort());
      repeater.setUp();
      repeaterJetty = createJetty(repeater);
      repeaterClient = createNewSolrServer(repeaterJetty.getLocalPort());

      
      NamedList<Object> details = getDetails(repeaterClient);
      
      assertEquals("repeater isMaster?", 
                   "true", details.get("isMaster"));
      assertEquals("repeater isSlave?", 
                   "true", details.get("isSlave"));
      assertNotNull("repeater has master section", 
                    details.get("master"));
      assertNotNull("repeater has slave section", 
                    details.get("slave"));

    } finally {
      try { 
        if (repeaterJetty != null) repeaterJetty.stop(); 
      } catch (Exception e) { /* :NOOP: */ }
      try { 
        if (repeater != null) repeater.tearDown();
      } catch (Exception e) { /* :NOOP: */ }
    }
  }

  @Test
  public void testReplicateAfterWrite2Slave() throws Exception {
    clearIndexWithReplication();
    nDocs--;
    for (int i = 0; i < nDocs; i++) {
      index(masterClient, "id", i, "name", "name = " + i);
    }

    String masterUrl = "http://localhost:" + masterJetty.getLocalPort() + "/solr/replication?command=disableReplication";
    URL url = new URL(masterUrl);
    InputStream stream = url.openStream();
    try {
      stream.close();
    } catch (IOException e) {
      //e.printStackTrace();
    }

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());

    // Make sure that both the index version and index generation on the slave is
    // higher than that of the master, just to make the test harder.

    index(slaveClient, "id", 551, "name", "name = " + 551);
    slaveClient.commit(true, true);
    index(slaveClient, "id", 552, "name", "name = " + 552);
    slaveClient.commit(true, true);
    index(slaveClient, "id", 553, "name", "name = " + 553);
    slaveClient.commit(true, true);
    index(slaveClient, "id", 554, "name", "name = " + 554);
    slaveClient.commit(true, true);
    index(slaveClient, "id", 555, "name", "name = " + 555);
    slaveClient.commit(true, true);


    //this doc is added to slave so it should show an item w/ that result
    SolrDocumentList slaveQueryResult = null;
    NamedList slaveQueryRsp;
    slaveQueryRsp = rQuery(1, "id:555", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(1, slaveQueryResult.getNumFound());

    masterUrl = "http://localhost:" + masterJetty.getLocalPort() + "/solr/replication?command=enableReplication";
    url = new URL(masterUrl);
    stream = url.openStream();
    try {
      stream.close();
    } catch (IOException e) {
      //e.printStackTrace();
    }

    //the slave should have done a full copy of the index so the doc with id:555 should not be there in the slave now
    slaveQueryRsp = rQuery(0, "id:555", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(0, slaveQueryResult.getNumFound());

    // make sure we replicated the correct index from the master
    slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());
  }

  @Test
  public void testIndexAndConfigReplication() throws Exception {
    clearIndexWithReplication();

    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());

    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    //start config files replication test
    masterClient.deleteByQuery("*:*");
    masterClient.commit();

    //change the schema on master
    master.copyConfigFile(CONF_DIR + "schema-replication2.xml", "schema.xml");

    masterJetty.stop();

    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    slaveJetty.stop();
    slaveJetty = createJetty(slave);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    //add a doc with new field and commit on master to trigger snappull from slave.
    index(masterClient, "id", "2000", "name", "name = " + 2000, "newname", "newname = " + 2000);
    masterClient.commit();

    NamedList masterQueryRsp2 = rQuery(1, "*:*", masterClient);
    SolrDocumentList masterQueryResult2 = (SolrDocumentList) masterQueryRsp2.get("response");
    assertEquals(1, masterQueryResult2.getNumFound());

    slaveQueryRsp = rQuery(1, "*:*", slaveClient);
    SolrDocument d = ((SolrDocumentList) slaveQueryRsp.get("response")).get(0);
    assertEquals("newname = 2000", (String) d.getFieldValue("newname"));

  }

  @Test
  public void testStopPoll() throws Exception {
    clearIndexWithReplication();

    // Test:
    // setup master/slave.
    // stop polling on slave, add a doc to master and verify slave hasn't picked it.
    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());

    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    // start stop polling test
    String slaveURL = "http://localhost:" + slaveJetty.getLocalPort() + "/solr/replication?command=disablepoll";
    URL url = new URL(slaveURL);
    InputStream stream = url.openStream();
    try {
      stream.close();
    } catch (IOException e) {
      //e.printStackTrace();
    }
    index(masterClient, "id", 501, "name", "name = " + 501);
    masterClient.commit();

    //get docs from master and check if number is equal to master
    masterQueryRsp = rQuery(nDocs+1, "*:*", masterClient);
    masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs+1, masterQueryResult.getNumFound());
    
    // NOTE: this test is wierd, we want to verify it DOESNT replicate...
    // for now, add a sleep for this.., but the logic is wierd.
    Thread.sleep(3000);
    
    //get docs from slave and check if number is not equal to master; polling is disabled
    slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());

    // re-enable replication
    slaveURL = "http://localhost:" + slaveJetty.getLocalPort() + "/solr/replication?command=enablepoll";
    url = new URL(slaveURL);
    stream = url.openStream();
    try {
      stream.close();
    } catch (IOException e) {
      //e.printStackTrace();
    }

    slaveQueryRsp = rQuery(nDocs+1, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs+1, slaveQueryResult.getNumFound());   
  }

  
  @Test
  public void testSnapPullWithMasterUrl() throws Exception {
    //change solrconfig on slave
    //this has no entry for pollinginterval
    slave.copyConfigFile(CONF_DIR + "solrconfig-slave1.xml", "solrconfig.xml");
    slaveJetty.stop();
    slaveJetty = createJetty(slave);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    masterClient.deleteByQuery("*:*");
    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());

    // snappull
    String masterUrl = "http://localhost:" + slaveJetty.getLocalPort() + "/solr/replication?command=fetchindex&masterUrl=";
    masterUrl += "http://localhost:" + masterJetty.getLocalPort() + "/solr/replication";
    URL url = new URL(masterUrl);
    InputStream stream = url.openStream();
    try {
      stream.close();
    } catch (IOException e) {
      //e.printStackTrace();
    }

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());
    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    // NOTE: at this point, the slave is not polling any more
    // restore it.
    slave.copyConfigFile(CONF_DIR + "solrconfig-slave.xml", "solrconfig.xml");
    slaveJetty.stop();
    slaveJetty = createJetty(slave);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());
  }


  @Test
  public void testReplicateAfterStartup() throws Exception {
    //stop slave
    slaveJetty.stop();

    nDocs--;
    masterClient.deleteByQuery("*:*");
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());

    //change solrconfig having 'replicateAfter startup' option on master
    master.copyConfigFile(CONF_DIR + "solrconfig-master2.xml",
                          "solrconfig.xml");

    masterJetty.stop();

    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    //start slave
    slaveJetty = createJetty(slave);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());

    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    // NOTE: the master only replicates after startup now!
    // revert that change.
    master.copyConfigFile(CONF_DIR + "solrconfig-master.xml", "solrconfig.xml");
    masterJetty.stop();
    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    //start slave
    slaveJetty.stop();
    slaveJetty = createJetty(slave);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());
  }


  @Test
  public void testIndexAndConfigAliasReplication() throws Exception {
    clearIndexWithReplication();

    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");

    assertEquals(nDocs, slaveQueryResult.getNumFound());

    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    //start config files replication test
    //clear master index
    masterClient.deleteByQuery("*:*");
    masterClient.commit();

    //change solrconfig on master
    master.copyConfigFile(CONF_DIR + "solrconfig-master1.xml", 
                          "solrconfig.xml");

    //change schema on master
    master.copyConfigFile(CONF_DIR + "schema-replication2.xml", 
                          "schema.xml");

    //keep a copy of the new schema
    master.copyConfigFile(CONF_DIR + "schema-replication2.xml", 
                          "schema-replication2.xml");

    masterJetty.stop();

    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    slaveJetty.stop();
    slaveJetty = createJetty(slave);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    //add a doc with new field and commit on master to trigger snappull from slave.
    index(masterClient, "id", "2000", "name", "name = " + 2000, "newname", "newname = " + 2000);
    masterClient.commit();

    NamedList masterQueryRsp2 = rQuery(1, "*:*", masterClient);
    SolrDocumentList masterQueryResult2 = (SolrDocumentList) masterQueryRsp2.get("response");
    assertEquals(1, masterQueryResult2.getNumFound());

    NamedList slaveQueryRsp2 = rQuery(1, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult2 = (SolrDocumentList) slaveQueryRsp2.get("response");
    assertEquals(1, slaveQueryResult2.getNumFound());

    index(slaveClient, "id", "2000", "name", "name = " + 2001, "newname", "newname = " + 2001);
    slaveClient.commit();

    slaveQueryRsp = rQuery(1, "*:*", slaveClient);
    SolrDocument d = ((SolrDocumentList) slaveQueryRsp.get("response")).get(0);
    assertEquals("newname = 2001", (String) d.getFieldValue("newname"));
  }


  
  @Test
  public void testBackup() throws Exception {
    masterJetty.stop();
    master.copyConfigFile(CONF_DIR + "solrconfig-master1.xml", 
                          "solrconfig.xml");

    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    nDocs--;
    masterClient.deleteByQuery("*:*");
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();
   
    class BackupThread extends Thread {
      volatile String fail = null;
      @Override
      public void run() {
        String masterUrl = "http://localhost:" + masterJetty.getLocalPort() + "/solr/replication?command=" + ReplicationHandler.CMD_BACKUP;
        URL url;
        InputStream stream = null;
        try {
          url = new URL(masterUrl);
          stream = url.openStream();
          stream.close();
        } catch (Exception e) {
          fail = e.getMessage();
        } finally {
          IOUtils.closeQuietly(stream);
        }

      };
    };
    BackupThread backupThread = new BackupThread();
    backupThread.start();
    
    File dataDir = new File(master.getDataDir());
    class CheckStatus extends Thread {
      volatile String fail = null;
      volatile String response = null;
      volatile boolean success = false;
      @Override
      public void run() {
        String masterUrl = "http://localhost:" + masterJetty.getLocalPort() + "/solr/replication?command=" + ReplicationHandler.CMD_DETAILS;
        URL url;
        InputStream stream = null;
        try {
          url = new URL(masterUrl);
          stream = url.openStream();
          response = IOUtils.toString(stream, "UTF-8");
          if(response.contains("<str name=\"status\">success</str>")) {
            success = true;
          }
          stream.close();
        } catch (Exception e) {
          fail = e.getMessage();
        } finally {
          IOUtils.closeQuietly(stream);
        }

      };
    };
    int waitCnt = 0;
    CheckStatus checkStatus = new CheckStatus();
    while(true) {
      checkStatus.run();
      if(checkStatus.fail != null) {
        fail(checkStatus.fail);
      }
      if(checkStatus.success) {
        break;
      }
      Thread.sleep(200);
      if(waitCnt == 10) {
        fail("Backup success not detected:" + checkStatus.response);
      }
      waitCnt++;
    }
    
    if(backupThread.fail != null) {
      fail(backupThread.fail);
    }

    File[] files = dataDir.listFiles(new FilenameFilter() {
      
      public boolean accept(File dir, String name) {
        if(name.startsWith("snapshot")) {
          return true;
        }
        return false;
      }
    });
    assertEquals(1, files.length);
    File snapDir = files[0];
    Directory dir = new SimpleFSDirectory(snapDir.getAbsoluteFile());
    IndexSearcher searcher = new IndexSearcher(dir, true);
    TopDocs hits = searcher.search(new MatchAllDocsQuery(), 1);

    assertEquals(nDocs, hits.totalHits);
    searcher.close();
    dir.close();
    AbstractSolrTestCase.recurseDelete(snapDir); // clean up the snap dir
  }

  /* character copy of file using UTF-8 */
  private static void copyFile(File src, File dst) throws IOException {
    copyFile(src, dst, null);
  }

  /**
   * character copy of file using UTF-8. If port is non-null, will be substituted any time "TEST_PORT" is found.
   */
  private static void copyFile(File src, File dst, Integer port) throws IOException {
    BufferedReader in = new BufferedReader(new FileReader(src));
    Writer out = new FileWriter(dst);

    for (String line = in.readLine(); null != line; line = in.readLine()) {

      if (null != port)
        line = line.replace("TEST_PORT", port.toString());
      out.write(line);
    }
    in.close();
    out.close();
  }

  private static class SolrInstance {

    private String name;
    private Integer testPort;
    private File homeDir;
    private File confDir;
    private File dataDir;

    /**
     * @param name used to pick new solr home dir, as well as which 
     *        "solrconfig-${name}.xml" file gets copied
     *        to solrconfig.xml in new conf dir.
     * @param testPort if not null, used as a replacement for
     *        TEST_PORT in the cloned config files.
     */
    public SolrInstance(String name, Integer testPort) {
      this.name = name;
      this.testPort = testPort;
    }

    public String getHomeDir() {
      return homeDir.toString();
    }

    public String getSchemaFile() {
      return CONF_DIR + "schema-replication1.xml";
    }

    public String getConfDir() {
      return confDir.toString();
    }

    public String getDataDir() {
      return dataDir.toString();
    }

    public String getSolrConfigFile() {
      return CONF_DIR + "solrconfig-"+name+".xml";
    }
    
    /** If it needs to change */
    public void setTestPort(Integer testPort) {
      this.testPort = testPort;
    }

    public void setUp() throws Exception {
      System.setProperty("solr.test.sys.prop1", "propone");
      System.setProperty("solr.test.sys.prop2", "proptwo");

      File home = new File(TEMP_DIR, 
                           getClass().getName() + "-" + 
                           System.currentTimeMillis());
                           

      homeDir = new File(home, name);
      dataDir = new File(homeDir, "data");
      confDir = new File(homeDir, "conf");

      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      File f = new File(confDir, "solrconfig.xml");
      copyConfigFile(getSolrConfigFile(), "solrconfig.xml");
      copyConfigFile(getSchemaFile(), "schema.xml");
    }

    public void tearDown() throws Exception {
      AbstractSolrTestCase.recurseDelete(homeDir);
    }

    public void copyConfigFile(String srcFile, String destFile) 
      throws IOException {

      copyFile(getFile(srcFile), 
               new File(confDir, destFile),
               testPort);
    }

  }
}
