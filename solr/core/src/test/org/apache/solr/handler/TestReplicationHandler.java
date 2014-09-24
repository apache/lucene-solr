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
package org.apache.solr.handler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CachingDirectoryFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.StandardDirectoryFactory;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for ReplicationHandler
 *
 *
 * @since 1.4
 */
@Slow
@SuppressSSL     // Currently unknown why SSL does not work with this test
public class TestReplicationHandler extends SolrTestCaseJ4 {


  private static final String CONF_DIR = "solr"
      + File.separator + "collection1" + File.separator + "conf"
      + File.separator;

  JettySolrRunner masterJetty, slaveJetty, repeaterJetty;
  SolrServer masterClient, slaveClient, repeaterClient;
  SolrInstance master = null, slave = null, repeater = null;

  static String context = "/solr";

  // number of docs to index... decremented for each test case to tell if we accidentally reuse
  // index from previous test method
  static int nDocs = 500;
  
  @BeforeClass
  public static void beforeClass() {

  }
  
  @Before
  public void setUp() throws Exception {
    super.setUp();
//    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    // For manual testing only
    // useFactory(null); // force an FS factory.
    master = new SolrInstance(createTempDir("solr-instance").toFile(), "master", null);
    master.setUp();
    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    slave = new SolrInstance(createTempDir("solr-instance").toFile(), "slave", masterJetty.getLocalPort());
    slave.setUp();
    slaveJetty = createJetty(slave);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());
  }

  public void clearIndexWithReplication() throws Exception {
    if (numFound(query("*:*", masterClient)) != 0) {
      masterClient.deleteByQuery("*:*");
      masterClient.commit();
      // wait for replication to sync & verify
      assertEquals(0, numFound(rQuery(0, "*:*", slaveClient)));
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    masterJetty.stop();
    slaveJetty.stop();
    master.tearDown();
    slave.tearDown();
    masterJetty = slaveJetty = null;
    master = slave = null;
    masterClient.shutdown();
    slaveClient.shutdown();
    masterClient = slaveClient = null;
  }

  private static JettySolrRunner createJetty(SolrInstance instance) throws Exception {
    FileUtils.copyFile(new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml"), new File(instance.getHomeDir(), "solr.xml"));
    JettySolrRunner jetty = new JettySolrRunner(instance.getHomeDir(), "/solr", 0);
    jetty.setDataDir(instance.getDataDir());
    jetty.start();
    return jetty;
  }

  private static SolrServer createNewSolrServer(int port) {
    try {
      // setup the server...
      HttpSolrServer s = new HttpSolrServer(buildUrl(port));
      s.setConnectionTimeout(15000);
      s.setSoTimeout(60000);
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
    params.add("sort","id desc");

    QueryResponse qres = s.query(params);

    res = qres.getResponse();

    return res;
  }

  /** will sleep up to 30 seconds, looking for expectedDocCount */
  private NamedList rQuery(int expectedDocCount, String query, SolrServer server) throws Exception {
    int timeSlept = 0;
    NamedList res = query(query, server);
    while (expectedDocCount != numFound(res)
           && timeSlept < 30000) {
      log.info("Waiting for " + expectedDocCount + " docs");
      timeSlept += 100;
      Thread.sleep(100);
      res = query(query, server);
    }
    log.info("Waited for {}ms and found {} docs", timeSlept, numFound(res));
    return res;
  }
  
  private long numFound(NamedList res) {
    return ((SolrDocumentList) res.get("response")).getNumFound();
  }

  private NamedList<Object> getDetails(SolrServer s) throws Exception {
    

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command","details");
    params.set("_trace","getDetails");
    params.set("qt","/replication");
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);

    assertNotNull("null response from server", res);

    @SuppressWarnings("unchecked") NamedList<Object> details 
      = (NamedList<Object>) res.get("details");

    assertNotNull("null details", details);

    return details;
  }
  
  private NamedList<Object> getCommits(SolrServer s) throws Exception {
    

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command","commits");
    params.set("_trace","getCommits");
    params.set("qt","/replication");
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);

    assertNotNull("null response from server", res);


    return res;
  }
  
  private NamedList<Object> getIndexVersion(SolrServer s) throws Exception {
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command","indexversion");
    params.set("_trace","getIndexVersion");
    params.set("qt","/replication");
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);

    assertNotNull("null response from server", res);


    return res;
  }
  
  private NamedList<Object> reloadCore(SolrServer s, String core) throws Exception {
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action","reload");
    params.set("core", core);
    params.set("qt","/admin/cores");
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);

    assertNotNull("null response from server", res);

    return res;
  }

  @Test
  public void doTestDetails() throws Exception {
    clearIndexWithReplication();
    { 
      NamedList<Object> details = getDetails(masterClient);
      
      assertEquals("master isMaster?", 
                   "true", details.get("isMaster"));
      assertEquals("master isSlave?", 
                   "false", details.get("isSlave"));
      assertNotNull("master has master section", 
                    details.get("master"));
    }

    // check details on the slave a couple of times before & after fetching
    for (int i = 0; i < 3; i++) {
      NamedList<Object> details = getDetails(slaveClient);
      
      assertEquals("slave isMaster?", 
                   "false", details.get("isMaster"));
      assertEquals("slave isSlave?", 
                   "true", details.get("isSlave"));
      assertNotNull("slave has slave section", 
                    details.get("slave"));
      // SOLR-2677: assert not false negatives
      Object timesFailed = ((NamedList)details.get("slave")).get(SnapPuller.TIMES_FAILED);
      assertEquals("slave has fetch error count",
                   null, timesFailed);

      if (3 != i) {
        // index & fetch
        index(masterClient, "id", i, "name", "name = " + i);
        masterClient.commit();
        pullFromTo(masterJetty, slaveJetty);
      }
    }

    SolrInstance repeater = null;
    JettySolrRunner repeaterJetty = null;
    SolrServer repeaterClient = null;
    try {
      repeater = new SolrInstance(createTempDir("solr-instance").toFile(), "repeater", masterJetty.getLocalPort());
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
      if (repeaterClient != null) repeaterClient.shutdown();
    }
  }


  /**
   * Verify that things still work if an IW has not been opened (and hence the CommitPoints have not been communicated to the deletion policy)
   */
  public void testNoWriter() throws Exception {
    useFactory(null);    // force a persistent directory

    // read-only setting (no opening from indexwriter)
    System.setProperty("solr.tests.nrtMode", "false");
    try {
    // stop and start so they see the new directory setting
    slaveJetty.stop();
    masterJetty.stop();
    slaveJetty.start(true);
    masterJetty.start(true);

    index(slaveClient, "id", "123456");
    slaveClient.commit();
    slaveJetty.stop();
    slaveJetty.start(true);
    } finally {
      System.clearProperty("solr.tests.nrtMode"); // dont mess with other tests
    }

    // Currently we open a writer on-demand.  This is to test that we are correctly testing
    // the code path when SolrDeletionPolicy.getLatestCommit() returns null.
    // When we are using an ephemeral directory, an IW will always be opened to create the index and hence
    // getLatestCommit will always be non-null.
    CoreContainer cores = ((SolrDispatchFilter) slaveJetty.getDispatchFilter().getFilter()).getCores();
    Collection<SolrCore> theCores = cores.getCores();
    assertEquals(1, theCores.size());
    SolrCore core = (SolrCore)theCores.toArray()[0];
    assertNull( core.getDeletionPolicy().getLatestCommit() );


    pullFromMasterToSlave();  // this will cause SnapPuller to be invoked and we will test when SolrDeletionPolicy.getLatestCommit() returns null

    resetFactory();
  }

  /**
   * Verify that empty commits and/or commits with openSearcher=false 
   * on the master do not cause subsequent replication problems on the slave 
   */
  public void testEmptyCommits() throws Exception {
    clearIndexWithReplication();
    
    // add a doc to master and commit
    index(masterClient, "id", "1", "name", "empty1");
    emptyUpdate(masterClient, "commit", "true");
    // force replication
    pullFromMasterToSlave();
    // verify doc is on slave
    rQuery(1, "name:empty1", slaveClient);
    assertVersions(masterClient, slaveClient);

    // do a completely empty commit on master and force replication
    emptyUpdate(masterClient, "commit", "true");
    pullFromMasterToSlave();

    // add another doc and verify slave gets it
    index(masterClient, "id", "2", "name", "empty2");
    emptyUpdate(masterClient, "commit", "true");
    // force replication
    pullFromMasterToSlave();

    rQuery(1, "name:empty2", slaveClient);
    assertVersions(masterClient, slaveClient);

    // add a third doc but don't open a new searcher on master
    index(masterClient, "id", "3", "name", "empty3");
    emptyUpdate(masterClient, "commit", "true", "openSearcher", "false");
    pullFromMasterToSlave();
    
    // verify slave can search the doc, but master doesn't
    rQuery(0, "name:empty3", masterClient);
    rQuery(1, "name:empty3", slaveClient);

    // final doc with hard commit, slave and master both showing all docs
    index(masterClient, "id", "4", "name", "empty4");
    emptyUpdate(masterClient, "commit", "true");
    pullFromMasterToSlave();

    String q = "name:(empty1 empty2 empty3 empty4)";
    rQuery(4, q, masterClient);
    rQuery(4, q, slaveClient);
    assertVersions(masterClient, slaveClient);

  }

  @Test
  public void doTestReplicateAfterWrite2Slave() throws Exception {
    clearIndexWithReplication();
    nDocs--;
    for (int i = 0; i < nDocs; i++) {
      index(masterClient, "id", i, "name", "name = " + i);
    }

    invokeReplicationCommand(masterJetty.getLocalPort(), "disableReplication");
    invokeReplicationCommand(slaveJetty.getLocalPort(), "disablepoll");
    
    masterClient.commit();

    assertEquals(nDocs, numFound(rQuery(nDocs, "*:*", masterClient)));

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
    assertEquals(1, numFound(rQuery(1, "id:555", slaveClient)));

    //Let's fetch the index rather than rely on the polling.
    invokeReplicationCommand(masterJetty.getLocalPort(), "enablereplication");
    invokeReplicationCommand(slaveJetty.getLocalPort(), "fetchindex");

    /*
    //the slave should have done a full copy of the index so the doc with id:555 should not be there in the slave now
    slaveQueryRsp = rQuery(0, "id:555", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(0, slaveQueryResult.getNumFound());

    // make sure we replicated the correct index from the master
    slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());
    
    */
  }

  //Simple function to wrap the invocation of replication commands on the various
  //jetty servers.
  private void invokeReplicationCommand(int pJettyPort, String pCommand) throws IOException
  {
    String masterUrl = buildUrl(pJettyPort) + "/replication?command=" + pCommand;
    URL u = new URL(masterUrl);
    InputStream stream = u.openStream();
    stream.close();
  }
  
  @Test
  public void doTestIndexAndConfigReplication() throws Exception {
    clearIndexWithReplication();

    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, numFound(masterQueryRsp));

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, numFound(slaveQueryRsp));

    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);
    
    assertVersions(masterClient, slaveClient);

    //start config files replication test
    masterClient.deleteByQuery("*:*");
    masterClient.commit();

    //change the schema on master
    master.copyConfigFile(CONF_DIR + "schema-replication2.xml", "schema.xml");

    masterJetty.stop();

    masterJetty = createJetty(master);
    masterClient.shutdown();
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    slaveJetty.stop();

    // setup an xslt dir to force subdir file replication
    File masterXsltDir = new File(master.getConfDir() + File.separator + "xslt");
    File masterXsl = new File(masterXsltDir, "dummy.xsl");
    assertTrue("could not make dir " + masterXsltDir, masterXsltDir.mkdirs());
    assertTrue(masterXsl.createNewFile());

    File slaveXsltDir = new File(slave.getConfDir() + File.separator + "xslt");
    File slaveXsl = new File(slaveXsltDir, "dummy.xsl");
    assertFalse(slaveXsltDir.exists());

    slaveJetty = createJetty(slave);
    slaveClient.shutdown();
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    //add a doc with new field and commit on master to trigger snappull from slave.
    index(masterClient, "id", "2000", "name", "name = " + 2000, "newname", "newname = " + 2000);
    masterClient.commit();

    assertEquals(1, numFound( rQuery(1, "*:*", masterClient)));
    
    slaveQueryRsp = rQuery(1, "*:*", slaveClient);
    assertVersions(masterClient, slaveClient);
    SolrDocument d = ((SolrDocumentList) slaveQueryRsp.get("response")).get(0);
    assertEquals("newname = 2000", (String) d.getFieldValue("newname"));

    assertTrue(slaveXsltDir.isDirectory());
    assertTrue(slaveXsl.exists());
    
    checkForSingleIndex(masterJetty);
    checkForSingleIndex(slaveJetty);
    
  }

  @Test
  public void doTestStopPoll() throws Exception {
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
    assertEquals(nDocs, numFound(masterQueryRsp));

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, numFound(slaveQueryRsp));

    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    // start stop polling test
    invokeReplicationCommand(slaveJetty.getLocalPort(), "disablepoll");
    
    index(masterClient, "id", 501, "name", "name = " + 501);
    masterClient.commit();

    //get docs from master and check if number is equal to master
    assertEquals(nDocs+1, numFound(rQuery(nDocs+1, "*:*", masterClient)));
    
    // NOTE: this test is wierd, we want to verify it DOESNT replicate...
    // for now, add a sleep for this.., but the logic is wierd.
    Thread.sleep(3000);
    
    //get docs from slave and check if number is not equal to master; polling is disabled
    assertEquals(nDocs, numFound(rQuery(nDocs, "*:*", slaveClient)));

    // re-enable replication
    invokeReplicationCommand(slaveJetty.getLocalPort(), "enablepoll");

    assertEquals(nDocs+1, numFound(rQuery(nDocs+1, "*:*", slaveClient)));
  }

  @Test
  public void doTestSnapPullWithMasterUrl() throws Exception {
    //change solrconfig on slave
    //this has no entry for pollinginterval
    slave.copyConfigFile(CONF_DIR + "solrconfig-slave1.xml", "solrconfig.xml");
    slaveJetty.stop();
    slaveJetty = createJetty(slave);
    slaveClient.shutdown();
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    masterClient.deleteByQuery("*:*");
    slaveClient.deleteByQuery("*:*");
    slaveClient.commit();
    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    // make sure prepareCommit doesn't mess up commit  (SOLR-3938)
    
    // todo: make SolrJ easier to pass arbitrary params to
    // TODO: precommit WILL screw with the rest of this test

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());

    // snappull
    String masterUrl = buildUrl(slaveJetty.getLocalPort()) + "/replication?command=fetchindex&masterUrl=";
    masterUrl += buildUrl(masterJetty.getLocalPort()) + "/replication";
    URL url = new URL(masterUrl);
    InputStream stream = url.openStream();
    stream.close();
    
    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());
    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    // snappull from the slave to the master
    
    for (int i = nDocs; i < nDocs + 3; i++)
      index(slaveClient, "id", i, "name", "name = " + i);

    slaveClient.commit();
    
    pullFromSlaveToMaster();
    rQuery(nDocs + 3, "*:*", masterClient);
    
    //get docs from slave and check if number is equal to master
    slaveQueryRsp = rQuery(nDocs + 3, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs + 3, slaveQueryResult.getNumFound());
    //compare results
    masterQueryRsp = rQuery(nDocs + 3, "*:*", masterClient);
    masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    assertVersions(masterClient, slaveClient);
    
    pullFromSlaveToMaster();
    
    //get docs from slave and check if number is equal to master
    slaveQueryRsp = rQuery(nDocs + 3, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs + 3, slaveQueryResult.getNumFound());
    //compare results
    masterQueryRsp = rQuery(nDocs + 3, "*:*", masterClient);
    masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);
    
    assertVersions(masterClient, slaveClient);
    
    // now force a new index directory
    for (int i = nDocs + 3; i < nDocs + 7; i++)
      index(masterClient, "id", i, "name", "name = " + i);
    
    masterClient.commit();
    
    pullFromSlaveToMaster();
    rQuery((int) slaveQueryResult.getNumFound(), "*:*", masterClient);
    
    //get docs from slave and check if number is equal to master
    slaveQueryRsp = rQuery(nDocs + 3, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs + 3, slaveQueryResult.getNumFound());
    //compare results
    masterQueryRsp = rQuery(nDocs + 3, "*:*", masterClient);
    masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);
    
    assertVersions(masterClient, slaveClient);
    pullFromSlaveToMaster();
    
    //get docs from slave and check if number is equal to master
    slaveQueryRsp = rQuery(nDocs + 3, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs + 3, slaveQueryResult.getNumFound());
    //compare results
    masterQueryRsp = rQuery(nDocs + 3, "*:*", masterClient);
    masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);
    
    assertVersions(masterClient, slaveClient);
    
    NamedList<Object> details = getDetails(masterClient);
   
    details = getDetails(slaveClient);
    
    checkForSingleIndex(masterJetty);
    checkForSingleIndex(slaveJetty);
  }
  
  
  @Test 
  public void doTestStressReplication() throws Exception {
    // change solrconfig on slave
    // this has no entry for pollinginterval
    
    // get us a straight standard fs dir rather than mock*dir
    boolean useStraightStandardDirectory = random().nextBoolean();
    
    if (useStraightStandardDirectory) {
      useFactory(null);
    }
    final String SLAVE_SCHEMA_1 = "schema-replication1.xml";
    final String SLAVE_SCHEMA_2 = "schema-replication2.xml";
    String slaveSchema = SLAVE_SCHEMA_1;

    try {
      
      slave.copyConfigFile(CONF_DIR +"solrconfig-slave1.xml", "solrconfig.xml");
      slave.copyConfigFile(CONF_DIR +slaveSchema, "schema.xml");
      slaveJetty.stop();
      slaveJetty = createJetty(slave);
      slaveClient.shutdown();
      slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

      master.copyConfigFile(CONF_DIR + "solrconfig-master3.xml",
          "solrconfig.xml");
      masterJetty.stop();
      masterJetty = createJetty(master);
      masterClient.shutdown();
      masterClient = createNewSolrServer(masterJetty.getLocalPort());
      
      masterClient.deleteByQuery("*:*");
      slaveClient.deleteByQuery("*:*");
      slaveClient.commit();
      
      int maxDocs = TEST_NIGHTLY ? 1000 : 200;
      int rounds = TEST_NIGHTLY ? 80 : 8;
      int totalDocs = 0;
      int id = 0;
      for (int x = 0; x < rounds; x++) {
        
        final boolean confCoreReload = random().nextBoolean();
        if (confCoreReload) {
          // toggle the schema file used

          slaveSchema = slaveSchema.equals(SLAVE_SCHEMA_1) ? 
            SLAVE_SCHEMA_2 : SLAVE_SCHEMA_1;
          master.copyConfigFile(CONF_DIR + slaveSchema, "schema.xml");
        }
        
        int docs = random().nextInt(maxDocs) + 1;
        for (int i = 0; i < docs; i++) {
          index(masterClient, "id", id++, "name", "name = " + i);
        }
        
        totalDocs += docs;
        masterClient.commit();
        
        NamedList masterQueryRsp = rQuery(totalDocs, "*:*", masterClient);
        SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp
            .get("response");
        assertEquals(totalDocs, masterQueryResult.getNumFound());
        
        // snappull
        Date slaveCoreStart = watchCoreStartAt(slaveClient, 30*1000, null);
        pullFromMasterToSlave();
        if (confCoreReload) {
          watchCoreStartAt(slaveClient, 30*1000, slaveCoreStart);
        }

        // get docs from slave and check if number is equal to master
        NamedList slaveQueryRsp = rQuery(totalDocs, "*:*", slaveClient);
        SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp
            .get("response");
        assertEquals(totalDocs, slaveQueryResult.getNumFound());
        // compare results
        String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult,
            slaveQueryResult, 0, null);
        assertEquals(null, cmp);
        
        assertVersions(masterClient, slaveClient);
        
        checkForSingleIndex(masterJetty);
        checkForSingleIndex(slaveJetty);
        
        if (random().nextBoolean()) {
          // move the slave ahead
          for (int i = 0; i < 3; i++) {
            index(slaveClient, "id", id++, "name", "name = " + i);
          }
          slaveClient.commit();
        }
        
      }
      
    } finally {
      if (useStraightStandardDirectory) {
        resetFactory();
      }
    }
  }

  private void checkForSingleIndex(JettySolrRunner jetty) {
    CoreContainer cores = ((SolrDispatchFilter) jetty.getDispatchFilter().getFilter()).getCores();
    Collection<SolrCore> theCores = cores.getCores();
    for (SolrCore core : theCores) {
      String ddir = core.getDataDir();
      CachingDirectoryFactory dirFactory = (CachingDirectoryFactory) core.getDirectoryFactory();
      synchronized (dirFactory) {
        Set<String> livePaths = dirFactory.getLivePaths();
        // one for data, one for hte index under data
        assertEquals(livePaths.toString(), 2, livePaths.size());
        // :TODO: assert that one of the paths is a subpath of hte other
      }
      if (dirFactory instanceof StandardDirectoryFactory) {
        System.out.println(Arrays.asList(new File(ddir).list()));
        assertEquals(Arrays.asList(new File(ddir).list()).toString(), 1, indexDirCount(ddir));
      }
    }
  }

  private int indexDirCount(String ddir) {
    String[] list = new File(ddir).list();
    int cnt = 0;
    for (String file : list) {
      if (!file.endsWith(".properties")) {
        cnt++;
      }
    }
    return cnt;
  }

  private void pullFromMasterToSlave() throws MalformedURLException,
      IOException {
    pullFromTo(masterJetty, slaveJetty);
  }
  
  @Test
  public void doTestRepeater() throws Exception {
    // no polling
    slave.copyConfigFile(CONF_DIR + "solrconfig-slave1.xml", "solrconfig.xml");
    slaveJetty.stop();
    slaveJetty = createJetty(slave);
    slaveClient.shutdown();
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    try {
      repeater = new SolrInstance(createTempDir("solr-instance").toFile(), "repeater", null);
      repeater.setUp();
      repeater.copyConfigFile(CONF_DIR + "solrconfig-repeater.xml",
          "solrconfig.xml");
      repeaterJetty = createJetty(repeater);
      if (repeaterClient != null) {
        repeaterClient.shutdown();
      }
      repeaterClient = createNewSolrServer(repeaterJetty.getLocalPort());
      
      for (int i = 0; i < 3; i++)
        index(masterClient, "id", i, "name", "name = " + i);

      masterClient.commit();
      
      pullFromTo(masterJetty, repeaterJetty);
      
      rQuery(3, "*:*", repeaterClient);
      
      pullFromTo(repeaterJetty, slaveJetty);
      
      rQuery(3, "*:*", slaveClient);
      
      assertVersions(masterClient, repeaterClient);
      assertVersions(repeaterClient, slaveClient);
      
      for (int i = 0; i < 4; i++)
        index(repeaterClient, "id", i, "name", "name = " + i);
      repeaterClient.commit();
      
      pullFromTo(masterJetty, repeaterJetty);
      
      rQuery(3, "*:*", repeaterClient);
      
      pullFromTo(repeaterJetty, slaveJetty);
      
      rQuery(3, "*:*", slaveClient);
      
      for (int i = 3; i < 6; i++)
        index(masterClient, "id", i, "name", "name = " + i);
      
      masterClient.commit();
      
      pullFromTo(masterJetty, repeaterJetty);
      
      rQuery(6, "*:*", repeaterClient);
      
      pullFromTo(repeaterJetty, slaveJetty);
      
      rQuery(6, "*:*", slaveClient);

    } finally {
      if (repeater != null) {
        repeaterJetty.stop();
        repeater.tearDown();
        repeaterJetty = null;
      }
    }
    
  }

  private void assertVersions(SolrServer client1, SolrServer client2) throws Exception {
    NamedList<Object> details = getDetails(client1);
    ArrayList<NamedList<Object>> commits = (ArrayList<NamedList<Object>>) details.get("commits");
    Long maxVersionClient1 = getVersion(client1);
    Long maxVersionClient2 = getVersion(client2);

    if (maxVersionClient1 > 0 && maxVersionClient2 > 0) {
      assertEquals(maxVersionClient1, maxVersionClient2);
    }
    
    // check vs /replication?command=indexversion call
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", "/replication");
    params.set("_trace", "assertVersions");
    params.set("command", "indexversion");
    QueryRequest req = new QueryRequest(params);
    NamedList<Object> resp = client1.request(req);
    
    Long version = (Long) resp.get("indexversion");
    assertEquals(maxVersionClient1, version);
    
    // check vs /replication?command=indexversion call
    resp = client2.request(req);
    version = (Long) resp.get("indexversion");
    assertEquals(maxVersionClient2, version);
  }

  private Long getVersion(SolrServer client) throws Exception {
    NamedList<Object> details;
    ArrayList<NamedList<Object>> commits;
    details = getDetails(client);
    commits = (ArrayList<NamedList<Object>>) details.get("commits");
    Long maxVersionSlave= 0L;
    for(NamedList<Object> commit : commits) {
      Long version = (Long) commit.get("indexVersion");
      maxVersionSlave = Math.max(version, maxVersionSlave);
    }
    return maxVersionSlave;
  }

  private void pullFromSlaveToMaster() throws MalformedURLException,
      IOException {
    pullFromTo(slaveJetty, masterJetty);
  }
  
  private void pullFromTo(JettySolrRunner from, JettySolrRunner to) throws IOException {
    String masterUrl;
    URL url;
    InputStream stream;
    masterUrl = buildUrl(to.getLocalPort())
        + "/replication?wait=true&command=fetchindex&masterUrl="
        + buildUrl(from.getLocalPort()) + "/replication";
    url = new URL(masterUrl);
    stream = url.openStream();
    stream.close();
  }

  @Test
  public void doTestReplicateAfterStartup() throws Exception {
    //stop slave
    slaveJetty.stop();

    nDocs--;
    masterClient.deleteByQuery("*:*");

    masterClient.commit();



    //change solrconfig having 'replicateAfter startup' option on master
    master.copyConfigFile(CONF_DIR + "solrconfig-master2.xml",
                          "solrconfig.xml");

    masterJetty.stop();

    masterJetty = createJetty(master);
    masterClient.shutdown();
    masterClient = createNewSolrServer(masterJetty.getLocalPort());
    
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();
    
    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());
    

    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    //start slave
    slaveJetty = createJetty(slave);
    slaveClient.shutdown();
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());

    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

  }
  
  @Test
  public void doTestReplicateAfterStartupWithNoActivity() throws Exception {
    useFactory(null);
    try {
      
      // stop slave
      slaveJetty.stop();
      
      nDocs--;
      masterClient.deleteByQuery("*:*");
      
      masterClient.commit();
      
      // change solrconfig having 'replicateAfter startup' option on master
      master.copyConfigFile(CONF_DIR + "solrconfig-master2.xml",
          "solrconfig.xml");
      
      masterJetty.stop();
      
      masterJetty = createJetty(master);
      masterClient.shutdown();
      masterClient = createNewSolrServer(masterJetty.getLocalPort());
      
      for (int i = 0; i < nDocs; i++)
        index(masterClient, "id", i, "name", "name = " + i);
      
      masterClient.commit();
      
      // now we restart to test what happens with no activity before the slave
      // tries to
      // replicate
      masterJetty.stop();
      masterJetty.start(true);
      
      // masterClient = createNewSolrServer(masterJetty.getLocalPort());
      
      NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
      SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp
          .get("response");
      assertEquals(nDocs, masterQueryResult.getNumFound());
      
      slave.setTestPort(masterJetty.getLocalPort());
      slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");
      
      // start slave
      slaveJetty = createJetty(slave);
      slaveClient.shutdown();
      slaveClient = createNewSolrServer(slaveJetty.getLocalPort());
      
      // get docs from slave and check if number is equal to master
      NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
      SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp
          .get("response");
      assertEquals(nDocs, slaveQueryResult.getNumFound());
      
      // compare results
      String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult,
          slaveQueryResult, 0, null);
      assertEquals(null, cmp);
      
    } finally {
      resetFactory();
    }
  }

  @Test
  public void doTestReplicateAfterCoreReload() throws Exception {
    int docs = TEST_NIGHTLY ? 200000 : 0;
    
    //stop slave
    slaveJetty.stop();


    //change solrconfig having 'replicateAfter startup' option on master
    master.copyConfigFile(CONF_DIR + "solrconfig-master3.xml",
                          "solrconfig.xml");

    masterJetty.stop();

    masterJetty = createJetty(master);
    masterClient.shutdown();
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    masterClient.deleteByQuery("*:*");
    for (int i = 0; i < docs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(docs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(docs, masterQueryResult.getNumFound());
    
    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    //start slave
    slaveJetty = createJetty(slave);
    slaveClient.shutdown();
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());
    
    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(docs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(docs, slaveQueryResult.getNumFound());
    
    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);
    
    Object version = getIndexVersion(masterClient).get("indexversion");
    NamedList<Object> commits = getCommits(masterClient);
    
    reloadCore(masterClient, "collection1");
    
    assertEquals(version, getIndexVersion(masterClient).get("indexversion"));
    assertEquals(commits.get("commits"), getCommits(masterClient).get("commits"));
    
    index(masterClient, "id", docs + 10, "name", "name = 1");
    index(masterClient, "id", docs + 20, "name", "name = 2");

    masterClient.commit();
    
    NamedList resp =  rQuery(docs + 2, "*:*", masterClient);
    masterQueryResult = (SolrDocumentList) resp.get("response");
    assertEquals(docs + 2, masterQueryResult.getNumFound());
    
    //get docs from slave and check if number is equal to master
    slaveQueryRsp = rQuery(docs + 2, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(docs + 2, slaveQueryResult.getNumFound());
    
  }

  @Test
  public void doTestIndexAndConfigAliasReplication() throws Exception {
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
    String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    //start config files replication test
    //clear master index
    masterClient.deleteByQuery("*:*");
    masterClient.commit();
    rQuery(0, "*:*", masterClient); // sanity check w/retry

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
    masterClient.shutdown();
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    slaveJetty.stop();
    slaveJetty = createJetty(slave);
    slaveClient.shutdown();
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    slaveClient.deleteByQuery("*:*");
    slaveClient.commit();
    rQuery(0, "*:*", slaveClient); // sanity check w/retry
    
    // record collection1's start time on slave
    final Date slaveStartTime = watchCoreStartAt(slaveClient, 30*1000, null);

    //add a doc with new field and commit on master to trigger snappull from slave.
    index(masterClient, "id", "2000", "name", "name = " + 2000, "newname", "n2000");
    masterClient.commit();
    rQuery(1, "newname:n2000", masterClient);  // sanity check

    // wait for slave to reload core by watching updated startTime
    watchCoreStartAt(slaveClient, 30*1000, slaveStartTime);

    NamedList masterQueryRsp2 = rQuery(1, "id:2000", masterClient);
    SolrDocumentList masterQueryResult2 = (SolrDocumentList) masterQueryRsp2.get("response");
    assertEquals(1, masterQueryResult2.getNumFound());

    NamedList slaveQueryRsp2 = rQuery(1, "id:2000", slaveClient);
    SolrDocumentList slaveQueryResult2 = (SolrDocumentList) slaveQueryRsp2.get("response");
    assertEquals(1, slaveQueryResult2.getNumFound());
    
    index(slaveClient, "id", "2001", "name", "name = " + 2001, "newname", "n2001");
    slaveClient.commit();

    slaveQueryRsp = rQuery(1, "id:2001", slaveClient);
    SolrDocument d = ((SolrDocumentList) slaveQueryRsp.get("response")).get(0);
    assertEquals("n2001", (String) d.getFieldValue("newname"));
    
    checkForSingleIndex(masterJetty);
    checkForSingleIndex(slaveJetty);
  }

  @Test
  public void testRateLimitedReplication() throws Exception {

    //clean index
    masterClient.deleteByQuery("*:*");
    slaveClient.deleteByQuery("*:*");
    masterClient.commit();
    slaveClient.commit();

    masterJetty.stop();
    slaveJetty.stop();

    //Start master with the new solrconfig
    master.copyConfigFile(CONF_DIR + "solrconfig-master-throttled.xml", "solrconfig.xml");
    useFactory(null);
    masterJetty = createJetty(master);
    masterClient.shutdown();
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    //index docs
    final int totalDocs = TestUtil.nextInt(random(), 50, 100);
    for (int i = 0; i < totalDocs; i++)
      index(masterClient, "id", i, "name", TestUtil.randomSimpleString(random(), 1000 , 5000));

    masterClient.commit();

    //Check Index Size
    String dataDir = master.getDataDir();
    masterClient.shutdown();
    masterJetty.stop();

    Directory dir = FSDirectory.open(Paths.get(dataDir, "index"));
    String[] files = dir.listAll();
    long totalBytes = 0;
    for(String file : files) {
      totalBytes += dir.fileLength(file);
    }

    float approximateTimeInSeconds = Math.round( totalBytes/1024/1024/0.1 ); // maxWriteMBPerSec=0.1 in solrconfig

    //Start again and replicate the data
    useFactory(null);
    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    //start slave
    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(CONF_DIR + "solrconfig-slave1.xml", "solrconfig.xml");
    slaveJetty = createJetty(slave);
    slaveClient.shutdown();
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    long startTime = System.nanoTime();

    pullFromMasterToSlave();

    //Add a few more docs in the master. Just to make sure that we are replicating the correct index point
    //These extra docs should not get replicated
    new Thread(new AddExtraDocs(masterClient, totalDocs)).start();

    //Wait and make sure that it actually replicated correctly.
    NamedList slaveQueryRsp = rQuery(totalDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(totalDocs, slaveQueryResult.getNumFound());

    long timeTaken = System.nanoTime() - startTime;

    long timeTakenInSeconds = TimeUnit.SECONDS.convert(timeTaken, TimeUnit.NANOSECONDS);

    //Let's make sure it took more than approximateTimeInSeconds to make sure that it was throttled
    boolean isElapsed = false;
    if(timeTakenInSeconds - approximateTimeInSeconds > 0) {
      isElapsed = true;
    }
    assertTrue(isElapsed);
  }

  private class AddExtraDocs implements Runnable {

    SolrServer masterClient;
    int startId;
    public AddExtraDocs(SolrServer masterClient, int startId) {
      this.masterClient = masterClient;
      this.startId = startId;
    }

    @Override
    public void run() {
      final int totalDocs = TestUtil.nextInt(random(), 1, 10);
      for (int i = 0; i < totalDocs; i++) {
        try {
          index(masterClient, "id", i + startId, "name", TestUtil.randomSimpleString(random(), 1000 , 5000));
        } catch (Exception e) {
          //Do nothing. Wasn't able to add doc.
        }
      }
      try {
        masterClient.commit();
      } catch (Exception e) {
        //Do nothing. No extra doc got committed.
      }
    }
  }
  
  /**
   * character copy of file using UTF-8. If port is non-null, will be substituted any time "TEST_PORT" is found.
   */
  private static void copyFile(File src, File dst, Integer port, boolean internalCompression) throws IOException {
    BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(src), StandardCharsets.UTF_8));
    Writer out = new OutputStreamWriter(new FileOutputStream(dst), StandardCharsets.UTF_8);

    for (String line = in.readLine(); null != line; line = in.readLine()) {

      if (null != port)
        line = line.replace("TEST_PORT", port.toString());
      
      line = line.replace("COMPRESSION", internalCompression?"internal":"false");

      out.write(line);
    }
    in.close();
    out.close();
  }

  private UpdateResponse emptyUpdate(SolrServer client, String... params) 
    throws SolrServerException, IOException {

    UpdateRequest req = new UpdateRequest();
    req.setParams(params(params));
    return req.process(client);
  }

  /**
   * Polls the SolrCore stats using the specified client until the "startTime" 
   * time for collection is after the specified "min".  Will loop for 
   * at most "timeout" milliseconds before throwing an assertion failure.
   * 
   * @param client The SolrServer to poll
   * @param timeout the max milliseconds to continue polling for
   * @param min the startTime value must exceed this value before the method will return, if null this method will return the first startTime value encountered.
   * @return the startTime value of collection
   */
  private Date watchCoreStartAt(SolrServer client, final long timeout, 
                                final Date min) throws InterruptedException, IOException, SolrServerException {
    final long sleepInterval = 200;
    long timeSlept = 0;

    SolrParams p = params("action","status", "core", "collection1");
    while (timeSlept < timeout) {
      QueryRequest req = new QueryRequest(p);
      req.setPath("/admin/cores");
      try {
        NamedList data = client.request(req);
        for (String k : new String[] {"status","collection1"}) {
          Object o = data.get(k);
          assertNotNull("core status rsp missing key: " + k, o);
          data = (NamedList) o;
        }
        Date startTime = (Date) data.get("startTime");
        assertNotNull("core has null startTime", startTime);
        if (null == min || startTime.after(min)) {
          return startTime;
        }
      } catch (SolrException e) {
        // workarround for SOLR-4668
        if (500 != e.code()) {
          throw e;
        } // else server possibly from the core reload in progress...
      }

      timeSlept += sleepInterval;
      Thread.sleep(sleepInterval);
    }
    fail("timed out waiting for collection1 startAt time to exceed: " + min);
    return min; // compilation neccessity
  }
  
  private static String buildUrl(int port) {
    return buildUrl(port, context);
  }

  static class SolrInstance {

    private String name;
    private Integer testPort;
    private File homeDir;
    private File confDir;
    private File dataDir;

    /**
     * @param homeDir Base directory to build solr configuration and index in
     * @param name used to pick which
     *        "solrconfig-${name}.xml" file gets copied
     *        to solrconfig.xml in new conf dir.
     * @param testPort if not null, used as a replacement for
     *        TEST_PORT in the cloned config files.
     */
    public SolrInstance(File homeDir, String name, Integer testPort) {
      this.homeDir = homeDir;
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
      return dataDir.getAbsolutePath();
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

      dataDir = new File(homeDir + "/collection1", "data");
      confDir = new File(homeDir + "/collection1", "conf");

      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      copyConfigFile(getSolrConfigFile(), "solrconfig.xml");
      copyConfigFile(getSchemaFile(), "schema.xml");
      copyConfigFile(CONF_DIR + "solrconfig.snippet.randomindexconfig.xml", 
                     "solrconfig.snippet.randomindexconfig.xml");
    }

    public void tearDown() throws Exception {
      IOUtils.rm(homeDir.toPath());
    }

    public void copyConfigFile(String srcFile, String destFile) 
      throws IOException {
      copyFile(getFile(srcFile), 
               new File(confDir, destFile),
               testPort, random().nextBoolean());
    }

  }
}
