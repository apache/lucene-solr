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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;

import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.CachingDirectoryFactory;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.StandardDirectoryFactory;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.matchers.JUnitMatchers.containsString;

/**
 * Test for ReplicationHandler
 *
 *
 * @since 1.4
 */
@Slow
@SuppressSSL     // Currently unknown why SSL does not work with this test
// commented 20-July-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
// commented out on: 24-Dec-2018 @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 23-Aug-2018
public class TestReplicationHandler extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String CONF_DIR = "solr"
      + File.separator + "collection1" + File.separator + "conf"
      + File.separator;

  JettySolrRunner masterJetty, secondaryJetty, repeaterJetty;
  HttpSolrClient masterClient, secondaryClient, repeaterClient;
  SolrInstance master = null, secondary = null, repeater = null;

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
    masterJetty = createAndStartJetty(master);
    masterClient = createNewSolrClient(masterJetty.getLocalPort());

    secondary = new SolrInstance(createTempDir("solr-instance").toFile(), "secondary", masterJetty.getLocalPort());
    secondary.setUp();
    secondaryJetty = createAndStartJetty(secondary);
    secondaryClient = createNewSolrClient(secondaryJetty.getLocalPort());
    
    System.setProperty("solr.indexfetcher.sotimeout2", "45000");
  }

  public void clearIndexWithReplication() throws Exception {
    if (numFound(query("*:*", masterClient)) != 0) {
      masterClient.deleteByQuery("*:*");
      masterClient.commit();
      // wait for replication to sync & verify
      assertEquals(0, numFound(rQuery(0, "*:*", secondaryClient)));
    }
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    if (null != masterJetty) {
      masterJetty.stop();
      masterJetty = null;
    }
    if (null != secondaryJetty) {
      secondaryJetty.stop();
      secondaryJetty = null;
    }
    if (null != masterClient) {
      masterClient.close();
      masterClient = null;
    }
    if (null != secondaryClient) {
      secondaryClient.close();
      secondaryClient = null;
    }
    System.clearProperty("solr.indexfetcher.sotimeout");
  }

  static JettySolrRunner createAndStartJetty(SolrInstance instance) throws Exception {
    FileUtils.copyFile(new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml"), new File(instance.getHomeDir(), "solr.xml"));
    Properties nodeProperties = new Properties();
    nodeProperties.setProperty("solr.data.dir", instance.getDataDir());
    JettyConfig jettyConfig = JettyConfig.builder().setContext("/solr").setPort(0).build();
    JettySolrRunner jetty = new JettySolrRunner(instance.getHomeDir(), nodeProperties, jettyConfig);
    jetty.start();
    return jetty;
  }

  static HttpSolrClient createNewSolrClient(int port) {
    try {
      // setup the client...
      final String baseUrl = buildUrl(port) + "/" + DEFAULT_TEST_CORENAME;
      HttpSolrClient client = getHttpSolrClient(baseUrl, 15000, 90000);
      return client;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  static int index(SolrClient s, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    return s.add(doc).getStatus();
  }

  @SuppressWarnings({"rawtypes"})
  NamedList query(String query, SolrClient s) throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();

    params.add("q", query);
    params.add("sort","id desc");

    QueryResponse qres = s.query(params);
    return qres.getResponse();
  }

  /** will sleep up to 30 seconds, looking for expectedDocCount */
  @SuppressWarnings({"rawtypes"})
  private NamedList rQuery(int expectedDocCount, String query, SolrClient client) throws Exception {
    int timeSlept = 0;
    NamedList res = query(query, client);
    while (expectedDocCount != numFound(res)
           && timeSlept < 30000) {
      log.info("Waiting for {} docs", expectedDocCount);
      timeSlept += 100;
      Thread.sleep(100);
      res = query(query, client);
    }
    if (log.isInfoEnabled()) {
      log.info("Waited for {}ms and found {} docs", timeSlept, numFound(res));
    }
    return res;
  }
  
  private long numFound(@SuppressWarnings({"rawtypes"})NamedList res) {
    return ((SolrDocumentList) res.get("response")).getNumFound();
  }

  private NamedList<Object> getDetails(SolrClient s) throws Exception {
    

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command","details");
    params.set("_trace","getDetails");
    params.set("qt",ReplicationHandler.PATH);
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);
    assertReplicationResponseSucceeded(res);

    @SuppressWarnings("unchecked") NamedList<Object> details 
      = (NamedList<Object>) res.get("details");

    assertNotNull("null details", details);

    return details;
  }
  
  private NamedList<Object> getIndexVersion(SolrClient s) throws Exception {
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command","indexversion");
    params.set("_trace","getIndexVersion");
    params.set("qt",ReplicationHandler.PATH);
    QueryRequest req = new QueryRequest(params);

    NamedList<Object> res = s.request(req);
    assertReplicationResponseSucceeded(res);

    return res;
  }
  
  private NamedList<Object> reloadCore(SolrClient s, String core) throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action","reload");
    params.set("core", core);
    params.set("qt","/admin/cores");
    QueryRequest req = new QueryRequest(params);

    try (HttpSolrClient adminClient = adminClient(s)) {
      NamedList<Object> res = adminClient.request(req);
      assertNotNull("null response from server", res);
      return res;
    }

  }

  private HttpSolrClient adminClient(SolrClient client) {
    String adminUrl = ((HttpSolrClient)client).getBaseURL().replace("/collection1", "");
    return getHttpSolrClient(adminUrl);
  }

  @Test
  public void doTestHandlerPathUnchanged() throws Exception {
    assertEquals("/replication", ReplicationHandler.PATH);
  }

  @Test
  public void doTestDetails() throws Exception {
    secondaryJetty.stop();
    
    secondary.setTestPort(masterJetty.getLocalPort());
    secondary.copyConfigFile(CONF_DIR + "solrconfig-secondary.xml", "solrconfig.xml");
    secondaryJetty = createAndStartJetty(secondary);
    
    secondaryClient.close();
    masterClient.close();
    masterClient = createNewSolrClient(masterJetty.getLocalPort());
    secondaryClient = createNewSolrClient(secondaryJetty.getLocalPort());
    
    clearIndexWithReplication();
    { 
      NamedList<Object> details = getDetails(masterClient);
      
      assertEquals("primary isPrimary?",
                   "true", details.get("isPrimary"));
      assertEquals("primary isSecondary?",
                   "false", details.get("isSecondary"));
      assertNotNull("primary has primary section",
                    details.get("primary"));
    }

    // check details on the secondary a couple of times before & after fetching
    for (int i = 0; i < 3; i++) {
      NamedList<Object> details = getDetails(secondaryClient);
      assertNotNull(i + ": " + details);
      assertNotNull(i + ": " + details.toString(), details.get("secondary"));

      if (i > 0) {
        rQuery(i, "*:*", secondaryClient);
        @SuppressWarnings({"rawtypes"})
        List replicatedAtCount = (List) ((NamedList) details.get("secondary")).get("indexReplicatedAtList");
        int tries = 0;
        while ((replicatedAtCount == null || replicatedAtCount.size() < i) && tries++ < 5) {
          Thread.sleep(1000);
          details = getDetails(secondaryClient);
          replicatedAtCount = (List) ((NamedList) details.get("secondary")).get("indexReplicatedAtList");
        }
        
        assertNotNull("Expected to see that the secondary has replicated" + i + ": " + details.toString(), replicatedAtCount);
        
        // we can have more replications than we added docs because a replication can legally fail and try 
        // again (sometimes we cannot merge into a live index and have to try again)
        assertTrue("i:" + i + " replicationCount:" + replicatedAtCount.size(), replicatedAtCount.size() >= i); 
      }

      assertEquals(i + ": " + "secondary isPrimary?", "false", details.get("isPrimary"));
      assertEquals(i + ": " + "secondary isSlave?", "true", details.get("isSlave"));
      assertNotNull(i + ": " + "secondary has secondary section", details.get("secondary"));
      // SOLR-2677: assert not false negatives
      Object timesFailed = ((NamedList)details.get("secondary")).get(IndexFetcher.TIMES_FAILED);
      // SOLR-7134: we can have a fail because some mock index files have no checksum, will
      // always be downloaded, and may not be able to be moved into the existing index
      assertTrue(i + ": " + "secondary has fetch error count: " + (String)timesFailed, timesFailed == null || ((String) timesFailed).equals("1"));

      if (3 != i) {
        // index & fetch
        index(masterClient, "id", i, "name", "name = " + i);
        masterClient.commit();
        pullFromTo(masterJetty, secondaryJetty);
      }
    }

    SolrInstance repeater = null;
    JettySolrRunner repeaterJetty = null;
    SolrClient repeaterClient = null;
    try {
      repeater = new SolrInstance(createTempDir("solr-instance").toFile(), "repeater", masterJetty.getLocalPort());
      repeater.setUp();
      repeaterJetty = createAndStartJetty(repeater);
      repeaterClient = createNewSolrClient(repeaterJetty.getLocalPort());

      
      NamedList<Object> details = getDetails(repeaterClient);
      
      assertEquals("repeater isPrimary?",
                   "true", details.get("isPrimary"));
      assertEquals("repeater isSecondary?",
                   "true", details.get("isSecondary"));
      assertNotNull("repeater has master section", 
                    details.get("master"));
      assertNotNull("repeater has secondary section",
                    details.get("secondary"));

    } finally {
      try { 
        if (repeaterJetty != null) repeaterJetty.stop(); 
      } catch (Exception e) { /* :NOOP: */ }
      if (repeaterClient != null) repeaterClient.close();
    }
  }


  /**
   * Verify that empty commits and/or commits with openSearcher=false
   * on the master do not cause subsequent replication problems on the secondary
   */
  public void testEmptyCommits() throws Exception {
    clearIndexWithReplication();
    
    // add a doc to master and commit
    index(masterClient, "id", "1", "name", "empty1");
    emptyUpdate(masterClient, "commit", "true");
    // force replication
    pullFromPrimaryToSecondary();
    // verify doc is on secondary
    rQuery(1, "name:empty1", secondaryClient);
    assertVersions(masterClient, secondaryClient);

    // do a completely empty commit on master and force replication
    emptyUpdate(masterClient, "commit", "true");
    pullFromPrimaryToSecondary();

    // add another doc and verify secondary gets it
    index(masterClient, "id", "2", "name", "empty2");
    emptyUpdate(masterClient, "commit", "true");
    // force replication
    pullFromPrimaryToSecondary();

    rQuery(1, "name:empty2", secondaryClient);
    assertVersions(masterClient, secondaryClient);

    // add a third doc but don't open a new searcher on master
    index(masterClient, "id", "3", "name", "empty3");
    emptyUpdate(masterClient, "commit", "true", "openSearcher", "false");
    pullFromPrimaryToSecondary();
    
    // verify secondary can search the doc, but master doesn't
    rQuery(0, "name:empty3", masterClient);
    rQuery(1, "name:empty3", secondaryClient);

    // final doc with hard commit, secondary and master both showing all docs
    index(masterClient, "id", "4", "name", "empty4");
    emptyUpdate(masterClient, "commit", "true");
    pullFromPrimaryToSecondary();

    String q = "name:(empty1 empty2 empty3 empty4)";
    rQuery(4, q, masterClient);
    rQuery(4, q, secondaryClient);
    assertVersions(masterClient, secondaryClient);

  }

  @Test
  public void doTestReplicateAfterWrite2Secondary() throws Exception {
    clearIndexWithReplication();
    nDocs--;
    for (int i = 0; i < nDocs; i++) {
      index(masterClient, "id", i, "name", "name = " + i);
    }

    invokeReplicationCommand(masterJetty.getLocalPort(), "disableReplication");
    invokeReplicationCommand(secondaryJetty.getLocalPort(), "disablepoll");
    
    masterClient.commit();

    assertEquals(nDocs, numFound(rQuery(nDocs, "*:*", masterClient)));

    // Make sure that both the index version and index generation on the secondary is
    // higher than that of the master, just to make the test harder.

    index(secondaryClient, "id", 551, "name", "name = " + 551);
    secondaryClient.commit(true, true);
    index(secondaryClient, "id", 552, "name", "name = " + 552);
    secondaryClient.commit(true, true);
    index(secondaryClient, "id", 553, "name", "name = " + 553);
    secondaryClient.commit(true, true);
    index(secondaryClient, "id", 554, "name", "name = " + 554);
    secondaryClient.commit(true, true);
    index(secondaryClient, "id", 555, "name", "name = " + 555);
    secondaryClient.commit(true, true);

    //this doc is added to secondary so it should show an item w/ that result
    assertEquals(1, numFound(rQuery(1, "id:555", secondaryClient)));

    //Let's fetch the index rather than rely on the polling.
    invokeReplicationCommand(masterJetty.getLocalPort(), "enablereplication");
    invokeReplicationCommand(secondaryJetty.getLocalPort(), "fetchindex");

    /*
    //the secondary should have done a full copy of the index so the doc with id:555 should not be there in the secondary now
    secondaryQueryRsp = rQuery(0, "id:555", secondaryClient);
    secondaryQueryResult = (SolrDocumentList) secondaryQueryRsp.get("response");
    assertEquals(0, secondaryQueryResult.getNumFound());

    // make sure we replicated the correct index from the master
    secondaryQueryRsp = rQuery(nDocs, "*:*", secondaryClient);
    secondaryQueryResult = (SolrDocumentList) secondaryQueryRsp.get("response");
    assertEquals(nDocs, secondaryQueryResult.getNumFound());
    
    */
  }

  //Simple function to wrap the invocation of replication commands on the various
  //jetty servers.
  static void invokeReplicationCommand(int pJettyPort, String pCommand) throws IOException
  {
    String masterUrl = buildUrl(pJettyPort) + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH+"?command=" + pCommand;
    URL u = new URL(masterUrl);
    InputStream stream = u.openStream();
    stream.close();
  }
  
  @Test
  public void doTestIndexAndConfigReplication() throws Exception {

    TestInjection.delayBeforeSecondaryCommitRefresh = random().nextInt(10);

    clearIndexWithReplication();

    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    @SuppressWarnings({"rawtypes"})
    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, numFound(masterQueryRsp));

    //get docs from secondary and check if number is equal to master
    @SuppressWarnings({"rawtypes"})
    NamedList secondaryQueryRsp = rQuery(nDocs, "*:*", secondaryClient);
    SolrDocumentList secondaryQueryResult = (SolrDocumentList) secondaryQueryRsp.get("response");
    assertEquals(nDocs, numFound(secondaryQueryRsp));

    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, secondaryQueryResult, 0, null);
    assertEquals(null, cmp);
    
    assertVersions(masterClient, secondaryClient);

    //start config files replication test
    masterClient.deleteByQuery("*:*");
    masterClient.commit();

    //change the schema on master
    master.copyConfigFile(CONF_DIR + "schema-replication2.xml", "schema.xml");

    masterJetty.stop();

    masterJetty = createAndStartJetty(master);
    masterClient.close();
    masterClient = createNewSolrClient(masterJetty.getLocalPort());

    secondary.setTestPort(masterJetty.getLocalPort());
    secondary.copyConfigFile(secondary.getSolrConfigFile(), "solrconfig.xml");

    secondaryJetty.stop();

    // setup an xslt dir to force subdir file replication
    File masterXsltDir = new File(master.getConfDir() + File.separator + "xslt");
    File masterXsl = new File(masterXsltDir, "dummy.xsl");
    assertTrue("could not make dir " + masterXsltDir, masterXsltDir.mkdirs());
    assertTrue(masterXsl.createNewFile());

    File secondaryXsltDir = new File(secondary.getConfDir() + File.separator + "xslt");
    File secondaryXsl = new File(secondaryXsltDir, "dummy.xsl");
    assertFalse(secondaryXsltDir.exists());

    secondaryJetty = createAndStartJetty(secondary);
    secondaryClient.close();
    secondaryClient = createNewSolrClient(secondaryJetty.getLocalPort());
    //add a doc with new field and commit on master to trigger index fetch from secondary.
    index(masterClient, "id", "2000", "name", "name = " + 2000, "newname", "newname = " + 2000);
    masterClient.commit();

    assertEquals(1, numFound( rQuery(1, "*:*", masterClient)));
    
    secondaryQueryRsp = rQuery(1, "*:*", secondaryClient);
    assertVersions(masterClient, secondaryClient);
    SolrDocument d = ((SolrDocumentList) secondaryQueryRsp.get("response")).get(0);
    assertEquals("newname = 2000", (String) d.getFieldValue("newname"));

    assertTrue(secondaryXsltDir.isDirectory());
    assertTrue(secondaryXsl.exists());
    
    checkForSingleIndex(masterJetty);
    checkForSingleIndex(secondaryJetty, true);
  }

  @Test
  public void doTestStopPoll() throws Exception {
    clearIndexWithReplication();

    // Test:
    // setup primary/secondary.
    // stop polling on secondary, add a doc to master and verify secondary hasn't picked it.
    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    @SuppressWarnings({"rawtypes"})
    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, numFound(masterQueryRsp));

    //get docs from secondary and check if number is equal to master
    @SuppressWarnings({"rawtypes"})
    NamedList secondaryQueryRsp = rQuery(nDocs, "*:*", secondaryClient);
    SolrDocumentList secondaryQueryResult = (SolrDocumentList) secondaryQueryRsp.get("response");
    assertEquals(nDocs, numFound(secondaryQueryRsp));

    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, secondaryQueryResult, 0, null);
    assertEquals(null, cmp);

    // start stop polling test
    invokeReplicationCommand(secondaryJetty.getLocalPort(), "disablepoll");
    
    index(masterClient, "id", 501, "name", "name = " + 501);
    masterClient.commit();

    //get docs from master and check if number is equal to master
    assertEquals(nDocs+1, numFound(rQuery(nDocs+1, "*:*", masterClient)));
    
    // NOTE: this test is wierd, we want to verify it DOESNT replicate...
    // for now, add a sleep for this.., but the logic is wierd.
    Thread.sleep(3000);
    
    //get docs from secondary and check if number is not equal to master; polling is disabled
    assertEquals(nDocs, numFound(rQuery(nDocs, "*:*", secondaryClient)));

    // re-enable replication
    invokeReplicationCommand(secondaryJetty.getLocalPort(), "enablepoll");

    assertEquals(nDocs+1, numFound(rQuery(nDocs+1, "*:*", secondaryClient)));
  }

  /**
   * We assert that if master is down for more than poll interval,
   * the secondary doesn't re-fetch the whole index from master again if
   * the index hasn't changed. See SOLR-9036
   */
  @Test
  public void doTestIndexFetchOnPrimaryRestart() throws Exception  {
    useFactory(null);
    try {
      clearIndexWithReplication();
      // change solrconfig having 'replicateAfter startup' option on master
      master.copyConfigFile(CONF_DIR + "solrconfig-master2.xml",
          "solrconfig.xml");

      masterJetty.stop();
      masterJetty.start();

      // close and re-create master client because its connection pool has stale connections
      masterClient.close();
      masterClient = createNewSolrClient(masterJetty.getLocalPort());

      nDocs--;
      for (int i = 0; i < nDocs; i++)
        index(masterClient, "id", i, "name", "name = " + i);

      masterClient.commit();

      @SuppressWarnings({"rawtypes"})
      NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
      SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
      assertEquals(nDocs, numFound(masterQueryRsp));

      //get docs from secondary and check if number is equal to master
      @SuppressWarnings({"rawtypes"})
      NamedList secondaryQueryRsp = rQuery(nDocs, "*:*", secondaryClient);
      SolrDocumentList secondaryQueryResult = (SolrDocumentList) secondaryQueryRsp.get("response");
      assertEquals(nDocs, numFound(secondaryQueryRsp));

      //compare results
      String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, secondaryQueryResult, 0, null);
      assertEquals(null, cmp);

      String timesReplicatedString = getSecondaryDetails("timesIndexReplicated");
      String timesFailed;
      Integer previousTimesFailed = null;
      if (timesReplicatedString == null) {
        timesFailed = "0";
      } else {
        int timesReplicated = Integer.parseInt(timesReplicatedString);
        timesFailed = getSecondaryDetails("timesFailed");
        if (null == timesFailed) {
          timesFailed = "0";
        }

        previousTimesFailed = Integer.parseInt(timesFailed);
        // Sometimes replication will fail because master's core is still loading; make sure there was one success
        assertEquals(1, timesReplicated - previousTimesFailed);

      }

      masterJetty.stop();

      final TimeOut waitForLeaderToShutdown = new TimeOut(300, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      waitForLeaderToShutdown.waitFor
        ("Gave up after waiting an obscene amount of time for leader to shut down",
         () -> masterJetty.isStopped() );
        
      for(int retries=0; ;retries++) { 

        Thread.yield(); // might not be necessary at all
        // poll interval on secondary is 1 second, so we just sleep for a few seconds
        Thread.sleep(2000);
        
        NamedList<Object> secondaryDetails=null;
        try {
          secondaryDetails = getSecondaryDetails();
          int failed = Integer.parseInt(getStringOrNull(secondaryDetails,"timesFailed"));
          if (previousTimesFailed != null) {
            assertTrue(failed > previousTimesFailed);
          }
          assertEquals(1, Integer.parseInt(getStringOrNull(secondaryDetails,"timesIndexReplicated")) - failed);
          break;
        } catch (NumberFormatException | AssertionError notYet) {
          if (log.isInfoEnabled()) {
            log.info("{}th attempt failure on {} details are {}", retries + 1, notYet, secondaryDetails); // logOk
          }
          if (retries>9) {
            log.error("giving up: ", notYet);
            throw notYet;
          } 
        }
      }
      
      masterJetty.start();

      // poll interval on secondary is 1 second, so we just sleep for a few seconds
      Thread.sleep(2000);
      //get docs from secondary and assert that they are still the same as before
      secondaryQueryRsp = rQuery(nDocs, "*:*", secondaryClient);
      secondaryQueryResult = (SolrDocumentList) secondaryQueryRsp.get("response");
      assertEquals(nDocs, numFound(secondaryQueryRsp));

    } finally {
      resetFactory();
    }
  }

  private String getSecondaryDetails(String keyName) throws SolrServerException, IOException {
    NamedList<Object> details = getSecondaryDetails();
    return getStringOrNull(details, keyName);
  }

  private String getStringOrNull(NamedList<Object> details, String keyName) {
    Object o = details.get(keyName);
    return o != null ? o.toString() : null;
  }

  private NamedList<Object> getSecondaryDetails() throws SolrServerException, IOException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.QT, "/replication");
    params.set("command", "details");
    QueryResponse response = secondaryClient.query(params);

    // details/secondary/timesIndexReplicated
    @SuppressWarnings({"unchecked"})
    NamedList<Object> details = (NamedList<Object>) response.getResponse().get("details");
    @SuppressWarnings({"unchecked"})
    NamedList<Object> secondary = (NamedList<Object>) details.get("secondary");
    return slave;
  }

  @Test
  public void doTestIndexFetchWithPrimaryUrl() throws Exception {
    //change solrconfig on slave
    //this has no entry for pollinginterval
    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(CONF_DIR + "solrconfig-slave1.xml", "solrconfig.xml");
    slaveJetty.stop();
    slaveJetty = createAndStartJetty(slave);
    slaveClient.close();
    slaveClient = createNewSolrClient(slaveJetty.getLocalPort());

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

    @SuppressWarnings({"rawtypes"})
    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());

    // index fetch
    String masterUrl = buildUrl(slaveJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH+"?command=fetchindex&masterUrl=";
    masterUrl += buildUrl(masterJetty.getLocalPort()) + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH;
    URL url = new URL(masterUrl);
    InputStream stream = url.openStream();
    stream.close();
    
    //get docs from slave and check if number is equal to master
    @SuppressWarnings({"rawtypes"})
    NamedList slaveQueryRsp = rQuery(nDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(nDocs, slaveQueryResult.getNumFound());
    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    // index fetch from the slave to the master
    
    for (int i = nDocs; i < nDocs + 3; i++)
      index(slaveClient, "id", i, "name", "name = " + i);

    slaveClient.commit();
    
    pullFromSecondaryToPrimary();
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
    
    pullFromSecondaryToPrimary();
    
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
    
    pullFromSecondaryToPrimary();
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
    pullFromSecondaryToPrimary();
    
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
  //commented 20-Sep-2018  @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // added 17-Aug-2018
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

      slave.setTestPort(masterJetty.getLocalPort());
      slave.copyConfigFile(CONF_DIR +"solrconfig-slave1.xml", "solrconfig.xml");
      slave.copyConfigFile(CONF_DIR +slaveSchema, "schema.xml");
      slaveJetty.stop();
      slaveJetty = createAndStartJetty(slave);
      slaveClient.close();
      slaveClient = createNewSolrClient(slaveJetty.getLocalPort());

      master.copyConfigFile(CONF_DIR + "solrconfig-master3.xml",
          "solrconfig.xml");
      masterJetty.stop();
      masterJetty = createAndStartJetty(master);
      masterClient.close();
      masterClient = createNewSolrClient(masterJetty.getLocalPort());
      
      masterClient.deleteByQuery("*:*");
      slaveClient.deleteByQuery("*:*");
      slaveClient.commit();
      
      int maxDocs = TEST_NIGHTLY ? 1000 : 75;
      int rounds = TEST_NIGHTLY ? 45 : 3;
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
        
        @SuppressWarnings({"rawtypes"})
        NamedList masterQueryRsp = rQuery(totalDocs, "*:*", masterClient);
        SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp
            .get("response");
        assertEquals(totalDocs, masterQueryResult.getNumFound());
        
        // index fetch
        Date slaveCoreStart = watchCoreStartAt(slaveClient, 30*1000, null);
        pullFromPrimaryToSecondary();
        if (confCoreReload) {
          watchCoreStartAt(slaveClient, 30*1000, slaveCoreStart);
        }

        // get docs from slave and check if number is equal to master
        @SuppressWarnings({"rawtypes"})
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
        
        if (!Constants.WINDOWS) {
          checkForSingleIndex(slaveJetty);
        }
        
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

  private CachingDirectoryFactory getCachingDirectoryFactory(SolrCore core) {
    return (CachingDirectoryFactory) core.getDirectoryFactory();
  }

  private void checkForSingleIndex(JettySolrRunner jetty) {
    checkForSingleIndex(jetty, false);
  }
  
  private void checkForSingleIndex(JettySolrRunner jetty, boolean afterReload) {
    CoreContainer cores = jetty.getCoreContainer();
    Collection<SolrCore> theCores = cores.getCores();
    for (SolrCore core : theCores) {
      String ddir = core.getDataDir();
      CachingDirectoryFactory dirFactory = getCachingDirectoryFactory(core);
      synchronized (dirFactory) {
        Set<String> livePaths = dirFactory.getLivePaths();
        // one for data, one for the index under data and one for the snapshot metadata.
        // we also allow one extra index dir - it may not be removed until the core is closed
        if (afterReload) {
          assertTrue(livePaths.toString() + ":" + livePaths.size(), 3 == livePaths.size() || 4 == livePaths.size());
        } else {
          assertTrue(livePaths.toString() + ":" + livePaths.size(), 3 == livePaths.size());
        }

        // :TODO: assert that one of the paths is a subpath of hte other
      }
      if (dirFactory instanceof StandardDirectoryFactory) {
        System.out.println(Arrays.asList(new File(ddir).list()));
        // we also allow one extra index dir - it may not be removed until the core is closed
        int cnt = indexDirCount(ddir);
        // if after reload, there may be 2 index dirs while the reloaded SolrCore closes.
        if (afterReload) {
          assertTrue("found:" + cnt + Arrays.asList(new File(ddir).list()).toString(), 1 == cnt || 2 == cnt);
        } else {
          assertTrue("found:" + cnt + Arrays.asList(new File(ddir).list()).toString(), 1 == cnt);
        }

      }
    }
  }

  private int indexDirCount(String ddir) {
    String[] list = new File(ddir).list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        File f = new File(dir, name);
        return f.isDirectory() && !SolrSnapshotMetaDataManager.SNAPSHOT_METADATA_DIR.equals(name);
      }
    });
    return list.length;
  }

  private void pullFromPrimaryToSecondary() throws MalformedURLException,
      IOException {
    pullFromTo(masterJetty, slaveJetty);
  }
  
  @Test
  public void doTestRepeater() throws Exception {
    // no polling
    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(CONF_DIR + "solrconfig-slave1.xml", "solrconfig.xml");
    slaveJetty.stop();
    slaveJetty = createAndStartJetty(slave);
    slaveClient.close();
    slaveClient = createNewSolrClient(slaveJetty.getLocalPort());

    try {
      repeater = new SolrInstance(createTempDir("solr-instance").toFile(), "repeater", masterJetty.getLocalPort());
      repeater.setUp();
      repeater.copyConfigFile(CONF_DIR + "solrconfig-repeater.xml",
          "solrconfig.xml");
      repeaterJetty = createAndStartJetty(repeater);
      if (repeaterClient != null) {
        repeaterClient.close();
      }
      repeaterClient = createNewSolrClient(repeaterJetty.getLocalPort());
      
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
        repeaterJetty = null;
      }
      if (repeaterClient != null) {
        repeaterClient.close();
      }
    }
    
  }

  private void assertVersions(SolrClient client1, SolrClient client2) throws Exception {
    NamedList<Object> details = getDetails(client1);
    @SuppressWarnings({"unchecked"})
    ArrayList<NamedList<Object>> commits = (ArrayList<NamedList<Object>>) details.get("commits");
    Long maxVersionClient1 = getVersion(client1);
    Long maxVersionClient2 = getVersion(client2);

    if (maxVersionClient1 > 0 && maxVersionClient2 > 0) {
      assertEquals(maxVersionClient1, maxVersionClient2);
    }
    
    // check vs /replication?command=indexversion call
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("qt", ReplicationHandler.PATH);
    params.set("_trace", "assertVersions");
    params.set("command", "indexversion");
    QueryRequest req = new QueryRequest(params);
    NamedList<Object> resp = client1.request(req);
    assertReplicationResponseSucceeded(resp);
    Long version = (Long) resp.get("indexversion");
    assertEquals(maxVersionClient1, version);
    
    // check vs /replication?command=indexversion call
    resp = client2.request(req);
    assertReplicationResponseSucceeded(resp);
    version = (Long) resp.get("indexversion");
    assertEquals(maxVersionClient2, version);
  }

  @SuppressWarnings({"unchecked"})
  private Long getVersion(SolrClient client) throws Exception {
    NamedList<Object> details;
    ArrayList<NamedList<Object>> commits;
    details = getDetails(client);
    commits = (ArrayList<NamedList<Object>>) details.get("commits");
    Long maxVersionSecondary= 0L;
    for(NamedList<Object> commit : commits) {
      Long version = (Long) commit.get("indexVersion");
      maxVersionSecondary = Math.max(version, maxVersionSecondary);
    }
    return maxVersionSecondary;
  }

  private void pullFromSecondaryToPrimary() throws MalformedURLException,
      IOException {
    pullFromTo(slaveJetty, masterJetty);
  }
  
  private void pullFromTo(JettySolrRunner from, JettySolrRunner to) throws IOException {
    String masterUrl;
    URL url;
    InputStream stream;
    masterUrl = buildUrl(to.getLocalPort())
        + "/" + DEFAULT_TEST_CORENAME
        + ReplicationHandler.PATH+"?wait=true&command=fetchindex&masterUrl="
        + buildUrl(from.getLocalPort())
        + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH;
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

    masterJetty = createAndStartJetty(master);
    masterClient.close();
    masterClient = createNewSolrClient(masterJetty.getLocalPort());
    
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();
    
    @SuppressWarnings({"rawtypes"})
    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());
    

    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    //start slave
    slaveJetty = createAndStartJetty(slave);
    slaveClient.close();
    slaveClient = createNewSolrClient(slaveJetty.getLocalPort());

    //get docs from slave and check if number is equal to master
    @SuppressWarnings({"rawtypes"})
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
      
      masterJetty = createAndStartJetty(master);
      masterClient.close();
      masterClient = createNewSolrClient(masterJetty.getLocalPort());
      
      for (int i = 0; i < nDocs; i++)
        index(masterClient, "id", i, "name", "name = " + i);
      
      masterClient.commit();
      
      // now we restart to test what happens with no activity before the slave
      // tries to
      // replicate
      masterJetty.stop();
      masterJetty.start();
      
      // masterClient = createNewSolrClient(masterJetty.getLocalPort());
      
      @SuppressWarnings({"rawtypes"})
      NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
      SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp
          .get("response");
      assertEquals(nDocs, masterQueryResult.getNumFound());
      
      slave.setTestPort(masterJetty.getLocalPort());
      slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");
      
      // start slave
      slaveJetty = createAndStartJetty(slave);
      slaveClient.close();
      slaveClient = createNewSolrClient(slaveJetty.getLocalPort());
      
      // get docs from slave and check if number is equal to master
      @SuppressWarnings({"rawtypes"})
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
    int docs = TEST_NIGHTLY ? 200000 : 10;
    
    //stop slave
    slaveJetty.stop();


    //change solrconfig having 'replicateAfter startup' option on master
    master.copyConfigFile(CONF_DIR + "solrconfig-master3.xml",
                          "solrconfig.xml");

    masterJetty.stop();

    masterJetty = createAndStartJetty(master);
    masterClient.close();
    masterClient = createNewSolrClient(masterJetty.getLocalPort());

    masterClient.deleteByQuery("*:*");
    for (int i = 0; i < docs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    @SuppressWarnings({"rawtypes"})
    NamedList masterQueryRsp = rQuery(docs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(docs, masterQueryResult.getNumFound());
    
    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    //start slave
    slaveJetty = createAndStartJetty(slave);
    slaveClient.close();
    slaveClient = createNewSolrClient(slaveJetty.getLocalPort());
    
    //get docs from slave and check if number is equal to master
    @SuppressWarnings({"rawtypes"})
    NamedList slaveQueryRsp = rQuery(docs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(docs, slaveQueryResult.getNumFound());
    
    //compare results
    String cmp = BaseDistributedSearchTestCase.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);
    
    Object version = getIndexVersion(masterClient).get("indexversion");
    
    reloadCore(masterClient, "collection1");
    
    assertEquals(version, getIndexVersion(masterClient).get("indexversion"));
    
    index(masterClient, "id", docs + 10, "name", "name = 1");
    index(masterClient, "id", docs + 20, "name", "name = 2");

    masterClient.commit();
    
    @SuppressWarnings({"rawtypes"})
    NamedList resp =  rQuery(docs + 2, "*:*", masterClient);
    masterQueryResult = (SolrDocumentList) resp.get("response");
    assertEquals(docs + 2, masterQueryResult.getNumFound());
    
    //get docs from slave and check if number is equal to master
    slaveQueryRsp = rQuery(docs + 2, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(docs + 2, slaveQueryResult.getNumFound());
    
  }

  @Test
  // 12-Jun-2018 @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 04-May-2018
  public void doTestIndexAndConfigAliasReplication() throws Exception {
    clearIndexWithReplication();

    nDocs--;
    for (int i = 0; i < nDocs; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    @SuppressWarnings({"rawtypes"})
    NamedList masterQueryRsp = rQuery(nDocs, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(nDocs, masterQueryResult.getNumFound());

    //get docs from slave and check if number is equal to master
    @SuppressWarnings({"rawtypes"})
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

    masterJetty = createAndStartJetty(master);
    masterClient.close();
    masterClient = createNewSolrClient(masterJetty.getLocalPort());

    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(slave.getSolrConfigFile(), "solrconfig.xml");

    slaveJetty.stop();
    slaveJetty = createAndStartJetty(slave);
    slaveClient.close();
    slaveClient = createNewSolrClient(slaveJetty.getLocalPort());

    slaveClient.deleteByQuery("*:*");
    slaveClient.commit();
    rQuery(0, "*:*", slaveClient); // sanity check w/retry
    
    // record collection1's start time on slave
    final Date slaveStartTime = watchCoreStartAt(slaveClient, 30*1000, null);

    //add a doc with new field and commit on master to trigger index fetch from slave.
    index(masterClient, "id", "2000", "name", "name = " + 2000, "newname", "n2000");
    masterClient.commit();
    rQuery(1, "newname:n2000", masterClient);  // sanity check

    // wait for slave to reload core by watching updated startTime
    watchCoreStartAt(slaveClient, 30*1000, slaveStartTime);

    @SuppressWarnings({"rawtypes"})
    NamedList masterQueryRsp2 = rQuery(1, "id:2000", masterClient);
    SolrDocumentList masterQueryResult2 = (SolrDocumentList) masterQueryRsp2.get("response");
    assertEquals(1, masterQueryResult2.getNumFound());

    @SuppressWarnings({"rawtypes"})
    NamedList slaveQueryRsp2 = rQuery(1, "id:2000", slaveClient);
    SolrDocumentList slaveQueryResult2 = (SolrDocumentList) slaveQueryRsp2.get("response");
    assertEquals(1, slaveQueryResult2.getNumFound());
    
    checkForSingleIndex(masterJetty);
    checkForSingleIndex(slaveJetty, true);
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
    masterJetty = createAndStartJetty(master);
    masterClient.close();
    masterClient = createNewSolrClient(masterJetty.getLocalPort());

    //index docs
    final int totalDocs = TestUtil.nextInt(random(), 17, 53);
    for (int i = 0; i < totalDocs; i++)
      index(masterClient, "id", i, "name", TestUtil.randomSimpleString(random(), 1000 , 5000));

    masterClient.commit();

    //Check Index Size
    String dataDir = master.getDataDir();
    masterClient.close();
    masterJetty.stop();

    Directory dir = FSDirectory.open(Paths.get(dataDir).resolve("index"));
    String[] files = dir.listAll();
    long totalBytes = 0;
    for(String file : files) {
      totalBytes += dir.fileLength(file);
    }

    float approximateTimeInSeconds = Math.round( totalBytes/1024/1024/0.1 ); // maxWriteMBPerSec=0.1 in solrconfig

    //Start again and replicate the data
    useFactory(null);
    masterJetty = createAndStartJetty(master);
    masterClient = createNewSolrClient(masterJetty.getLocalPort());

    //start slave
    slave.setTestPort(masterJetty.getLocalPort());
    slave.copyConfigFile(CONF_DIR + "solrconfig-slave1.xml", "solrconfig.xml");
    slaveJetty = createAndStartJetty(slave);
    slaveClient.close();
    slaveClient = createNewSolrClient(slaveJetty.getLocalPort());

    long startTime = System.nanoTime();

    pullFromPrimaryToSecondary();

    //Add a few more docs in the master. Just to make sure that we are replicating the correct index point
    //These extra docs should not get replicated
    new Thread(new AddExtraDocs(masterClient, totalDocs)).start();

    //Wait and make sure that it actually replicated correctly.
    @SuppressWarnings({"rawtypes"})
    NamedList slaveQueryRsp = rQuery(totalDocs, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(totalDocs, slaveQueryResult.getNumFound());

    long timeTaken = System.nanoTime() - startTime;

    long timeTakenInSeconds = TimeUnit.SECONDS.convert(timeTaken, TimeUnit.NANOSECONDS);

    //Let's make sure it took more than approximateTimeInSeconds to make sure that it was throttled
    log.info("approximateTimeInSeconds = {} timeTakenInSeconds = {}"
        , approximateTimeInSeconds, timeTakenInSeconds);
    assertTrue(timeTakenInSeconds - approximateTimeInSeconds > 0);
  }

  @Test
  public void doTestIllegalFilePaths() throws Exception {
    // Loop through the file=, cf=, tlogFile= params and prove that it throws exception for path traversal attempts
    String absFile = Paths.get("foo").toAbsolutePath().toString();
    List<String> illegalFilenames = Arrays.asList(absFile, "../dir/traversal", "illegal\rfile\nname\t");
    List<String> params = Arrays.asList(ReplicationHandler.FILE, ReplicationHandler.CONF_FILE_SHORT, ReplicationHandler.TLOG_FILE);
    for (String param : params) {
      for (String filename : illegalFilenames) {
        expectThrows(Exception.class, () ->
            invokeReplicationCommand(masterJetty.getLocalPort(), "filecontent&" + param + "=" + filename));
      }
    }
  }

  @Test
  public void testFileListShouldReportErrorsWhenTheyOccur() throws Exception {
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication")
        .add("wt", "json")
        .add("command", "filelist")
        .add("generation", "-2"); // A 'generation' value not matching any commit point should cause error.
    QueryResponse response = slaveClient.query(q);
    NamedList<Object> resp = response.getResponse();
    assertNotNull(resp);
    assertEquals("ERROR", resp.get("status"));
    assertEquals("invalid index generation", resp.get("message"));
  }

  @Test
  public void testFetchIndexShouldReportErrorsWhenTheyOccur() throws Exception  {
    int masterPort = masterJetty.getLocalPort();
    masterJetty.stop();
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication")
        .add("wt", "json")
        .add("wait", "true")
        .add("command", "fetchindex")
        .add("masterUrl", buildUrl(masterPort));
    QueryResponse response = slaveClient.query(q);
    NamedList<Object> resp = response.getResponse();
    assertNotNull(resp);
    assertEquals("Fetch index with wait=true should have returned an error response", "ERROR", resp.get("status"));
  }

  @Test
  public void testShouldReportErrorWhenRequiredCommandArgMissing() throws Exception {
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication")
        .add("wt", "json");
    SolrException thrown = expectThrows(SolrException.class, () -> {
      slaveClient.query(q);
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: command"));
  }

  @Test
  public void testShouldReportErrorWhenDeletingBackupButNameMissing() {
    SolrQuery q = new SolrQuery();
    q.add("qt", "/replication")
        .add("wt", "json")
        .add("command", "deletebackup");
    SolrException thrown = expectThrows(SolrException.class, () -> {
      slaveClient.query(q);
    });
    assertEquals(SolrException.ErrorCode.BAD_REQUEST.code, thrown.code());
    assertThat(thrown.getMessage(), containsString("Missing required parameter: name"));
  }

  @Test
  public void testEmptyBackups() throws Exception {
    final File backupDir = createTempDir().toFile();
    final BackupStatusChecker backupStatus = new BackupStatusChecker(masterClient);

    masterJetty.getCoreContainer().getAllowPaths().add(backupDir.toPath());

    { // initial request w/o any committed docs
      final String backupName = "empty_backup1";
      final GenericSolrRequest req = new GenericSolrRequest
        (SolrRequest.METHOD.GET, "/replication",
         params("command", "backup",
                "location", backupDir.getAbsolutePath(),
                "name", backupName));
      final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      final SimpleSolrResponse rsp = req.process(masterClient);

      final String dirName = backupStatus.waitForBackupSuccess(backupName, timeout);
      assertEquals("Did not get expected dir name for backup, did API change?",
                   "snapshot.empty_backup1", dirName);
      assertTrue(dirName + " doesn't exist in expected location for backup " + backupName,
                 new File(backupDir, dirName).exists());
    }
    
    index(masterClient, "id", "1", "name", "foo");
    
    { // second backup w/uncommited doc
      final String backupName = "empty_backup2";
      final GenericSolrRequest req = new GenericSolrRequest
        (SolrRequest.METHOD.GET, "/replication",
         params("command", "backup",
                "location", backupDir.getAbsolutePath(),
                "name", backupName));
      final TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      final SimpleSolrResponse rsp = req.process(masterClient);
      
      final String dirName = backupStatus.waitForBackupSuccess(backupName, timeout);
      assertEquals("Did not get expected dir name for backup, did API change?",
                   "snapshot.empty_backup2", dirName);
      assertTrue(dirName + " doesn't exist in expected location for backup " + backupName,
                 new File(backupDir, dirName).exists());
    }

    // confirm backups really are empty
    for (int i = 1; i <=2; i++) {
      final String name = "snapshot.empty_backup"+i;
      try (Directory dir = new NIOFSDirectory(new File(backupDir, name).toPath());
           IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(name + " is not empty", 0, reader.numDocs());
      }
    }
  }
  
  
  private class AddExtraDocs implements Runnable {

    SolrClient masterClient;
    int startId;
    public AddExtraDocs(SolrClient masterClient, int startId) {
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
    try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(src), StandardCharsets.UTF_8));
         Writer out = new OutputStreamWriter(new FileOutputStream(dst), StandardCharsets.UTF_8)) {

      for (String line = in.readLine(); null != line; line = in.readLine()) {
        if (null != port) {
          line = line.replace("TEST_PORT", port.toString());
        }
        line = line.replace("COMPRESSION", internalCompression ? "internal" : "false");
        out.write(line);
      }
    }
  }

  private UpdateResponse emptyUpdate(SolrClient client, String... params)
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
   * @param client The SolrClient to poll
   * @param timeout the max milliseconds to continue polling for
   * @param min the startTime value must exceed this value before the method will return, if null this method will return the first startTime value encountered.
   * @return the startTime value of collection
   */
  private Date watchCoreStartAt(SolrClient client, final long timeout,
                                final Date min) throws InterruptedException, IOException, SolrServerException {
    final long sleepInterval = 200;
    long timeSlept = 0;

    try (HttpSolrClient adminClient = adminClient(client)) {
      SolrParams p = params("action", "status", "core", "collection1");
      while (timeSlept < timeout) {
        QueryRequest req = new QueryRequest(p);
        req.setPath("/admin/cores");
        try {
          @SuppressWarnings({"rawtypes"})
          NamedList data = adminClient.request(req);
          for (String k : new String[]{"status", "collection1"}) {
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
  }

  private void assertReplicationResponseSucceeded(@SuppressWarnings({"rawtypes"})NamedList response) {
    assertNotNull("null response from server", response);
    assertNotNull("Expected replication response to have 'status' field", response.get("status"));
    assertEquals("OK", response.get("status"));
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

      Properties props = new Properties();
      props.setProperty("name", "collection1");

      writeCoreProperties(homeDir.toPath().resolve("collection1"), props, "TestReplicationHandler");

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

    public void copyConfigFile(String srcFile, String destFile) 
      throws IOException {
      copyFile(getFile(srcFile), 
               new File(confDir, destFile),
               testPort, random().nextBoolean());
    }

  }
}
