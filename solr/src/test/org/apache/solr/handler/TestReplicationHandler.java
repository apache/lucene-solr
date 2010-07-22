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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

import java.io.*;
import java.net.URL;

/**
 * Test for ReplicationHandler
 *
 * @version $Id$
 * @since 1.4
 */
public class TestReplicationHandler extends SolrTestCaseJ4 {


  private static final String CONF_DIR = "." + File.separator + "solr" + File.separator + "conf" + File.separator;
  private static final String SLAVE_CONFIG = CONF_DIR + "solrconfig-slave.xml";

  static JettySolrRunner masterJetty, slaveJetty;
  static SolrServer masterClient, slaveClient;
  static SolrInstance master = null, slave = null;

  static String context = "/solr";

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

  @Before
  public void setUp() throws Exception {
    super.setUp();
    masterClient.deleteByQuery("*:*");
    masterClient.commit();
    rQuery(0, "*:*", masterClient);
    slaveClient.deleteByQuery("*:*");
    slaveClient.commit();
    rQuery(0, "*:*", slaveClient);
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

  @Test
  public void testIndexAndConfigReplication() throws Exception {

    //add 500 docs to master
    for (int i = 0; i < 500; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(500, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(500, masterQueryResult.getNumFound());

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(500, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(500, slaveQueryResult.getNumFound());

    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    //start config files replication test
    masterClient.deleteByQuery("*:*");
    masterClient.commit();

    //change the schema on master
    copyFile(new File(CONF_DIR + "schema-replication2.xml"), new File(master.getConfDir(), "schema.xml"));

    masterJetty.stop();

    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    copyFile(new File(SLAVE_CONFIG), new File(slave.getConfDir(), "solrconfig.xml"), masterJetty.getLocalPort());

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
  public void testIndexAndConfigAliasReplication() throws Exception {

    //add 500 docs to master
    for (int i = 0; i < 500; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(500, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(500, masterQueryResult.getNumFound());

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(500, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");

    assertEquals(500, slaveQueryResult.getNumFound());

    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    //start config files replication test
    //clear master index
    masterClient.deleteByQuery("*:*");
    masterClient.commit();

    //change solrconfig on master
    copyFile(new File(CONF_DIR + "solrconfig-master1.xml"), new File(master.getConfDir(), "solrconfig.xml"));

    //change schema on master
    copyFile(new File(CONF_DIR + "schema-replication2.xml"), new File(master.getConfDir(), "schema.xml"));

    //keep a copy of the new schema
    copyFile(new File(CONF_DIR + "schema-replication2.xml"), new File(master.getConfDir(), "schema-replication2.xml"));

    masterJetty.stop();

    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    copyFile(new File(SLAVE_CONFIG), new File(slave.getConfDir(), "solrconfig.xml"), masterJetty.getLocalPort());

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
  public void testStopPoll() throws Exception {
    // Test:
    // setup master/slave.
    // stop polling on slave, add a doc to master and verify slave hasn't picked it.

    //add 500 docs to master
    for (int i = 0; i < 500; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(500, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(500, masterQueryResult.getNumFound());

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(500, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(500, slaveQueryResult.getNumFound());

    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    // start stop polling test
    String masterUrl = "http://localhost:" + slaveJetty.getLocalPort() + "/solr/replication?command=disablepoll";
    URL url = new URL(masterUrl);
    InputStream stream = url.openStream();
    try {
      stream.close();
    } catch (IOException e) {
      //e.printStackTrace();
    }
    index(masterClient, "id", 501, "name", "name = " + 501);
    masterClient.commit();

    //get docs from master and check if number is equal to master
    masterQueryRsp = rQuery(501, "*:*", masterClient);
    masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(501, masterQueryResult.getNumFound());
    
    // NOTE: this test is wierd, we want to verify it DOESNT replicate...
    // for now, add a sleep for this.., but the logic is wierd.
    Thread.sleep(3000);
    
    //get docs from slave and check if number is not equal to master; polling is disabled
    slaveQueryRsp = rQuery(500, "*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(500, slaveQueryResult.getNumFound());

  }

  @Test
  public void testSnapPullWithMasterUrl() throws Exception {
    //change solrconfig on slave
    //this has no entry for pollinginterval
    copyFile(new File(CONF_DIR + "solrconfig-slave1.xml"), new File(slave.getConfDir(), "solrconfig.xml"));
    slaveJetty.stop();
    slaveJetty = createJetty(slave);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    //add 500 docs to master
    for (int i = 0; i < 500; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(500, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(500, masterQueryResult.getNumFound());

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
    NamedList slaveQueryRsp = rQuery(500, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(500, slaveQueryResult.getNumFound());
    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);
  }

  @Test
  public void testReplicateAfterStartup() throws Exception {
    //stop slave
    slaveJetty.stop();

    //add 500 docs to master
    for (int i = 0; i < 500; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = rQuery(500, "*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(500, masterQueryResult.getNumFound());

    //change solrconfig having 'replicateAfter startup' option on master
    copyFile(new File(CONF_DIR + "solrconfig-master2.xml"),
            new File(master.getConfDir(), "solrconfig.xml"));

    masterJetty.stop();

    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    copyFile(new File(SLAVE_CONFIG), new File(slave.getConfDir(), "solrconfig.xml"), masterJetty.getLocalPort());

    //start slave
    slaveJetty = createJetty(slave);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = rQuery(500, "*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(500, slaveQueryResult.getNumFound());

    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

  }

  @Test
  public void testReplicateAfterWrite2Slave() throws Exception {
    //add 50 docs to master
    int nDocs = 50;
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

    NamedList masterQueryRsp = rQuery(50, "*:*", masterClient);
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
    NamedList slaveQueryRsp = rQuery(1, "id:555", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
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
  }
  
  @Test
  public void testBackup() throws Exception {

    masterJetty.stop();
    copyFile(new File(CONF_DIR + "solrconfig-master1.xml"), new File(master.getConfDir(), "solrconfig.xml"));

    masterJetty = createJetty(master);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());


    //add 500 docs to master
    for (int i = 0; i < 500; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();
   
    class BackupThread extends Thread {
      volatile String fail = null;
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
      public void run() {
        String masterUrl = "http://localhost:" + masterJetty.getLocalPort() + "/solr/replication?command=" + ReplicationHandler.CMD_DETAILS;
        URL url;
        InputStream stream = null;
        try {
          url = new URL(masterUrl);
          stream = url.openStream();
          response = IOUtils.toString(stream);
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

    assertEquals(500, hits.totalHits);
    searcher.close();
    dir.close();
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

    String name;
    Integer masterPort;
    File homeDir;
    File confDir;
    File dataDir;

    /**
     * if masterPort is null, this instance is a master -- otherwise this instance is a slave, and assumes the master is
     * on localhost at the specified port.
     */
    public SolrInstance(String name, Integer port) {
      this.name = name;
      this.masterPort = port;
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
      String fname = "";
      if (null == masterPort)
        fname = CONF_DIR + "solrconfig-master.xml";
      else
        fname = SLAVE_CONFIG;
      return fname;
    }

    public void setUp() throws Exception {
      System.setProperty("solr.test.sys.prop1", "propone");
      System.setProperty("solr.test.sys.prop2", "proptwo");

      String home = System.getProperty("java.io.tmpdir")
              + File.separator
              + getClass().getName() + "-" + System.currentTimeMillis();

      if (null == masterPort) {
        homeDir = new File(home + "master");
        dataDir = new File(home + "master", "data");
        confDir = new File(home + "master", "conf");
      } else {
        homeDir = new File(home + "slave");
        dataDir = new File(home + "slave", "data");
        confDir = new File(home + "slave", "conf");
      }

      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      File f = new File(confDir, "solrconfig.xml");
      copyFile(new File(getSolrConfigFile()), f, masterPort);
      f = new File(confDir, "schema.xml");
      copyFile(new File(getSchemaFile()), f);
    }

    public void tearDown() throws Exception {
      AbstractSolrTestCase.recurseDelete(homeDir);
    }
  }
}
