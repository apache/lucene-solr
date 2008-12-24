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

import junit.framework.TestCase;
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

import java.io.*;
import java.net.URL;

/**
 * Test for ReplicationHandler
 *
 * @version $Id$
 * @since 1.4
 */
public class TestReplicationHandler extends TestCase {

  JettySolrRunner masterJetty, slaveJetty;
  SolrServer masterClient, slaveClient;
  SolrInstance master = null, slave = null;

  String context = "/solr";

  public void setUp() throws Exception {
    master = new SolrInstance("master", 1);
    slave = new SolrInstance("slave", 0);
    master.setUp();
    slave.setUp();

    masterJetty = createJetty(master, 9999);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    slaveJetty = createJetty(slave, 0);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

  }

  @Override
  public void tearDown() throws Exception {
    destroyServers();
    master.tearDown();
    slave.tearDown();
  }

  private void destroyServers() throws Exception {
    masterJetty.stop();
    slaveJetty.stop();
  }

  private JettySolrRunner createJetty(SolrInstance instance, int port) throws Exception {
    System.setProperty("solr.solr.home", instance.getHomeDir());
    System.setProperty("solr.data.dir", instance.getDataDir());

    JettySolrRunner jetty = new JettySolrRunner("/solr", port);

    jetty.start();
    return jetty;
  }

  protected SolrServer createNewSolrServer(int port) {
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

  public void testIndexAndConfigReplication() throws Exception {

    //add 500 docs to master
    for (int i = 0; i < 500; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = query("*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(500, masterQueryResult.getNumFound());

    //sleep for pollinterval time 4s, to let slave pull data.
    Thread.sleep(3000);
    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = query("*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(500, slaveQueryResult.getNumFound());

    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);

    //start config files replication test
    masterClient.deleteByQuery("*:*");
    masterClient.commit();

    //change the schema on master
    copyFile(new File("." + File.separator +
            "solr" + File.separator +
            "conf" + File.separator + "schema-replication2.xml"),
            new File(master.getConfDir(), "schema.xml"));

    masterJetty.stop();

    masterJetty = createJetty(master, 9999);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    //add a doc with new field and commit on master to trigger snappull from slave.
    index(masterClient, "id", "2000", "name", "name = " + 2000, "newname", "newname = " + 2000);
    masterClient.commit();

    //sleep for 4s for replication to happen.
    Thread.sleep(3000);

    slaveQueryRsp = query("*:*", slaveClient);
    SolrDocument d = ((SolrDocumentList) slaveQueryRsp.get("response")).get(0);
    assertEquals("newname = 2000", (String) d.getFieldValue("newname"));

  }

  public void testIndexAndConfigAliasReplication() throws Exception {

    //add 500 docs to master
    for (int i = 0; i < 500; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = query("*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(500, masterQueryResult.getNumFound());

    //sleep for pollinterval time 3s, to let slave pull data.
    Thread.sleep(3000);
    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = query("*:*", slaveClient);
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
    copyFile(new File("." + File.separator +
            "solr" + File.separator +
            "conf" + File.separator + "solrconfig-master1.xml"),
            new File(master.getConfDir(), "solrconfig.xml"));

    //change schema on master
    copyFile(new File("." + File.separator +
            "solr" + File.separator +
            "conf" + File.separator + "schema-replication2.xml"),
            new File(master.getConfDir(), "schema.xml"));

    //keep a copy of the new schema
    copyFile(new File("." + File.separator +
            "solr" + File.separator +
            "conf" + File.separator + "schema-replication2.xml"),
            new File(master.getConfDir(), "schema-replication2.xml"));

    masterJetty.stop();

    masterJetty = createJetty(master, 9999);
    masterClient = createNewSolrServer(masterJetty.getLocalPort());

    //add a doc with new field and commit on master to trigger snappull from slave.
    index(masterClient, "id", "2000", "name", "name = " + 2000, "newname", "newname = " + 2000);
    masterClient.commit();

    //sleep for 3s for replication to happen.
    Thread.sleep(3000);

    index(slaveClient, "id", "2000", "name", "name = " + 2001, "newname", "newname = " + 2001);
    slaveClient.commit();

    slaveQueryRsp = query("*:*", slaveClient);
    SolrDocument d = ((SolrDocumentList) slaveQueryRsp.get("response")).get(0);
    assertEquals("newname = 2001", (String) d.getFieldValue("newname"));

  }


  public void testStopPoll() throws Exception {
    // Test:
    // setup master/slave.
    // stop polling on slave, add a doc to master and verify slave hasn't picked it.

    //add 500 docs to master
    for (int i = 0; i < 500; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = query("*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(500, masterQueryResult.getNumFound());

    //sleep for pollinterval time 3s, to let slave pull data.
    Thread.sleep(3000);
    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = query("*:*", slaveClient);
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
    //sleep for pollinterval time 3s, to let slave pull data.
    Thread.sleep(3000);
    //get docs from slave and check if number is equal to master
    slaveQueryRsp = query("*:*", slaveClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(500, slaveQueryResult.getNumFound());
    //get docs from slave and check if number is equal to master
    slaveQueryRsp = query("*:*", masterClient);
    slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(501, slaveQueryResult.getNumFound());
  }

  public void testSnapPullWithMasterUrl() throws Exception {
    //change solrconfig on slave
    //this has no entry for pollinginterval
    copyFile(new File("." + File.separator +
            "solr" + File.separator +
            "conf" + File.separator + "solrconfig-slave1.xml"),
            new File(slave.getConfDir(), "solrconfig.xml"));
    slaveJetty.stop();
    slaveJetty = createJetty(slave, 0);
    slaveClient = createNewSolrServer(slaveJetty.getLocalPort());

    //add 500 docs to master
    for (int i = 0; i < 500; i++)
      index(masterClient, "id", i, "name", "name = " + i);

    masterClient.commit();

    NamedList masterQueryRsp = query("*:*", masterClient);
    SolrDocumentList masterQueryResult = (SolrDocumentList) masterQueryRsp.get("response");
    assertEquals(500, masterQueryResult.getNumFound());

    // snappull
    String masterUrl = "http://localhost:" + slaveJetty.getLocalPort() + "/solr/replication?command=snappull&masterUrl=";
    masterUrl += "http://localhost:" + masterJetty.getLocalPort() + "/solr/replication";
    URL url = new URL(masterUrl);
    InputStream stream = url.openStream();
    try {
      stream.close();
    } catch (IOException e) {
      //e.printStackTrace();
    }
    Thread.sleep(3000);
    //get docs from slave and check if number is equal to master
    NamedList slaveQueryRsp = query("*:*", slaveClient);
    SolrDocumentList slaveQueryResult = (SolrDocumentList) slaveQueryRsp.get("response");
    assertEquals(500, slaveQueryResult.getNumFound());
    //compare results
    String cmp = TestDistributedSearch.compare(masterQueryResult, slaveQueryResult, 0, null);
    assertEquals(null, cmp);
  }

  void copyFile(File src, File dst) throws IOException {
    InputStream in = new FileInputStream(src);
    OutputStream out = new FileOutputStream(dst);

    byte[] buf = new byte[1024];
    int len;
    while ((len = in.read(buf)) > 0)
      out.write(buf, 0, len);
    in.close();
    out.close();
  }

  private class SolrInstance extends AbstractSolrTestCase {

    String name;
    int type;
    File homeDir;
    File confDir;

    public SolrInstance(String name, int type) {
      this.name = name;
      this.type = type;
    }

    public String getHomeDir() {
      return homeDir.toString();
    }

    @Override
    public String getSchemaFile() {
      return "." + File.separator + "solr" + File.separator + "conf" + File.separator + "schema-replication1.xml";
    }

    public String getConfDir() {
      return confDir.toString();
    }

    public String getDataDir() {
      return dataDir.toString();
    }

    @Override
    public String getSolrConfigFile() {
      String fname = "";
      if (type == 1)
        fname = "." + File.separator + "solr" + File.separator + "conf" + File.separator + "solrconfig-master.xml";
      if (type == 0)
        fname = "." + File.separator + "solr" + File.separator + "conf" + File.separator + "solrconfig-slave.xml";
      System.out.println(fname);
      return fname;
    }

    public void setUp() throws Exception {
      System.setProperty("solr.test.sys.prop1", "propone");
      System.setProperty("solr.test.sys.prop2", "proptwo");

      String home = System.getProperty("java.io.tmpdir")
              + File.separator
              + getClass().getName() + "-" + System.currentTimeMillis();

      if (type == 1) {
        homeDir = new File(home + "master");
        dataDir = new File(home + "master", "data");
        confDir = new File(home + "master", "conf");
      }
      if (type == 0) {
        homeDir = new File(home + "slave");
        dataDir = new File(home + "slave", "data");
        confDir = new File(home + "slave", "conf");
      }

      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      File f = new File(confDir, "solrconfig.xml");
      copyFile(new File(getSolrConfigFile()), f);
      f = new File(confDir, "schema.xml");
      copyFile(new File(getSchemaFile()), f);
    }

    public void tearDown() throws Exception {
      super.tearDown();
      AbstractSolrTestCase.recurseDelete(homeDir);
    }
  }
}
