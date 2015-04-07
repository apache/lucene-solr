package org.apache.solr.handler;

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


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL     // Currently unknown why SSL does not work with this test
public class TestRestoreCore extends SolrJettyTestBase {

  JettySolrRunner masterJetty;
  TestReplicationHandler.SolrInstance master = null;
  SolrClient masterClient;

  private static final String CONF_DIR = "solr" + File.separator + "collection1" + File.separator + "conf"
      + File.separator;

  private static String context = "/solr";

  private static JettySolrRunner createJetty(TestReplicationHandler.SolrInstance instance) throws Exception {
    FileUtils.copyFile(new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml"), new File(instance.getHomeDir(), "solr.xml"));
    JettySolrRunner jetty = new JettySolrRunner(instance.getHomeDir(), "/solr", 0);
    jetty.setDataDir(instance.getDataDir());
    jetty.start();
    return jetty;
  }

  private static SolrClient createNewSolrClient(int port) {
    try {
      // setup the client...
      HttpSolrClient client = new HttpSolrClient(buildUrl(port, context) + "/" + DEFAULT_TEST_CORENAME);
      client.setConnectionTimeout(15000);
      client.setSoTimeout(60000);
      client.setDefaultMaxConnectionsPerHost(100);
      client.setMaxTotalConnections(100);
      return client;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }


  @Before
  public void setUp() throws Exception {
    super.setUp();
    String configFile = "solrconfig-master.xml";

    master = new TestReplicationHandler.SolrInstance(createTempDir("solr-instance").toFile(), "master", null);
    master.setUp();
    master.copyConfigFile(CONF_DIR + configFile, "solrconfig.xml");

    masterJetty = createJetty(master);
    masterClient = createNewSolrClient(masterJetty.getLocalPort());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    masterClient.close();
    masterClient  = null;
    masterJetty.stop();
    masterJetty = null;
    master = null;
  }

  @Test
  public void testSimpleRestore() throws Exception {

    int nDocs = TestReplicationHandlerBackup.indexDocs(masterClient);

    String snapshotName;
    String location;
    String params = "";

    //Use the default backup location or an externally provided location.
    if (random().nextBoolean()) {
      location = createTempDir().toFile().getAbsolutePath();
      params += "&location=" + URLEncoder.encode(location, "UTF-8");
    }

    //named snapshot vs default snapshot name
    if (random().nextBoolean()) {
      snapshotName = TestUtil.randomSimpleString(random(), 1, 5);
      params += "&name=" + snapshotName;
    }

    TestReplicationHandlerBackup.runBackupCommand(masterJetty, ReplicationHandler.CMD_BACKUP, params);

    CheckBackupStatus checkBackupStatus = new CheckBackupStatus((HttpSolrClient) masterClient, null);
    while (!checkBackupStatus.success) {
      checkBackupStatus.fetchStatus();
      Thread.sleep(1000);
    }

    //Modify existing index before we call restore.

    //Delete a few docs
    int numDeletes = TestUtil.nextInt(random(), 1, nDocs);
    for(int i=0; i<numDeletes; i++) {
      masterClient.deleteByQuery("id:" + i);
    }
    masterClient.commit();

    //Add a few more
    int moreAdds = TestUtil.nextInt(random(), 1, 100);
    for (int i=0; i<moreAdds; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i + nDocs);
      doc.addField("name", "name = " + (i + nDocs));
      masterClient.add(doc);
    }
    //Purposely not calling commit once in a while. There can be some docs which are not committed
    if (usually()) {
      masterClient.commit();
    }

    TestReplicationHandlerBackup.runBackupCommand(masterJetty, ReplicationHandler.CMD_RESTORE, params);

    while (!fetchRestoreStatus()) {
      Thread.sleep(1000);
    }

    //See if restore was successful by checking if all the docs are present again
    verifyDocs(nDocs);
  }

  @Test
  public void testFailedRestore() throws Exception {
    int nDocs = TestReplicationHandlerBackup.indexDocs(masterClient);

    String location = createTempDir().toFile().getAbsolutePath();
    String snapshotName = TestUtil.randomSimpleString(random(), 1, 5);
    String params = "&name=" + snapshotName + "&location=" + URLEncoder.encode(location, "UTF-8");

    TestReplicationHandlerBackup.runBackupCommand(masterJetty, ReplicationHandler.CMD_BACKUP, params);

    CheckBackupStatus checkBackupStatus = new CheckBackupStatus((HttpSolrClient) masterClient, null);
    while (!checkBackupStatus.success) {
      checkBackupStatus.fetchStatus();
      Thread.sleep(1000);
    }

    //Remove the segments_n file so that the backup index is corrupted.
    //Restore should fail and it should automatically rollback to the original index.
    Path restoreIndexPath = Paths.get(location, "snapshot." + snapshotName);
    Path segmentFileName = Files.newDirectoryStream(restoreIndexPath, IndexFileNames.SEGMENTS + "*").iterator().next();
    Files.delete(segmentFileName);

    TestReplicationHandlerBackup.runBackupCommand(masterJetty, ReplicationHandler.CMD_RESTORE, params);

    try {
      while (!fetchRestoreStatus()) {
        Thread.sleep(1000);
      }
      fail("Should have thrown an error because restore could not have been successful");
    } catch (AssertionError e) {
      //supposed to happen
    }

    verifyDocs(nDocs);

    //make sure we can write to the index again
    nDocs = TestReplicationHandlerBackup.indexDocs(masterClient);
    verifyDocs(nDocs);

  }

  private void verifyDocs(int nDocs) throws SolrServerException, IOException {
    ModifiableSolrParams queryParams = new ModifiableSolrParams();
    queryParams.set("q", "*:*");
    QueryResponse response = masterClient.query(queryParams);

    assertEquals(0, response.getStatus());
    assertEquals(nDocs, response.getResults().getNumFound());
  }

  private boolean fetchRestoreStatus() throws IOException {
    String masterUrl = buildUrl(masterJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME +
        "/replication?command=" + ReplicationHandler.CMD_RESTORE_STATUS;
    final Pattern pException = Pattern.compile("<str name=\"exception\">(.*?)</str>");

    InputStream stream = null;
    try {
      URL url = new URL(masterUrl);
      stream = url.openStream();
      String response = IOUtils.toString(stream, "UTF-8");
      Matcher matcher = pException.matcher(response);
      if(matcher.find()) {
        fail("Failed to complete restore action with exception " + matcher.group(1));
      }
      if(response.contains("<str name=\"status\">success</str>")) {
        return true;
      } else if (response.contains("<str name=\"status\">failed</str>")){
        fail("Restore Failed");
      }
      stream.close();
    } finally {
      IOUtils.closeQuietly(stream);
    }
    return false;
  }
}
