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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL     // Currently unknown why SSL does not work with this test
public class TestReplicationHandlerBackup extends SolrJettyTestBase {

  JettySolrRunner masterJetty;
  TestReplicationHandler.SolrInstance master = null;
  SolrClient masterClient;
  
  private static final String CONF_DIR = "solr" + File.separator + "collection1" + File.separator + "conf"
      + File.separator;

  private static String context = "/solr";

  boolean addNumberToKeepInRequest = true;
  String backupKeepParamName = ReplicationHandler.NUMBER_BACKUPS_TO_KEEP_REQUEST_PARAM;

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
    String configFile = "solrconfig-master1.xml";

    if(random().nextBoolean()) {
      configFile = "solrconfig-master1-keepOneBackup.xml";
      addNumberToKeepInRequest = false;
      backupKeepParamName = ReplicationHandler.NUMBER_BACKUPS_TO_KEEP_INIT_PARAM;
    }
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
  public void testBackupOnCommit() throws Exception {
    //Index
    int nDocs = indexDocs();

    //Confirm if completed
    CheckBackupStatus checkBackupStatus = new CheckBackupStatus((HttpSolrClient) masterClient);
    while (!checkBackupStatus.success) {
      checkBackupStatus.fetchStatus();
      Thread.sleep(1000);
    }

    //Validate
    Path snapDir = Files.newDirectoryStream(Paths.get(master.getDataDir()), "snapshot*").iterator().next();
    verify(snapDir, nDocs);
  }

  private void verify(Path backup, int nDocs) throws IOException {
    try (Directory dir = new SimpleFSDirectory(backup)) {
      IndexReader reader = DirectoryReader.open(dir);
      IndexSearcher searcher = new IndexSearcher(reader);
      TopDocs hits = searcher.search(new MatchAllDocsQuery(), 1);
      assertEquals(nDocs, hits.totalHits);
      reader.close();
      dir.close();
    }
  }

  private int indexDocs() throws IOException, SolrServerException {
    int nDocs = TestUtil.nextInt(random(), 1, 100);
    masterClient.deleteByQuery("*:*");
    for (int i = 0; i < nDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("name", "name = " + i);
      masterClient.add(doc);
    }

    masterClient.commit();
    return nDocs;
  }


  @Test
  public void doTestBackup() throws Exception {

    int nDocs = indexDocs();

    Path[] snapDir = new Path[2];
    boolean namedBackup = random().nextBoolean();
    String firstBackupTimestamp = null;

    String[] backupNames = null;
    if (namedBackup) {
      backupNames = new String[2];
    }
    for (int i = 0; i < 2; i++) {
      BackupCommand backupCommand;
      final String backupName = TestUtil.randomSimpleString(random(), 1, 20);
      if (!namedBackup) {
        backupCommand = new BackupCommand(addNumberToKeepInRequest, backupKeepParamName, ReplicationHandler.CMD_BACKUP);
      } else {
        backupCommand = new BackupCommand(backupName, ReplicationHandler.CMD_BACKUP);
        backupNames[i] = backupName;
      }
      backupCommand.runCommand();
      if (backupCommand.fail != null) {
        fail(backupCommand.fail);
      }

      CheckBackupStatus checkBackupStatus = new CheckBackupStatus((HttpSolrClient) masterClient, firstBackupTimestamp);
      while (!checkBackupStatus.success) {
        checkBackupStatus.fetchStatus();
        Thread.sleep(1000);
      }
      if (i == 0) {
        firstBackupTimestamp = checkBackupStatus.backupTimestamp;
      }

      if (!namedBackup) {
        snapDir[i] = Files.newDirectoryStream(Paths.get(master.getDataDir()), "snapshot*").iterator().next();
      } else {
        snapDir[i] = Files.newDirectoryStream(Paths.get(master.getDataDir()), "snapshot." + backupName).iterator().next();
      }
      verify(snapDir[i], nDocs);

    }

    if (!namedBackup && Files.exists(snapDir[0])) {
      fail("The first backup should have been cleaned up because " + backupKeepParamName + " was set to 1.");
    }

    //Test Deletion of named backup
    if(namedBackup) {
      testDeleteNamedBackup(backupNames);
    }
  }

  private void testDeleteNamedBackup(String backupNames[]) throws InterruptedException, IOException {
    String lastTimestamp = null;
    for (int i = 0; i < 2; i++) {
      BackupCommand deleteBackupCommand = new BackupCommand(backupNames[i], ReplicationHandler.CMD_DELETE_BACKUP);
      deleteBackupCommand.runCommand();
      CheckDeleteBackupStatus checkDeleteBackupStatus = new CheckDeleteBackupStatus(backupNames[i], lastTimestamp);
      while (true) {
        boolean success = checkDeleteBackupStatus.fetchStatus();
        if (success) {
          lastTimestamp = checkDeleteBackupStatus.lastTimestamp;
          if (i == 0) {
            Thread.sleep(1000); //make the timestamp change
          }
          break;
        }
        Thread.sleep(200);
      }

      if (deleteBackupCommand.fail != null) {
        fail(deleteBackupCommand.fail);
      }
    }
  }

  private class BackupCommand {
    String fail = null;
    final boolean addNumberToKeepInRequest;
    String backupKeepParamName;
    String backupName;
    String cmd;
    
    BackupCommand(boolean addNumberToKeepInRequest, String backupKeepParamName, String command) {
      this.addNumberToKeepInRequest = addNumberToKeepInRequest;
      this.backupKeepParamName = backupKeepParamName;
      this.cmd = command;
    }
    BackupCommand(String backupName, String command) {
      this.backupName = backupName;
      addNumberToKeepInRequest = false;
      this.cmd = command;
    }
    
    public void runCommand() {
      String masterUrl;
      if(backupName != null) {
        masterUrl = buildUrl(masterJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME + "/replication?command=" + cmd +
            "&name=" +  backupName;
      } else {
        masterUrl = buildUrl(masterJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME + "/replication?command=" + cmd +
            (addNumberToKeepInRequest ? "&" + backupKeepParamName + "=1" : "");
      }

      InputStream stream = null;
      try {
        URL url = new URL(masterUrl);
        stream = url.openStream();
        stream.close();
      } catch (Exception e) {
        fail = e.getMessage();
      } finally {
        IOUtils.closeQuietly(stream);
      }

    }
  }

  private class CheckDeleteBackupStatus {
    String response = null;
    private String backupName;
    final Pattern p = Pattern.compile("<str name=\"snapshotDeletedAt\">(.*?)</str>");
    String lastTimestamp;
    
    private CheckDeleteBackupStatus(String backupName, String lastTimestamp) {
      this.backupName = backupName;
      this.lastTimestamp = lastTimestamp;
    }

    public boolean fetchStatus() throws IOException {
      String masterUrl = buildUrl(masterJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME + "/replication?command=" + ReplicationHandler.CMD_DETAILS;
      URL url;
      InputStream stream = null;
      try {
        url = new URL(masterUrl);
        stream = url.openStream();
        response = IOUtils.toString(stream, "UTF-8");
        if(response.contains("<str name=\"status\">success</str>")) {
          Matcher m = p.matcher(response);
          if(m.find() && (lastTimestamp == null || !lastTimestamp.equals(m.group(1)))) {
            lastTimestamp = m.group(1);
            return true;
          }
        } else if(response.contains("<str name=\"status\">Unable to delete snapshot: " + backupName + "</str>" )) {
          return false;
        }
        stream.close();
      } finally {
        IOUtils.closeQuietly(stream);
      }
      return false;
    };
  }
}
