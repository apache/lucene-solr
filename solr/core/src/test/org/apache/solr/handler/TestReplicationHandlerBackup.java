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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Properties;
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
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static long docsSeed; // see indexDocs()
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static JettySolrRunner createAndStartJetty(TestReplicationHandler.SolrInstance instance) throws Exception {
    FileUtils.copyFile(new File(SolrTestCaseJ4.TEST_HOME(), "solr.xml"), new File(instance.getHomeDir(), "solr.xml"));
    Properties nodeProperties = new Properties();
    nodeProperties.setProperty("solr.data.dir", instance.getDataDir());
    JettyConfig jettyConfig = JettyConfig.builder().setContext("/solr").setPort(0).build();
    JettySolrRunner jetty = new JettySolrRunner(instance.getHomeDir(), nodeProperties, jettyConfig);
    jetty.start();
    return jetty;
  }

  private static SolrClient createNewSolrClient(int port) {
    try {
      // setup the client...
      final String baseUrl = buildUrl(port, context);
      HttpSolrClient client = getHttpSolrClient(baseUrl, 15000, 60000);
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

    masterJetty = createAndStartJetty(master);
    masterClient = createNewSolrClient(masterJetty.getLocalPort());
    docsSeed = random().nextLong();
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
    int nDocs = BackupRestoreUtils.indexDocs(masterClient, DEFAULT_TEST_COLLECTION_NAME, docsSeed);

    //Confirm if completed
    CheckBackupStatus checkBackupStatus = new CheckBackupStatus((HttpSolrClient) masterClient, DEFAULT_TEST_CORENAME);
    while (!checkBackupStatus.success) {
      checkBackupStatus.fetchStatus();
      Thread.sleep(1000);
    }

    //Validate
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(master.getDataDir()), "snapshot*")) {
      Path snapDir = stream.iterator().next();
      verify(snapDir, nDocs);
    }
  }

  private void verify(Path backup, int nDocs) throws IOException {
    try (Directory dir = new SimpleFSDirectory(backup);
        IndexReader reader = DirectoryReader.open(dir)) {
      IndexSearcher searcher = new IndexSearcher(reader);
      TopDocs hits = searcher.search(new MatchAllDocsQuery(), 1);
      assertEquals(nDocs, hits.totalHits.value);
    }
  }


  @Test
  public void doTestBackup() throws Exception {

    int nDocs = BackupRestoreUtils.indexDocs(masterClient, DEFAULT_TEST_COLLECTION_NAME, docsSeed);

    //Confirm if completed
    CheckBackupStatus checkBackupStatus = new CheckBackupStatus((HttpSolrClient) masterClient, DEFAULT_TEST_CORENAME);
    while (!checkBackupStatus.success) {
      checkBackupStatus.fetchStatus();
      Thread.sleep(1000);
    }

    Path[] snapDir = new Path[5]; //One extra for the backup on commit
    //First snapshot location
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(master.getDataDir()), "snapshot*")) {
      snapDir[0] = stream.iterator().next();
    }

    boolean namedBackup = random().nextBoolean();
    String firstBackupTimestamp = null;

    String[] backupNames = null;
    if (namedBackup) {
      backupNames = new String[4];
    }
    for (int i = 0; i < 4; i++) {
      final String backupName = TestUtil.randomSimpleString(random(), 1, 20);
      if (!namedBackup) {
        if (addNumberToKeepInRequest) {
          runBackupCommand(masterJetty, ReplicationHandler.CMD_BACKUP, "&" + backupKeepParamName + "=2");
        } else {
          runBackupCommand(masterJetty, ReplicationHandler.CMD_BACKUP, "");
        }
      } else {
          runBackupCommand(masterJetty, ReplicationHandler.CMD_BACKUP, "&name=" +  backupName);
        backupNames[i] = backupName;
      }

     checkBackupStatus = new CheckBackupStatus((HttpSolrClient) masterClient, DEFAULT_TEST_CORENAME, firstBackupTimestamp);
      while (!checkBackupStatus.success) {
        checkBackupStatus.fetchStatus();
        Thread.sleep(1000);
      }
      if (i == 0) {
        firstBackupTimestamp = checkBackupStatus.backupTimestamp;
      }

      if (!namedBackup) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(master.getDataDir()), "snapshot*")) {
          snapDir[i+1] = stream.iterator().next();
        }
      } else {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(master.getDataDir()), "snapshot." + backupName)) {
          snapDir[i+1] = stream.iterator().next();
        }
      }
      verify(snapDir[i+1], nDocs);

    }

    
    //Test Deletion of named backup
    if (namedBackup) {
      testDeleteNamedBackup(backupNames);
    } else {
      //5 backups got created. 4 explicitly and one because a commit was called.
      // Only the last two should still exist.
      int count =0;
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(master.getDataDir()), "snapshot*")) {
        Iterator<Path> iter = stream.iterator();
        while (iter.hasNext()) {
          iter.next();
          count ++;
        }
      }

      //There will be 2 backups, otherwise 1
      if (backupKeepParamName.equals(ReplicationHandler.NUMBER_BACKUPS_TO_KEEP_REQUEST_PARAM)) {
        assertEquals(2, count);

        if (Files.exists(snapDir[0]) || Files.exists(snapDir[1]) || Files.exists(snapDir[2])) {
          fail("Backup should have been cleaned up because " + backupKeepParamName + " was set to 2.");
        }
      } else {
        assertEquals(1, count);

        if (Files.exists(snapDir[0]) || Files.exists(snapDir[1]) || Files.exists(snapDir[2])
            || Files.exists(snapDir[3])) {
          fail("Backup should have been cleaned up because " + backupKeepParamName + " was set to 1.");
        }
      }

    }
  }

  private void testDeleteNamedBackup(String backupNames[]) throws InterruptedException, IOException {
    String lastTimestamp = null;
    for (int i = 0; i < 2; i++) {
      runBackupCommand(masterJetty, ReplicationHandler.CMD_DELETE_BACKUP, "&name=" +backupNames[i]);
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
    }
  }

  public static void runBackupCommand(JettySolrRunner masterJetty, String cmd, String params) throws IOException {
    String masterUrl = buildUrl(masterJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME
        + ReplicationHandler.PATH+"?wt=xml&command=" + cmd + params;
    InputStream stream = null;
    try {
      URL url = new URL(masterUrl);
      stream = url.openStream();
      stream.close();
    } finally {
      IOUtils.closeQuietly(stream);
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
      String masterUrl = buildUrl(masterJetty.getLocalPort(), context) + "/" + DEFAULT_TEST_CORENAME + ReplicationHandler.PATH + "?wt=xml&command=" + ReplicationHandler.CMD_DETAILS;
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
    }
  }
}
