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
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
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
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL     // Currently unknown why SSL does not work with this test
public class TestReplicationHandlerBackup extends SolrJettyTestBase {

  JettySolrRunner masterJetty;
  TestReplicationHandler.SolrInstance master = null;
  SolrServer masterClient;
  
  private static final String CONF_DIR = "solr"
      + File.separator + "collection1" + File.separator + "conf"
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

  private static SolrServer createNewSolrServer(int port) {
    try {
      // setup the server...
      HttpSolrServer s = new HttpSolrServer(buildUrl(port, context));
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
    masterClient = createNewSolrServer(masterJetty.getLocalPort());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    masterClient.shutdown();
    masterClient  = null;
    masterJetty.stop();
    master.tearDown();
    masterJetty = null;
    master = null;
  }


  @Test
  public void doTestBackup() throws Exception {

    int nDocs = TestUtil.nextInt(random(), 1, 100);
    masterClient.deleteByQuery("*:*");
    for (int i = 0; i < nDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("name", "name = " + i);
      masterClient.add(doc);
    }

    masterClient.commit();

    File[] snapDir = new File[2];
    boolean namedBackup = random().nextBoolean();
    try {
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

        File dataDir = new File(master.getDataDir());

        CheckBackupStatus checkBackupStatus = new CheckBackupStatus(firstBackupTimestamp);
        while (true) {
          checkBackupStatus.fetchStatus();
          if (checkBackupStatus.success) {
            if (i == 0) {
              firstBackupTimestamp = checkBackupStatus.backupTimestamp;
              Thread.sleep(1000); //ensure the next backup will have a different timestamp.
            }
            break;
          }
          Thread.sleep(200);
        }

        if (backupCommand.fail != null) {
          fail(backupCommand.fail);
        }
        File[] files = null;
        if (!namedBackup) {
          files = dataDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
              if (name.startsWith("snapshot")) {
                return true;
              }
              return false;
            }
          });
        } else {
          files = dataDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
              if (name.equals("snapshot." + backupName)) {
                return true;
              }
              return false;
            }
          });
        }
        assertEquals(1, files.length);
        snapDir[i] = files[0];
        Directory dir = new SimpleFSDirectory(snapDir[i].getAbsoluteFile().toPath());
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs hits = searcher.search(new MatchAllDocsQuery(), 1);
        assertEquals(nDocs, hits.totalHits);
        reader.close();
        dir.close();

      }

      if (!namedBackup && snapDir[0].exists()) {
        fail("The first backup should have been cleaned up because " + backupKeepParamName + " was set to 1.");
      }

      //Test Deletion of named backup
      if(namedBackup) {
        testDeleteNamedBackup(backupNames);
      }

    } finally {
      if(!namedBackup) {
        Path toDelete[] = new Path[snapDir.length];
        for (int i = 0; i < snapDir.length; i++) {
          toDelete[i] = snapDir[i].toPath();
        }
        org.apache.lucene.util.IOUtils.rm(toDelete);
      }
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

  private class CheckBackupStatus {
    String response = null;
    boolean success = false;
    String backupTimestamp = null;
    final String lastBackupTimestamp;
    final Pattern p = Pattern.compile("<str name=\"snapshotCompletedAt\">(.*?)</str>");
    final Pattern pException = Pattern.compile("<str name=\"snapShootException\">(.*?)</str>");

    CheckBackupStatus(String lastBackupTimestamp) {
      this.lastBackupTimestamp = lastBackupTimestamp;
    }

    public void fetchStatus() throws IOException {
      String masterUrl = buildUrl(masterJetty.getLocalPort(), "/solr") + "/replication?command=" + ReplicationHandler.CMD_DETAILS;
      URL url;
      InputStream stream = null;
      try {
        url = new URL(masterUrl);
        stream = url.openStream();
        response = IOUtils.toString(stream, "UTF-8");
        if(pException.matcher(response).find()) {
          fail("Failed to create backup");
        }
        if(response.contains("<str name=\"status\">success</str>")) {
          Matcher m = p.matcher(response);
          if(!m.find()) {
            fail("could not find the completed timestamp in response.");
          }
          backupTimestamp = m.group(1);
          if(!backupTimestamp.equals(lastBackupTimestamp)) {
            success = true;
          }
        }
        stream.close();
      } finally {
        IOUtils.closeQuietly(stream);
      }

    };
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
      String masterUrl = null;
      if(backupName != null) {
        masterUrl = buildUrl(masterJetty.getLocalPort(), context) + "/replication?command=" + cmd +
            "&name=" +  backupName;
      } else {
        masterUrl = buildUrl(masterJetty.getLocalPort(), context) + "/replication?command=" + cmd +
            (addNumberToKeepInRequest ? "&" + backupKeepParamName + "=1" : "");
      }

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
      String masterUrl = buildUrl(masterJetty.getLocalPort(), context) + "/replication?command=" + ReplicationHandler.CMD_DETAILS;
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
