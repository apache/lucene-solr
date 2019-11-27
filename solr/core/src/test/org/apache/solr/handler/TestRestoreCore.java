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
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL     // Currently unknown why SSL does not work with this test
public class TestRestoreCore extends SolrJettyTestBase {

  JettySolrRunner masterJetty;
  TestReplicationHandler.SolrInstance master = null;
  SolrClient masterClient;

  private static final String CONF_DIR = "solr" + File.separator + DEFAULT_TEST_CORENAME + File.separator + "conf"
      + File.separator;

  private static String context = "/solr";
  private static long docsSeed; // see indexDocs()

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
    String configFile = "solrconfig-master.xml";

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
    if (null != masterClient) {
      masterClient.close();
      masterClient  = null;
    }
    if (null != masterJetty) {
      masterJetty.stop();
      masterJetty = null;
    }
    master = null;
  }

  @Test
  public void testSimpleRestore() throws Exception {

    int nDocs = usually() ? BackupRestoreUtils.indexDocs(masterClient, "collection1", docsSeed) : 0;

    final BackupStatusChecker backupStatus
      = new BackupStatusChecker(masterClient, "/" + DEFAULT_TEST_CORENAME + "/replication");
    final String oldBackupDir = backupStatus.checkBackupSuccess();
    String snapshotName = null;
    String location;
    String params = "";
    String baseUrl = masterJetty.getBaseUrl().toString();

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

    if (null == snapshotName) {
      backupStatus.waitForDifferentBackupDir(oldBackupDir, 30);
    } else {
      backupStatus.waitForBackupSuccess(snapshotName, 30);
    }

    int numRestoreTests = nDocs > 0 ? TestUtil.nextInt(random(), 1, 5) : 1;

    for (int attempts=0; attempts<numRestoreTests; attempts++) {
      //Modify existing index before we call restore.

      if (nDocs > 0) {
        //Delete a few docs
        int numDeletes = TestUtil.nextInt(random(), 1, nDocs);
        for(int i=0; i<numDeletes; i++) {
          masterClient.deleteByQuery(DEFAULT_TEST_CORENAME, "id:" + i);
        }
        masterClient.commit(DEFAULT_TEST_CORENAME);

        //Add a few more
        int moreAdds = TestUtil.nextInt(random(), 1, 100);
        for (int i=0; i<moreAdds; i++) {
          SolrInputDocument doc = new SolrInputDocument();
          doc.addField("id", i + nDocs);
          doc.addField("name", "name = " + (i + nDocs));
          masterClient.add(DEFAULT_TEST_CORENAME, doc);
        }
        //Purposely not calling commit once in a while. There can be some docs which are not committed
        if (usually()) {
          masterClient.commit(DEFAULT_TEST_CORENAME);
        }
      }

      TestReplicationHandlerBackup.runBackupCommand(masterJetty, ReplicationHandler.CMD_RESTORE, params);

      while (!fetchRestoreStatus(baseUrl, DEFAULT_TEST_CORENAME)) {
        Thread.sleep(1000);
      }

      //See if restore was successful by checking if all the docs are present again
      BackupRestoreUtils.verifyDocs(nDocs, masterClient, DEFAULT_TEST_CORENAME);
    }

  }

  @Test
  public void testFailedRestore() throws Exception {
    int nDocs = BackupRestoreUtils.indexDocs(masterClient, "collection1", docsSeed);

    String location = createTempDir().toFile().getAbsolutePath();
    String snapshotName = TestUtil.randomSimpleString(random(), 1, 5);
    String params = "&name=" + snapshotName + "&location=" + URLEncoder.encode(location, "UTF-8");
    String baseUrl = masterJetty.getBaseUrl().toString();

    TestReplicationHandlerBackup.runBackupCommand(masterJetty, ReplicationHandler.CMD_BACKUP, params);

    final BackupStatusChecker backupStatus
      = new BackupStatusChecker(masterClient, "/" + DEFAULT_TEST_CORENAME + "/replication");
    final String backupDirName = backupStatus.waitForBackupSuccess(snapshotName, 30);

    //Remove the segments_n file so that the backup index is corrupted.
    //Restore should fail and it should automatically rollback to the original index.
    final Path restoreIndexPath = Paths.get(location, backupDirName);
    assertTrue("Does not exist: " + restoreIndexPath, Files.exists(restoreIndexPath));
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(restoreIndexPath, IndexFileNames.SEGMENTS + "*")) {
      Path segmentFileName = stream.iterator().next();
      Files.delete(segmentFileName);
    }

    TestReplicationHandlerBackup.runBackupCommand(masterJetty, ReplicationHandler.CMD_RESTORE, params);

    expectThrows(AssertionError.class, () -> {
        for (int i = 0; i < 10; i++) {
          // this will throw an assertion once we get what we expect
          fetchRestoreStatus(baseUrl, DEFAULT_TEST_CORENAME);
          Thread.sleep(50);
        }
        // if we never got an assertion let expectThrows complain
      });

    BackupRestoreUtils.verifyDocs(nDocs, masterClient, DEFAULT_TEST_CORENAME);

    //make sure we can write to the index again
    nDocs = BackupRestoreUtils.indexDocs(masterClient, "collection1", docsSeed);
    BackupRestoreUtils.verifyDocs(nDocs, masterClient, DEFAULT_TEST_CORENAME);

  }

  public static boolean fetchRestoreStatus (String baseUrl, String coreName) throws IOException {
    String masterUrl = baseUrl + "/" + coreName +
        ReplicationHandler.PATH + "?wt=xml&command=" + ReplicationHandler.CMD_RESTORE_STATUS;
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
