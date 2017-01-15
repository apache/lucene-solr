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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;

@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
@SolrTestCaseJ4.SuppressSSL     // Currently unknown why SSL does not work with this test
public class TestHdfsBackupRestoreCore extends SolrCloudTestCase {
  public static final String HDFS_REPO_SOLR_XML = "<solr>\n" +
      "\n" +
      "  <str name=\"shareSchema\">${shareSchema:false}</str>\n" +
      "  <str name=\"configSetBaseDir\">${configSetBaseDir:configsets}</str>\n" +
      "  <str name=\"coreRootDirectory\">${coreRootDirectory:.}</str>\n" +
      "\n" +
      "  <shardHandlerFactory name=\"shardHandlerFactory\" class=\"HttpShardHandlerFactory\">\n" +
      "    <str name=\"urlScheme\">${urlScheme:}</str>\n" +
      "    <int name=\"socketTimeout\">${socketTimeout:90000}</int>\n" +
      "    <int name=\"connTimeout\">${connTimeout:15000}</int>\n" +
      "  </shardHandlerFactory>\n" +
      "\n" +
      "  <solrcloud>\n" +
      "    <str name=\"host\">127.0.0.1</str>\n" +
      "    <int name=\"hostPort\">${hostPort:8983}</int>\n" +
      "    <str name=\"hostContext\">${hostContext:solr}</str>\n" +
      "    <int name=\"zkClientTimeout\">${solr.zkclienttimeout:30000}</int>\n" +
      "    <bool name=\"genericCoreNodeNames\">${genericCoreNodeNames:true}</bool>\n" +
      "    <int name=\"leaderVoteWait\">10000</int>\n" +
      "    <int name=\"distribUpdateConnTimeout\">${distribUpdateConnTimeout:45000}</int>\n" +
      "    <int name=\"distribUpdateSoTimeout\">${distribUpdateSoTimeout:340000}</int>\n" +
      "  </solrcloud>\n" +
      "  \n" +
      "  <backup>\n" +
      "    <repository  name=\"hdfs\" class=\"org.apache.solr.core.backup.repository.HdfsBackupRepository\"> \n" +
      "      <str name=\"location\">${solr.hdfs.default.backup.path}</str>\n" +
      "      <str name=\"solr.hdfs.home\">${solr.hdfs.home:}</str>\n" +
      "      <str name=\"solr.hdfs.confdir\">${solr.hdfs.confdir:}</str>\n" +
      "      <str name=\"solr.hdfs.permissions.umask-mode\">${solr.hdfs.permissions.umask-mode:000}</str>\n" +
      "    </repository>\n" +
      "  </backup>\n" +
      "  \n" +
      "</solr>\n";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static MiniDFSCluster dfsCluster;
  private static String hdfsUri;
  private static FileSystem fs;
  private static long docsSeed; // see indexDocs()

  @BeforeClass
  public static void setupClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());
    hdfsUri = HdfsTestUtil.getURI(dfsCluster);
    try {
      URI uri = new URI(hdfsUri);
      Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
      conf.setBoolean("fs.hdfs.impl.disable.cache", true);
      fs = FileSystem.get(uri, conf);

      if (fs instanceof DistributedFileSystem) {
        // Make sure dfs is not in safe mode
        while (((DistributedFileSystem) fs).setSafeMode(SafeModeAction.SAFEMODE_GET, true)) {
          log.warn("The NameNode is in SafeMode - Solr will wait 5 seconds and try again.");
          try {
            Thread.sleep(5000);
          } catch (InterruptedException e) {
            Thread.interrupted();
            // continue
          }
        }
      }

      fs.mkdirs(new org.apache.hadoop.fs.Path("/backup"));
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    }

    System.setProperty("solr.hdfs.default.backup.path", "/backup");
    System.setProperty("solr.hdfs.home", hdfsUri + "/solr");
    useFactory("solr.StandardDirectoryFactory");

    configureCluster(1)// nodes
    .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
    .withSolrXml(HDFS_REPO_SOLR_XML)
    .configure();

    docsSeed = random().nextLong();
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    System.clearProperty("solr.hdfs.home");
    System.clearProperty("solr.hdfs.default.backup.path");
    System.clearProperty("test.build.data");
    System.clearProperty("test.cache.data");
    IOUtils.closeQuietly(fs);
    fs = null;
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
  }

  @Test
  public void test() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "HdfsBackupRestore";
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1);
    create.process(solrClient);

    int nDocs = BackupRestoreUtils.indexDocs(solrClient, collectionName, docsSeed);

    DocCollection collectionState = solrClient.getZkStateReader().getClusterState().getCollection(collectionName);
    assertEquals(1, collectionState.getActiveSlices().size());
    Slice shard = collectionState.getActiveSlices().iterator().next();
    assertEquals(1, shard.getReplicas().size());
    Replica replica = shard.getReplicas().iterator().next();

    String replicaBaseUrl = replica.getStr(BASE_URL_PROP);
    String coreName = replica.getStr(ZkStateReader.CORE_NAME_PROP);
    String backupName = TestUtil.randomSimpleString(random(), 1, 5);

    boolean testViaReplicationHandler = random().nextBoolean();
    String baseUrl = cluster.getJettySolrRunners().get(0).getBaseUrl().toString();

    try (SolrClient masterClient = getHttpSolrClient(replicaBaseUrl)) {
      // Create a backup.
      if (testViaReplicationHandler) {
        log.info("Running Backup via replication handler");
        BackupRestoreUtils.runReplicationHandlerCommand(baseUrl, coreName, ReplicationHandler.CMD_BACKUP, "hdfs", backupName);
        CheckBackupStatus checkBackupStatus = new CheckBackupStatus((HttpSolrClient) masterClient, coreName, null);
        while (!checkBackupStatus.success) {
          checkBackupStatus.fetchStatus();
          Thread.sleep(1000);
        }
      } else {
        log.info("Running Backup via core admin api");
        Map<String,String> params = new HashMap<>();
        params.put("name", backupName);
        params.put(CoreAdminParams.BACKUP_REPOSITORY, "hdfs");
        BackupRestoreUtils.runCoreAdminCommand(replicaBaseUrl, coreName, CoreAdminAction.BACKUPCORE.toString(), params);
      }

      int numRestoreTests = nDocs > 0 ? TestUtil.nextInt(random(), 1, 5) : 1;
      for (int attempts=0; attempts<numRestoreTests; attempts++) {
        //Modify existing index before we call restore.
        if (nDocs > 0) {
          //Delete a few docs
          int numDeletes = TestUtil.nextInt(random(), 1, nDocs);
          for(int i=0; i<numDeletes; i++) {
            masterClient.deleteByQuery(collectionName, "id:" + i);
          }
          masterClient.commit(collectionName);

          //Add a few more
          int moreAdds = TestUtil.nextInt(random(), 1, 100);
          for (int i=0; i<moreAdds; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", i + nDocs);
            doc.addField("name", "name = " + (i + nDocs));
            masterClient.add(collectionName, doc);
          }
          //Purposely not calling commit once in a while. There can be some docs which are not committed
          if (usually()) {
            masterClient.commit(collectionName);
          }
        }
        // Snapshooter prefixes "snapshot." to the backup name.
        if (testViaReplicationHandler) {
          log.info("Running Restore via replication handler");
          // Snapshooter prefixes "snapshot." to the backup name.
          BackupRestoreUtils.runReplicationHandlerCommand(baseUrl, coreName, ReplicationHandler.CMD_RESTORE, "hdfs", backupName);
          while (!TestRestoreCore.fetchRestoreStatus(baseUrl, coreName)) {
            Thread.sleep(1000);
          }
        } else {
          log.info("Running Restore via core admin api");
          Map<String,String> params = new HashMap<>();
          params.put("name", "snapshot." + backupName);
          params.put(CoreAdminParams.BACKUP_REPOSITORY, "hdfs");
          BackupRestoreUtils.runCoreAdminCommand(replicaBaseUrl, coreName, CoreAdminAction.RESTORECORE.toString(), params);
        }
        //See if restore was successful by checking if all the docs are present again
        BackupRestoreUtils.verifyDocs(nDocs, masterClient, coreName);

        // Verify the permissions for the backup folder.
        FileStatus status = fs.getFileStatus(new org.apache.hadoop.fs.Path("/backup/snapshot."+backupName));
        FsPermission perm = status.getPermission();
        assertEquals(FsAction.ALL, perm.getUserAction());
        assertEquals(FsAction.ALL, perm.getGroupAction());
        assertEquals(FsAction.ALL, perm.getOtherAction());
      }
    }
  }
}
