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
package org.apache.solr.cloud;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.COLL_CONF;
import static org.apache.solr.core.backup.BackupManager.*;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants.SafeModeAction;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.backup.BackupManager;
import org.apache.solr.core.backup.repository.HdfsBackupRepository;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the tests for HDFS integration for Solr backup/restore capability.
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class TestHdfsCloudBackupRestore extends AbstractCloudBackupRestoreTestCase {
  public static final String SOLR_XML = "<solr>\n" +
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
      "    </repository>\n" +
      "  </backup>\n" +
      "  \n" +
      "</solr>\n";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static MiniDFSCluster dfsCluster;
  private static String hdfsUri;
  private static FileSystem fs;

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

    configureCluster(NUM_SHARDS)// nodes
    .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
    .withSolrXml(SOLR_XML)
    .configure();
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

  @Override
  public String getCollectionName() {
    return "hdfsbackuprestore";
  }

  @Override
  public String getBackupRepoName() {
    return "hdfs";
  }

  @Override
  public String getBackupLocation() {
    return null;
  }

  protected void testConfigBackupOnly(String configName, String collectionName) throws Exception {
    String backupName = "configonlybackup";
    CloudSolrClient solrClient = cluster.getSolrClient();

    CollectionAdminRequest.Backup backup = CollectionAdminRequest.backupCollection(collectionName, backupName)
        .setRepositoryName(getBackupRepoName())
        .setIndexBackupStrategy(CollectionAdminParams.NO_INDEX_BACKUP_STRATEGY);
    backup.process(solrClient);

    Map<String,String> params = new HashMap<>();
    params.put("location", "/backup");
    params.put("solr.hdfs.home", hdfsUri + "/solr");

    HdfsBackupRepository repo = new HdfsBackupRepository();
    repo.init(new NamedList<>(params));
    BackupManager mgr = new BackupManager(repo, solrClient.getZkStateReader());

    URI baseLoc = repo.createURI("/backup");

    Properties props = mgr.readBackupProperties(baseLoc, backupName);
    assertNotNull(props);
    assertEquals(collectionName, props.getProperty(COLLECTION_NAME_PROP));
    assertEquals(backupName, props.getProperty(BACKUP_NAME_PROP));
    assertEquals(configName, props.getProperty(COLL_CONF));

    DocCollection collectionState = mgr.readCollectionState(baseLoc, backupName, collectionName);
    assertNotNull(collectionState);
    assertEquals(collectionName, collectionState.getName());

    URI configDirLoc = repo.resolve(baseLoc, backupName, ZK_STATE_DIR, CONFIG_STATE_DIR, configName);
    assertTrue(repo.exists(configDirLoc));

    Collection<String> expected = Arrays.asList(BACKUP_PROPS_FILE, ZK_STATE_DIR);
    URI backupLoc = repo.resolve(baseLoc, backupName);
    String[] dirs = repo.listAll(backupLoc);
    for (String d : dirs) {
      assertTrue(expected.contains(d));
    }
  }

}
