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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest.Create;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.ConfigSetProperties;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.txn.TxnHeader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.solr.common.cloud.ZkConfigManager.CONFIGS_ZKNODE;

/**
 * Test the ConfigSets API under ZK failure.  In particular,
 * if create fails, ensure proper cleanup occurs so we aren't
 * left with a partially created ConfigSet.
 */
public class TestConfigSetsAPIZkFailure extends SolrTestCaseJ4 {
  private MiniSolrCloudCluster solrCluster;
  private ZkTestServer zkTestServer;
  private static final String BASE_CONFIGSET_NAME = "baseConfigSet1";
  private static final String CONFIGSET_NAME = "configSet1";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    final Path testDir = createTempDir();
    String zkDir = testDir.resolve("zookeeper/server1/data").toString();
    zkTestServer = new ZkTestServer(zkDir);
    zkTestServer.run();
    zkTestServer.setZKDatabase(new FailureDuringCopyZKDatabase(zkTestServer.getZKDatabase(), zkTestServer));
    solrCluster = new MiniSolrCloudCluster(1, testDir,
        MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML, buildJettyConfig("/solr"), zkTestServer);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    solrCluster.shutdown();
    zkTestServer.shutdown();
    super.tearDown();
  }

  @Test
  public void testCreateZkFailure() throws Exception {
    final String baseUrl = solrCluster.getJettySolrRunners().get(0).getBaseUrl().toString();
    final SolrClient solrClient = getHttpSolrClient(baseUrl);

    final Map<String, String> oldProps = ImmutableMap.of("immutable", "true");
    setupBaseConfigSet(BASE_CONFIGSET_NAME, oldProps);

    SolrZkClient zkClient = new SolrZkClient(solrCluster.getZkServer().getZkAddress(),
        AbstractZkTestCase.TIMEOUT, AbstractZkTestCase.TIMEOUT, null);
    try {
      ZkConfigManager configManager = new ZkConfigManager(zkClient);
      assertFalse(configManager.configExists(CONFIGSET_NAME));

      Create create = new Create();
      create.setBaseConfigSetName(BASE_CONFIGSET_NAME).setConfigSetName(CONFIGSET_NAME);
      try {
        ConfigSetAdminResponse response = create.process(solrClient);
        Assert.fail("Expected solr exception");
      } catch (RemoteSolrException se) {
        // partial creation should have been cleaned up
        assertFalse(configManager.configExists(CONFIGSET_NAME));
        assertEquals(SolrException.ErrorCode.SERVER_ERROR.code, se.code());
      }
    } finally {
      zkClient.close();
    }

    solrClient.close();
  }

  private void setupBaseConfigSet(String baseConfigSetName, Map<String, String> oldProps) throws Exception {
    final File configDir = getFile("solr").toPath().resolve("configsets/configset-2/conf").toFile();
    final File tmpConfigDir = createTempDir().toFile();
    tmpConfigDir.deleteOnExit();
    FileUtils.copyDirectory(configDir, tmpConfigDir);
    if (oldProps != null) {
      FileUtils.write(new File(tmpConfigDir, ConfigSetProperties.DEFAULT_FILENAME),
          getConfigSetProps(oldProps), StandardCharsets.UTF_8);
    }
    solrCluster.uploadConfigSet(tmpConfigDir.toPath(), baseConfigSetName);
  }

  private StringBuilder getConfigSetProps(Map<String, String> map) {
    return new StringBuilder(new String(Utils.toJSON(map), StandardCharsets.UTF_8));
  }

  private static class FailureDuringCopyZKDatabase extends ForwardingZKDatabase {
    private final ZkTestServer zkTestServer;

    public FailureDuringCopyZKDatabase(ZKDatabase zkdb, ZkTestServer zkTestServer) {
      super(zkdb);
      this.zkTestServer = zkTestServer;
    }

    @Override
    public byte[] getData(String path, Stat stat, Watcher watcher) throws KeeperException.NoNodeException {
      // we know we are doing a copy when we are getting data from the base config set and
      // the new config set (partially) exists
      String zkAddress = zkTestServer.getZkAddress();
      String chroot = zkAddress.substring(zkAddress.lastIndexOf("/"));
      if (path.startsWith(chroot + CONFIGS_ZKNODE + "/" + BASE_CONFIGSET_NAME)
          && !path.contains(ConfigSetProperties.DEFAULT_FILENAME)) {
        List<String> children = null;
        try {
          children = getChildren(chroot + CONFIGS_ZKNODE + "/" + CONFIGSET_NAME, null, null);
        } catch (KeeperException.NoNodeException e) {}
        if (children != null && children.size() > 0) {
          throw new RuntimeException("sample zookeeper error");
        }
      }
      return super.getData(path, stat, watcher);
    }
  }

  private static class ForwardingZKDatabase extends ZKDatabase {
    private ZKDatabase zkdb;

    public ForwardingZKDatabase(ZKDatabase zkdb) {
      super(null);
      this.zkdb = zkdb;
    }

    @Override
    public boolean isInitialized() {
      return zkdb.isInitialized();
    }

    @Override
    public void clear() {
      zkdb.clear();
    }

    @Override
    public DataTree getDataTree() {
      return zkdb.getDataTree();
    }

    @Override
    public long getmaxCommittedLog() {
      return zkdb.getmaxCommittedLog();
    }

    @Override
    public long getminCommittedLog() {
      return zkdb.getminCommittedLog();
    }

    @Override
    public ReentrantReadWriteLock getLogLock() {
      return zkdb.getLogLock();
    }

    @Override
    public synchronized LinkedList<Proposal> getCommittedLog() {
      return zkdb.getCommittedLog();
    }

    @Override
    public long getDataTreeLastProcessedZxid() {
      return zkdb.getDataTreeLastProcessedZxid();
    }

    @Override
    public void setDataTreeInit(boolean b) {
      zkdb.setDataTreeInit(b);
    }

    @Override
    public Collection<Long> getSessions() {
      return zkdb.getSessions();
    }

    @Override
    public ConcurrentHashMap<Long, Integer> getSessionWithTimeOuts() {
      return zkdb.getSessionWithTimeOuts();
    }

    @Override
    public long loadDataBase() throws IOException {
      return zkdb.loadDataBase();
    }

    @Override
    public void addCommittedProposal(Request request) {
      zkdb.addCommittedProposal(request);
    }

    @Override
    public void removeCnxn(ServerCnxn cnxn) {
      zkdb.removeCnxn(cnxn);
    }

    @Override
    public void killSession(long sessionId, long zxid) {
      zkdb.killSession(sessionId, zxid);
    }

    @Override
    public void dumpEphemerals(PrintWriter pwriter) {
      zkdb.dumpEphemerals(pwriter);
    }

    @Override
    public int getNodeCount() {
      return zkdb.getNodeCount();
    }

    @Override
    public HashSet<String> getEphemerals(long sessionId) {
      return zkdb.getEphemerals(sessionId);
    }

    @Override
    public void setlastProcessedZxid(long zxid) {
      zkdb.setlastProcessedZxid(zxid);
    }

    @Override
    public ProcessTxnResult processTxn(TxnHeader hdr, Record txn) {
      return zkdb.processTxn(hdr, txn);
    }

    @Override
    public Stat statNode(String path, ServerCnxn serverCnxn) throws KeeperException.NoNodeException {
      return zkdb.statNode(path, serverCnxn);
    }

    @Override
    public DataNode getNode(String path) {
      return zkdb.getNode(path);
    }

    @Override
    public List<ACL> convertLong(Long aclL) {
      return zkdb.convertLong(aclL);
    }

    @Override
    public byte[] getData(String path, Stat stat, Watcher watcher)
    throws KeeperException.NoNodeException {
      return zkdb.getData(path, stat, watcher);
    }

    @Override
    public void setWatches(long relativeZxid, List<String> dataWatches,
            List<String> existWatches, List<String> childWatches, Watcher watcher) {
      zkdb.setWatches(relativeZxid, dataWatches, existWatches, childWatches, watcher);
    }

    @Override
    public List<ACL> getACL(String path, Stat stat) throws NoNodeException {
      return zkdb.getACL(path, stat);
    }

    @Override
    public List<String> getChildren(String path, Stat stat, Watcher watcher)
    throws KeeperException.NoNodeException {
      return zkdb.getChildren(path, stat, watcher);
    }

    @Override
    public boolean isSpecialPath(String path) {
      return zkdb.isSpecialPath(path);
    }

    @Override
    public int getAclSize() {
      return zkdb.getAclSize();
    }

    @Override
    public boolean truncateLog(long zxid) throws IOException {
      return zkdb.truncateLog(zxid);
    }

    @Override
    public void deserializeSnapshot(InputArchive ia) throws IOException {
      zkdb.deserializeSnapshot(ia);
    }

    @Override
    public void serializeSnapshot(OutputArchive oa) throws IOException,
    InterruptedException {
      zkdb.serializeSnapshot(oa);
    }

    @Override
    public boolean append(Request si) throws IOException {
      return zkdb.append(si);
    }

    @Override
    public void rollLog() throws IOException {
      zkdb.rollLog();
    }

    @Override
    public void commit() throws IOException {
      zkdb.commit();
    }

    @Override
    public void close() throws IOException {
      zkdb.close();
    }
  }
}
