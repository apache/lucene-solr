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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.Diagnostics;
import org.apache.solr.core.MockDirectoryFactory;
import org.apache.solr.util.SolrCLI;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public abstract class AbstractDistribZkTestBase extends BaseDistributedSearchTestCase {

  private static final String REMOVE_VERSION_FIELD = "remove.version.field";
  private static final String ENABLE_UPDATE_LOG = "enable.update.log";
  private static final String ZK_HOST = "zkHost";
  private static final String ZOOKEEPER_FORCE_SYNC = "zookeeper.forceSync";
  protected static final String DEFAULT_COLLECTION = "collection1";
  protected volatile ZkTestServer zkServer;
  private AtomicInteger homeCount = new AtomicInteger();

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void beforeThisClass() throws Exception {
    // Only For Manual Testing: this will force an fs based dir factory
    //useFactory(null);
  }

  @Override
  public void distribSetUp() throws Exception {
    super.distribSetUp();

    Path zkDir = testDir.toPath().resolve("zookeeper/server1/data");
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();

    System.setProperty(ZK_HOST, zkServer.getZkAddress());
    System.setProperty(ENABLE_UPDATE_LOG, "true");
    System.setProperty(REMOVE_VERSION_FIELD, "true");
    System.setProperty(ZOOKEEPER_FORCE_SYNC, "false");
    System.setProperty(MockDirectoryFactory.SOLR_TESTS_ALLOW_READING_FILES_STILL_OPEN_FOR_WRITE, "true");

    String schema = getCloudSchemaFile();
    if (schema == null) schema = "schema.xml";
    zkServer.buildZooKeeper(getCloudSolrConfig(), schema);

    // set some system properties for use by tests
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
  }

  protected String getCloudSolrConfig() {
    return "solrconfig-tlog.xml";
  }

  protected String getCloudSchemaFile() {
    return getSchemaFile();
  }

  @Override
  protected void createServers(int numShards) throws Exception {
    // give everyone there own solrhome
    File controlHome = new File(new File(getSolrHome()).getParentFile(), "control" + homeCount.incrementAndGet());
    FileUtils.copyDirectory(new File(getSolrHome()), controlHome);
    setupJettySolrHome(controlHome);

    controlJetty = createJetty(controlHome, null);      // let the shardId default to shard1
    controlJetty.start();
    controlClient = createNewSolrClient(controlJetty.getLocalPort());

    assertTrue(CollectionAdminRequest
        .createCollection("control_collection", 1, 1)
        .setCreateNodeSet(controlJetty.getNodeName())
        .process(controlClient).isSuccess());

    ZkStateReader zkStateReader = jettys.get(0).getCoreContainer().getZkController()
        .getZkStateReader();

    waitForRecoveriesToFinish("control_collection", zkStateReader, false, true, 15);

    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= numShards; i++) {
      if (sb.length() > 0) sb.append(',');
      // give everyone there own solrhome
      File jettyHome = new File(new File(getSolrHome()).getParentFile(), "jetty" + homeCount.incrementAndGet());
      setupJettySolrHome(jettyHome);
      JettySolrRunner j = createJetty(jettyHome, null, "shard" + (i + 2));
      j.start();
      jettys.add(j);
      clients.add(createNewSolrClient(j.getLocalPort()));
      sb.append(buildUrl(j.getLocalPort()));
    }

    shards = sb.toString();

  }

  protected void waitForRecoveriesToFinish(String collection, ZkStateReader zkStateReader, boolean verbose)
      throws Exception {
    waitForRecoveriesToFinish(collection, zkStateReader, verbose, true);
  }

  protected void waitForRecoveriesToFinish(String collection, ZkStateReader zkStateReader, boolean verbose, boolean failOnTimeout)
      throws Exception {
    waitForRecoveriesToFinish(collection, zkStateReader, verbose, failOnTimeout, 330);
  }

  public static void waitForRecoveriesToFinish(String collection,
      ZkStateReader zkStateReader, boolean verbose, boolean failOnTimeout, long timeoutSeconds)
      throws Exception {
    log.info("Wait for recoveries to finish - collection: " + collection + " failOnTimeout:" + failOnTimeout + " timeout (sec):" + timeoutSeconds);
    try {
      zkStateReader.waitForState(collection, timeoutSeconds, TimeUnit.SECONDS, (liveNodes, docCollection) -> {
        if (docCollection == null)
          return false;
        boolean sawLiveRecovering = false;

        assertNotNull("Could not find collection:" + collection, docCollection);
        Map<String,Slice> slices = docCollection.getSlicesMap();
        assertNotNull("Could not find collection:" + collection, slices);
        for (Map.Entry<String,Slice> entry : slices.entrySet()) {
          Slice slice = entry.getValue();
          if (slice.getState() == Slice.State.CONSTRUCTION) { // similar to replica recovering; pretend its the same
                                                              // thing
            if (verbose) System.out.println("Found a slice in construction state; will wait.");
            sawLiveRecovering = true;
          }
          Map<String,Replica> shards = slice.getReplicasMap();
          for (Map.Entry<String,Replica> shard : shards.entrySet()) {
            if (verbose) System.out.println("replica:" + shard.getValue().getName() + " rstate:"
                + shard.getValue().getStr(ZkStateReader.STATE_PROP)
                + " live:"
                + liveNodes.contains(shard.getValue().getNodeName()));
            final Replica.State state = shard.getValue().getState();
            if ((state == Replica.State.RECOVERING || state == Replica.State.DOWN
                || state == Replica.State.RECOVERY_FAILED)
                && liveNodes.contains(shard.getValue().getStr(ZkStateReader.NODE_NAME_PROP))) {
              return false;
            }
          }
        }
        if (!sawLiveRecovering) {
          if (!sawLiveRecovering) {
            if (verbose) System.out.println("no one is recoverying");
          } else {
            if (verbose) System.out.println("Gave up waiting for recovery to finish..");
            return false;
          }
          return true;
        } else {
          return false;
        }
      });
    } catch (TimeoutException | InterruptedException e) {
      Diagnostics.logThreadDumps("Gave up waiting for recovery to finish.  THREAD DUMP:");
      zkStateReader.getZkClient().printLayoutToStream(System.out);
      fail("There are still nodes recoverying - waited for " + timeoutSeconds + " seconds");
    }

    log.info("Recoveries finished - collection: " + collection);
  }


  public static void waitForCollectionToDisappear(String collection,
      ZkStateReader zkStateReader, boolean verbose, boolean failOnTimeout, int timeoutSeconds)
      throws Exception {
    log.info("Wait for collection to disappear - collection: " + collection + " failOnTimeout:" + failOnTimeout + " timeout (sec):" + timeoutSeconds);

    zkStateReader.waitForState(collection, timeoutSeconds, TimeUnit.SECONDS, (docCollection) -> {
      if (docCollection == null)
        return true;
      return false;
    });
    log.info("Collection has disappeared - collection: " + collection);
  }

  static void waitForNewLeader(CloudSolrClient cloudClient, String shardName, Replica oldLeader, TimeOut timeOut)
      throws Exception {
    log.info("Will wait for a node to become leader for {} secs", timeOut.timeLeft(SECONDS));
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    zkStateReader.forceUpdateCollection(DEFAULT_COLLECTION);

    for (; ; ) {
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection coll = clusterState.getCollection("collection1");
      Slice slice = coll.getSlice(shardName);
      if (slice.getLeader() != null && !slice.getLeader().equals(oldLeader) && slice.getLeader().getState() == Replica.State.ACTIVE) {
        log.info("Old leader {}, new leader {}. New leader got elected in {} ms", oldLeader, slice.getLeader(),timeOut.timeElapsed(MILLISECONDS) );
        break;
      }

      if (timeOut.hasTimedOut()) {
        Diagnostics.logThreadDumps("Could not find new leader in specified timeout");
        zkStateReader.getZkClient().printLayoutToStream(System.out);
        fail("Could not find new leader even after waiting for " + timeOut.timeElapsed(MILLISECONDS) + "ms");
      }

      Thread.sleep(100);
    }

    zkStateReader.waitForState("collection1", timeOut.timeLeft(SECONDS), TimeUnit.SECONDS, (docCollection) -> {
      if (docCollection == null)
        return false;

      Slice slice = docCollection.getSlice(shardName);
      if (slice != null && slice.getLeader() != null && !slice.getLeader().equals(oldLeader) && slice.getLeader().getState() == Replica.State.ACTIVE) {
        log.info("Old leader {}, new leader {}. New leader got elected in {} ms", oldLeader, slice.getLeader(), timeOut.timeElapsed(MILLISECONDS) );
        return true;
      }
      return false;
    });
  }

  public static void verifyReplicaStatus(ZkStateReader reader, String collection, String shard, String coreNodeName,
      Replica.State expectedState) throws InterruptedException, TimeoutException {
    log.info("verifyReplicaStatus ({}) shard={} coreNodeName={}", collection, shard, coreNodeName);
    reader.waitForState(collection, 15000, TimeUnit.MILLISECONDS,
        (collectionState) -> collectionState != null && collectionState.getSlice(shard) != null
            && collectionState.getSlice(shard).getReplicasMap().get(coreNodeName) != null
            && collectionState.getSlice(shard).getReplicasMap().get(coreNodeName).getState() == expectedState);
  }

  protected static void assertAllActive(String collection, ZkStateReader zkStateReader)
      throws KeeperException, InterruptedException {

      zkStateReader.forceUpdateCollection(collection);
      ClusterState clusterState = zkStateReader.getClusterState();
      final DocCollection docCollection = clusterState.getCollectionOrNull(collection);
      if (docCollection == null || docCollection.getSlices() == null) {
        throw new IllegalArgumentException("Cannot find collection:" + collection);
      }

      Map<String,Slice> slices = docCollection.getSlicesMap();
      for (Map.Entry<String,Slice> entry : slices.entrySet()) {
        Slice slice = entry.getValue();
        if (slice.getState() != Slice.State.ACTIVE) {
          fail("Not all shards are ACTIVE - found a shard " + slice.getName() + " that is: " + slice.getState());
        }
        Map<String,Replica> shards = slice.getReplicasMap();
        for (Map.Entry<String,Replica> shard : shards.entrySet()) {
          Replica replica = shard.getValue();
          if (replica.getState() != Replica.State.ACTIVE) {
            fail("Not all replicas are ACTIVE - found a replica " + replica.getName() + " that is: " + replica.getState());
          }
        }
      }
  }

  @Override
  public void distribTearDown() throws Exception {
    resetExceptionIgnores();

    try {
      zkServer.shutdown();
    } catch (Exception e) {
      throw new RuntimeException("Exception shutting down Zk Test Server.", e);
    } finally {
      try {
        super.distribTearDown();
      } finally {
        System.clearProperty(ZK_HOST);
        System.clearProperty("collection");
        System.clearProperty(ENABLE_UPDATE_LOG);
        System.clearProperty(REMOVE_VERSION_FIELD);
        System.clearProperty("solr.directoryFactory");
        System.clearProperty("solr.test.sys.prop1");
        System.clearProperty("solr.test.sys.prop2");
        System.clearProperty(ZOOKEEPER_FORCE_SYNC);
        System.clearProperty(MockDirectoryFactory.SOLR_TESTS_ALLOW_READING_FILES_STILL_OPEN_FOR_WRITE);

      }

    }
  }

  protected void printLayout() throws Exception {
    SolrZkClient zkClient = new SolrZkClient(zkServer.getZkHost(), AbstractZkTestCase.TIMEOUT);
    zkClient.printLayoutToStream(System.out);
    zkClient.close();
  }

  protected void restartZk(int pauseMillis) throws Exception {
    log.info("Restarting ZK with a pause of {}ms in between", pauseMillis);
    zkServer.shutdown();
    // disconnect enough to test stalling, if things stall, then clientSoTimeout w""ill be hit
    Thread.sleep(pauseMillis);
    zkServer = new ZkTestServer(zkServer.getZkDir(), zkServer.getPort());
    zkServer.run(false);
  }


  // Copy a configset up from some path on the local  machine to ZK.
  // Example usage:
  //
  // copyConfigUp(TEST_PATH().resolve("configsets"), "cloud-minimal", "configset-name", zk_address);

  static protected void copyConfigUp(Path configSetDir, String srcConfigSet, String dstConfigName, String zkAddr) throws Exception {
    String[] args = new String[]{
        "-confname", dstConfigName,
        "-confdir", srcConfigSet,
        "-zkHost", zkAddr,
        "-configsetsDir", configSetDir.toAbsolutePath().toString(),
    };

    SolrCLI.ConfigSetUploadTool tool = new SolrCLI.ConfigSetUploadTool();

    int res = tool.runTool(SolrCLI.processCommandLineArgs(SolrCLI.joinCommonAndToolOptions(tool.getOptions()), args));
    assertEquals("Tool should have returned 0 for success, returned: " + res, res, 0);

  }

}
