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

package org.apache.solr.cloud.autoscaling.sim;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.NotEmptyException;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.client.solrj.impl.ZkDistribStateManager;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test compares a ZK-based {@link DistribStateManager} to the simulated one.
 */
public class TestSimDistribStateManager extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private DistribStateManager stateManager;
  private ZkTestServer zkTestServer;
  private SolrZkClient solrZkClient;
  private boolean simulated;
  private SimDistribStateManager.Node root;

  @Before
  public void setup() throws Exception {
    simulated = random().nextBoolean();
    if (simulated) {
      root = SimDistribStateManager.createNewRootNode();
    } else {
      zkTestServer = new ZkTestServer(createTempDir("zkDir"));
      zkTestServer.run();
    }
    reInit();
  }

  private void reInit() throws Exception {
    if (stateManager != null) {
      stateManager.close();
    }
    if (simulated) {
      stateManager = new SimDistribStateManager(root);
    } else {
      if (solrZkClient != null) {
        solrZkClient.close();
      }
      solrZkClient = new SolrZkClient(zkTestServer.getZkHost(), 30000);
      stateManager = new ZkDistribStateManager(solrZkClient);
    }
    if (log.isInfoEnabled()) {
      log.info("Using {}", stateManager.getClass().getName());
    }
  }

  private DistribStateManager createDistribStateManager() {
    if (simulated) {
      return new SimDistribStateManager(root);
    } else {
      SolrZkClient cli = new SolrZkClient(zkTestServer.getZkHost(), 30000);
      return new ZkDistribStateManager(cli);
    }
  }

  private void destroyDistribStateManager(DistribStateManager mgr) throws Exception {
    mgr.close();
    if (mgr instanceof ZkDistribStateManager) {
      ((ZkDistribStateManager)mgr).getZkClient().close();
    }
  }

  @After
  public void teardown() throws Exception {
    if (solrZkClient != null) {
      solrZkClient.close();
      solrZkClient = null;
    }
    if (zkTestServer != null) {
      zkTestServer.shutdown();
      zkTestServer = null;
    }
    if (stateManager != null) {
      stateManager.close();
    }
    stateManager = null;
  }

  @Test
  public void testHasData() throws Exception {
    assertFalse(stateManager.hasData("/hasData/foo"));
    assertFalse(stateManager.hasData("/hasData/bar"));
    try {
      stateManager.createData("/hasData/foo", new byte[0], CreateMode.PERSISTENT);
      fail("should have failed (parent /hasData doesn't exist)");
    } catch (NoSuchElementException e) {
      // expected
    }
    stateManager.makePath("/hasData");
    stateManager.createData("/hasData/foo", new byte[0], CreateMode.PERSISTENT);
    stateManager.createData("/hasData/bar", new byte[0], CreateMode.PERSISTENT);
    assertTrue(stateManager.hasData("/hasData/foo"));
    assertTrue(stateManager.hasData("/hasData/bar"));
  }

  @Test
  public void testRemoveData() throws Exception {
    assertFalse(stateManager.hasData("/removeData/foo"));
    assertFalse(stateManager.hasData("/removeData/foo/bar"));
    assertFalse(stateManager.hasData("/removeData/baz"));
    assertFalse(stateManager.hasData("/removeData/baz/1/2/3"));
    stateManager.makePath("/removeData/foo/bar");
    stateManager.makePath("/removeData/baz/1/2/3");
    assertTrue(stateManager.hasData("/removeData/foo"));
    assertTrue(stateManager.hasData("/removeData/foo/bar"));
    assertTrue(stateManager.hasData("/removeData/baz/1/2/3"));
    try {
      stateManager.removeData("/removeData/foo", -1);
      fail("should have failed (node has children)");
    } catch (NotEmptyException e) {
      // expected
    }
    stateManager.removeData("/removeData/foo/bar", -1);
    stateManager.removeData("/removeData/foo", -1);
    // test recursive listing and removal
    stateManager.removeRecursively("/removeData/baz/1", false, false);
    assertFalse(stateManager.hasData("/removeData/baz/1/2"));
    assertTrue(stateManager.hasData("/removeData/baz/1"));
    // should silently ignore
    stateManager.removeRecursively("/removeData/baz/1/2", true, true);
    stateManager.removeRecursively("/removeData/baz/1", false, true);
    assertFalse(stateManager.hasData("/removeData/baz/1"));
    try {
      stateManager.removeRecursively("/removeData/baz/1", false, true);
      fail("should throw exception - missing path");
    } catch (NoSuchElementException e) {
      // expected
    }
    stateManager.removeRecursively("/removeData", true, true);
    assertFalse(stateManager.hasData("/removeData"));
  }

  @Test
  public void testListData() throws Exception {
    assertFalse(stateManager.hasData("/listData/foo"));
    assertFalse(stateManager.hasData("/listData/foo/bar"));
    try {
      stateManager.createData("/listData/foo/bar", new byte[0], CreateMode.PERSISTENT);
      fail("should not succeed");
    } catch (NoSuchElementException e) {
      // expected
    }
    try {
      stateManager.listData("/listData/foo");
      fail("should not succeed");
    } catch (NoSuchElementException e) {
      // expected
    }
    stateManager.makePath("/listData");
    List<String> kids = stateManager.listData("/listData");
    assertEquals(0, kids.size());
    stateManager.makePath("/listData/foo");
    kids = stateManager.listData("/listData");
    assertEquals(1, kids.size());
    assertEquals("foo", kids.get(0));
    stateManager.createData("/listData/foo/bar", new byte[0], CreateMode.PERSISTENT);
    stateManager.createData("/listData/foo/baz", new byte[0], CreateMode.PERSISTENT);
    kids = stateManager.listData("/listData/foo");
    assertEquals(2, kids.size());
    assertTrue(kids.contains("bar"));
    assertTrue(kids.contains("baz"));
    try {
      stateManager.createData("/listData/foo/bar", new byte[0], CreateMode.PERSISTENT);
      fail("should not succeed");
    } catch (AlreadyExistsException e) {
      // expected
    }
  }

  static final byte[] firstData = new byte[] {
      (byte)0xca, (byte)0xfe, (byte)0xba, (byte)0xbe
  };

  static final byte[] secondData = new byte[] {
      (byte)0xbe, (byte)0xba, (byte)0xfe, (byte)0xca
  };

  @Test
  public void testCreateMode() throws Exception {
    stateManager.makePath("/createMode");
    stateManager.createData("/createMode/persistent", firstData, CreateMode.PERSISTENT);
    stateManager.createData("/createMode/persistent_seq", firstData, CreateMode.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      stateManager.createData("/createMode/persistent_seq/data", firstData, CreateMode.PERSISTENT_SEQUENTIAL);
    }
    // check what happens with gaps
    stateManager.createData("/createMode/persistent_seq/data", firstData, CreateMode.PERSISTENT_SEQUENTIAL);
    stateManager.removeData("/createMode/persistent_seq/data" + String.format(Locale.ROOT, "%010d", 10), -1);
    stateManager.createData("/createMode/persistent_seq/data", firstData, CreateMode.PERSISTENT_SEQUENTIAL);

    stateManager.createData("/createMode/ephemeral", firstData, CreateMode.EPHEMERAL);
    stateManager.createData("/createMode/ephemeral_seq", firstData, CreateMode.PERSISTENT);
    for (int i = 0; i < 10; i++) {
      stateManager.createData("/createMode/ephemeral_seq/data", firstData, CreateMode.EPHEMERAL_SEQUENTIAL);
    }
    assertTrue(stateManager.hasData("/createMode"));
    assertTrue(stateManager.hasData("/createMode/persistent"));
    assertTrue(stateManager.hasData("/createMode/ephemeral"));
    List<String> kids = stateManager.listData("/createMode/persistent_seq");
    assertEquals(11, kids.size());
    kids = stateManager.listData("/createMode/ephemeral_seq");
    assertEquals(10, kids.size());
    for (int i = 0; i < 10; i++) {
      assertTrue(stateManager.hasData("/createMode/persistent_seq/data" + String.format(Locale.ROOT, "%010d", i)));
    }
    assertFalse(stateManager.hasData("/createMode/persistent_seq/data" + String.format(Locale.ROOT, "%010d", 10)));
    assertTrue(stateManager.hasData("/createMode/persistent_seq/data" + String.format(Locale.ROOT, "%010d", 11)));

    for (int i = 0; i < 10; i++) {
      assertTrue(stateManager.hasData("/createMode/ephemeral_seq/data" + String.format(Locale.ROOT, "%010d", i)));
    }
    // check that ephemeral nodes disappear on disconnect
    reInit();
    assertTrue(stateManager.hasData("/createMode/persistent"));
    for (int i = 0; i < 10; i++) {
      assertTrue(stateManager.hasData("/createMode/persistent_seq/data" + String.format(Locale.ROOT, "%010d", i)));
    }
    assertTrue(stateManager.hasData("/createMode/persistent_seq/data" + String.format(Locale.ROOT, "%010d", 11)));

    assertFalse(stateManager.hasData("/createMode/ephemeral"));
    assertTrue(stateManager.hasData("/createMode/ephemeral_seq"));
    kids = stateManager.listData("/createMode/ephemeral_seq");
    assertEquals(0, kids.size());
  }

  @Test
  public void testCanCreateNodesWithDataAtTopLevel() throws Exception {
    final String path = stateManager.createData("/topLevelNodeWithData", new String("helloworld").getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT);
    assertEquals("/topLevelNodeWithData", path);
  }

  static class OnceWatcher implements Watcher {
    CountDownLatch triggered = new CountDownLatch(1);
    WatchedEvent event;

    @Override
    public void process(WatchedEvent event) {
      if (triggered.getCount() == 0) {
        fail("Watch was already triggered once!");
      }
      triggered.countDown();
      this.event = event;
    }
  }

  @Test
  public void testGetSetRemoveData() throws Exception {
    stateManager.makePath("/getData");
    stateManager.createData("/getData/persistentData", firstData, CreateMode.PERSISTENT);
    OnceWatcher nodeWatcher = new OnceWatcher();
    VersionedData vd = stateManager.getData("/getData/persistentData", nodeWatcher);
    assertNotNull(vd);
    assertEquals(0, vd.getVersion());
    assertTrue(Arrays.equals(firstData, vd.getData()));

    // update data, test versioning
    try {
      stateManager.setData("/getData/persistentData", secondData, 1);
      fail("should have failed");
    } catch (BadVersionException e) {
      // expected
    }
    // watch should not have fired
    assertEquals(1, nodeWatcher.triggered.getCount());

    stateManager.setData("/getData/persistentData", secondData, 0);
    if (!nodeWatcher.triggered.await(5, TimeUnit.SECONDS)) {
      fail("Node watch should have fired!");
    }
    // watch should not fire now because it needs to be reset
    stateManager.setData("/getData/persistentData", secondData, -1);

    // create ephemeral node using another ZK connection
    DistribStateManager ephemeralMgr = createDistribStateManager();
    ephemeralMgr.createData("/getData/ephemeralData", firstData, CreateMode.EPHEMERAL);

    nodeWatcher = new OnceWatcher();
    vd = stateManager.getData("/getData/ephemeralData", nodeWatcher);
    destroyDistribStateManager(ephemeralMgr);
    if (!nodeWatcher.triggered.await(5, TimeUnit.SECONDS)) {
      fail("Node watch should have fired!");
    }
    assertTrue(stateManager.hasData("/getData/persistentData"));
    assertFalse(stateManager.hasData("/getData/ephemeralData"));

    nodeWatcher = new OnceWatcher();
    vd = stateManager.getData("/getData/persistentData", nodeWatcher);
    // try wrong version
    try {
      stateManager.removeData("/getData/persistentData", vd.getVersion() - 1);
      fail("should have failed");
    } catch (BadVersionException e) {
      // expected
    }
    // watch should not have fired
    assertEquals(1, nodeWatcher.triggered.getCount());

    stateManager.removeData("/getData/persistentData", vd.getVersion());
    if (!nodeWatcher.triggered.await(5, TimeUnit.SECONDS)) {
      fail("Node watch should have fired!");
    }
  }

  @Test
  public void testNewlyCreatedPathsStartWithVersionZero() throws Exception {
    stateManager.makePath("/createdWithoutData");
    VersionedData vd = stateManager.getData("/createdWithoutData", null);
    assertEquals(0, vd.getVersion());

    stateManager.createData("/createdWithData", new String("helloworld").getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT);
    vd = stateManager.getData("/createdWithData");
    assertEquals(0, vd.getVersion());
  }

  @Test
  public void testModifiedDataNodesGetUpdatedVersion() throws Exception {
    stateManager.createData("/createdWithData", new String("foo").getBytes(StandardCharsets.UTF_8), CreateMode.PERSISTENT);
    VersionedData vd = stateManager.getData("/createdWithData");
    assertEquals(0, vd.getVersion());

    stateManager.setData("/createdWithData", new String("bar").getBytes(StandardCharsets.UTF_8), 0);
    vd = stateManager.getData("/createdWithData");
    assertEquals(1, vd.getVersion());
  }

  // This is a little counterintuitive, so probably worth its own testcase so we don't break it accidentally.
  @Test
  public void testHasDataReturnsTrueForExistingButEmptyNodes() throws Exception {
    stateManager.makePath("/nodeWithoutData");
    assertTrue(stateManager.hasData("/nodeWithoutData"));
  }

  @Test
  public void testMulti() throws Exception {

  }

}
