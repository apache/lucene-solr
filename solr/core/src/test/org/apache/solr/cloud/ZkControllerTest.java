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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.zookeeper.CreateMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@SolrTestCaseJ4.SuppressSSL
public class ZkControllerTest extends SolrTestCaseJ4 {

  private static final String COLLECTION_NAME = "collection1";

  static final int TIMEOUT = 10000;

  @BeforeClass
  public static void beforeClass() throws Exception {

  }

  @AfterClass
  public static void afterClass() throws Exception {

  }

  public void testNodeNameUrlConversion() throws Exception {

    // nodeName from parts
    assertEquals("localhost:8888_solr",
                 ZkController.generateNodeName("localhost", "8888", "solr"));
    assertEquals("localhost:8888_solr",
                 ZkController.generateNodeName("localhost", "8888", "/solr"));
    assertEquals("localhost:8888_solr",
                 ZkController.generateNodeName("localhost", "8888", "/solr/"));
    // root context
    assertEquals("localhost:8888_", 
                 ZkController.generateNodeName("localhost", "8888", ""));
    assertEquals("localhost:8888_", 
                 ZkController.generateNodeName("localhost", "8888", "/"));
    // subdir
    assertEquals("foo-bar:77_solr%2Fsub_dir",
                 ZkController.generateNodeName("foo-bar", "77", "solr/sub_dir"));
    assertEquals("foo-bar:77_solr%2Fsub_dir",
                 ZkController.generateNodeName("foo-bar", "77", "/solr/sub_dir"));
    assertEquals("foo-bar:77_solr%2Fsub_dir",
                 ZkController.generateNodeName("foo-bar", "77", "/solr/sub_dir/"));

    // setup a SolrZkClient to do some getBaseUrlForNodeName testing
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      try (SolrZkClient client = new SolrZkClient(server.getZkAddress(), TIMEOUT)) {

        ZkController.createClusterZkNodes(client);

        try (ZkStateReader zkStateReader = new ZkStateReader(client)) {
          zkStateReader.createClusterStateWatchersAndUpdate();

          // getBaseUrlForNodeName
          assertEquals("http://zzz.xxx:1234/solr",
              zkStateReader.getBaseUrlForNodeName("zzz.xxx:1234_solr"));
          assertEquals("http://xxx:99",
              zkStateReader.getBaseUrlForNodeName("xxx:99_"));
          assertEquals("http://foo-bar.baz.org:9999/some_dir",
              zkStateReader.getBaseUrlForNodeName("foo-bar.baz.org:9999_some_dir"));
          assertEquals("http://foo-bar.baz.org:9999/solr/sub_dir",
              zkStateReader.getBaseUrlForNodeName("foo-bar.baz.org:9999_solr%2Fsub_dir"));

          // generateNodeName + getBaseUrlForNodeName
          assertEquals("http://foo:9876/solr",
              zkStateReader.getBaseUrlForNodeName
                  (ZkController.generateNodeName("foo", "9876", "solr")));
          assertEquals("http://foo:9876/solr",
              zkStateReader.getBaseUrlForNodeName
                  (ZkController.generateNodeName("foo", "9876", "/solr")));
          assertEquals("http://foo:9876/solr",
              zkStateReader.getBaseUrlForNodeName
                  (ZkController.generateNodeName("foo", "9876", "/solr/")));
          assertEquals("http://foo.bar.com:9876/solr/sub_dir",
              zkStateReader.getBaseUrlForNodeName
                  (ZkController.generateNodeName("foo.bar.com", "9876", "solr/sub_dir")));
          assertEquals("http://foo.bar.com:9876/solr/sub_dir",
              zkStateReader.getBaseUrlForNodeName
                  (ZkController.generateNodeName("foo.bar.com", "9876", "/solr/sub_dir/")));
          assertEquals("http://foo-bar:9876",
              zkStateReader.getBaseUrlForNodeName
                  (ZkController.generateNodeName("foo-bar", "9876", "")));
          assertEquals("http://foo-bar:9876",
              zkStateReader.getBaseUrlForNodeName
                  (ZkController.generateNodeName("foo-bar", "9876", "/")));
          assertEquals("http://foo-bar.com:80/some_dir",
              zkStateReader.getBaseUrlForNodeName
                  (ZkController.generateNodeName("foo-bar.com", "80", "some_dir")));
          assertEquals("http://foo-bar.com:80/some_dir",
              zkStateReader.getBaseUrlForNodeName
                  (ZkController.generateNodeName("foo-bar.com", "80", "/some_dir")));

        }

        ClusterProperties cp = new ClusterProperties(client);
        cp.setClusterProperty("urlScheme", "https");
        
        //Verify the URL Scheme is taken into account

        try (ZkStateReader zkStateReader = new ZkStateReader(client)) {

          zkStateReader.createClusterStateWatchersAndUpdate();

          assertEquals("https://zzz.xxx:1234/solr",
              zkStateReader.getBaseUrlForNodeName("zzz.xxx:1234_solr"));

          assertEquals("https://foo-bar.com:80/some_dir",
              zkStateReader.getBaseUrlForNodeName
                  (ZkController.generateNodeName("foo-bar.com", "80", "/some_dir")));

        }
      }
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testReadConfigName() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    CoreContainer cc = null;

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      String actualConfigName = "firstConfig";

      zkClient.makePath(ZkConfigManager.CONFIGS_ZKNODE + "/" + actualConfigName, true);
      
      Map<String,Object> props = new HashMap<>();
      props.put("configName", actualConfigName);
      ZkNodeProps zkProps = new ZkNodeProps(props);
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/"
              + COLLECTION_NAME, Utils.toJSON(zkProps),
          CreateMode.PERSISTENT, true);

      zkClient.close();
      
      cc = getCoreContainer();

      CloudConfig cloudConfig = new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "solr").build();
      ZkController zkController = new ZkController(cc, server.getZkAddress(), TIMEOUT, cloudConfig,
          new CurrentCoreDescriptorProvider() {
            
            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });
      try {
        String configName = zkController.getZkStateReader().readConfigName(COLLECTION_NAME);
        assertEquals(configName, actualConfigName);
      } finally {
        zkController.close();
      }
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
      server.shutdown();
    }

  }

  public void testGetHostName() throws Exception {
    String zkDir = createTempDir("zkData").toFile().getAbsolutePath();
    CoreContainer cc = null;

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      cc = getCoreContainer();
      ZkController zkController = null;

      try {
        CloudConfig cloudConfig = new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "solr").build();
        zkController = new ZkController(cc, server.getZkAddress(), TIMEOUT, cloudConfig, new CurrentCoreDescriptorProvider() {

          @Override
          public List<CoreDescriptor> getCurrentDescriptors() {
            // do nothing
            return null;
          }
        });
      } catch (IllegalArgumentException e) {
        fail("ZkController did not normalize host name correctly");
      } finally {
        if (zkController != null)
          zkController.close();
      }
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
      server.shutdown();
    }
  }

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-7736")
  public void testPublishAndWaitForDownStates() throws Exception  {
    String zkDir = createTempDir("testPublishAndWaitForDownStates").toFile().getAbsolutePath();
    CoreContainer cc = null;

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      cc = getCoreContainer();
      ZkController zkController = null;

      try {
        CloudConfig cloudConfig = new CloudConfig.CloudConfigBuilder("127.0.0.1", 8983, "solr").build();
        zkController = new ZkController(cc, server.getZkAddress(), TIMEOUT, cloudConfig, new CurrentCoreDescriptorProvider() {

          @Override
          public List<CoreDescriptor> getCurrentDescriptors() {
            // do nothing
            return null;
          }
        });

        HashMap<String, DocCollection> collectionStates = new HashMap<>();
        HashMap<String, Replica> replicas = new HashMap<>();
        // add two replicas with the same core name but one of them should be on a different node
        // than this ZkController instance
        for (int i=1; i<=2; i++)  {
          Replica r = new Replica("core_node" + i,
              map(ZkStateReader.STATE_PROP, i == 1 ? "active" : "down",
              ZkStateReader.NODE_NAME_PROP, i == 1 ? "127.0.0.1:8983_solr" : "non_existent_host",
              ZkStateReader.CORE_NAME_PROP, "collection1"));
          replicas.put("core_node" + i, r);
        }
        HashMap<String, Object> sliceProps = new HashMap<>();
        sliceProps.put("state", Slice.State.ACTIVE.toString());
        Slice slice = new Slice("shard1", replicas, sliceProps);
        DocCollection c = new DocCollection("testPublishAndWaitForDownStates", map("shard1", slice), Collections.emptyMap(), DocRouter.DEFAULT);
        ClusterState state = new ClusterState(0, Collections.emptySet(), map("testPublishAndWaitForDownStates", c));
        byte[] bytes = Utils.toJSON(state);
        zkController.getZkClient().makePath(ZkStateReader.getCollectionPath("testPublishAndWaitForDownStates"), bytes, CreateMode.PERSISTENT, true);

        zkController.getZkStateReader().forceUpdateCollection("testPublishAndWaitForDownStates");
        assertTrue(zkController.getZkStateReader().getClusterState().hasCollection("testPublishAndWaitForDownStates"));
        assertNotNull(zkController.getZkStateReader().getClusterState().getCollection("testPublishAndWaitForDownStates"));

        long now = System.nanoTime();
        long timeout = now + TimeUnit.NANOSECONDS.convert(ZkController.WAIT_DOWN_STATES_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        zkController.publishAndWaitForDownStates();
        assertTrue("The ZkController.publishAndWaitForDownStates should have timed out but it didn't", System.nanoTime() >= timeout);
      } finally {
        if (zkController != null)
          zkController.close();
      }
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
      server.shutdown();
    }
  }

  private CoreContainer getCoreContainer() {
    return new MockCoreContainer();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

  private static class MockCoreContainer extends CoreContainer {
    UpdateShardHandler updateShardHandler = new UpdateShardHandler(UpdateShardHandlerConfig.DEFAULT);

    public MockCoreContainer() {
      super(SolrXmlConfig.fromString(null, "<solr/>"));
      this.shardHandlerFactory = new HttpShardHandlerFactory();
      this.coreAdminHandler = new CoreAdminHandler();
    }

    @Override
    public void load() {
    }

    @Override
    public UpdateShardHandler getUpdateShardHandler() {
      return updateShardHandler;
    }

    @Override
    public void shutdown() {
      updateShardHandler.close();
      super.shutdown();
    }
  }    
}
