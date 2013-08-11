package org.apache.solr.cloud;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.util.ExternalPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slow
public class ZkControllerTest extends SolrTestCaseJ4 {

  private static final String COLLECTION_NAME = "collection1";

  static final int TIMEOUT = 10000;

  private static final boolean DEBUG = false;
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
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
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      try {

        // getBaseUrlForNodeName
        assertEquals("http://zzz.xxx:1234/solr",
                     zkClient.getBaseUrlForNodeName("zzz.xxx:1234_solr"));
        assertEquals("http://xxx:99",
                     zkClient.getBaseUrlForNodeName("xxx:99_"));
        assertEquals("http://foo-bar.baz.org:9999/some_dir",
                     zkClient.getBaseUrlForNodeName("foo-bar.baz.org:9999_some_dir"));
        assertEquals("http://foo-bar.baz.org:9999/solr/sub_dir",
                     zkClient.getBaseUrlForNodeName("foo-bar.baz.org:9999_solr%2Fsub_dir"));
        
        // generateNodeName + getBaseUrlForNodeName
        assertEquals("http://foo:9876/solr",
                     zkClient.getBaseUrlForNodeName
                     (ZkController.generateNodeName("foo","9876","solr")));
        assertEquals("http://foo:9876/solr",
                     zkClient.getBaseUrlForNodeName
                     (ZkController.generateNodeName("foo","9876","/solr")));
        assertEquals("http://foo:9876/solr",
                     zkClient.getBaseUrlForNodeName
                     (ZkController.generateNodeName("foo","9876","/solr/")));
        assertEquals("http://foo.bar.com:9876/solr/sub_dir",
                     zkClient.getBaseUrlForNodeName
                     (ZkController.generateNodeName("foo.bar.com","9876","solr/sub_dir")));
        assertEquals("http://foo.bar.com:9876/solr/sub_dir",
                     zkClient.getBaseUrlForNodeName
                     (ZkController.generateNodeName("foo.bar.com","9876","/solr/sub_dir/")));
        assertEquals("http://foo-bar:9876",
                     zkClient.getBaseUrlForNodeName
                     (ZkController.generateNodeName("foo-bar","9876","")));
        assertEquals("http://foo-bar:9876",
                     zkClient.getBaseUrlForNodeName
                     (ZkController.generateNodeName("foo-bar","9876","/")));
        assertEquals("http://foo-bar.com:80/some_dir",
                     zkClient.getBaseUrlForNodeName
                     (ZkController.generateNodeName("foo-bar.com","80","some_dir")));
        assertEquals("http://foo-bar.com:80/some_dir",
                     zkClient.getBaseUrlForNodeName
                     (ZkController.generateNodeName("foo-bar.com","80","/some_dir")));
      } finally {
        zkClient.close();
      }
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void testReadConfigName() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";
    CoreContainer cc = null;

    ZkTestServer server = new ZkTestServer(zkDir);
    try {
      server.run();

      AbstractZkTestCase.tryCleanSolrZkNode(server.getZkHost());
      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      SolrZkClient zkClient = new SolrZkClient(server.getZkAddress(), TIMEOUT);
      String actualConfigName = "firstConfig";

      zkClient.makePath(ZkController.CONFIGS_ZKNODE + "/" + actualConfigName, true);
      
      Map<String,Object> props = new HashMap<String,Object>();
      props.put("configName", actualConfigName);
      ZkNodeProps zkProps = new ZkNodeProps(props);
      zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/"
          + COLLECTION_NAME, ZkStateReader.toJSON(zkProps),
          CreateMode.PERSISTENT, true);

      if (DEBUG) {
        zkClient.printLayoutToStdOut();
      }
      zkClient.close();
      
      cc = getCoreContainer();
      
      ZkController zkController = new ZkController(cc, server.getZkAddress(), TIMEOUT, 10000,
          "127.0.0.1", "8983", "solr", 0, true, 10000, 10000, new CurrentCoreDescriptorProvider() {
            
            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });
      try {
        String configName = zkController.readConfigName(COLLECTION_NAME);
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

  @Test
  public void testUploadToCloud() throws Exception {
    String zkDir = dataDir.getAbsolutePath() + File.separator
        + "zookeeper/server1/data";

    ZkTestServer server = new ZkTestServer(zkDir);
    ZkController zkController = null;
    boolean testFinished = false;
    CoreContainer cc = null;
    try {
      server.run();

      AbstractZkTestCase.makeSolrZkNode(server.getZkHost());

      cc = getCoreContainer();
      
      zkController = new ZkController(cc, server.getZkAddress(),
          TIMEOUT, 10000, "127.0.0.1", "8983", "solr", 0, true, 10000, 10000, new CurrentCoreDescriptorProvider() {
            
            @Override
            public List<CoreDescriptor> getCurrentDescriptors() {
              // do nothing
              return null;
            }
          });

      zkController.uploadToZK(new File(ExternalPaths.EXAMPLE_HOME + "/collection1/conf"),
          ZkController.CONFIGS_ZKNODE + "/config1");
      
      // uploading again should overwrite, not error...
      zkController.uploadToZK(new File(ExternalPaths.EXAMPLE_HOME + "/collection1/conf"),
          ZkController.CONFIGS_ZKNODE + "/config1");

      if (DEBUG) {
        zkController.printLayoutToStdOut();
      }
      testFinished = true;
    } finally {
      if (!testFinished & zkController != null) {
        zkController.getZkClient().printLayoutToStdOut();
      }
      
      if (zkController != null) {
        zkController.close();
      }
      if (cc != null) {
        cc.shutdown();
      }
      server.shutdown();
    }

  }

  private CoreContainer getCoreContainer() {
    CoreContainer cc = new CoreContainer(TEMP_DIR.getAbsolutePath());
    cc.load();
    return cc;
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
  }

}
