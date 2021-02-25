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

package org.apache.solr.cloud.hdfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
public class HDFSCollectionsAPITest extends SolrCloudTestCase {

  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setupClass() throws Exception {
    configureCluster(2).configure();

    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());

    ZkConfigManager configManager = new ZkConfigManager(zkClient());
    configManager.uploadConfigDir(configset("cloud-hdfs"), "conf1");
  }


  @AfterClass
  public static void teardownClass() throws Exception {
    try {
      shutdownCluster(); // need to close before the MiniDFSCluster
      cluster = null;
    } finally {
      try {
        HdfsTestUtil.teardownClass(dfsCluster);
      } finally {
        dfsCluster = null;
      }
    }
  }

  public void testDataDirIsNotReused() throws Exception {
    JettySolrRunner jettySolrRunner = cluster.getJettySolrRunner(0);
    String collection = "test";
    cluster.getSolrClient().setDefaultCollection(collection);
    CollectionAdminRequest.createCollection(collection, "conf1", 1, 1)
        .setCreateNodeSet(jettySolrRunner.getNodeName()).process(cluster.getSolrClient());
    waitForState("", collection, clusterShape(1, 1));
    cluster.getSolrClient().setDefaultCollection(collection);
    cluster.getSolrClient().add(new SolrInputDocument("id", "1"));
    cluster.getSolrClient().add(new SolrInputDocument("id", "2"));
    cluster.getSolrClient().commit();
    cluster.getSolrClient().add(new SolrInputDocument("id", "3"));

    jettySolrRunner.stop();
    waitForState("", collection, (liveNodes, collectionState) -> {
      Replica replica = collectionState.getSlice("shard1").getReplicas().iterator().next();
      return replica.getState() == Replica.State.DOWN;
    });
    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());

    jettySolrRunner.start();

    CollectionAdminRequest.createCollection(collection, "conf1", 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(1).getNodeName()).process(cluster.getSolrClient());
    waitForState("", collection, clusterShape(1, 1));
    QueryResponse response = cluster.getSolrClient().query(collection, new SolrQuery("*:*"));
    assertEquals(0L, response.getResults().getNumFound());
  }

}
