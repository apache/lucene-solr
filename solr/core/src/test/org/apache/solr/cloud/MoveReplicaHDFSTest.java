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

import java.io.IOException;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class, // hdfs currently leaks thread(s)
    MoveReplicaHDFSTest.ForkJoinThreadsFilter.class
})
public class MoveReplicaHDFSTest extends MoveReplicaTest {

  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("solr.hdfs.blockcache.enabled", "false");
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());

    ZkConfigManager configManager = new ZkConfigManager(zkClient());
    configManager.uploadConfigDir(configset("cloud-hdfs"), "conf1");

    System.setProperty("solr.hdfs.home", HdfsTestUtil.getDataDir(dfsCluster, "data"));
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    cluster.shutdown(); // need to close before the MiniDFSCluster
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
  }

  @Test
  public void testDataDirAndUlogAreMaintained() throws IOException, SolrServerException {
    String coll = "movereplicatest_coll2";
    CollectionAdminRequest.createCollection(coll, "conf1", 1, 1)
        .setCreateNodeSet("")
        .process(cluster.getSolrClient());
    String hdfsUri = HdfsTestUtil.getURI(dfsCluster);
    String dataDir = hdfsUri + "/dummyFolder/dataDir";
    String ulogDir = hdfsUri + "/dummyFolder2/ulogDir";
    CollectionAdminResponse res = CollectionAdminRequest
        .addReplicaToShard(coll, "shard1")
        .setDataDir(dataDir)
        .setUlogDir(ulogDir)
        .setNode(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());

    ulogDir += "/tlog";
    ZkStateReader zkStateReader = cluster.getSolrClient().getZkStateReader();
    ClusterStateUtil.waitForAllActiveAndLiveReplicas(zkStateReader, 120000);
    DocCollection docCollection = zkStateReader.getClusterState().getCollection(coll);
    Replica replica = docCollection.getReplicas().iterator().next();
    assertTrue(replica.getStr("ulogDir"), replica.getStr("ulogDir").equals(ulogDir) || replica.getStr("ulogDir").equals(ulogDir+'/'));
    assertTrue(replica.getStr("dataDir"),replica.getStr("dataDir").equals(dataDir) || replica.getStr("dataDir").equals(dataDir+'/'));

    new CollectionAdminRequest.MoveReplica(coll, replica.getName(), cluster.getJettySolrRunner(1).getNodeName())
        .process(cluster.getSolrClient());
    ClusterStateUtil.waitForAllActiveAndLiveReplicas(zkStateReader, 120000);
    docCollection = zkStateReader.getClusterState().getCollection(coll);
    assertEquals(1, docCollection.getSlice("shard1").getReplicas().size());
    replica = docCollection.getReplicas().iterator().next();
    assertEquals(replica.getNodeName(), cluster.getJettySolrRunner(1).getNodeName());
    assertTrue(replica.getStr("ulogDir"), replica.getStr("ulogDir").equals(ulogDir) || replica.getStr("ulogDir").equals(ulogDir+'/'));
    assertTrue(replica.getStr("dataDir"),replica.getStr("dataDir").equals(dataDir) || replica.getStr("dataDir").equals(dataDir+'/'));

  }

  public static class ForkJoinThreadsFilter implements ThreadFilter {

    @Override
    public boolean reject(Thread t) {
      String name = t.getName();
      if (name.startsWith("ForkJoinPool.commonPool")) {
        return true;
      }
      return false;
    }
  }

}
