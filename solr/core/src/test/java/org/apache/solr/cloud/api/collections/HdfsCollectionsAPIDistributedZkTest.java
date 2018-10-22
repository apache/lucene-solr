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
package org.apache.solr.cloud.api.collections;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.cloud.hdfs.HdfsTestUtil;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.util.BadHdfsThreadsFilter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@Nightly
@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
//commented 23-AUG-2018  @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 12-Jun-2018
public class HdfsCollectionsAPIDistributedZkTest extends CollectionsAPIDistributedZkTest {

  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void setupClass() throws Exception {
    System.setProperty("solr.hdfs.blockcache.blocksperbank", "512");
    System.setProperty("tests.hdfs.numdatanodes", "1");
   
    dfsCluster = HdfsTestUtil.setupClass(createTempDir().toFile().getAbsolutePath());

    ZkConfigManager configManager = new ZkConfigManager(zkClient());
    configManager.uploadConfigDir(configset("cloud-hdfs"), "conf");
    configManager.uploadConfigDir(configset("cloud-hdfs"), "conf2");

    System.setProperty("solr.hdfs.home", HdfsTestUtil.getDataDir(dfsCluster, "data"));
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    cluster.shutdown(); // need to close before the MiniDFSCluster
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
    System.clearProperty("solr.hdfs.blockcache.blocksperbank");
    System.clearProperty("tests.hdfs.numdatanodes");
    System.clearProperty("solr.hdfs.home");
  }

  @Test
  public void moveReplicaTest() throws Exception {
    cluster.waitForAllNodes(5000);
    String coll = "movereplicatest_coll";

    CloudSolrClient cloudClient = cluster.getSolrClient();

    CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(coll, "conf", 2, 2);
    create.setMaxShardsPerNode(2);
    cloudClient.request(create);

    for (int i = 0; i < 10; i++) {
      cloudClient.add(coll, sdoc("id",String.valueOf(i)));
      cloudClient.commit(coll);
    }

    List<Slice> slices = new ArrayList<>(cloudClient.getZkStateReader().getClusterState().getCollection(coll).getSlices());
    Collections.shuffle(slices, random());
    Slice slice = null;
    Replica replica = null;
    for (Slice s : slices) {
      slice = s;
      for (Replica r : s.getReplicas()) {
        if (s.getLeader() != r) {
          replica = r;
        }
      }
    }
    String dataDir = getDataDir(replica);

    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    String targetNode = null;
    for (String node : liveNodes) {
      if (!replica.getNodeName().equals(node)) {
        targetNode = node;
        break;
      }
    }
    assertNotNull(targetNode);

    CollectionAdminRequest.MoveReplica moveReplica = new CollectionAdminRequest.MoveReplica(coll, replica.getName(), targetNode);
    moveReplica.process(cloudClient);

    checkNumOfCores(cloudClient, replica.getNodeName(), 0);
    checkNumOfCores(cloudClient, targetNode, 2);

    waitForState("Wait for recovery finish failed",coll, clusterShape(2,2));
    slice = cloudClient.getZkStateReader().getClusterState().getCollection(coll).getSlice(slice.getName());
    boolean found = false;
    for (Replica newReplica : slice.getReplicas()) {
      if (getDataDir(newReplica).equals(dataDir)) {
        found = true;
      }
    }
    assertTrue(found);


    // data dir is reused so replication will be skipped
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      SolrMetricManager manager = jetty.getCoreContainer().getMetricManager();
      List<String> registryNames = manager.registryNames().stream()
          .filter(s -> s.startsWith("solr.core.")).collect(Collectors.toList());
      for (String registry : registryNames) {
        Map<String, Metric> metrics = manager.registry(registry).getMetrics();
        Counter counter = (Counter) metrics.get("REPLICATION./replication.requests");
        if (counter != null) {
          assertEquals(0, counter.getCount());
        }
      }
    }
  }


  private void checkNumOfCores(CloudSolrClient cloudClient, String nodeName, int expectedCores) throws IOException, SolrServerException {
    assertEquals(nodeName + " does not have expected number of cores",expectedCores, getNumOfCores(cloudClient, nodeName));
  }

  private int getNumOfCores(CloudSolrClient cloudClient, String nodeName) throws IOException, SolrServerException {
    try (HttpSolrClient coreclient = getHttpSolrClient(cloudClient.getZkStateReader().getBaseUrlForNodeName(nodeName))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(null, coreclient);
      return status.getCoreStatus().size();
    }
  }

  private String getDataDir(Replica replica) throws IOException, SolrServerException {
    try (HttpSolrClient coreclient = getHttpSolrClient(replica.getBaseUrl())) {
      CoreStatus status = CoreAdminRequest.getCoreStatus(replica.getCoreName(), coreclient);
      return status.getDataDirectory();
    }
  }
}
