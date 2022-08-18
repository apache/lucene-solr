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

package org.apache.solr.common.cloud;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCase;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.util.CompressionUtil;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.nio.file.Path;
import java.util.Map;

public class SolrZkClientCompressedDataTest extends SolrTestCase {

    @Test
    public void getData() throws Exception {
        Path zkDir = createTempDir("testGetData");

        ZkTestServer server = new ZkTestServer(zkDir);

        SolrZkClient zkClient = null;

        try {
            server.run();

            zkClient = new SolrZkClient(server.getZkAddress(), 60000);
            ZkController.createClusterZkNodes(zkClient);
            zkClient.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/c1", true);

            try (SolrZooKeeper solrZooKeeper = new SolrZooKeeper(server.getZkAddress(), 60000, null)) {
                String state = "{\"c1\":{\n" +
                        "\"pullReplicas\":\"0\",\n" +
                        "\"replicationFactor\":\"1\",\n" +
                        "\"router\":{\"name\":\"compositeId\"},\n" +
                        "\"maxShardsPerNode\":\"1\",\n" +
                        "\"autoAddReplicas\":\"false\",\n" +
                        "\"nrtReplicas\":\"1\",\n" +
                        "\"tlogReplicas\":\"0\",\n" +
                        "\"shards\":{\"shard1\":{\n" +
                        "\"range\":\"80000000-7fffffff\",\n" +
                        "\"state\":\"active\",\n" +
                        "\"replicas\":{\"core_node2\":{\n" +
                        "\"core\":\"test_shard1_replica_n1\",\n" +
                        "\"node_name\":\"127.0.0.1:8983_solr\",\n" +
                        "\"base_url\":\"http://127.0.0.1:8983/solr\",\n" +
                        "\"state\":\"active\",\n" +
                        "\"type\":\"NRT\",\n" +
                        "\"force_set_state\":\"false\",\n" +
                        "\"leader\":\"true\"}}}}}}";
                byte[] arr = state.getBytes();
                byte[] compressedData = CompressionUtil.compressBytes(arr);
                ZkACLProvider aclProvider = new DefaultZkACLProvider();
                String path = ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json";
                solrZooKeeper.create(path, compressedData, aclProvider.getACLsToAdd(path), CreateMode.PERSISTENT);

                byte[] data = zkClient.getData(ZkStateReader.COLLECTIONS_ZKNODE + "/c1/state.json", null, null, true);
                Map map = (Map) Utils.fromJSON(data);
                assertNotNull(map.get("c1"));
            }
        } finally {
            IOUtils.close(zkClient);
            server.shutdown();
        }
    }
}