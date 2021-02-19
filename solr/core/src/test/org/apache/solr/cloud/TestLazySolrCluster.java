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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cluster.api.CollectionConfig;
import org.apache.solr.cluster.api.SimpleMap;
import org.apache.solr.cluster.api.SolrCollection;
import org.apache.solr.common.LazySolrCluster;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.CreateMode;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;


public class TestLazySolrCluster extends SolrCloudTestCase {
    @BeforeClass
    public static void setupCluster() throws Exception {
        configureCluster(5)
                .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
                .configure();
    }

    public void test() throws Exception {
        CloudSolrClient cloudClient = cluster.getSolrClient();
        String collection = "testLazyCluster1";
        cloudClient.request(CollectionAdminRequest.createCollection(collection, "conf1", 2, 2));
        cluster.waitForActiveCollection(collection, 2, 4);
        collection = "testLazyCluster2";
        cloudClient.request(CollectionAdminRequest.createCollection(collection, "conf1", 2, 2));
        cluster.waitForActiveCollection(collection, 2, 4);

        LazySolrCluster solrCluster = new LazySolrCluster(cluster.getSolrClient().getZkStateReader());
        SimpleMap<SolrCollection> colls = solrCluster.collections();

        SolrCollection c = colls.get("testLazyCluster1");
        assertNotNull(c);
        c = colls.get("testLazyCluster2");
        assertNotNull(c);
        int[] count = new int[1];
        solrCluster.collections().forEachEntry((s, solrCollection) -> count[0]++);
        assertEquals(2, count[0]);

        count[0] = 0;

        assertEquals(2, solrCluster.collections().get("testLazyCluster1").shards().size());
        solrCluster.collections().get("testLazyCluster1").shards()
                .forEachEntry((s, shard) -> shard.replicas().forEachEntry((s1, replica) -> count[0]++));
        assertEquals(4, count[0]);

        assertEquals(5, solrCluster.nodes().size());
        SolrZkClient zkClient = cloudClient.getZkStateReader().getZkClient();
        zkClient.create(ZkStateReader.CONFIGS_ZKNODE + "/conf1/a", null, CreateMode.PERSISTENT, true);
        zkClient.create(ZkStateReader.CONFIGS_ZKNODE + "/conf1/a/aa1", new byte[1024], CreateMode.PERSISTENT, true);
        zkClient.create(ZkStateReader.CONFIGS_ZKNODE + "/conf1/a/aa2", new byte[1024 * 2], CreateMode.PERSISTENT, true);

        List<String> allFiles =  new ArrayList<>();
        byte[] buf = new byte[3*1024];
        CollectionConfig conf1 = solrCluster.configs().get("conf1");
        conf1.resources().abortableForEach((s, resource) -> {
            allFiles.add(s);
            if("a/aa1".equals(s)) {
                resource.get(is -> assertEquals(1024,  is.read(buf)));
            }
            if("a/aa2".equals(s)) {
                resource.get(is -> assertEquals(2*1024,  is.read(buf)));
            }
            if("a".equals(s)) {
                resource.get(is -> assertEquals(-1, is.read()));
            }
            return Boolean.TRUE;
        });
        assertEquals(5, allFiles.size());

    }


}
