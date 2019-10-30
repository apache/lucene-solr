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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.LuceneTestCase.Nightly;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConcurrentCreateCollectionTest extends SolrCloudTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    cluster.deleteAllCollections();
  }



  public void testConcurrentCreatePlacement() throws Exception {
    final int nThreads = 20;
    final int createsPerThread = 1;
    final int repFactor = 1;

    final CloudSolrClient client = cluster.getSolrClient();

    final AtomicInteger collectionNum = new AtomicInteger();
    Thread[] indexThreads = new Thread[nThreads];


    for (int i=0; i<nThreads; i++) {
      indexThreads[i] = new Thread(() -> {
        try {
          for (int j=0; j<createsPerThread; j++) {
            int num = collectionNum.incrementAndGet();
            String collectionName = "collection" + num;
            CollectionAdminRequest
                .createCollection("collection" + num, "conf", 1, repFactor)
                .setMaxShardsPerNode(1)
                .process(client);
            cluster.waitForActiveCollection(collectionName, 1, repFactor);
            // Thread.sleep(5000);
          }
        } catch (Exception e) {
          fail(e.getMessage());
        }
      });
    }

    for (Thread thread : indexThreads) {
      thread.start();
    }

    for (Thread thread : indexThreads) {
      thread.join();
    }

    Map<String,List<Replica>> map = new HashMap<>();
    ClusterState cstate = client.getZkStateReader().getClusterState();
    for (DocCollection collection : cstate.getCollectionsMap().values()) {
      for (Replica replica : collection.getReplicas()) {
        String url = replica.getBaseUrl();
        List<Replica> replicas = map.get(url);
        if (replicas == null) {
          replicas = new ArrayList<>();
          map.put(url, replicas);
        }
        replicas.add(replica);
      }
    }

    // check if nodes are balanced
    List<Replica> prev = null;
    for (List<Replica> replicas : map.values()) {
      if (prev != null && prev.size() != replicas.size()) {
        log.error("UNBALANCED CLUSTER: prev node replica count=" + prev.size() + " current=" + replicas.size() + "\n" + cstate.getCollectionsMap());
        log.error("Replica lists per node: " + map);
        assertEquals(prev.size(), replicas.size());
      }
      prev = replicas;
    }

  }


  
}
