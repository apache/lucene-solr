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


import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.TestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.response.SolrPingResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.util.LogLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;


/**
 * This test would be faster if we simulated the zk state instead.
 */
@Slow
@LogLevel("org.apache.solr.common.cloud.PerReplicaStatesOps=DEBUG;org.apache.solr.cloud.Overseer=INFO;org.apache.solr.common.cloud=INFO;org.apache.solr.cloud.api.collections=INFO;org.apache.solr.cloud.overseer=INFO")
public class PerReplicaStatesIntegrationTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());





  public void testPerReplicaStateCollection() throws Exception {

    String testCollection = "perReplicaState_test";

    MiniSolrCloudCluster cluster =
        configureCluster(3)
            .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .configure();
    try {
      int liveNodes = cluster.getJettySolrRunners().size();
      CollectionAdminRequest.createCollection(testCollection, "conf", 2, 2)
          .setMaxShardsPerNode(liveNodes)
          .setPerReplicaState(Boolean.TRUE)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(testCollection, 2, 4);
      final SolrClient clientUnderTest = cluster.getSolrClient();
      final SolrPingResponse response = clientUnderTest.ping(testCollection);
      assertEquals("This should be OK", 0, response.getStatus());
      DocCollection c = cluster.getSolrClient().getZkStateReader().getCollection(testCollection);
      c.forEachReplica((s, replica) -> assertNotNull(replica.getReplicaState()));
      PerReplicaStates prs = PerReplicaStates.fetch(ZkStateReader.getCollectionPath(testCollection), cluster.getZkClient(), null);
      assertEquals(4, prs.states.size());
      JettySolrRunner jsr = cluster.startJettySolrRunner();
      // Now let's do an add replica
      CollectionAdminRequest
          .addReplicaToShard(testCollection, "shard1")
          .process(cluster.getSolrClient());
      prs = PerReplicaStates.fetch(ZkStateReader.getCollectionPath(testCollection), cluster.getZkClient(), null);
      assertEquals(5, prs.states.size());

      testCollection = "perReplicaState_testv2";
      new V2Request.Builder("/collections")
          .withMethod(POST)
          .withPayload("{create: {name: perReplicaState_testv2, config : conf, numShards : 2, nrtReplicas : 2, perReplicaState : true, maxShardsPerNode : 5}}")
          .build()
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(testCollection, 2, 4);
      c = cluster.getSolrClient().getZkStateReader().getCollection(testCollection);
      c.forEachReplica((s, replica) -> assertNotNull(replica.getReplicaState()));
      prs = PerReplicaStates.fetch(ZkStateReader.getCollectionPath(testCollection), cluster.getZkClient(), null);
      assertEquals(4, prs.states.size());
    }finally {
      cluster.shutdown();
    }


  }

  public void testRestart() throws Exception {
    String testCollection = "prs_restart_test";
    MiniSolrCloudCluster cluster =
        configureCluster(1)
            .addConfig("conf", getFile("solrj").toPath().resolve("solr").resolve("configsets").resolve("streaming").resolve("conf"))
            .withJettyConfig(jetty -> jetty.enableV2(true))
            .configure();
    try {
      CollectionAdminRequest.createCollection(testCollection, "conf", 1, 1)
          .setPerReplicaState(Boolean.TRUE)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(testCollection, 1, 1);

      DocCollection c = cluster.getSolrClient().getZkStateReader().getCollection(testCollection);
      c.forEachReplica((s, replica) -> assertNotNull(replica.getReplicaState()));
      String collectionPath = ZkStateReader.getCollectionPath(testCollection);
      PerReplicaStates prs = PerReplicaStates.fetch(collectionPath, SolrCloudTestCase.cluster.getZkClient(), null);
      assertEquals(1, prs.states.size());

      JettySolrRunner jsr = cluster.startJettySolrRunner();
      assertEquals(2,cluster.getJettySolrRunners().size());

      // Now let's do an add replica
      CollectionAdminRequest
          .addReplicaToShard(testCollection, "shard1")
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(testCollection, 1, 2);
      prs = PerReplicaStates.fetch(collectionPath, SolrCloudTestCase.cluster.getZkClient(), null);
      assertEquals(2, prs.states.size());
      c = cluster.getSolrClient().getZkStateReader().getCollection(testCollection);
      prs.states.forEachEntry((s, state) -> assertEquals(Replica.State.ACTIVE, state.state));

      String replicaName = null;
      for (Replica r : c.getSlice("shard1").getReplicas()) {
        if(r.getNodeName() .equals(jsr.getNodeName())) {
          replicaName = r.getName();
        }
      }

      if(replicaName != null) {
        log.info("restarting the node : {}, state.json v: {} downreplica :{}", jsr.getNodeName(), c.getZNodeVersion(), replicaName);
        jsr.stop();
        c = cluster.getSolrClient().getZkStateReader().getCollection(testCollection);
        log.info("after down node, state.json v: {}", c.getZNodeVersion());
        prs =  PerReplicaStates.fetch(collectionPath, SolrCloudTestCase.cluster.getZkClient(), null);
        PerReplicaStates.State st = prs.get(replicaName);
        assertNotEquals(Replica.State.ACTIVE, st.state);
        jsr.start();
        cluster.waitForActiveCollection(testCollection, 1, 2);
        prs =  PerReplicaStates.fetch(collectionPath, SolrCloudTestCase.cluster.getZkClient(), null);
        prs.states.forEachEntry((s, state) -> assertEquals(Replica.State.ACTIVE, state.state));
      }

    } finally {
      cluster.shutdown();
    }

  }
}
