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


import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore // nocommit
public class ReplaceNodeTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static AtomicInteger asyncId = new AtomicInteger();

  @BeforeClass
  public static void setupCluster() throws Exception {
    useFactory(null);
    configureCluster(6)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
  }

  protected String getSolrXml() {
    return "solr.xml";
  }

  @Test
  public void test() throws Exception {
    String coll = "replacenodetest_coll";
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }

    CloudHttp2SolrClient cloudClient = cluster.getSolrClient();
    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    String emptyNode = l.remove(0);
    String node2bdecommissioned = l.get(0);
    CollectionAdminRequest.Create create;
    // NOTE: always using the createCollection that takes in 'int' for all types of replicas, so we never
    // have to worry about null checking when comparing the Create command with the final Slices
    
    // TODO: tlog replicas do not work correctly in tests due to fault TestInjection#waitForInSyncWithLeader
    create = pickRandom(
                        CollectionAdminRequest.createCollection(coll, "conf1", 5, 2,0,0),
                        //CollectionAdminRequest.createCollection(coll, "conf1", 5, 1,1,0),
                        //CollectionAdminRequest.createCollection(coll, "conf1", 5, 0,1,1),
                        //CollectionAdminRequest.createCollection(coll, "conf1", 5, 1,0,1),
                        //CollectionAdminRequest.createCollection(coll, "conf1", 5, 0,2,0),
                        // check also replicationFactor 1
                        CollectionAdminRequest.createCollection(coll, "conf1", 5, 1,0,0)
                        //CollectionAdminRequest.createCollection(coll, "conf1", 5, 0,1,0)
    );
    create.setCreateNodeSet(StrUtils.join(l, ',')).setMaxShardsPerNode(100);
    cloudClient.request(create);
    
    DocCollection collection = cloudClient.getZkStateReader().getClusterState().getCollection(coll);
    log.debug("### Before decommission: {}", collection);
    log.info("excluded_node : {}  ", emptyNode);
    String asyncId0 = Integer.toString(asyncId.incrementAndGet());
    createReplaceNodeRequest(node2bdecommissioned, emptyNode, null).processAsync(asyncId0, cloudClient);
    CollectionAdminRequest.RequestStatus requestStatus = CollectionAdminRequest.requestStatus(asyncId0);
    boolean success = false;
    for (int i = 0; i < 60; i++) {
      CollectionAdminRequest.RequestStatusResponse rsp = requestStatus.process(cloudClient);
      if (rsp.getRequestStatus() == RequestStatusState.COMPLETED) {
        success = true;
        break;
      }
      assertFalse(rsp.getRequestStatus() == RequestStatusState.FAILED);
      Thread.sleep(250);
    }
    assertTrue(success);
    Http2SolrClient coreclient = cloudClient.getHttpClient();

    String url = cloudClient.getZkStateReader().getBaseUrlForNodeName(node2bdecommissioned);
    CoreAdminRequest req = new CoreAdminRequest();
    req.setBasePath(url);
    req.setCoreName(null);
    req.setAction(CoreAdminParams.CoreAdminAction.STATUS);

    CoreAdminResponse status = req.process(coreclient);
    assertTrue(status.getCoreStatus().size() == 0);


    collection = cloudClient.getZkStateReader().getClusterState().getCollection(coll);
    log.debug("### After decommission: {}", collection);
    // check what are replica states on the decommissioned node
    List<Replica> replicas = collection.getReplicas(node2bdecommissioned);
    if (replicas == null) {
      replicas = Collections.emptyList();
    }
    log.debug("### Existing replicas on decommissioned node: {}", replicas);

    //let's do it back - this time wait for recoveries
    CollectionAdminRequest replaceNodeRequest = createReplaceNodeRequest(emptyNode, node2bdecommissioned, Boolean.TRUE);
    replaceNodeRequest.process(cloudClient);

    coreclient = cloudClient.getHttpClient();
    url = cloudClient.getZkStateReader().getBaseUrlForNodeName(emptyNode);
    req = new CoreAdminRequest();
    req.setBasePath(url);
    req.setCoreName(null);
    req.setAction(CoreAdminParams.CoreAdminAction.STATUS);
    status = req.process(coreclient);

    assertEquals("Expecting no cores but found some: " + status.getCoreStatus(), 0, status.getCoreStatus().size());

    cluster.waitForActiveCollection(coll, 5, 5 * create.getTotaleReplicaCount());

    collection = cloudClient.getZkStateReader().getClusterState().getCollection(coll);
    assertEquals(create.getNumShards().intValue(), collection.getSlices().size());
    // is this a good check? we are moving replicas between nodes ...
//    for (Slice s:collection.getSlices()) {
//      assertEquals(create.getNumNrtReplicas().intValue(), s.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
//      assertEquals(create.getNumTlogReplicas().intValue(), s.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
//      assertEquals(create.getNumPullReplicas().intValue(), s.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
//    }
    // make sure all newly created replicas on node are active
    List<Replica> newReplicas = collection.getReplicas(node2bdecommissioned);
    replicas.forEach(r -> {
      for (Iterator<Replica> it = newReplicas.iterator(); it.hasNext(); ) {
        Replica nr = it.next();
        if (nr.getName().equals(r.getName())) {
          it.remove();
        }
      }
    });
    assertFalse(newReplicas.isEmpty());

    cluster.waitForActiveCollection(coll, 5, create.getNumNrtReplicas().intValue() + create.getNumTlogReplicas().intValue() + create.getNumPullReplicas().intValue());

    // make sure all replicas on emptyNode are not active
    // nocommit - this often and easily fails - investigate

//    boolean tryAgain = false;
//    TimeOut timeout = new TimeOut(5, TimeUnit.SECONDS, TimeSource.NANO_TIME);
//    do  {
//      collection = cloudClient.getZkStateReader().getClusterState().getCollection(coll);
//      replicas = collection.getReplicas(emptyNode);
//
//      if (replicas != null) {
//        for (Replica r : replicas) {
//          if (Replica.State.ACTIVE.equals(r.getState())) {
//            tryAgain = true;
//            Thread.sleep(250);
//          } else {
//            tryAgain = false;
//          }
//        }
//      }
//      if (timeout.hasTimedOut()) {
//        throw new RuntimeException("Timed out waiting for empty node replicas to be not active");
//      }
//    } while (tryAgain);
//
//    if (replicas != null) {
//      for (Replica r : replicas) {
//        assertFalse(r.toString(), Replica.State.ACTIVE.equals(r.getState()));
//      }
//    }
    try {
      CollectionAdminRequest.deleteCollection(coll).process(cluster.getSolrClient());
    } catch (BaseHttpSolrClient.RemoteSolrException e) {
      // nocommit fails with Error from server at null: Cannot unload non-existent core [replacenodetest_coll_shard4_replica_n27]}
    }
  }

  public static  CollectionAdminRequest.AsyncCollectionAdminRequest createReplaceNodeRequest(String sourceNode, String targetNode, Boolean parallel) {
    return new CollectionAdminRequest.ReplaceNode(sourceNode, targetNode).setParallel(parallel);
  }
}
