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
package org.apache.solr.cloud.policy;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ClientDataProvider;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.util.Utils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LuceneTestCase.Slow
public class TestPolicyCloud extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(5)
        .addConfig("conf", configset("cloud-minimal"))
        .configure();
  }

  @org.junit.Rule
  public ExpectedException expectedException = ExpectedException.none();

  @After
  public void removeCollections() throws Exception {
    cluster.deleteAllCollections();
  }


  public void testDataProvider() throws IOException, SolrServerException {
    CollectionAdminRequest.createCollectionWithImplicitRouter("policiesTest", "conf", "shard1", 2)
        .process(cluster.getSolrClient());
    DocCollection rulesCollection = getCollectionState("policiesTest");
    ClientDataProvider provider = new ClientDataProvider(cluster.getSolrClient());

    Map<String, Object> val = provider.getNodeValues(rulesCollection.getReplicas().get(0).getNodeName(), Arrays.asList("freedisk", "cores"));
    assertTrue(((Number) val.get("cores")).intValue() > 0);
    assertTrue("freedisk value is "+((Number) val.get("freedisk")).intValue() , ((Number) val.get("freedisk")).intValue() > 0);
    System.out.println(Utils.toJSONString(val));
  }

  /*public void testMultiReplicaPlacement() {
    String autoScaleJson ="";


    Map<String,Map> nodeValues = (Map<String, Map>) Utils.fromJSONString( "{" +
        "node1:{cores:12, freedisk: 334, heap:10480}," +
        "node2:{cores:4, freedisk: 749, heap:6873}," +
        "node3:{cores:7, freedisk: 262, heap:7834}," +
        "node4:{cores:8, freedisk: 375, heap:16900, nodeRole:overseer}" +
        "}");

    ClusterDataProvider dataProvider = new ClusterDataProvider() {
      @Override
      public Map<String, Object> getNodeValues(String node, Collection<String> keys) {
        return null;
      }

      @Override
      public Map<String, Map<String, List<Policy.ReplicaInfo>>> getReplicaInfo(String node, Collection<String> keys) {
        return null;
      }

      @Override
      public Collection<String> getNodes() {
        return null;
      }
    };
    Map<String, List<String>> locations = Policy.getReplicaLocations("newColl", (Map<String, Object>) Utils.fromJSONString(autoScaleJson),
        "policy1", dataProvider, Arrays.asList("shard1", "shard2"), 3);


  }*/


}
